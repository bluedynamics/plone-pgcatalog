"""Unit tests for search backend abstraction (no PostgreSQL needed)."""

from plone.pgcatalog.backends import BM25Backend
from plone.pgcatalog.backends import detect_and_set_backend
from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.backends import reset_backend
from plone.pgcatalog.backends import SearchBackend
from plone.pgcatalog.backends import set_backend
from plone.pgcatalog.backends import TsvectorBackend

import pytest


@pytest.fixture(autouse=True)
def _reset_backend():
    """Reset the singleton backend after each test."""
    yield
    reset_backend()


# ── Interface tests ──────────────────────────────────────────────────


class TestSearchBackendInterface:
    def test_tsvector_is_search_backend(self):
        assert isinstance(TsvectorBackend(), SearchBackend)

    def test_bm25_is_search_backend(self):
        assert isinstance(BM25Backend(), SearchBackend)


# ── TsvectorBackend ──────────────────────────────────────────────────


class TestTsvectorBackend:
    def setup_method(self):
        self.backend = TsvectorBackend()

    def test_get_extra_columns_returns_one(self):
        cols = self.backend.get_extra_columns()
        assert len(cols) == 1
        assert cols[0].name == "searchable_text"

    def test_get_extra_columns_has_setweight(self):
        cols = self.backend.get_extra_columns()
        assert "setweight" in cols[0].value_expr

    def test_get_schema_sql_empty(self):
        assert self.backend.get_schema_sql() == ""

    def test_process_search_data_empty(self):
        assert self.backend.process_search_data({"idx": {"Title": "foo"}}) == {}

    def test_rank_ascending_false(self):
        assert self.backend.rank_ascending is False

    def test_uncatalog_extra_empty(self):
        assert self.backend.uncatalog_extra() == {}

    def test_detect_always_true(self):
        assert TsvectorBackend.detect(None) is True
        assert TsvectorBackend.detect("bad_dsn") is True

    def test_build_search_clause_structure(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        where, params, rank = self.backend.build_search_clause("security", "en", pname)

        assert "searchable_text @@" in where
        assert "plainto_tsquery" in where
        assert "pgcatalog_lang_to_regconfig" in where
        assert "ts_rank_cd" in rank
        assert len(params) == 2
        # Check param values
        text_key = next(k for k in params if "text" in k)
        lang_key = next(k for k in params if "lang" in k)
        assert params[text_key] == "security"
        assert params[lang_key] == "en"

    def test_build_search_clause_empty_lang(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        _, params, _ = self.backend.build_search_clause("test", "", pname)
        lang_key = next(k for k in params if "lang" in k)
        assert params[lang_key] == ""


# ── BM25Backend ──────────────────────────────────────────────────────


class TestBM25Backend:
    def setup_method(self):
        self.backend = BM25Backend()

    def test_get_extra_columns_returns_two(self):
        cols = self.backend.get_extra_columns()
        assert len(cols) == 2
        names = [c.name for c in cols]
        assert "searchable_text" in names
        assert "search_bm25" in names

    def test_searchable_text_column_same_as_tsvector(self):
        bm25_cols = self.backend.get_extra_columns()
        tsv_cols = TsvectorBackend().get_extra_columns()
        bm25_st = next(c for c in bm25_cols if c.name == "searchable_text")
        tsv_st = tsv_cols[0]
        assert bm25_st.value_expr == tsv_st.value_expr

    def test_bm25_column_has_tokenize(self):
        cols = self.backend.get_extra_columns()
        bm25_col = next(c for c in cols if c.name == "search_bm25")
        assert "tokenize" in bm25_col.value_expr
        assert "CASE WHEN" in bm25_col.value_expr

    def test_get_schema_sql_has_extensions(self):
        sql = self.backend.get_schema_sql()
        assert "pg_tokenizer" in sql
        assert "vchord_bm25" in sql
        assert "bm25vector" in sql
        assert "CREATE INDEX" in sql
        assert "bm25_ops" in sql

    def test_get_schema_sql_has_tokenizer_creation(self):
        sql = self.backend.get_schema_sql()
        assert "create_tokenizer" in sql
        assert "pgcatalog_default" in sql
        assert "EXCEPTION WHEN OTHERS" in sql
        assert "bert_base_uncased" in sql

    def test_process_search_data_title_boost(self):
        pending = {
            "idx": {"Title": "Security", "Description": "A policy doc"},
            "searchable_text": "Full body text about security policies",
        }
        result = self.backend.process_search_data(pending)
        assert "search_bm25" in result
        text = result["search_bm25"]
        # Title should appear 3 times (boosting)
        assert text.count("Security") == 3
        assert "A policy doc" in text
        assert "Full body text" in text

    def test_process_search_data_missing_fields(self):
        result = self.backend.process_search_data({"idx": {}})
        assert result["search_bm25"] is None

    def test_process_search_data_no_idx(self):
        result = self.backend.process_search_data({})
        assert result["search_bm25"] is None

    def test_process_search_data_title_only(self):
        result = self.backend.process_search_data(
            {"idx": {"Title": "Hello"}, "searchable_text": None}
        )
        assert result["search_bm25"] == "Hello Hello Hello"

    def test_rank_ascending_true(self):
        assert self.backend.rank_ascending is True

    def test_uncatalog_extra_has_bm25(self):
        extra = self.backend.uncatalog_extra()
        assert extra == {"search_bm25": None}

    def test_detect_returns_false_on_bad_dsn(self):
        assert BM25Backend.detect(None) is False
        assert BM25Backend.detect("") is False

    def test_detect_returns_false_on_unreachable(self):
        assert BM25Backend.detect("host=localhost port=1 dbname=x") is False

    def test_build_search_clause_structure(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        where, params, rank = self.backend.build_search_clause("security", "en", pname)

        # WHERE uses tsvector GIN pre-filter (same as TsvectorBackend)
        assert "searchable_text @@" in where
        assert "plainto_tsquery" in where

        # Rank uses BM25 operator
        assert "<&>" in rank
        assert "to_bm25query" in rank
        assert "tokenize" in rank

        # Should have 3 params: text, lang, bm25q
        assert len(params) == 3
        text_key = next(k for k in params if "text" in k)
        bm25q_key = next(k for k in params if "bm25q" in k)
        assert params[text_key] == "security"
        assert params[bm25q_key] == "security"

    def test_custom_tokenizer_name(self):
        backend = BM25Backend(tokenizer_name="my_custom")
        cols = backend.get_extra_columns()
        bm25_col = next(c for c in cols if c.name == "search_bm25")
        assert "my_custom" in bm25_col.value_expr

        sql = backend.get_schema_sql()
        assert "my_custom" in sql


# ── Singleton management ─────────────────────────────────────────────


class TestBackendSingleton:
    def test_default_is_tsvector(self):
        backend = get_backend()
        assert isinstance(backend, TsvectorBackend)

    def test_set_backend(self):
        bm25 = BM25Backend()
        set_backend(bm25)
        assert get_backend() is bm25

    def test_reset_backend(self):
        set_backend(BM25Backend())
        reset_backend()
        assert isinstance(get_backend(), TsvectorBackend)

    def test_detect_and_set_fallback(self):
        # No PG extensions → falls back to tsvector
        backend = detect_and_set_backend(None)
        assert isinstance(backend, TsvectorBackend)
        assert isinstance(get_backend(), TsvectorBackend)

    def test_detect_and_set_bad_dsn(self):
        backend = detect_and_set_backend("host=localhost port=1 dbname=x")
        assert isinstance(backend, TsvectorBackend)
