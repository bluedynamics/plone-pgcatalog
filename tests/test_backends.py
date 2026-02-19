"""Unit tests for search backend abstraction (no PostgreSQL needed)."""

from plone.pgcatalog.backends import _build_tokenizer_toml
from plone.pgcatalog.backends import _normalize_lang
from plone.pgcatalog.backends import BM25Backend
from plone.pgcatalog.backends import detect_and_set_backend
from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.backends import LANG_TOKENIZER_MAP
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


# ── Language normalization ────────────────────────────────────────────


class TestNormalizeLang:
    def test_simple(self):
        assert _normalize_lang("en") == "en"

    def test_with_region(self):
        assert _normalize_lang("pt-br") == "pt"

    def test_with_underscore(self):
        assert _normalize_lang("zh_CN") == "zh"

    def test_uppercase(self):
        assert _normalize_lang("DE") == "de"

    def test_empty(self):
        assert _normalize_lang("") == ""

    def test_none(self):
        assert _normalize_lang(None) == ""


# ── Tokenizer TOML generation ────────────────────────────────────────


class TestBuildTokenizerToml:
    def test_fallback_no_stemmer(self):
        toml = _build_tokenizer_toml(None)
        assert "bert_base_uncased" in toml
        assert "unicode_segmentation" in toml
        assert "stemmer" not in toml
        assert "skip_non_alphanumeric" in toml

    def test_english_has_porter2(self):
        toml = _build_tokenizer_toml("en")
        assert "english_porter2" in toml
        assert "stemmer" in toml

    def test_german_has_german_stemmer(self):
        toml = _build_tokenizer_toml("de")
        assert "german" in toml
        assert "stemmer" in toml

    def test_chinese_uses_jieba(self):
        toml = _build_tokenizer_toml("zh")
        assert "jieba" in toml
        assert "stemmer" not in toml
        # jieba doesn't use skip_non_alphanumeric
        assert "skip_non_alphanumeric" not in toml

    def test_japanese_uses_lindera(self):
        toml = _build_tokenizer_toml("ja")
        assert "lindera" in toml
        assert "stemmer" not in toml

    def test_korean_uses_lindera(self):
        toml = _build_tokenizer_toml("ko")
        assert "lindera" in toml

    def test_unknown_lang_no_stemmer(self):
        toml = _build_tokenizer_toml("xx")
        assert "stemmer" not in toml
        assert "unicode_segmentation" in toml

    def test_all_mapped_langs_produce_valid_toml(self):
        for lang in LANG_TOKENIZER_MAP:
            toml = _build_tokenizer_toml(lang)
            assert "bert_base_uncased" in toml
            assert "pre_tokenizer" in toml


# ── BM25Backend (default: single language) ───────────────────────────


class TestBM25BackendDefault:
    """Tests for BM25Backend with default settings (backward compat)."""

    def setup_method(self):
        self.backend = BM25Backend()

    def test_default_languages(self):
        assert self.backend.languages == ["en"]

    def test_get_extra_columns_count(self):
        cols = self.backend.get_extra_columns()
        # searchable_text + search_bm25_en + search_bm25 (fallback) = 3
        assert len(cols) == 3

    def test_extra_column_names(self):
        cols = self.backend.get_extra_columns()
        names = [c.name for c in cols]
        assert "searchable_text" in names
        assert "search_bm25_en" in names
        assert "search_bm25" in names

    def test_searchable_text_column_same_as_tsvector(self):
        bm25_cols = self.backend.get_extra_columns()
        tsv_cols = TsvectorBackend().get_extra_columns()
        bm25_st = next(c for c in bm25_cols if c.name == "searchable_text")
        tsv_st = tsv_cols[0]
        assert bm25_st.value_expr == tsv_st.value_expr

    def test_language_column_has_tokenize(self):
        cols = self.backend.get_extra_columns()
        en_col = next(c for c in cols if c.name == "search_bm25_en")
        assert "tokenize" in en_col.value_expr
        assert "pgcatalog_en" in en_col.value_expr

    def test_fallback_column_has_default_tokenizer(self):
        cols = self.backend.get_extra_columns()
        fb_col = next(c for c in cols if c.name == "search_bm25")
        assert "pgcatalog_default" in fb_col.value_expr

    def test_get_schema_sql_has_extensions(self):
        sql = self.backend.get_schema_sql()
        assert "pg_tokenizer" in sql
        assert "vchord_bm25" in sql

    def test_get_schema_sql_has_en_column(self):
        sql = self.backend.get_schema_sql()
        assert "search_bm25_en" in sql
        assert "pgcatalog_en" in sql
        assert "idx_os_search_bm25_en" in sql

    def test_get_schema_sql_has_fallback(self):
        sql = self.backend.get_schema_sql()
        assert "pgcatalog_default" in sql
        assert "idx_os_search_bm25" in sql

    def test_process_search_data_routes_to_en(self):
        pending = {
            "idx": {
                "Title": "Security",
                "Description": "A policy doc",
                "Language": "en",
            },
            "searchable_text": "Full body text about security policies",
        }
        result = self.backend.process_search_data(pending)
        assert result["search_bm25_en"] is not None
        assert result["search_bm25"] is not None  # fallback always populated
        assert result["search_bm25_en"].count("Security") == 3  # title boost

    def test_process_search_data_non_configured_lang(self):
        pending = {
            "idx": {"Title": "Titre", "Description": "", "Language": "fr"},
            "searchable_text": "Texte",
        }
        result = self.backend.process_search_data(pending)
        assert result["search_bm25_en"] is None  # not French
        assert result["search_bm25"] is not None  # fallback

    def test_process_search_data_missing_fields(self):
        result = self.backend.process_search_data({"idx": {}})
        assert result["search_bm25_en"] is None
        assert result["search_bm25"] is None

    def test_process_search_data_no_idx(self):
        result = self.backend.process_search_data({})
        assert result["search_bm25_en"] is None
        assert result["search_bm25"] is None

    def test_process_search_data_title_only(self):
        result = self.backend.process_search_data(
            {"idx": {"Title": "Hello", "Language": "en"}, "searchable_text": None}
        )
        assert result["search_bm25_en"] == "Hello Hello Hello"

    def test_rank_ascending_true(self):
        assert self.backend.rank_ascending is True

    def test_uncatalog_extra(self):
        extra = self.backend.uncatalog_extra()
        assert extra == {"search_bm25": None, "search_bm25_en": None}

    def test_detect_returns_false_on_bad_dsn(self):
        assert BM25Backend.detect(None) is False
        assert BM25Backend.detect("") is False

    def test_detect_returns_false_on_unreachable(self):
        assert BM25Backend.detect("host=localhost port=1 dbname=x") is False

    def test_build_search_clause_en(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        where, params, rank = self.backend.build_search_clause("security", "en", pname)

        assert "searchable_text @@" in where
        assert "<&>" in rank
        assert "search_bm25_en" in rank
        assert "pgcatalog_en" in rank
        assert "idx_os_search_bm25_en" in rank
        assert len(params) == 3

    def test_build_search_clause_fallback_for_unknown_lang(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        _, _, rank = self.backend.build_search_clause("test", "fr", pname)
        assert "search_bm25 <&>" in rank  # fallback column
        assert "pgcatalog_default" in rank

    def test_build_search_clause_fallback_for_empty_lang(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        _, _, rank = self.backend.build_search_clause("test", "", pname)
        assert "search_bm25 <&>" in rank


# ── BM25Backend (multilingual) ───────────────────────────────────────


class TestBM25BackendMultilingual:
    """Tests for BM25Backend with multiple languages configured."""

    def setup_method(self):
        self.backend = BM25Backend(languages=["en", "de", "zh"])

    def test_languages_stored(self):
        assert self.backend.languages == ["en", "de", "zh"]

    def test_language_normalization(self):
        b = BM25Backend(languages=["en", "pt-br", "zh-CN", "DE"])
        assert b.languages == ["en", "pt", "zh", "de"]

    def test_deduplication(self):
        b = BM25Backend(languages=["en", "en", "de"])
        assert b.languages == ["en", "de"]

    def test_get_extra_columns_count(self):
        cols = self.backend.get_extra_columns()
        # searchable_text + en + de + zh + fallback = 5
        assert len(cols) == 5

    def test_extra_column_names(self):
        cols = self.backend.get_extra_columns()
        names = [c.name for c in cols]
        assert names == [
            "searchable_text",
            "search_bm25_en",
            "search_bm25_de",
            "search_bm25_zh",
            "search_bm25",
        ]

    def test_en_column_uses_porter2(self):
        cols = self.backend.get_extra_columns()
        en_col = next(c for c in cols if c.name == "search_bm25_en")
        assert "pgcatalog_en" in en_col.value_expr

    def test_de_column_uses_german_tokenizer(self):
        cols = self.backend.get_extra_columns()
        de_col = next(c for c in cols if c.name == "search_bm25_de")
        assert "pgcatalog_de" in de_col.value_expr

    def test_zh_column_uses_chinese_tokenizer(self):
        cols = self.backend.get_extra_columns()
        zh_col = next(c for c in cols if c.name == "search_bm25_zh")
        assert "pgcatalog_zh" in zh_col.value_expr

    def test_schema_sql_has_all_languages(self):
        sql = self.backend.get_schema_sql()
        assert "search_bm25_en" in sql
        assert "search_bm25_de" in sql
        assert "search_bm25_zh" in sql
        assert "pgcatalog_en" in sql
        assert "pgcatalog_de" in sql
        assert "pgcatalog_zh" in sql
        assert "pgcatalog_default" in sql

    def test_schema_sql_has_all_indexes(self):
        sql = self.backend.get_schema_sql()
        assert "idx_os_search_bm25_en" in sql
        assert "idx_os_search_bm25_de" in sql
        assert "idx_os_search_bm25_zh" in sql
        assert "idx_os_search_bm25" in sql  # fallback

    def test_process_routes_to_german(self):
        pending = {
            "idx": {"Title": "Vulkan", "Description": "", "Language": "de"},
            "searchable_text": "Text über Vulkane",
        }
        result = self.backend.process_search_data(pending)
        assert result["search_bm25_de"] is not None
        assert result["search_bm25_en"] is None
        assert result["search_bm25_zh"] is None
        assert result["search_bm25"] is not None  # fallback

    def test_process_routes_to_chinese(self):
        pending = {
            "idx": {"Title": "火山", "Description": "", "Language": "zh"},
            "searchable_text": "关于火山的文章",
        }
        result = self.backend.process_search_data(pending)
        assert result["search_bm25_zh"] is not None
        assert result["search_bm25_en"] is None
        assert result["search_bm25_de"] is None
        assert result["search_bm25"] is not None

    def test_process_unconfigured_lang_goes_to_fallback_only(self):
        pending = {
            "idx": {"Title": "Volcan", "Description": "", "Language": "fr"},
            "searchable_text": "Texte sur les volcans",
        }
        result = self.backend.process_search_data(pending)
        assert result["search_bm25_en"] is None
        assert result["search_bm25_de"] is None
        assert result["search_bm25_zh"] is None
        assert result["search_bm25"] is not None

    def test_process_regional_variant(self):
        pending = {
            "idx": {"Title": "Test", "Description": "", "Language": "de-AT"},
            "searchable_text": "Austrian German",
        }
        result = self.backend.process_search_data(pending)
        assert result["search_bm25_de"] is not None  # de-AT → de

    def test_build_search_clause_german(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        _, _, rank = self.backend.build_search_clause("Vulkan", "de", pname)
        assert "search_bm25_de" in rank
        assert "pgcatalog_de" in rank
        assert "idx_os_search_bm25_de" in rank

    def test_build_search_clause_chinese(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        _, _, rank = self.backend.build_search_clause("火山", "zh", pname)
        assert "search_bm25_zh" in rank
        assert "pgcatalog_zh" in rank

    def test_build_search_clause_unconfigured_uses_fallback(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        _, _, rank = self.backend.build_search_clause("test", "fr", pname)
        assert "search_bm25 <&>" in rank
        assert "pgcatalog_default" in rank

    def test_build_search_clause_regional_variant(self):
        counter = [0]

        def pname(prefix):
            counter[0] += 1
            return f"p_{prefix}_{counter[0]}"

        _, _, rank = self.backend.build_search_clause("test", "de-AT", pname)
        assert "search_bm25_de" in rank

    def test_uncatalog_extra_all_columns(self):
        extra = self.backend.uncatalog_extra()
        assert extra == {
            "search_bm25": None,
            "search_bm25_en": None,
            "search_bm25_de": None,
            "search_bm25_zh": None,
        }


# ── LANG_TOKENIZER_MAP coverage ──────────────────────────────────────


class TestLangTokenizerMap:
    def test_all_snowball_langs_have_stemmers(self):
        stemmer_langs = [
            "ar",
            "hy",
            "eu",
            "ca",
            "da",
            "nl",
            "en",
            "et",
            "fi",
            "fr",
            "de",
            "el",
            "hi",
            "hu",
            "id",
            "ga",
            "it",
            "lt",
            "ne",
            "nb",
            "nn",
            "no",
            "pt",
            "ro",
            "ru",
            "sr",
            "es",
            "sv",
            "ta",
            "tr",
            "yi",
        ]
        for lang in stemmer_langs:
            assert lang in LANG_TOKENIZER_MAP, f"{lang} missing from map"
            assert "stemmer" in LANG_TOKENIZER_MAP[lang]

    def test_cjk_langs_have_segmenters(self):
        assert LANG_TOKENIZER_MAP["zh"]["pre_tokenizer"] == "jieba"
        assert LANG_TOKENIZER_MAP["ja"]["pre_tokenizer"] == "lindera"
        assert LANG_TOKENIZER_MAP["ko"]["pre_tokenizer"] == "lindera"

    def test_cjk_langs_no_stemmer(self):
        for lang in ("zh", "ja", "ko"):
            assert "stemmer" not in LANG_TOKENIZER_MAP[lang]


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

    def test_detect_and_set_passes_languages(self):
        # Even with bad DSN (falls back to tsvector), languages param
        # should be accepted without error
        backend = detect_and_set_backend(None, languages=["en", "de"])
        assert isinstance(backend, TsvectorBackend)
