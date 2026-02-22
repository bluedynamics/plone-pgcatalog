"""Tests for security hardening fixes (CAT-C1, CAT-M1, CAT-M3).

Validates input sanitization and SQL injection prevention:
- BM25Backend rejects unknown language codes (CAT-C1)
- BM25Backend validates generated DDL identifiers (CAT-C1)
- clear_catalog_data uses safe SQL composition for column names (CAT-M1)
- DRI query validates index names defensively (CAT-M3)
"""

from plone.pgcatalog.backends import BM25Backend
from plone.pgcatalog.backends import LANG_TOKENIZER_MAP
from plone.pgcatalog.backends import reset_backend
from plone.pgcatalog.backends import set_backend
from plone.pgcatalog.catalog import clear_catalog_data
from plone.pgcatalog.dri import DateRecurringIndexTranslator
from psycopg import sql as pgsql
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _reset():
    """Reset backend singleton after each test."""
    yield
    reset_backend()


# ---------------------------------------------------------------------------
# CAT-C1: BM25Backend language code validation
# ---------------------------------------------------------------------------


class TestBM25LanguageValidation:
    """BM25Backend.__init__ must reject unknown language codes."""

    def test_rejects_unknown_language(self):
        with pytest.raises(ValueError, match="Unknown language code"):
            BM25Backend(languages=["xx"])

    def test_rejects_sql_injection_language(self):
        with pytest.raises(ValueError, match="Unknown language code"):
            BM25Backend(languages=["en; DROP TABLE"])

    def test_rejects_empty_string_language(self):
        """Empty string after normalization is not in LANG_TOKENIZER_MAP."""
        with pytest.raises(ValueError, match="Unknown language code"):
            BM25Backend(languages=[""])

    def test_rejects_mixed_valid_invalid(self):
        """If any language is invalid, the whole init fails."""
        with pytest.raises(ValueError, match="Unknown language code"):
            BM25Backend(languages=["en", "xx"])

    def test_accepts_all_known_languages(self):
        """Every key in LANG_TOKENIZER_MAP must be accepted."""
        for lang in LANG_TOKENIZER_MAP:
            backend = BM25Backend(languages=[lang])
            assert lang in backend.languages

    def test_accepts_valid_language_en(self):
        backend = BM25Backend(languages=["en"])
        assert backend.languages == ["en"]

    def test_accepts_valid_multilingual(self):
        backend = BM25Backend(languages=["en", "de", "fr"])
        assert backend.languages == ["en", "de", "fr"]

    def test_accepts_regional_variants(self):
        """Regional variants like 'pt-br' normalize to 'pt'."""
        backend = BM25Backend(languages=["pt-br", "zh-CN"])
        assert backend.languages == ["pt", "zh"]

    def test_rejects_unknown_regional_variant(self):
        """'xx-YY' normalizes to 'xx' which is not in the map."""
        with pytest.raises(ValueError, match="Unknown language code"):
            BM25Backend(languages=["xx-YY"])


class TestBM25IdentifierValidation:
    """Generated col/idx/tok names are validated as safe SQL identifiers."""

    def test_generated_names_are_valid(self):
        backend = BM25Backend(languages=["en", "de"])
        # These should all pass validate_identifier (called in __init__)
        assert backend._col_name("en") == "search_bm25_en"
        assert backend._idx_name("en") == "idx_os_search_bm25_en"
        assert backend._tok_name("en") == "pgcatalog_en"
        assert backend._col_name() == "search_bm25"
        assert backend._idx_name() == "idx_os_search_bm25"
        assert backend._tok_name() == "pgcatalog_default"


class TestBM25InstallSchemaSQL:
    """install_schema uses psycopg.sql for DDL construction."""

    def test_install_schema_uses_sql_identifiers(self):
        """Verify install_schema calls conn.execute with Composed objects."""
        backend = BM25Backend(languages=["en"])
        mock_conn = MagicMock()
        backend.install_schema(mock_conn)

        calls = mock_conn.execute.call_args_list
        # Should have multiple calls
        assert len(calls) > 0

        # Check that at least some calls use psycopg.sql.Composed objects
        # (ALTER TABLE, CREATE INDEX, DO $$ blocks)
        composed_calls = [c for c in calls if isinstance(c[0][0], pgsql.Composed)]
        # We expect at least 3 composed calls per language (ALTER, DO, CREATE INDEX)
        # plus 1 for fallback tokenizer = at least 4
        assert len(composed_calls) >= 4, (
            f"Expected at least 4 Composed SQL calls, got {len(composed_calls)}"
        )


class TestBM25GetSchemaSqlSafety:
    """get_schema_sql returns safe DDL strings with validated identifiers."""

    def test_schema_sql_contains_validated_names(self):
        backend = BM25Backend(languages=["en", "de"])
        sql = backend.get_schema_sql()

        # All identifiers should be present and safe
        assert "search_bm25_en" in sql
        assert "search_bm25_de" in sql
        assert "pgcatalog_en" in sql
        assert "pgcatalog_de" in sql
        assert "idx_os_search_bm25_en" in sql
        assert "idx_os_search_bm25_de" in sql

    def test_schema_sql_no_injection_possible(self):
        """Attempting to create a backend with injection payload fails at init."""
        with pytest.raises(ValueError):
            BM25Backend(languages=["en'; DROP TABLE object_state;--"])


# ---------------------------------------------------------------------------
# CAT-M1: clear_catalog_data safe SQL composition
# ---------------------------------------------------------------------------


class TestClearCatalogDataSafeSQL:
    """clear_catalog_data must use psycopg.sql for extra column NULLing."""

    def test_generates_safe_sql_with_bm25_backend(self):
        """With BM25 backend, extra columns are safely quoted."""
        backend = BM25Backend(languages=["en"])
        set_backend(backend)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.rowcount = 0
        mock_conn.cursor.return_value = mock_cursor

        clear_catalog_data(mock_conn)

        # Check the execute call
        execute_call = mock_cursor.execute.call_args
        query = execute_call[0][0]

        # The query should be a psycopg.sql.Composed object
        assert isinstance(query, pgsql.Composed), (
            f"Expected Composed SQL, got {type(query).__name__}"
        )

    def test_generates_safe_sql_with_tsvector_backend(self):
        """With Tsvector backend (no extra columns), plain SQL is fine."""
        # Default backend is Tsvector (no extra columns)
        reset_backend()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.rowcount = 0
        mock_conn.cursor.return_value = mock_cursor

        clear_catalog_data(mock_conn)

        # Even with no extra columns, the query should still be Composed
        execute_call = mock_cursor.execute.call_args
        query = execute_call[0][0]
        assert isinstance(query, pgsql.Composed)

    def test_extra_columns_properly_quoted(self):
        """Verify column names in the SQL are properly identifier-quoted."""
        backend = BM25Backend(languages=["en"])
        set_backend(backend)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.rowcount = 0
        mock_conn.cursor.return_value = mock_cursor

        clear_catalog_data(mock_conn)

        execute_call = mock_cursor.execute.call_args
        query = execute_call[0][0]

        # Render the SQL to check content
        rendered = query.as_string(None)
        assert "search_bm25" in rendered
        assert "search_bm25_en" in rendered
        assert "= NULL" in rendered


# ---------------------------------------------------------------------------
# CAT-M3: DRI index name validation
# ---------------------------------------------------------------------------


class TestDRIIndexNameValidation:
    """DRI query() must validate index_name defensively."""

    def test_query_rejects_invalid_index_name(self):
        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )
        with pytest.raises(ValueError, match="Invalid identifier"):
            translator.query(
                "start'; DROP TABLE --",
                None,
                {"query": "2025-01-01"},
            )

    def test_query_accepts_valid_index_name(self):
        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )
        # Should not raise
        sql, params = translator.query(
            "start",
            None,
            {"query": "2025-01-01", "range": "min"},
        )
        assert "start" in sql
        assert len(params) > 0

    def test_query_rejects_hyphenated_name(self):
        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )
        with pytest.raises(ValueError, match="Invalid identifier"):
            translator.query(
                "my-index",
                None,
                {"query": "2025-01-01"},
            )

    def test_constructor_validates_date_attr(self):
        """Constructor already validates date_attr."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            DateRecurringIndexTranslator(
                date_attr="bad;name", recurdef_attr="recurrence"
            )
