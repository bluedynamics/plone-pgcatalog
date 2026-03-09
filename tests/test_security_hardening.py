"""Tests for security hardening and review fixes.

Validates input sanitization, SQL injection prevention, batching,
and observability improvements:
- BM25Backend rejects unknown language codes (CAT-C1)
- BM25Backend validates generated DDL identifiers (CAT-C1)
- clear_catalog_data uses safe SQL composition for column names (CAT-M1)
- DRI query validates index names defensively (CAT-M3)
- Unknown query keys validated before SQL interpolation (CAT-Q1)
- Startup DDL uses psycopg.sql composition (CAT-S1)
- reindex_index uses server-side cursor batching (CAT-P1)
- Extraction failures emit debug logs (CAT-O1)
- Fallback pool atexit shutdown hook (CAT-L1)
"""

from plone.pgcatalog.backends import BM25Backend
from plone.pgcatalog.backends import LANG_TOKENIZER_MAP
from plone.pgcatalog.backends import reset_backend
from plone.pgcatalog.backends import set_backend
from plone.pgcatalog.dri import DateRecurringIndexTranslator
from plone.pgcatalog.maintenance import clear_catalog_data
from plone.pgcatalog.query import build_query
from psycopg import sql as pgsql
from unittest.mock import MagicMock

import os
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


# ---------------------------------------------------------------------------
# CAT-Q1: Unknown query key validation in _process_index fallback
# ---------------------------------------------------------------------------


class TestUnknownQueryKeyValidation:
    """build_query must reject unsafe key names in the fallback path."""

    def test_rejects_sql_injection_in_key(self):
        """Keys with SQL injection payloads must raise ValueError."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            build_query({"'; DROP TABLE object_state;--": "value"})

    def test_rejects_key_with_single_quote(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            build_query({"field'name": "value"})

    def test_rejects_key_with_semicolon(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            build_query({"field;name": "value"})

    def test_rejects_key_with_parentheses(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            build_query({"field()": "value"})

    def test_rejects_key_with_spaces(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            build_query({"field name": "value"})

    def test_rejects_key_with_hyphen(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            build_query({"my-index": "value"})

    def test_rejects_key_starting_with_digit(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            build_query({"1field": "value"})

    def test_accepts_valid_unknown_key(self):
        """Valid identifier names must still work as fallback field queries."""
        qr = build_query({"Language": "en"})
        assert "Language" in qr["where"]
        assert len(qr["params"]) > 0

    def test_accepts_underscore_key(self):
        qr = build_query({"_my_field": "value"})
        assert "_my_field" in qr["where"]

    def test_accepts_alphanumeric_key(self):
        qr = build_query({"field123": "value"})
        assert "field123" in qr["where"]


# ---------------------------------------------------------------------------
# CAT-S1: Startup DDL uses psycopg.sql composition
# ---------------------------------------------------------------------------


class TestEnsureTextIndexesSafeSQL:
    """_ensure_text_indexes must use psycopg.sql for DDL, not f-strings."""

    def test_ddl_uses_sql_composition(self):
        """Verify the generated DDL uses psycopg.sql objects, not raw strings."""
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.startup import _ensure_text_indexes

        registry = get_registry()
        registry.register("test_ddl_field", IndexType.TEXT, "test_ddl_field")
        try:
            mock_storage = MagicMock()
            mock_storage._dsn = "postgresql://test/test"

            with MagicMock() as mock_connect:
                mock_conn = MagicMock()
                mock_connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
                mock_connect.return_value.__exit__ = MagicMock(return_value=False)

                import psycopg as _psycopg

                orig_connect = _psycopg.connect
                _psycopg.connect = mock_connect
                try:
                    _ensure_text_indexes(mock_storage)
                finally:
                    _psycopg.connect = orig_connect

                if mock_conn.execute.called:
                    stmt = mock_conn.execute.call_args[0][0]
                    assert isinstance(stmt, pgsql.Composed), (
                        f"Expected Composed SQL, got {type(stmt).__name__}"
                    )
        finally:
            registry._indexes.pop("test_ddl_field", None)


# ---------------------------------------------------------------------------
# CAT-P1: reindex_index batching
# ---------------------------------------------------------------------------


class TestReindexIndexBatching:
    """reindex_index must use server-side cursor with batched fetches."""

    def test_uses_named_cursor(self):
        """Server-side cursor requires a name parameter."""
        from plone.pgcatalog.maintenance import reindex_index

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        reindex_index(mock_conn, "portal_type")

        # cursor() must be called with a name for server-side cursor
        mock_conn.cursor.assert_called_once_with(name="reindex_cursor")

    def test_fetches_in_batches(self):
        """Rows should be fetched via fetchmany, not fetchall."""
        from plone.pgcatalog.maintenance import reindex_index

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Simulate two batches of 2 rows, then empty
        row1 = {"zoid": 1, "idx": {"portal_type": "Document"}}
        row2 = {"zoid": 2, "idx": {"portal_type": "News"}}
        row3 = {"zoid": 3, "idx": {"portal_type": "Event"}}
        mock_cursor.fetchmany.side_effect = [
            [row1, row2],
            [row3],
            [],
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        count = reindex_index(mock_conn, "portal_type", batch_size=2)
        assert count == 3
        assert mock_cursor.fetchmany.call_count == 3


# ---------------------------------------------------------------------------
# CAT-O1: Extraction failure logging
# ---------------------------------------------------------------------------


class TestExtractionLogging:
    """Extraction failures must emit debug logs instead of silently passing."""

    def test_index_extraction_failure_logs(self):
        """Failed index extraction emits a debug log."""
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.extraction import extract_idx

        registry = get_registry()
        registry.register("broken_idx", IndexType.FIELD, "broken_idx")
        try:
            wrapper = MagicMock()
            wrapper.broken_idx.side_effect = RuntimeError("test error")

            import logging

            logger = logging.getLogger("plone.pgcatalog.extraction")
            orig_debug = logger.debug
            calls = []
            logger.debug = lambda *a, **kw: calls.append((a, kw))
            try:
                idx = extract_idx(wrapper)
            finally:
                logger.debug = orig_debug

            assert "broken_idx" not in idx
            # At least one debug call about the extraction failure
            assert any("broken_idx" in str(c) for c in calls)
        finally:
            registry._indexes.pop("broken_idx", None)

    def test_translator_extraction_failure_logs(self):
        """Failed translator extraction emits a debug log."""
        from plone.pgcatalog.extraction import extract_from_translators
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import getSiteManager

        translator = MagicMock()
        translator.extract.side_effect = RuntimeError("broken")

        sm = getSiteManager()
        sm.registerUtility(translator, IPGIndexTranslator, name="bad_trans")
        try:
            import logging

            logger = logging.getLogger("plone.pgcatalog.extraction")
            calls = []
            orig_debug = logger.debug
            logger.debug = lambda *a, **kw: calls.append((a, kw))
            try:
                idx = {}
                extract_from_translators(MagicMock(), idx)
            finally:
                logger.debug = orig_debug

            assert "bad_trans" not in idx
            assert any("bad_trans" in str(c) for c in calls)
        finally:
            sm.unregisterUtility(provided=IPGIndexTranslator, name="bad_trans")


# ---------------------------------------------------------------------------
# CAT-L1: Fallback pool atexit shutdown hook
# ---------------------------------------------------------------------------


class TestFallbackPoolShutdown:
    """_fallback_pool must register an atexit close hook."""

    def test_atexit_hook_registered_on_pool_creation(self):
        """Creating a fallback pool registers an atexit handler."""
        from unittest.mock import patch

        import atexit
        import plone.pgcatalog.pool as pool_mod

        # Save original state
        orig_pool = pool_mod._fallback_pool
        pool_mod._fallback_pool = None

        registered = []
        with (
            patch.dict(os.environ, {"PGCATALOG_DSN": "postgresql://test/test"}),
            patch("psycopg_pool.ConnectionPool") as MockPool,
            patch.object(
                atexit, "register", side_effect=lambda fn: registered.append(fn)
            ),
        ):
            mock_pool = MagicMock()
            MockPool.return_value = mock_pool

            result = pool_mod._pool_from_env()
            assert result is mock_pool
            # atexit.register must have been called
            assert len(registered) == 1

        # Restore
        pool_mod._fallback_pool = orig_pool

    def test_atexit_hook_closes_pool(self):
        """The registered atexit handler calls pool.close()."""
        from unittest.mock import patch

        import atexit
        import plone.pgcatalog.pool as pool_mod

        orig_pool = pool_mod._fallback_pool
        pool_mod._fallback_pool = None

        registered = []
        with (
            patch.dict(os.environ, {"PGCATALOG_DSN": "postgresql://test/test"}),
            patch("psycopg_pool.ConnectionPool") as MockPool,
            patch.object(
                atexit, "register", side_effect=lambda fn: registered.append(fn)
            ),
        ):
            mock_pool = MagicMock()
            MockPool.return_value = mock_pool

            pool_mod._pool_from_env()

            # Call the registered handler
            registered[0]()
            mock_pool.close.assert_called_once()

        pool_mod._fallback_pool = orig_pool
