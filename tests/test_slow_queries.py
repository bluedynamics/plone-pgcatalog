"""Tests for slow query logging."""

from plone.pgcatalog.search import _record_slow_query
from plone.pgcatalog.search import _SLOW_QUERY_MS
from unittest import mock

import os


class TestSlowQueryThreshold:
    """Test slow query threshold configuration."""

    def test_default_threshold(self):
        assert _SLOW_QUERY_MS == 10.0

    def test_env_override(self):
        with mock.patch.dict(os.environ, {"PGCATALOG_SLOW_QUERY_MS": "50"}):
            # Reimport to pick up env var
            from plone.pgcatalog import search

            import importlib

            importlib.reload(search)
            assert search._SLOW_QUERY_MS == 50.0
            # Restore
            os.environ.pop("PGCATALOG_SLOW_QUERY_MS", None)
            importlib.reload(search)


class TestRecordSlowQuery:
    """Test best-effort slow query recording."""

    def test_silently_ignores_errors(self):
        """Recording should not raise even if the table doesn't exist."""
        mock_conn = mock.Mock()
        mock_conn.execute.side_effect = Exception("table not found")
        # Should not raise
        _record_slow_query(
            mock_conn,
            ["portal_type", "path_parent"],
            15.5,
            "SELECT * FROM object_state WHERE ...",
            {"portal_type": "Document"},
        )

    def test_calls_insert(self):
        """Recording should attempt an INSERT."""
        mock_conn = mock.Mock()
        _record_slow_query(
            mock_conn,
            ["portal_type"],
            25.0,
            "SELECT ...",
            {"type": "File"},
        )
        mock_conn.execute.assert_called_once()
        call_sql = mock_conn.execute.call_args[0][0]
        assert "pgcatalog_slow_queries" in call_sql
