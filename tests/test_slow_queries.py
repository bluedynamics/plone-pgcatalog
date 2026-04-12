"""Tests for slow query logging."""

from plone.pgcatalog.search import _log_all_queries_enabled
from plone.pgcatalog.search import _record_slow_query
from plone.pgcatalog.search import _SLOW_QUERY_MS
from plone.pgcatalog.search import _truncate_params_repr
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


class TestQueryLoggingConfiguration:
    """Test query logging configuration via environment variables."""

    def test_default_log_all_queries_disabled(self, monkeypatch):
        """Without the env var, logging is disabled."""
        monkeypatch.delenv("PGCATALOG_LOG_ALL_QUERIES", raising=False)
        assert _log_all_queries_enabled() is False

    def test_log_all_queries_enabled_values(self, monkeypatch):
        """Truthy values enable logging (case-insensitive)."""
        for value in ("1", "true", "yes", "True", "YES", "TrUe"):
            monkeypatch.setenv("PGCATALOG_LOG_ALL_QUERIES", value)
            assert _log_all_queries_enabled() is True, f"{value!r} should enable"

    def test_log_all_queries_falsey_values(self, monkeypatch):
        """Non-truthy values leave logging disabled."""
        for value in ("", "0", "false", "no", "off", "disabled"):
            monkeypatch.setenv("PGCATALOG_LOG_ALL_QUERIES", value)
            assert _log_all_queries_enabled() is False, f"{value!r} should disable"

    def test_log_all_queries_runtime_toggle(self, monkeypatch):
        """Toggling the env var takes effect on the next call (no restart)."""
        monkeypatch.delenv("PGCATALOG_LOG_ALL_QUERIES", raising=False)
        assert _log_all_queries_enabled() is False
        monkeypatch.setenv("PGCATALOG_LOG_ALL_QUERIES", "1")
        assert _log_all_queries_enabled() is True
        monkeypatch.setenv("PGCATALOG_LOG_ALL_QUERIES", "0")
        assert _log_all_queries_enabled() is False


class TestTruncateParamsRepr:
    """Test params truncation for log output."""

    def test_short_params_unchanged(self):
        assert _truncate_params_repr({"a": 1}) == "{'a': 1}"

    def test_empty_params(self):
        assert _truncate_params_repr({}) == "{}"

    def test_long_params_truncated(self):
        huge = {"paths": [f"/plone/page-{i}" for i in range(1000)]}
        result = _truncate_params_repr(huge)
        assert len(result) <= 2100  # 2000 + truncation marker
        assert result.endswith("... (truncated)")

    def test_short_list_unchanged(self):
        # Small enough to fit under 2000 bytes
        result = _truncate_params_repr({"paths": ["/a", "/b", "/c"]})
        assert not result.endswith("(truncated)")
