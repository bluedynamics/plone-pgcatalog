"""Tests for slow query logging and suggestion."""

from plone.pgcatalog.catalog import _suggest_index
from plone.pgcatalog.search import _record_slow_query
from plone.pgcatalog.search import _SLOW_QUERY_MS
from unittest import mock

import os


class TestSuggestIndex:
    """Test composite index suggestion from query keys."""

    def test_two_keys(self):
        result = _suggest_index(["path_parent", "portal_type"])
        assert "idx_os_cat_path_parent_portal_type" in result
        assert "(idx->>'path_parent')" in result
        assert "(idx->>'portal_type')" in result

    def test_three_keys(self):
        result = _suggest_index(["path_parent", "portal_type", "review_state"])
        assert "path_parent" in result
        assert "portal_type" in result
        assert "review_state" in result

    def test_single_key_returns_none(self):
        assert _suggest_index(["portal_type"]) is None

    def test_filters_non_idx_fields(self):
        result = _suggest_index(
            ["portal_type", "sort_on", "SearchableText", "review_state"]
        )
        assert result is not None
        assert "sort_on" not in result
        assert "SearchableText" not in result
        assert "portal_type" in result
        assert "review_state" in result

    def test_all_non_idx_returns_none(self):
        assert _suggest_index(["sort_on", "b_size", "b_start"]) is None

    def test_caps_at_three_fields(self):
        result = _suggest_index(["aaa", "bbb", "ccc", "ddd", "eee"])
        # Should only include first 3
        assert "(idx->>'aaa'), (idx->>'bbb'), (idx->>'ccc')" in result
        assert "ddd" not in result


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
