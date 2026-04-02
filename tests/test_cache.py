"""Tests for plone.pgcatalog.cache — query result cache with TID invalidation."""

from datetime import datetime
from datetime import UTC
from unittest import mock

import pytest


class TestNormalizeQuery:
    """Tests for _normalize_query and _normalize_value."""

    def test_normalize_sorts_keys(self):
        from plone.pgcatalog.cache import _normalize_query

        q1 = {"portal_type": "Document", "review_state": "published"}
        q2 = {"review_state": "published", "portal_type": "Document"}
        assert _normalize_query(q1) == _normalize_query(q2)

    def test_normalize_sorts_nested_keys(self):
        from plone.pgcatalog.cache import _normalize_query

        q1 = {"path": {"query": "/plone", "depth": 1}}
        q2 = {"path": {"depth": 1, "query": "/plone"}}
        assert _normalize_query(q1) == _normalize_query(q2)

    def test_normalize_sorts_lists(self):
        from plone.pgcatalog.cache import _normalize_query

        q1 = {"portal_type": ["Document", "File", "Image"]}
        q2 = {"portal_type": ["Image", "Document", "File"]}
        assert _normalize_query(q1) == _normalize_query(q2)

    def test_normalize_rounds_datetime(self):
        from plone.pgcatalog.cache import _normalize_query

        # Two datetimes 30 seconds apart within the same 60s TTR window
        dt1 = datetime(2026, 4, 1, 12, 0, 10, tzinfo=UTC)
        dt2 = datetime(2026, 4, 1, 12, 0, 40, tzinfo=UTC)
        q1 = {"effective": dt1}
        q2 = {"effective": dt2}
        assert _normalize_query(q1) == _normalize_query(q2)

    def test_normalize_different_ttr_windows_differ(self):
        from plone.pgcatalog.cache import _normalize_query

        # Two datetimes in different 60s TTR windows
        dt1 = datetime(2026, 4, 1, 12, 0, 10, tzinfo=UTC)
        dt2 = datetime(2026, 4, 1, 12, 1, 10, tzinfo=UTC)
        q1 = {"effective": dt1}
        q2 = {"effective": dt2}
        assert _normalize_query(q1) != _normalize_query(q2)

    def test_normalize_zope_datetime(self):
        from plone.pgcatalog.cache import _normalize_query

        # Mock Zope DateTime with timeTime() method
        dt1 = mock.Mock()
        dt1.timeTime.return_value = 1743508810.0  # some timestamp
        dt2 = mock.Mock()
        dt2.timeTime.return_value = 1743508840.0  # 30 seconds later, same TTR window
        q1 = {"effective": dt1}
        q2 = {"effective": dt2}
        assert _normalize_query(q1) == _normalize_query(q2)

    def test_normalize_preserves_non_datetime_values(self):
        from plone.pgcatalog.cache import _normalize_query

        q = {"portal_type": "Document", "limit": 10, "is_folderish": True}
        result = _normalize_query(q)
        assert "Document" in result
        assert "10" in result

    def test_normalize_handles_sets(self):
        from plone.pgcatalog.cache import _normalize_query

        q1 = {"types": {"Document", "File"}}
        q2 = {"types": {"File", "Document"}}
        assert _normalize_query(q1) == _normalize_query(q2)

    def test_normalize_deterministic(self):
        from plone.pgcatalog.cache import _normalize_query

        q = {
            "portal_type": ["Document", "File"],
            "review_state": "published",
            "path": {"query": "/plone/folder", "depth": 2},
        }
        # Must be deterministic across multiple calls
        assert _normalize_query(q) == _normalize_query(q)


class TestQueryCache:
    """Tests for the QueryCache class."""

    def test_cache_hit_miss(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)
        tid = b"\x00\x01"
        rows = [{"zoid": 1}, {"zoid": 2}]

        # Miss
        result = cache.get("key1", tid)
        assert result is None

        # Put
        cache.put("key1", rows, None, 50.0, tid)

        # Hit
        result = cache.get("key1", tid)
        assert result is not None
        assert result[0] == rows
        assert result[1] is None

    def test_cache_hit_returns_actual_count(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)
        tid = b"\x00\x01"
        rows = [{"zoid": 1}]

        cache.put("key1", rows, 42, 10.0, tid)
        result = cache.get("key1", tid)
        assert result == (rows, 42)

    def test_cache_tid_invalidation(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)
        tid1 = b"\x00\x01"
        tid2 = b"\x00\x02"
        rows = [{"zoid": 1}]

        cache.put("key1", rows, None, 50.0, tid1)
        assert cache.get("key1", tid1) is not None

        # Different TID should invalidate
        result = cache.get("key1", tid2)
        assert result is None

    def test_cache_tid_invalidation_on_put(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)
        tid1 = b"\x00\x01"
        tid2 = b"\x00\x02"

        cache.put("key1", [{"zoid": 1}], None, 50.0, tid1)
        # Put with new TID clears old entries
        cache.put("key2", [{"zoid": 2}], None, 30.0, tid2)

        assert cache.get("key1", tid2) is None
        result = cache.get("key2", tid2)
        assert result is not None

    def test_cache_cost_eviction(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=2)
        tid = b"\x00\x01"

        # Fill cache with two entries
        cache.put("cheap", [{"zoid": 1}], None, 10.0, tid)
        cache.put("expensive", [{"zoid": 2}], None, 100.0, tid)

        # Adding a third should evict the cheapest
        cache.put("medium", [{"zoid": 3}], None, 50.0, tid)

        assert cache.get("cheap", tid) is None  # evicted
        assert cache.get("expensive", tid) is not None  # kept
        assert cache.get("medium", tid) is not None  # kept

    def test_cache_cost_eviction_does_not_evict_existing_key(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=2)
        tid = b"\x00\x01"

        cache.put("key1", [{"zoid": 1}], None, 10.0, tid)
        cache.put("key2", [{"zoid": 2}], None, 20.0, tid)

        # Updating key1 should not evict anything (key already exists)
        cache.put("key1", [{"zoid": 1, "extra": True}], None, 5.0, tid)
        assert cache.get("key1", tid) is not None
        assert cache.get("key2", tid) is not None

    def test_cache_disabled_when_size_zero(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=0)
        tid = b"\x00\x01"
        rows = [{"zoid": 1}]

        cache.put("key1", rows, None, 50.0, tid)
        result = cache.get("key1", tid)
        assert result is None

    def test_cache_disabled_negative_size(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=-1)
        tid = b"\x00\x01"

        cache.put("key1", [{"zoid": 1}], None, 10.0, tid)
        assert cache.get("key1", tid) is None

    def test_cache_clear(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)
        tid = b"\x00\x01"

        cache.put("key1", [{"zoid": 1}], None, 50.0, tid)
        cache.clear()
        assert cache.get("key1", tid) is None

    def test_cache_stats(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)
        tid = b"\x00\x01"

        # Generate some hits and misses
        cache.put("key1", [{"zoid": 1}, {"zoid": 2}], None, 85.0, tid)
        cache.put("key2", [{"zoid": 3}], 100, 20.0, tid)
        cache.get("key1", tid)  # hit
        cache.get("key1", tid)  # hit
        cache.get("key3", tid)  # miss

        stats = cache.stats()
        assert stats["enabled"] is True
        assert stats["max_entries"] == 10
        assert stats["current_entries"] == 2
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["hit_rate"] == pytest.approx(66.7, abs=0.1)
        assert stats["last_tid"] == tid
        assert stats["invalidations"] == 1  # first put set the TID
        assert len(stats["top_entries"]) == 2
        # Top entries sorted by cost_ms descending
        assert stats["top_entries"][0]["cost_ms"] == 85.0
        assert stats["top_entries"][0]["rows"] == 2
        assert stats["top_entries"][0]["hits"] == 2

    def test_cache_stats_disabled(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=0)
        stats = cache.stats()
        assert stats["enabled"] is False

    def test_cache_stats_invalidation_count(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)

        cache.put("key1", [], None, 10.0, b"\x01")
        cache.get("key1", b"\x02")  # invalidation
        cache.get("key1", b"\x03")  # another invalidation

        stats = cache.stats()
        assert stats["invalidations"] == 3  # first put + 2 gets with new TIDs

    def test_cache_hit_increments_entry_hits(self):
        from plone.pgcatalog.cache import QueryCache

        cache = QueryCache(max_entries=10)
        tid = b"\x00\x01"

        cache.put("key1", [{"zoid": 1}], None, 50.0, tid)
        cache.get("key1", tid)
        cache.get("key1", tid)
        cache.get("key1", tid)

        stats = cache.stats()
        assert stats["top_entries"][0]["hits"] == 3


class TestGetQueryCache:
    """Tests for the module-level singleton."""

    def test_returns_same_instance(self):
        from plone.pgcatalog.cache import get_query_cache

        cache1 = get_query_cache()
        cache2 = get_query_cache()
        assert cache1 is cache2

    def test_returns_query_cache_instance(self):
        from plone.pgcatalog.cache import get_query_cache
        from plone.pgcatalog.cache import QueryCache

        cache = get_query_cache()
        assert isinstance(cache, QueryCache)
