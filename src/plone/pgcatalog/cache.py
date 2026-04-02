"""Process-wide query result cache with TID-based invalidation.

Caches catalog query results in memory. Invalidated when MAX(tid)
changes (any ZODB commit). Cost-based eviction keeps expensive
queries in cache while cheap ones are evicted first.

Configured via environment variables:
- PGCATALOG_QUERY_CACHE_SIZE: max entries (default 200, 0 = disabled)
- PGCATALOG_QUERY_CACHE_TTR: datetime rounding in seconds (default 60)
"""

from datetime import datetime

import logging
import os
import threading


log = logging.getLogger(__name__)

_CACHE_SIZE = int(os.environ.get("PGCATALOG_QUERY_CACHE_SIZE", "200"))
_CACHE_TTR = int(os.environ.get("PGCATALOG_QUERY_CACHE_TTR", "60"))


def _round_datetime(dt):
    """Round a datetime (stdlib or Zope DateTime) to TTR granularity."""
    if _CACHE_TTR <= 0:
        return dt
    try:
        # Zope DateTime — convert to timestamp via timeTime()
        ts = dt.timeTime()
    except AttributeError:
        try:
            # stdlib datetime
            ts = dt.timestamp()
        except (AttributeError, OSError):
            return dt
    rounded_ts = ts - (ts % _CACHE_TTR)
    return rounded_ts


def _normalize_query(query):
    """Normalize a query dict for stable cache key generation.

    - Sort dict keys recursively
    - Sort list values (convert to tuple)
    - Round datetime values to TTR granularity
    - Return a string suitable for hashing (repr of the normalized structure)
    """
    return repr(_normalize_value(query))


def _normalize_value(value):
    """Recursively normalize a value into a hashable representation."""
    if isinstance(value, dict):
        return tuple(sorted((k, _normalize_value(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        normalized = tuple(_normalize_value(v) for v in value)
        # Sort if all elements are comparable (same type, or all strings, etc.)
        try:
            return tuple(sorted(normalized))
        except TypeError:
            return normalized
    if isinstance(value, datetime):
        return _round_datetime(value)
    # Check for Zope DateTime (duck-type via timeTime method)
    if hasattr(value, "timeTime"):
        return _round_datetime(value)
    if isinstance(value, set):
        return tuple(sorted(_normalize_value(v) for v in value))
    if isinstance(value, frozenset):
        return tuple(sorted(_normalize_value(v) for v in value))
    return value


class QueryCache:
    """Process-wide query result cache."""

    def __init__(self, max_entries=_CACHE_SIZE):
        self.max_entries = max_entries
        self._lock = threading.Lock()
        self._cache = {}  # key -> {"rows": [...], "actual_count": int|None, "cost_ms": float, "hits": int}
        self._last_tid = None
        self._hits = 0
        self._misses = 0
        self._invalidations = 0

    def get(self, key, current_tid):
        """Look up a cached result. Returns (rows, actual_count) or None."""
        if self.max_entries <= 0:
            return None
        with self._lock:
            if current_tid != self._last_tid:
                self._cache.clear()
                self._last_tid = current_tid
                self._invalidations += 1
                return None
            entry = self._cache.get(key)
            if entry is not None:
                entry["hits"] += 1
                self._hits += 1
                return entry["rows"], entry["actual_count"]
            self._misses += 1
            return None

    def put(self, key, rows, actual_count, cost_ms, current_tid):
        """Store a result. Evicts cheapest entry if full."""
        if self.max_entries <= 0:
            return
        with self._lock:
            if current_tid != self._last_tid:
                self._cache.clear()
                self._last_tid = current_tid
                self._invalidations += 1
            if len(self._cache) >= self.max_entries and key not in self._cache:
                # Evict entry with lowest cost_ms
                cheapest_key = min(self._cache, key=lambda k: self._cache[k]["cost_ms"])
                del self._cache[cheapest_key]
            self._cache[key] = {
                "rows": rows,
                "actual_count": actual_count,
                "cost_ms": cost_ms,
                "hits": 0,
            }

    def clear(self):
        """Clear the cache."""
        with self._lock:
            self._cache.clear()

    def stats(self):
        """Return cache statistics for ZMI display."""
        with self._lock:
            entries = []
            for _key, entry in sorted(
                self._cache.items(), key=lambda kv: kv[1]["cost_ms"], reverse=True
            )[:20]:
                entries.append(
                    {
                        "cost_ms": round(entry["cost_ms"], 1),
                        "hits": entry["hits"],
                        "rows": len(entry["rows"]),
                    }
                )
            return {
                "enabled": self.max_entries > 0,
                "max_entries": self.max_entries,
                "current_entries": len(self._cache),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(
                    self._hits / max(self._hits + self._misses, 1) * 100, 1
                ),
                "last_tid": self._last_tid,
                "invalidations": self._invalidations,
                "ttr_seconds": _CACHE_TTR,
                "top_entries": entries,
            }


# Module-level singleton
_query_cache = QueryCache()


def get_query_cache():
    """Return the module-level QueryCache singleton."""
    return _query_cache
