"""Process-wide query result cache with catalog change counter invalidation.

Caches catalog query results in memory. Invalidated when the
``pgcatalog_change_seq`` counter changes (only on actual catalog
writes, not on unrelated ZODB commits). Bounded by entries and by
rows; the least recently used entry is evicted first.

The cache owns its rows: ``put`` stores copies and ``get`` returns
copies, so brains built from a cached result never write ``idx`` or
``meta`` into the cache (#280).

Configured via environment variables:
- PGCATALOG_QUERY_CACHE_SIZE: max entries (default 200, 0 = disabled)
- PGCATALOG_QUERY_CACHE_MAX_ROWS: max rows over all entries (default 100000)
- PGCATALOG_QUERY_CACHE_MAX_ENTRY_ROWS: larger results are not cached
  (default 10000)
- PGCATALOG_QUERY_CACHE_TTR: datetime rounding in seconds (default 60)
"""

from collections import OrderedDict
from datetime import datetime

import logging
import os
import threading


log = logging.getLogger(__name__)

_CACHE_SIZE = int(os.environ.get("PGCATALOG_QUERY_CACHE_SIZE", "200"))
_CACHE_MAX_ROWS = int(os.environ.get("PGCATALOG_QUERY_CACHE_MAX_ROWS", "100000"))
_CACHE_MAX_ENTRY_ROWS = int(
    os.environ.get("PGCATALOG_QUERY_CACHE_MAX_ENTRY_ROWS", "10000")
)
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

    def __init__(
        self,
        max_entries=_CACHE_SIZE,
        max_rows=_CACHE_MAX_ROWS,
        max_entry_rows=_CACHE_MAX_ENTRY_ROWS,
    ):
        self.max_entries = max_entries
        self.max_rows = max_rows
        self.max_entry_rows = max_entry_rows
        self._lock = threading.Lock()
        # key -> {"rows": [...], "actual_count": int|None, "cost_ms": float, "hits": int}
        # Ordered from least to most recently used.
        self._cache = OrderedDict()
        self._rows = 0
        self._last_tid = None
        self._hits = 0
        self._misses = 0
        self._invalidations = 0
        self._too_large = 0

    def _check_tid(self, current_tid):
        """Drop all entries when the catalog changed.  Caller holds the lock."""
        if current_tid == self._last_tid:
            return True
        self._cache.clear()
        self._rows = 0
        self._last_tid = current_tid
        self._invalidations += 1
        return False

    def get(self, key, current_tid):
        """Look up a cached result. Returns (rows, actual_count) or None.

        The rows are fresh copies; callers may mutate them.
        """
        if self.max_entries <= 0:
            return None
        with self._lock:
            if not self._check_tid(current_tid):
                return None
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None
            self._cache.move_to_end(key)
            entry["hits"] += 1
            self._hits += 1
            rows = entry["rows"]
            actual_count = entry["actual_count"]
        return [dict(row) for row in rows], actual_count

    def put(self, key, rows, actual_count, cost_ms, current_tid):
        """Store a copy of a result.  Evicts least recently used entries."""
        if self.max_entries <= 0:
            return
        if len(rows) > min(self.max_entry_rows, self.max_rows):
            with self._lock:
                self._too_large += 1
            return
        rows = [dict(row) for row in rows]
        with self._lock:
            self._check_tid(current_tid)
            old = self._cache.pop(key, None)
            if old is not None:
                self._rows -= len(old["rows"])
            while self._cache and (
                len(self._cache) >= self.max_entries
                or self._rows + len(rows) > self.max_rows
            ):
                _key, evicted = self._cache.popitem(last=False)
                self._rows -= len(evicted["rows"])
            self._cache[key] = {
                "rows": rows,
                "actual_count": actual_count,
                "cost_ms": cost_ms,
                "hits": 0,
            }
            self._rows += len(rows)

    def clear(self):
        """Clear the cache."""
        with self._lock:
            self._cache.clear()
            self._rows = 0

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
                "max_rows": self.max_rows,
                "max_entry_rows": self.max_entry_rows,
                "current_rows": self._rows,
                "too_large": self._too_large,
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
