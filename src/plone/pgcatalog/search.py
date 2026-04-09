"""Core search executor and supporting types.

Provides ``_run_search()`` which translates a ZCatalog-style query dict
into SQL, executes it, and returns ``CatalogSearchResults`` with
``PGCatalogBrain`` instances.  No Plone dependency — testable standalone.
"""

from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain
from plone.pgcatalog.query import build_query

import logging
import os
import time


log = logging.getLogger(__name__)

_SLOW_QUERY_MS = float(os.environ.get("PGCATALOG_SLOW_QUERY_MS", "10"))


def _record_slow_query(conn, query_keys, duration_ms, sql, params):
    """Insert a slow query record into pgcatalog_slow_queries.

    Best-effort — silently ignores errors (table may not exist yet,
    or the connection may be in a read-only snapshot).
    """
    try:
        from psycopg.types.json import Json

        conn.execute(
            "INSERT INTO pgcatalog_slow_queries "
            "(query_keys, duration_ms, query_text, params) "
            "VALUES (%(keys)s, %(ms)s, %(sql)s, %(params)s)",
            {
                "keys": query_keys,
                "ms": duration_ms,
                "sql": sql,
                "params": Json({k: repr(v) for k, v in (params or {}).items()}),
            },
        )
    except Exception:
        pass  # best-effort — don't break search on logging failure


class _PendingBrain:
    """Minimal brain for objects in pending store (not yet in PG).

    Provides just enough interface for ``reindexObjectSecurity`` to
    find and reindex the object.
    """

    __slots__ = ("_obj", "_path")

    def __init__(self, path, obj):
        self._path = path
        self._obj = obj

    def getPath(self):
        return self._path

    def _unrestrictedGetObject(self):
        return self._obj


# Fixed set of columns for catalog queries (never user-supplied).
# Lazy mode: only zoid + path; idx fetched on demand via batch load.
# Eager mode: includes idx (backward compat for direct _run_search calls).
_SELECT_COLS_LAZY = "zoid, path"
_SELECT_COLS_LAZY_COUNTED = "zoid, path, COUNT(*) OVER() AS _total_count"
_SELECT_COLS_EAGER = "zoid, path, idx, meta"
_SELECT_COLS_EAGER_COUNTED = "zoid, path, idx, meta, COUNT(*) OVER() AS _total_count"


def _build_results(rows, actual_count, catalog, lazy_conn):
    """Build CatalogSearchResults from raw rows."""
    brains = [PGCatalogBrain(row, catalog=catalog) for row in rows]
    results = CatalogSearchResults(
        brains, actual_result_count=actual_count, conn=lazy_conn
    )
    if lazy_conn is not None:
        for brain in brains:
            brain._result_set = results
    return results


def _run_search(conn, query, catalog=None, lazy_conn=None):
    """Execute a prepared query dict and return CatalogSearchResults.

    Results are cached process-wide with TID-based invalidation.
    On cache hit, brains are rebuilt from cached rows (no PG query).

    When ``lazy_conn`` is provided, uses **lazy mode**: only ``zoid`` and
    ``path`` are selected.  The ``idx`` JSONB is fetched on demand when
    brain metadata is first accessed.

    Args:
        conn: psycopg connection (dict_row factory)
        query: ZCatalog-style query dict (security already applied if needed)
        catalog: reference for brain.getObject() traversal (optional)
        lazy_conn: connection for deferred idx batch loading (optional)

    Returns:
        CatalogSearchResults with PGCatalogBrain instances
    """
    from plone.pgcatalog.cache import _normalize_query as normalize_query
    from plone.pgcatalog.cache import get_query_cache

    cache = get_query_cache()

    # Get catalog change counter for cache validation.
    # Uses pgcatalog_change_seq (incremented only on catalog writes) instead
    # of MAX(tid) which changes on every ZODB write (#94).
    current_tid = None
    if cache.max_entries > 0 and catalog is not None:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT last_value FROM pgcatalog_change_seq")
                row = cur.fetchone()
            current_tid = row["last_value"] if row else None
        except Exception:
            log.warning("Query cache: change counter lookup failed", exc_info=True)

    # Cache lookup
    cache_key = normalize_query(query)
    if current_tid is not None:
        cached = cache.get(cache_key, current_tid)
        if cached is not None:
            rows, actual_count = cached
            return _build_results(rows, actual_count, catalog, lazy_conn)

    # Cache miss — execute query
    qr = build_query(query)

    has_limit = qr["limit"] is not None
    if lazy_conn is not None:
        cols = _SELECT_COLS_LAZY_COUNTED if has_limit else _SELECT_COLS_LAZY
    else:
        cols = _SELECT_COLS_EAGER_COUNTED if has_limit else _SELECT_COLS_EAGER

    sql = f"SELECT {cols} FROM object_state WHERE {qr['where']}"
    if qr["order_by"]:
        sql += f" ORDER BY {qr['order_by']}"
    if qr["limit"]:
        sql += f" LIMIT {qr['limit']}"
    if qr["offset"]:
        sql += f" OFFSET {qr['offset']}"

    t0 = time.monotonic()
    with conn.cursor() as cur:
        cur.execute(sql, qr["params"], prepare=True)
        rows = cur.fetchall()
    duration_ms = (time.monotonic() - t0) * 1000

    if duration_ms > _SLOW_QUERY_MS:
        query_keys = sorted(query.keys())
        log.warning(
            "Slow catalog query (%.1f ms): keys=%s sql=%s",
            duration_ms,
            query_keys,
            sql,
        )
        _record_slow_query(conn, query_keys, duration_ms, sql, qr["params"])

    actual_count = None
    if has_limit and rows:
        first = rows[0]
        actual_count = first["_total_count"] if isinstance(first, dict) else first[-1]
        rows = [{k: v for k, v in r.items() if k != "_total_count"} for r in rows]
    elif has_limit:
        actual_count = 0

    # Store in cache
    if current_tid is not None:
        cache.put(cache_key, rows, actual_count, duration_ms, current_tid)

    return _build_results(rows, actual_count, catalog, lazy_conn)
