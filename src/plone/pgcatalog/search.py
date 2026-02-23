"""Core search executor and supporting types.

Provides ``_run_search()`` which translates a ZCatalog-style query dict
into SQL, executes it, and returns ``CatalogSearchResults`` with
``PGCatalogBrain`` instances.  No Plone dependency — testable standalone.
"""

from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain
from plone.pgcatalog.query import build_query


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
_SELECT_COLS_EAGER = "zoid, path, idx"
_SELECT_COLS_EAGER_COUNTED = "zoid, path, idx, COUNT(*) OVER() AS _total_count"


def _run_search(conn, query, catalog=None, lazy_conn=None):
    """Execute a prepared query dict and return CatalogSearchResults.

    Builds the SQL once and uses a ``COUNT(*) OVER()`` window function
    when a LIMIT is present so only *one* query is executed.

    When ``lazy_conn`` is provided, uses **lazy mode**: only ``zoid`` and
    ``path`` are selected.  The ``idx`` JSONB is fetched on demand when
    brain metadata is first accessed (via
    ``CatalogSearchResults._load_idx_batch()``), using the same connection
    (and thus the same REPEATABLE READ snapshot).

    Without ``lazy_conn``, uses **eager mode**: ``idx`` is included in the
    SELECT for backward compatibility with tests and direct callers.

    Args:
        conn: psycopg connection (dict_row factory)
        query: ZCatalog-style query dict (security already applied if needed)
        catalog: reference for brain.getObject() traversal (optional)
        lazy_conn: connection for deferred idx batch loading (optional)

    Returns:
        CatalogSearchResults with PGCatalogBrain instances
    """
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

    with conn.cursor() as cur:
        cur.execute(sql, qr["params"], prepare=True)
        rows = cur.fetchall()

    actual_count = None
    if has_limit and rows:
        first = rows[0]
        actual_count = first["_total_count"] if isinstance(first, dict) else first[-1]
        # Strip the window-function column from each row
        rows = [{k: v for k, v in r.items() if k != "_total_count"} for r in rows]
    elif has_limit:
        # LIMIT set but no rows — actual count is 0
        actual_count = 0

    brains = [PGCatalogBrain(row, catalog=catalog) for row in rows]
    results = CatalogSearchResults(
        brains, actual_result_count=actual_count, conn=lazy_conn
    )

    # Wire brains to result set for lazy loading
    if lazy_conn is not None:
        for brain in brains:
            brain._result_set = results

    return results
