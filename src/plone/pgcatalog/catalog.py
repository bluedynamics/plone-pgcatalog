"""PGCatalogTool — PostgreSQL-backed catalog for Plone.

Standalone catalog that provides the ZCatalog-like API (catalog_object,
uncatalog_object, searchResults) backed by PostgreSQL queries on object_state.

When integrated with Plone (via subclassing Products.CMFPlone.CatalogTool),
this class provides the same API but uses PG instead of BTrees indexes.
"""

import logging

from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain
from plone.pgcatalog.columns import ALL_IDX_KEYS
from plone.pgcatalog.columns import convert_value
from plone.pgcatalog.indexing import catalog_object as _sql_catalog
from plone.pgcatalog.indexing import reindex_object as _sql_reindex
from plone.pgcatalog.indexing import uncatalog_object as _sql_uncatalog
from plone.pgcatalog.query import apply_security_filters
from plone.pgcatalog.query import build_query
from plone.pgcatalog.query import execute_query

log = logging.getLogger(__name__)


class PGCatalogTool:
    """PostgreSQL-backed catalog tool.

    This class provides the catalog API using direct SQL operations on the
    object_state table.  It works standalone (without Plone) — callers
    provide idx dicts directly.

    In a Plone integration (Phase 7+), this can be subclassed to inherit
    from Products.CMFPlone.CatalogTool.CatalogTool, adding plone.indexer
    wrapping and Zope acquisition support.

    Args:
        conn: psycopg connection (dict_row factory recommended)
    """

    def __init__(self, conn):
        self._conn = conn

    # -- Write path ----------------------------------------------------------

    def catalog_object(
        self, zoid, path, idx, searchable_text=None, language="simple"
    ):
        """Index an object into the catalog.

        Args:
            zoid: integer object ID (must already exist in object_state)
            path: physical path string, e.g. "/plone/folder/doc"
            idx: dict of index/metadata values for idx JSONB
            searchable_text: plain text for full-text search (optional)
            language: PostgreSQL text search config name (default "simple")
        """
        # Convert all values to JSON-safe types
        safe_idx = {k: convert_value(v) for k, v in idx.items() if k in ALL_IDX_KEYS}
        _sql_catalog(
            self._conn,
            zoid=zoid,
            path=path,
            idx=safe_idx,
            searchable_text=searchable_text,
            language=language,
        )

    def uncatalog_object(self, zoid):
        """Remove an object from the catalog.

        Clears path, idx, and searchable_text to NULL.
        The base object_state row is preserved.

        Args:
            zoid: integer object ID
        """
        _sql_uncatalog(self._conn, zoid=zoid)

    def reindex_object(
        self, zoid, idxs=None, idx_updates=None, searchable_text=None, language="simple"
    ):
        """Reindex specific fields for an object.

        Args:
            zoid: integer object ID
            idxs: list of index names to reindex (for compatibility; mapped to idx_updates)
            idx_updates: dict of idx keys to update (merged into existing idx)
            searchable_text: if provided, update the tsvector
            language: PostgreSQL text search config name
        """
        if idx_updates is None:
            idx_updates = {}

        # Convert values to JSON-safe types
        safe_updates = {k: convert_value(v) for k, v in idx_updates.items()}

        kwargs = {"conn": self._conn, "zoid": zoid, "idx_updates": safe_updates}
        if searchable_text is not None:
            kwargs["searchable_text"] = searchable_text
            kwargs["language"] = language
        _sql_reindex(**kwargs)

    # -- Read path -----------------------------------------------------------

    def searchResults(self, query=None, secured=False, roles=None, **kw):
        """Search the catalog.

        Args:
            query: ZCatalog-style query dict
            secured: if True, inject security filters (requires roles)
            roles: list of allowed roles/users for security filtering
            **kw: additional query parameters (merged into query)

        Returns:
            CatalogSearchResults with PGCatalogBrain instances
        """
        if query is None:
            query = {}
        query.update(kw)

        if secured and roles is not None:
            show_inactive = query.pop("show_inactive", False)
            query = apply_security_filters(query, roles, show_inactive=show_inactive)

        # Build and execute query
        qr = build_query(query)

        # Get total count if there's a limit (for actual_result_count)
        actual_count = None
        if qr["limit"]:
            actual_count = self._count_results(qr)

        rows = execute_query(self._conn, query)
        brains = [PGCatalogBrain(row, catalog=self) for row in rows]

        return CatalogSearchResults(brains, actual_result_count=actual_count)

    def __call__(self, query=None, **kw):
        """Alias for searchResults."""
        return self.searchResults(query, **kw)

    def unrestrictedSearchResults(self, query=None, **kw):
        """Search without security filters."""
        return self.searchResults(query, secured=False, **kw)

    # -- Maintenance ---------------------------------------------------------

    def refreshCatalog(self, conn=None):
        """Re-catalog all objects that have catalog data.

        Re-reads idx/path from each cataloged row and re-applies.
        This is a lightweight refresh — it does NOT re-extract values
        from the actual Zope objects (that requires Plone integration).

        For a full refresh from Plone objects, use clearFindAndRebuild()
        in the Plone-integrated subclass.
        """
        conn = conn or self._conn
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid, path, idx, searchable_text "
                "FROM object_state WHERE idx IS NOT NULL"
            )
            rows = cur.fetchall()

        count = 0
        for row in rows:
            if row["path"] and row["idx"]:
                _sql_catalog(
                    conn,
                    zoid=row["zoid"],
                    path=row["path"],
                    idx=row["idx"],
                )
                count += 1

        log.info("refreshCatalog: re-indexed %d objects", count)
        return count

    def reindexIndex(self, name, conn=None):
        """Re-apply a specific idx key across all cataloged objects.

        This reads the current value from idx and writes it back,
        which is useful after changing an expression index or adding
        a new one.  For re-extracting values from objects, the Plone
        integration layer is needed.

        Args:
            name: index name (idx JSONB key) to refresh
        """
        conn = conn or self._conn
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid, idx FROM object_state "
                "WHERE idx IS NOT NULL AND idx ? %(key)s",
                {"key": name},
            )
            rows = cur.fetchall()

        count = 0
        for row in rows:
            value = row["idx"].get(name)
            if value is not None:
                _sql_reindex(conn, zoid=row["zoid"], idx_updates={name: value})
                count += 1

        log.info("reindexIndex(%r): updated %d objects", name, count)
        return count

    # -- Helpers -------------------------------------------------------------

    def _count_results(self, qr):
        """Count total matching rows (ignoring LIMIT/OFFSET)."""
        sql = f"SELECT COUNT(*) FROM object_state WHERE {qr['where']}"
        with self._conn.cursor() as cur:
            cur.execute(sql, qr["params"])
            row = cur.fetchone()
            # dict_row returns {"count": N}
            return row["count"] if isinstance(row, dict) else row[0]
