"""PGCatalogTool — PostgreSQL-backed catalog for Plone.

Two classes:

- ``PGCatalogTool``: standalone, testable without Plone.  Callers
  provide idx dicts and a psycopg connection directly.

- ``PlonePGCatalogTool``: subclass of Products.CMFPlone.CatalogTool that
  delegates index extraction to plone.indexer and obtains its PG
  connection from the zodb-pgjsonb storage.  Registered as
  ``portal_catalog`` via GenericSetup.  Only available when Plone is
  installed.
"""

from AccessControl import ClassSecurityInfo
from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain
from plone.pgcatalog.columns import ALL_IDX_KEYS
from plone.pgcatalog.columns import convert_value
from plone.pgcatalog.columns import KNOWN_INDEXES
from plone.pgcatalog.indexing import catalog_object as _sql_catalog
from plone.pgcatalog.indexing import reindex_object as _sql_reindex
from plone.pgcatalog.indexing import uncatalog_object as _sql_uncatalog
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.query import apply_security_filters
from plone.pgcatalog.query import build_query
from Products.CMFPlone.CatalogTool import CatalogTool
from zope.interface import implementer

import logging


log = logging.getLogger(__name__)

# Fixed set of columns for catalog queries (never user-supplied)
_SELECT_COLS = "zoid, path, idx, state"
_SELECT_COLS_COUNTED = "zoid, path, idx, state, COUNT(*) OVER() AS _total_count"


# ---------------------------------------------------------------------------
# Shared search execution (DRY)
# ---------------------------------------------------------------------------


def _run_search(conn, query, catalog=None):
    """Execute a prepared query dict and return CatalogSearchResults.

    Builds the SQL once and uses a ``COUNT(*) OVER()`` window function
    when a LIMIT is present so only *one* query is executed.

    Args:
        conn: psycopg connection (dict_row factory)
        query: ZCatalog-style query dict (security already applied if needed)
        catalog: reference for brain.getObject() traversal (optional)

    Returns:
        CatalogSearchResults with PGCatalogBrain instances
    """
    qr = build_query(query)

    has_limit = qr["limit"] is not None
    cols = _SELECT_COLS_COUNTED if has_limit else _SELECT_COLS

    sql = f"SELECT {cols} FROM object_state WHERE {qr['where']}"
    if qr["order_by"]:
        sql += f" ORDER BY {qr['order_by']}"
    if qr["limit"]:
        sql += f" LIMIT {qr['limit']}"
    if qr["offset"]:
        sql += f" OFFSET {qr['offset']}"

    with conn.cursor() as cur:
        cur.execute(sql, qr["params"])
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
    return CatalogSearchResults(brains, actual_result_count=actual_count)


class PGCatalogTool:
    """PostgreSQL-backed catalog tool.

    This class provides the catalog API using direct SQL operations on the
    object_state table.  It works standalone (without Plone) — callers
    provide idx dicts directly.

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
            idxs: list of index names to reindex (for compatibility)
            idx_updates: dict of idx keys to update (merged into existing idx)
            searchable_text: if provided, update the tsvector
            language: PostgreSQL text search config name
        """
        if idx_updates is None:
            idx_updates = {}

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

        return _run_search(self._conn, query, catalog=self)

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

    def clearFindAndRebuild(self, conn=None):
        """Clear all catalog data and rebuild from scratch.

        Clears all idx/path/searchable_text columns to NULL.  In standalone
        mode this is all we can do — the Plone subclass overrides this to
        re-walk the content tree.
        """
        conn = conn or self._conn
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE object_state SET "
                "path = NULL, parent_path = NULL, path_depth = NULL, "
                "idx = NULL, searchable_text = NULL "
                "WHERE idx IS NOT NULL"
            )
            count = cur.rowcount

        log.info("clearFindAndRebuild: cleared %d objects", count)
        return count


# ---------------------------------------------------------------------------
# Plone-aware subclass
# ---------------------------------------------------------------------------


@implementer(IPGCatalogTool)
class PlonePGCatalogTool(CatalogTool):
    """Plone CatalogTool that uses PostgreSQL instead of BTrees.

    Inherits from Products.CMFPlone.CatalogTool.CatalogTool for full
    Plone compatibility (manage UI, plone.indexer, security).
    Overrides both read and write methods to use PG queries.

    The PG connection is obtained from the zodb-pgjsonb storage.
    """

    meta_type = "PG Catalog Tool"
    security = ClassSecurityInfo()

    def _get_pg_conn(self):
        """Get a psycopg connection from the ZODB storage."""
        from plone.pgcatalog.config import get_dsn

        dsn = get_dsn(self)
        if dsn is None:
            raise RuntimeError(
                "Cannot determine PG DSN. "
                "Set PGCATALOG_DSN or use zodb-pgjsonb storage."
            )
        from psycopg.rows import dict_row

        import psycopg

        return psycopg.connect(dsn, row_factory=dict_row)

    # -- Write path (PG + parent BTrees for compatibility) -------------------

    def catalog_object(self, object, uid=None, idxs=None, update_metadata=1, pghandler=None):  # noqa: A002
        """Index an object into both PG and BTrees.

        Extracts index values via plone.indexer, writes them to the
        PG idx JSONB column, and also calls the parent CatalogTool
        for BTrees compatibility.
        """
        if uid is None:
            try:
                uid = "/".join(object.getPhysicalPath())
            except AttributeError:
                log.warning("catalog_object: obj has no getPhysicalPath, skipping PG")
                return super().catalog_object(object, uid, idxs, update_metadata, pghandler)

        # Extract ZODB oid -> integer zoid
        zoid = self._obj_to_zoid(object)
        if zoid is not None:
            wrapper = self._wrap_object(object)
            idx = self._extract_idx(wrapper, idxs)
            searchable_text = self._extract_searchable_text(wrapper)

            conn = self._get_pg_conn()
            try:
                _sql_catalog(
                    conn,
                    zoid=zoid,
                    path=uid,
                    idx=idx,
                    searchable_text=searchable_text,
                )
                conn.commit()
            except Exception:
                log.debug("PG catalog_object failed for %s", uid, exc_info=True)
            finally:
                conn.close()

        # Always call parent for BTrees compatibility
        return super().catalog_object(object, uid, idxs, update_metadata, pghandler)

    def uncatalog_object(self, uid):
        """Remove an object from both PG and BTrees."""
        conn = self._get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT zoid FROM object_state WHERE path = %(path)s",
                    {"path": uid},
                )
                row = cur.fetchone()
            if row:
                _sql_uncatalog(conn, zoid=row["zoid"])
                conn.commit()
        except Exception:
            log.debug("PG uncatalog_object failed for %s", uid, exc_info=True)
        finally:
            conn.close()

        return super().uncatalog_object(uid)

    # -- Read path (PG only) ------------------------------------------------

    def searchResults(self, query=None, **kw):
        """Search using PG instead of ZCatalog BTrees."""
        from AccessControl import getSecurityManager

        if query is None:
            query = {}
        query.update(kw)

        # Security: inject allowedRolesAndUsers
        user = getSecurityManager().getUser()
        roles = self._listAllowedRolesAndUsers(user)
        show_inactive = query.pop("show_inactive", False)

        # Check permission for inactive content
        if not show_inactive:
            from AccessControl.SecurityManagement import _checkPermission
            from Products.CMFCore.permissions import AccessInactivePortalContent

            if _checkPermission(AccessInactivePortalContent, self):
                show_inactive = True

        query = apply_security_filters(
            query, roles, show_inactive=show_inactive
        )

        conn = self._get_pg_conn()
        try:
            return _run_search(conn, query, catalog=self)
        finally:
            conn.close()

    __call__ = searchResults

    def unrestrictedSearchResults(self, REQUEST=None, **kw):
        """Search without security filters."""
        if REQUEST is None:
            REQUEST = {}
        REQUEST.update(kw)

        conn = self._get_pg_conn()
        try:
            return _run_search(conn, REQUEST, catalog=self)
        finally:
            conn.close()

    # -- Helpers -------------------------------------------------------------

    def _wrap_object(self, obj):
        """Wrap an object with IIndexableObject for plone.indexer."""
        from plone.indexer.interfaces import IIndexableObject
        from zope.component import queryMultiAdapter

        wrapper = queryMultiAdapter((obj, self), IIndexableObject)
        return wrapper if wrapper is not None else obj

    @staticmethod
    def _obj_to_zoid(obj):
        """Extract the integer zoid from a persistent object's _p_oid."""
        oid = getattr(obj, "_p_oid", None)
        if oid is None:
            return None
        return int.from_bytes(oid, "big")

    def _extract_idx(self, wrapper, idxs=None):
        """Extract all idx values from a wrapped indexable object."""
        idx = {}
        for name, (_idx_type, idx_key) in KNOWN_INDEXES.items():
            if idx_key is None:
                continue  # composite/special (path, SearchableText, effectiveRange)
            if idxs and name not in idxs:
                continue  # partial reindex - skip unrequested indexes
            try:
                value = getattr(wrapper, name, None)
                if callable(value):
                    value = value()
                idx[idx_key] = convert_value(value)
            except Exception:
                pass  # indexer raised - skip this field
        return idx

    @staticmethod
    def _extract_searchable_text(wrapper):
        """Extract SearchableText from a wrapped indexable object."""
        try:
            value = getattr(wrapper, "SearchableText", None)
            if callable(value):
                value = value()
            return value if isinstance(value, str) else None
        except Exception:
            return None
