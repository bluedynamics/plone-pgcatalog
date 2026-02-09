"""PlonePGCatalogTool — PostgreSQL-backed catalog for Plone.

Subclass of Products.CMFPlone.CatalogTool that delegates index extraction
to plone.indexer and obtains its PG connection from the zodb-pgjsonb
storage's connection pool.  Registered as ``portal_catalog`` via GenericSetup.

Index extraction uses the dynamic ``IndexRegistry`` from ``columns.py``,
which is populated at startup from each Plone site's ZCatalog indexes
(via ``sync_from_catalog()``).  Custom index types not in the registry
can be handled by ``IPGIndexTranslator`` utilities.

Module-level functions (_run_search, refresh_catalog, reindex_index,
clear_catalog_data) are testable without Plone.
"""

from AccessControl import ClassSecurityInfo
from contextlib import contextmanager
from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain
from plone.pgcatalog.columns import compute_path_info
from plone.pgcatalog.columns import convert_value
from plone.pgcatalog.columns import get_registry
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
# Module-level functions (testable without Plone)
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


def refresh_catalog(conn):
    """Re-catalog all objects that have catalog data.

    Re-reads idx/path from each cataloged row and re-applies.
    This is a lightweight refresh — it does NOT re-extract values
    from the actual Zope objects (that requires Plone integration).
    """
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

    log.info("refresh_catalog: re-indexed %d objects", count)
    return count


def reindex_index(conn, name):
    """Re-apply a specific idx key across all cataloged objects.

    Args:
        conn: psycopg connection
        name: index name (idx JSONB key) to refresh
    """
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

    log.info("reindex_index(%r): updated %d objects", name, count)
    return count


def clear_catalog_data(conn):
    """Clear all catalog data (path, idx, searchable_text).

    The base object_state rows are preserved.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE object_state SET "
            "path = NULL, parent_path = NULL, path_depth = NULL, "
            "idx = NULL, searchable_text = NULL "
            "WHERE idx IS NOT NULL"
        )
        count = cur.rowcount

    log.info("clear_catalog_data: cleared %d objects", count)
    return count


# ---------------------------------------------------------------------------
# Plone CatalogTool subclass
# ---------------------------------------------------------------------------


@implementer(IPGCatalogTool)
class PlonePGCatalogTool(CatalogTool):
    """Plone CatalogTool that uses PostgreSQL instead of BTrees.

    Inherits from Products.CMFPlone.CatalogTool.CatalogTool for full
    Plone compatibility (manage UI, plone.indexer, security).
    Overrides both read and write methods to use PG queries.

    PG connections are borrowed from the zodb-pgjsonb storage's pool.
    """

    meta_type = "PG Catalog Tool"
    security = ClassSecurityInfo()

    @contextmanager
    def _pg_connection(self):
        """Borrow a connection from the storage's connection pool."""
        from plone.pgcatalog.config import get_pool

        pool = get_pool(self)
        conn = pool.getconn()
        try:
            yield conn
        finally:
            pool.putconn(conn)

    # -- Write path (PG annotation + parent BTrees) --------------------------

    def _set_pg_annotation(self, obj, uid=None):
        """Set ``_pgcatalog_pending`` annotation on a persistent object.

        Must be called DURING the request (before ZODB serializes the
        object), not from the IndexQueue's before_commit hook.

        Returns True if the annotation was set, False otherwise.
        """
        from plone.pgcatalog.config import ANNOTATION_KEY

        if uid is None:
            try:
                uid = "/".join(obj.getPhysicalPath())
            except AttributeError:
                log.warning("_set_pg_annotation: no getPhysicalPath on %s", type(obj).__name__)
                return False

        zoid = self._obj_to_zoid(obj)
        if zoid is None:
            log.debug("_set_pg_annotation: no _p_oid on %s at %s", type(obj).__name__, uid)
            return False

        wrapper = self._wrap_object(obj)
        idx = self._extract_idx(wrapper)
        searchable_text = self._extract_searchable_text(wrapper)
        parent_path, path_depth = compute_path_info(uid)

        setattr(obj, ANNOTATION_KEY, {
            "path": uid,
            "parent_path": parent_path,
            "path_depth": path_depth,
            "idx": idx,
            "searchable_text": searchable_text,
        })
        log.debug("_set_pg_annotation: SET on %s zoid=%d path=%s", type(obj).__name__, zoid, uid)
        return True

    def indexObject(self, object):  # noqa: A002
        """Set PG annotation immediately for new objects, then delegate.

        ``CatalogAware.indexObject()`` calls this for newly-added
        content.  We set the annotation NOW because the IndexQueue
        defers the actual ``catalog_object()`` call to ``before_commit``
        which is too late (ZODB has already serialized the object).
        """
        self._set_pg_annotation(object)
        super().indexObject(object)

    def reindexObject(self, object, idxs=None, update_metadata=1, uid=None):  # noqa: A002
        """Set PG annotation immediately, then delegate to parent.

        Same timing issue as ``indexObject`` — must annotate NOW.
        """
        self._set_pg_annotation(object, uid)
        super().reindexObject(
            object, idxs=idxs or [], update_metadata=update_metadata, uid=uid,
        )

    def catalog_object(self, object, uid=None, idxs=None, update_metadata=1, pghandler=None):  # noqa: A002
        """Index an object (called from _reindexObject / _indexObject).

        Also sets PG annotation as a safety net — may be called directly
        without going through indexObject/reindexObject.
        """
        self._set_pg_annotation(object, uid)
        return super().catalog_object(object, uid, idxs, update_metadata, pghandler)

    def uncatalog_object(self, uid):
        """Remove catalog data from both PG and BTrees.

        Sets a ``None`` sentinel annotation so the state processor
        NULLs all catalog columns during the ZODB commit.
        """
        from plone.pgcatalog.config import ANNOTATION_KEY

        try:
            obj = self.unrestrictedTraverse(uid, None)
        except Exception:
            obj = None

        if obj is not None and getattr(obj, "_p_oid", None) is not None:
            setattr(obj, ANNOTATION_KEY, None)
        else:
            try:
                with self._pg_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT zoid FROM object_state WHERE path = %(path)s",
                            {"path": uid},
                        )
                        row = cur.fetchone()
                    if row:
                        _sql_uncatalog(conn, zoid=row["zoid"])
            except Exception:
                log.debug("PG uncatalog_object failed for %s", uid, exc_info=True)

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
            from Products.CMFCore.permissions import AccessInactivePortalContent

            sm = getSecurityManager()
            if sm.checkPermission(AccessInactivePortalContent, self):
                show_inactive = True

        query = apply_security_filters(
            query, roles, show_inactive=show_inactive
        )

        with self._pg_connection() as conn:
            return _run_search(conn, query, catalog=self)

    __call__ = searchResults

    def unrestrictedSearchResults(self, REQUEST=None, **kw):
        """Search without security filters."""
        if REQUEST is None:
            REQUEST = {}
        REQUEST.update(kw)

        with self._pg_connection() as conn:
            return _run_search(conn, REQUEST, catalog=self)

    # -- Maintenance ---------------------------------------------------------

    def refreshCatalog(self, clear=0):
        """Re-catalog all objects from their stored idx data."""
        with self._pg_connection() as conn:
            if clear:
                clear_catalog_data(conn)
            return refresh_catalog(conn)

    def reindexIndex(self, name, REQUEST=None):
        """Re-apply a specific idx key across all cataloged objects."""
        with self._pg_connection() as conn:
            return reindex_index(conn, name)

    def clearFindAndRebuild(self):
        """Clear all catalog data."""
        with self._pg_connection() as conn:
            return clear_catalog_data(conn)

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
        """Extract all idx values from a wrapped indexable object.

        Iterates the dynamic ``IndexRegistry`` for indexes (using
        ``source_attrs`` for attribute lookup) and metadata columns.
        Indexes with ``idx_key=None`` (special: SearchableText,
        effectiveRange, path) are skipped — they have dedicated columns.
        """
        registry = get_registry()
        idx = {}

        # Extract index values
        for name, (_idx_type, idx_key, source_attrs) in registry.items():
            if idx_key is None:
                continue  # composite/special (path, SearchableText, effectiveRange)
            if idxs and name not in idxs:
                continue  # partial reindex — skip unrequested indexes
            try:
                value = None
                for attr in source_attrs:
                    value = getattr(wrapper, attr, None)
                    if callable(value):
                        value = value()
                    if value is not None:
                        break
                idx[idx_key] = convert_value(value)
            except Exception:
                pass  # indexer raised — skip this field

        # Extract metadata-only columns (not indexes, but stored in idx JSONB)
        for meta_name in registry.metadata:
            if meta_name in idx:
                continue  # already extracted as an index
            if idxs and meta_name not in idxs:
                continue
            try:
                value = getattr(wrapper, meta_name, None)
                if callable(value):
                    value = value()
                idx[meta_name] = convert_value(value)
            except Exception:
                pass

        # IPGIndexTranslator fallback: custom extractors
        self._extract_from_translators(wrapper, idx)

        return idx

    def _extract_from_translators(self, wrapper, idx):
        """Call IPGIndexTranslator.extract() for all registered translators."""
        try:
            from plone.pgcatalog.interfaces import IPGIndexTranslator
            from zope.component import getUtilitiesFor

            for name, translator in getUtilitiesFor(IPGIndexTranslator):
                try:
                    extra = translator.extract(wrapper, name)
                    if extra and isinstance(extra, dict):
                        idx.update(extra)
                except Exception:
                    pass  # translator raised — skip
        except Exception:
            pass  # no component architecture available

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
