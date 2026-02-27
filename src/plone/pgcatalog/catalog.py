"""PlonePGCatalogTool — PostgreSQL-backed catalog for Plone.

Standalone catalog tool (no ZCatalog inheritance) that delegates index
extraction to plone.indexer and obtains its PG connection from the
zodb-pgjsonb storage's connection pool.  Registered as ``portal_catalog``
via GenericSetup.

Index extraction uses the dynamic ``IndexRegistry`` from ``columns.py``,
which is populated at startup from each Plone site's ZCatalog indexes
(via ``sync_from_catalog()``).  Custom index types not in the registry
can be handled by ``IPGIndexTranslator`` utilities.

Module structure:
- ``extraction.py`` — index value extraction from content objects
- ``maintenance.py`` — standalone PG maintenance ops, compat shims
- ``search.py`` — core search executor (``_run_search``)
"""

from AccessControl import ClassSecurityInfo
from AccessControl.class_init import InitializeClass
from AccessControl.Permissions import manage_zcatalog_entries
from AccessControl.Permissions import manage_zcatalog_indexes
from AccessControl.Permissions import search_zcatalog
from Acquisition import aq_base
from Acquisition import aq_inner
from Acquisition import aq_parent
from App.special_dtml import DTMLFile
from BTrees.Length import Length
from contextlib import contextmanager
from OFS.Folder import Folder
from plone.base.utils import base_hasattr
from plone.base.utils import safe_callable
from plone.pgcatalog.backends import BM25Backend
from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.columns import compute_path_info
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.extraction import extract_content_type
from plone.pgcatalog.extraction import extract_from_translators
from plone.pgcatalog.extraction import extract_idx
from plone.pgcatalog.extraction import extract_searchable_text
from plone.pgcatalog.extraction import obj_to_zoid
from plone.pgcatalog.extraction import wrap_object
from plone.pgcatalog.indexing import uncatalog_object as _sql_uncatalog
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.maintenance import _CatalogCompat
from plone.pgcatalog.maintenance import _make_unsupported
from plone.pgcatalog.maintenance import _UNSUPPORTED
from plone.pgcatalog.maintenance import clear_catalog_data
from plone.pgcatalog.maintenance import reindex_index
from plone.pgcatalog.pending import _get_pending
from plone.pgcatalog.pending import _local
from plone.pgcatalog.pending import set_partial_pending
from plone.pgcatalog.pending import set_pending
from plone.pgcatalog.pgindex import PGCatalogIndexes
from plone.pgcatalog.pool import get_pool
from plone.pgcatalog.pool import get_request_connection
from plone.pgcatalog.pool import get_storage_connection
from plone.pgcatalog.query import apply_security_filters
from plone.pgcatalog.search import _PendingBrain
from plone.pgcatalog.search import _run_search
from Products.CMFCore.utils import UniqueObject
from Products.ZCatalog.interfaces import IZCatalog
from zope.interface import implementer

import logging
import warnings


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PlonePGCatalogTool
# ---------------------------------------------------------------------------


@implementer(IPGCatalogTool, IZCatalog)
class PlonePGCatalogTool(UniqueObject, Folder):
    """PostgreSQL-backed catalog tool for Plone.

    Standalone implementation (no ZCatalog inheritance) that stores all
    catalog data in PostgreSQL via the ``idx`` JSONB column.  PG connections
    are borrowed from the zodb-pgjsonb storage's connection pool.
    """

    id = "portal_catalog"
    meta_type = "PG Catalog Tool"
    security = ClassSecurityInfo()
    # Default roles match ZCatalog: search is public, management is Manager-only
    security.setPermissionDefault(manage_zcatalog_entries, ("Manager",))
    security.setPermissionDefault(manage_zcatalog_indexes, ("Manager",))
    security.setPermissionDefault(search_zcatalog, ("Anonymous", "Manager"))

    # -- Read access: Search ZCatalog ----------------------------------------
    security.declareProtected(search_zcatalog, "searchResults")
    security.declareProtected(search_zcatalog, "__call__")
    security.declareProtected(search_zcatalog, "indexes")
    security.declareProtected(search_zcatalog, "schema")
    security.declareProtected(search_zcatalog, "getpath")
    security.declareProtected(search_zcatalog, "getrid")
    security.declareProtected(search_zcatalog, "getIndexDataForRID")
    security.declareProtected(search_zcatalog, "uniqueValuesFor")
    security.declareProtected(search_zcatalog, "search")
    security.declareProtected(search_zcatalog, "all_meta_types")
    # Unsupported stubs — same permission as ZCatalog so callers get
    # NotImplementedError, not Unauthorized
    security.declareProtected(search_zcatalog, "getAllBrains")
    security.declareProtected(search_zcatalog, "searchAll")
    security.declareProtected(search_zcatalog, "getobject")
    security.declareProtected(search_zcatalog, "getMetadataForUID")
    security.declareProtected(search_zcatalog, "getMetadataForRID")
    security.declareProtected(search_zcatalog, "getIndexDataForUID")
    security.declareProtected(search_zcatalog, "index_objects")

    # -- Write access: Manage ZCatalog Entries -------------------------------
    security.declareProtected(manage_zcatalog_entries, "catalog_object")
    security.declareProtected(manage_zcatalog_entries, "uncatalog_object")
    security.declareProtected(manage_zcatalog_entries, "refreshCatalog")
    security.declareProtected(manage_zcatalog_entries, "reindexIndex")
    security.declareProtected(manage_zcatalog_entries, "clearFindAndRebuild")
    security.declareProtected(manage_zcatalog_entries, "manage_catalogClear")
    security.declareProtected(manage_zcatalog_entries, "manage_catalogReindex")
    security.declareProtected(manage_zcatalog_entries, "manage_catalogRebuild")

    # -- Index management: Manage ZCatalogIndex Entries ----------------------
    security.declareProtected(manage_zcatalog_indexes, "addIndex")
    security.declareProtected(manage_zcatalog_indexes, "delIndex")
    security.declareProtected(manage_zcatalog_indexes, "addColumn")
    security.declareProtected(manage_zcatalog_indexes, "delColumn")
    security.declareProtected(manage_zcatalog_indexes, "getIndexObjects")

    # -- ZMI pages: Manage ZCatalog Entries ----------------------------------
    security.declareProtected(manage_zcatalog_entries, "manage_catalogView")
    security.declareProtected(manage_zcatalog_entries, "manage_catalogAdvanced")
    security.declareProtected(manage_zcatalog_entries, "manage_objectInformation")
    security.declareProtected(
        manage_zcatalog_entries, "manage_catalogIndexesAndMetadata"
    )
    security.declareProtected(manage_zcatalog_entries, "manage_get_catalog_summary")
    security.declareProtected(manage_zcatalog_entries, "manage_get_catalog_objects")
    security.declareProtected(manage_zcatalog_entries, "manage_get_object_detail")
    security.declareProtected(
        manage_zcatalog_entries, "manage_get_indexes_and_metadata"
    )

    # -- Private methods (Python-only, no through-the-web access) ------------
    security.declarePrivate("unrestrictedSearchResults")
    security.declarePrivate("_unrestrictedSearchResults")
    security.declarePrivate("_listAllowedRolesAndUsers")
    security.declarePrivate("_increment_counter")
    security.declarePrivate("indexObject")
    security.declarePrivate("unindexObject")
    security.declarePrivate("reindexObject")
    security.declarePrivate("_indexObject")
    security.declarePrivate("_unindexObject")
    security.declarePrivate("_reindexObject")

    _counter = None

    manage_options = (
        {"action": "manage_catalogView", "label": "Catalog"},
        {"action": "manage_catalogAdvanced", "label": "Advanced"},
        {"action": "manage_catalogIndexesAndMetadata", "label": "Indexes & Metadata"},
    )

    # Simplified Advanced tab: only Update Catalog and Clear and Rebuild.
    # ZCatalog's subtransactions and progress logging don't apply to PG.
    manage_catalogAdvanced = DTMLFile("www/catalogAdvanced", globals())

    # PG-backed Catalog tab and object detail view.
    manage_catalogView = DTMLFile("www/catalogView", globals())
    manage_objectInformation = DTMLFile("www/catalogObjectInformation", globals())

    # Merged Indexes & Metadata tab.
    manage_catalogIndexesAndMetadata = DTMLFile(
        "www/catalogIndexesAndMetadata", globals()
    )

    # Wrap each index with PGIndex — provides PG-backed _index and uniqueValues().
    Indexes = PGCatalogIndexes()

    def __init__(self, id=None):  # noqa: A002
        if id is not None:
            self.id = id
        self._catalog = _CatalogCompat()

    # -- Methods copied from CMFPlone.CatalogTool (group-aware security) ----

    def _listAllowedRolesAndUsers(self, user):
        """Return roles + groups for security filtering (from CMFPlone)."""
        result = user.getRoles()
        if "Anonymous" in result:
            return ["Anonymous"]
        result = list(result)
        if hasattr(aq_base(user), "getGroups"):
            groups = [f"user:{x}" for x in user.getGroups()]
            if groups:
                result = result + groups
        result.insert(0, f"user:{user.getId()}")
        result.append("Anonymous")
        return result

    def _increment_counter(self):
        if self._counter is None:
            self._counter = Length()
        self._counter.change(1)

    @security.private
    def getCounter(self):
        return (self._counter is not None and self._counter()) or 0

    # -- ZCatalog-compatible API (PG-backed) --------------------------------

    def indexes(self):
        """Return list of index names (from _catalog.indexes)."""
        return list(self._catalog.indexes.keys())

    def schema(self):
        """Return list of metadata column names (from IndexRegistry)."""
        return sorted(get_registry().metadata)

    def addIndex(self, name, index_type, extra=None):
        """Add an index object to _catalog.indexes and sync IndexRegistry.

        Args:
            name: index name
            index_type: either a string (meta_type like 'FieldIndex')
                        or an index object implementing IPluggableIndex
            extra: extra configuration (passed to index constructor)
        """
        from Products.PluginIndexes.interfaces import IPluggableIndex

        if isinstance(index_type, str):
            # Resolve type string to index class via Products.meta_types
            # (same approach as ZCatalog.addIndex — query all_meta_types
            # with IPluggableIndex filter)
            products = self.all_meta_types(interfaces=(IPluggableIndex,))
            base = None
            for info in products:
                if info.get("name") == index_type:
                    base = info.get("instance")
                    break

            if base is None:
                raise ValueError(f"Unknown index type: {index_type!r}")

            # Each index type has its own constructor signature —
            # inspect to pass the right args (same as ZCatalog)
            varnames = base.__init__.__code__.co_varnames
            if "extra" in varnames:
                index_obj = base(name, extra=extra, caller=self)
            elif "caller" in varnames:
                index_obj = base(name, caller=self)
            else:
                index_obj = base(name)
        elif IPluggableIndex.providedBy(index_type):
            index_obj = index_type
        else:
            raise ValueError(f"Invalid index_type: {index_type!r}")

        self._catalog.indexes[name] = index_obj

        # Sync to IndexRegistry
        from plone.pgcatalog.columns import META_TYPE_MAP
        from plone.pgcatalog.columns import SPECIAL_INDEXES

        meta_type = getattr(index_obj, "meta_type", None)
        idx_type = META_TYPE_MAP.get(meta_type) if meta_type else None
        if idx_type is not None:
            idx_key = None if name in SPECIAL_INDEXES else name
            source_attrs = None
            if hasattr(index_obj, "getIndexSourceNames"):
                try:
                    source_attrs = list(index_obj.getIndexSourceNames())
                except Exception:
                    pass
            if not source_attrs:
                source_attrs = [name]
            get_registry().register(name, idx_type, idx_key, source_attrs)

    def delIndex(self, name):
        """Remove an index from _catalog.indexes."""
        if name in self._catalog.indexes:
            del self._catalog.indexes[name]

    def addColumn(self, name, default_value=None):
        """Register a metadata column in the IndexRegistry."""
        get_registry().add_metadata(name)
        # Persist in _catalog.schema so sync_from_catalog() finds it on restart
        self._catalog.schema[name] = len(self._catalog.schema)

    def delColumn(self, name):
        """Remove a metadata column from the IndexRegistry."""
        get_registry().metadata.discard(name)
        self._catalog.schema.pop(name, None)

    def getIndexObjects(self):
        """Return list of index objects (wrapped via PGCatalogIndexes)."""
        result = []
        for name in self._catalog.indexes:
            idx = self.Indexes._getOb(name, None)
            if idx is not None:
                result.append(idx)
        return result

    def getIndexDataForRID(self, rid):
        """Return idx JSONB dict for a record ID (ZOID)."""
        conn = self._get_pg_read_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT idx FROM object_state WHERE zoid = %(zoid)s",
                {"zoid": int(rid)},
            )
            row = cur.fetchone()
        if row is None:
            return {}
        return row["idx"] or {}

    def manage_catalogClear(self, REQUEST=None, RESPONSE=None, URL1=None):
        """Clear all catalog data (ZMI action)."""
        with self._pg_connection() as conn:
            clear_catalog_data(conn)
        if RESPONSE is not None:
            RESPONSE.redirect(
                URL1 + "/manage_catalogAdvanced?manage_tabs_message=Catalog+cleared."
            )

    def manage_catalogReindex(self, REQUEST=None, RESPONSE=None, URL1=None):
        """Re-catalog all currently indexed objects (ZMI action)."""
        self.refreshCatalog(clear=0)
        if RESPONSE is not None:
            RESPONSE.redirect(
                URL1 + "/manage_catalogAdvanced?manage_tabs_message=Catalog+updated."
            )

    def manage_catalogRebuild(self, REQUEST=None, RESPONSE=None, URL1=None):
        """Clear and rebuild all catalog data (ZMI action)."""
        self.clearFindAndRebuild()
        if RESPONSE is not None:
            RESPONSE.redirect(
                URL1 + "/manage_catalogAdvanced?manage_tabs_message=Catalog+rebuilt."
            )

    def uniqueValuesFor(self, name):
        """Return unique values for the given index name."""
        return tuple(self.Indexes._getOb(name).uniqueValues())

    # -- Deprecated proxy methods -------------------------------------------

    def search(self, *args, **kw):
        """Deprecated: use searchResults() instead."""
        warnings.warn(
            "portal_catalog.search() is deprecated, use searchResults() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.searchResults(*args, **kw)

    @contextmanager
    def _pg_connection(self):
        """Get a connection, preferring request-scoped reuse.

        If a request-scoped connection exists (created by a prior call
        in the same Zope request), reuses it without pool overhead.
        Otherwise, borrows from the pool and returns it at context exit.

        ``searchResults()`` calls ``get_request_connection()`` to
        establish the thread-local conn; subsequent ``_pg_connection()``
        calls within the same request find and reuse it.
        """
        # Check for request-scoped connection (set by searchResults)
        existing = getattr(_local, "pgcat_conn", None)
        if existing is not None and not existing.closed:
            yield existing
            return

        # Fallback: borrow from pool (tests, maintenance, no request)
        pool = get_pool(self)
        conn = pool.getconn()
        try:
            yield conn
        finally:
            pool.putconn(conn)

    def _get_pg_read_connection(self):
        """Get PG connection for catalog read queries.

        Prefers the ZODB storage instance's connection so catalog queries
        share the same REPEATABLE READ snapshot as object loads.
        Falls back to a pool connection for tests/scripts without ZODB.
        """
        conn = get_storage_connection(self)
        if conn is not None:
            return conn

        # Fallback: pool connection (tests, scripts, non-ZODB contexts)
        pool = get_pool(self)
        return get_request_connection(pool)

    # -- Write path (PG annotation only, no ZCatalog BTrees) -----------------

    def _set_pg_annotation(self, obj, uid=None):
        """Register pending catalog data for a persistent object.

        Stores catalog data in a thread-local registry (not on the
        object itself) to avoid CMFEditions version copies inheriting
        the annotation.  The CatalogStateProcessor reads from the
        same registry during tpc_vote.

        Must be called DURING the request (before ZODB serializes the
        object), not from the IndexQueue's before_commit hook.

        Returns True if data was registered, False otherwise.
        """
        if uid is None:
            try:
                uid = "/".join(obj.getPhysicalPath())
            except AttributeError:
                log.warning(
                    "_set_pg_annotation: no getPhysicalPath on %s", type(obj).__name__
                )
                return False

        zoid = self._obj_to_zoid(obj)
        if zoid is None:
            log.debug(
                "_set_pg_annotation: no _p_oid on %s at %s", type(obj).__name__, uid
            )
            return False

        wrapper = self._wrap_object(obj)
        idx = self._extract_idx(wrapper)
        searchable_text = self._extract_searchable_text(wrapper)
        content_type = extract_content_type(wrapper)
        parent_path, path_depth = compute_path_info(uid)

        # Store built-in path data in idx JSONB for unified path queries
        idx["path"] = uid
        idx["path_parent"] = parent_path
        idx["path_depth"] = path_depth

        set_pending(
            zoid,
            {
                "path": uid,
                "idx": idx,
                "searchable_text": searchable_text,
                "content_type": content_type,
            },
        )
        # Mark the object as dirty so ZODB stores it (triggering the processor)
        obj._p_changed = True
        log.debug(
            "_set_pg_annotation: SET on %s zoid=%d path=%s",
            type(obj).__name__,
            zoid,
            uid,
        )
        return True

    def _partial_reindex(self, obj, idxs):
        """Perform a lightweight partial reindex for specific indexes.

        Extracts only the requested index values and registers a JSONB
        merge update.  Does NOT set ``_p_changed`` — avoids ZODB
        serialization.

        Args:
            obj: persistent object
            idxs: list/tuple of index names to reindex

        Returns:
            True if partial reindex was registered, False otherwise
            (caller should fall through to full reindex).
        """
        zoid = self._obj_to_zoid(obj)
        if zoid is None:
            return False

        # Special indexes (SearchableText, effectiveRange, path) need
        # full reindex because they use dedicated columns, not idx JSONB.
        registry = get_registry()
        for name in idxs:
            entry = registry.get(name)
            if entry is not None and entry[1] is None:  # idx_key is None
                return False

        wrapper = self._wrap_object(obj)
        idx_updates = self._extract_idx(wrapper, idxs=idxs)

        if not idx_updates:
            return True  # Nothing to update, but partial path handled it

        set_partial_pending(zoid, idx_updates)
        return True

    def __url(self, ob):
        return "/".join(ob.getPhysicalPath())

    def indexObject(self, object):  # noqa: A002
        """Queue-aware: enqueue index request (or direct if queue disabled).

        ``CatalogAware.indexObject()`` calls this for newly-added
        content.  We set the annotation NOW because the IndexQueue
        defers the actual ``catalog_object()`` call to ``before_commit``
        which is too late (ZODB has already serialized the object).

        Does NOT delegate to ZCatalog — all catalog data flows to
        PostgreSQL via CatalogStateProcessor during tpc_vote.
        """
        self._set_pg_annotation(object)

    def unindexObject(self, object):  # noqa: A002
        """Queue-aware: enqueue unindex request."""
        url = self.__url(object)
        self.uncatalog_object(url)

    def reindexObject(self, object, idxs=None, update_metadata=1, uid=None):  # noqa: A002
        """Reindex an object, optionally only specific indexes.

        When ``idxs`` is a non-empty list, performs a lightweight partial
        reindex: extracts only the requested index values and registers
        a JSONB merge update (no ZODB serialization, no ``_p_changed``).

        When ``idxs`` is empty/None, performs a full reindex.
        """
        if idxs and self._partial_reindex(object, idxs):
            return
        self._set_pg_annotation(object, uid)

    # -- Direct (non-queued) methods called by CMFCore.indexing.PortalCatalogProcessor

    def _indexObject(self, object):  # noqa: A002
        """Direct index — called by IndexQueue processor."""
        url = self.__url(object)
        self.catalog_object(object, url)

    def _unindexObject(self, object):  # noqa: A002
        """Direct unindex — called by IndexQueue processor."""
        url = self.__url(object)
        self.uncatalog_object(url)

    def _reindexObject(self, object, idxs=None, update_metadata=1, uid=None):  # noqa: A002
        """Direct reindex — called by IndexQueue processor."""
        if uid is None:
            uid = self.__url(object)
        if idxs:
            idxs = [i for i in idxs if i in self._catalog.indexes]
        self.catalog_object(object, uid, idxs, update_metadata)

    def catalog_object(
        self,
        object,  # noqa: A002
        uid=None,
        idxs=None,
        update_metadata=1,
        pghandler=None,
    ):
        """Index an object via PG annotation.

        When ``idxs`` is a non-empty list, attempts partial reindex.
        Falls through to full reindex if partial fails (e.g. no _p_oid).
        """
        if idxs and self._partial_reindex(object, idxs):
            return
        self._set_pg_annotation(object, uid)

    def uncatalog_object(self, uid):
        """Remove catalog data from PG.

        Registers a ``None`` sentinel in the pending store so the
        state processor NULLs all catalog columns during ZODB commit.

        Does NOT delegate to ZCatalog — BTree writes are skipped.
        """
        try:
            obj = self.unrestrictedTraverse(uid, None)
        except Exception:
            obj = None

        if obj is not None and getattr(obj, "_p_oid", None) is not None:
            from ZODB.utils import u64

            set_pending(u64(obj._p_oid), None)
            obj._p_changed = True
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

    # -- Read path (PG only) ------------------------------------------------

    def searchResults(self, query=None, **kw):
        """Search using PG instead of ZCatalog BTrees."""
        # NOTE: No application-level rate limiting is applied to search queries.
        # Deploy a reverse proxy (e.g. nginx, HAProxy) with rate limiting on
        # search endpoints (@@search, @@search-results) for production use.
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

        query = apply_security_filters(query, roles, show_inactive=show_inactive)

        conn = self._get_pg_read_connection()
        return _run_search(conn, query, catalog=self, lazy_conn=conn)

    __call__ = searchResults

    def unrestrictedSearchResults(self, REQUEST=None, **kw):
        """Search without security filters."""
        if REQUEST is None:
            REQUEST = {}
        REQUEST.update(kw)

        conn = self._get_pg_read_connection()
        results = _run_search(conn, REQUEST, catalog=self, lazy_conn=conn)

        # Extend results with pending objects for path queries.
        # Needed for reindexObjectSecurity: newly created objects are
        # only in the pending store (not yet committed to PG), so a
        # normal PG query won't find them.
        path_query = REQUEST.get("path")
        if path_query is not None:
            pending_brains = self._pending_brains_for_path(
                path_query, {b.getPath() for b in results}
            )
            if pending_brains:
                all_brains = list(results) + pending_brains
                results = CatalogSearchResults(all_brains)

        return results

    def _unrestrictedSearchResults(self, REQUEST=None, **kw):
        """Like unrestrictedSearchResults, without processQueue().

        Provided for forward compatibility with Products.CMFCore PR #155
        which uses this method in reindexObjectSecurity.  Our implementation
        is identical to unrestrictedSearchResults since we have no queue.
        """
        return self.unrestrictedSearchResults(REQUEST, **kw)

    def _pending_brains_for_path(self, path_query, found_paths):
        """Return synthetic brains for pending objects matching a path query.

        Scans the thread-local pending store for objects whose path
        matches ``path_query`` and that are not already in ``found_paths``.
        Loads objects from the ZODB connection cache by OID.
        """
        from ZODB.utils import p64

        pending = _get_pending()
        if not pending:
            return []

        jar = getattr(self, "_p_jar", None)
        if jar is None:
            return []

        # Normalize path query to a prefix string
        if isinstance(path_query, str):
            prefix = path_query
        elif isinstance(path_query, dict):
            prefix = path_query.get("query", "")
        else:
            return []

        if not prefix:
            return []

        brains = []
        for zoid, data in pending.items():
            if data is None:
                continue  # uncatalog sentinel
            obj_path = data.get("path")
            if obj_path is None or obj_path in found_paths:
                continue
            # Path match: exact or starts-with (subtree)
            if obj_path != prefix and not obj_path.startswith(prefix + "/"):
                continue
            try:
                obj = jar.get(p64(zoid))
            except Exception:
                continue
            if obj is not None:
                brains.append(_PendingBrain(obj_path, obj))

        return brains

    # -- Maintenance ---------------------------------------------------------

    def refreshCatalog(self, clear=0, pghandler=None):
        """Re-catalog objects from ZODB, optionally clearing first.

        With ``clear=1``: equivalent to ``clearFindAndRebuild()``
        (traverses the entire portal tree).

        With ``clear=0``: re-catalogs only objects already known
        to the PG catalog by resolving each path from ZODB and
        re-extracting index values.

        ``pghandler`` is accepted for ZCatalog API compatibility
        but ignored.
        """
        if clear:
            return self.clearFindAndRebuild()

        # Re-index objects already in the PG catalog
        conn = self._get_pg_read_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT path FROM object_state "
                "WHERE path IS NOT NULL AND idx IS NOT NULL"
            )
            rows = cur.fetchall()

        count = 0
        for row in rows:
            path = row["path"]
            try:
                obj = self.unrestrictedTraverse(path, None)
                if obj is not None:
                    self.catalog_object(obj, path)
                    count += 1
            except Exception:
                log.warning("Failed to recatalog %s", path, exc_info=True)

        log.info("refreshCatalog: re-indexed %d objects", count)
        return count

    def reindexIndex(self, name, REQUEST=None, pghandler=None):
        """Re-apply a specific idx key across all cataloged objects.

        ``pghandler`` is accepted for ZCatalog API compatibility
        (used by ``manage_reindexIndex`` and plone.distribution)
        but ignored — PG-based reindexing is fast enough without
        progress reporting.
        """
        with self._pg_connection() as conn:
            return reindex_index(conn, name)

    def clearFindAndRebuild(self):
        """Clear all catalog data and rebuild from content objects.

        1. Clears PG catalog columns (path, idx, searchable_text, etc.)
        2. Traverses the entire portal tree and re-indexes each
           contentish object (i.e. objects with a ``reindexObject``
           method).  Non-content objects like ``acl_users`` are skipped.
        """
        with self._pg_connection() as conn:
            clear_catalog_data(conn)

        def _index_content(obj, path):
            if aq_base(obj) is aq_base(self):
                return
            if base_hasattr(obj, "reindexObject") and safe_callable(obj.reindexObject):
                uid = "/".join(obj.getPhysicalPath())
                self.catalog_object(obj, uid)

        portal = aq_parent(aq_inner(self))
        _index_content(portal, "")
        portal.ZopeFindAndApply(
            portal,
            search_sub=True,
            apply_func=_index_content,
        )

    # -- ZCatalog internal API (PG-backed) ----------------------------------

    def getpath(self, rid):
        """Return the path for a record ID (ZOID).

        Replaces ZCatalog's ``_catalog.paths[rid]`` BTree lookup.
        Used by ``plone.app.uuid.uuidToPhysicalPath()`` and others.
        Raises ``KeyError`` if the rid is not found (matching ZCatalog).
        """
        conn = self._get_pg_read_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT path FROM object_state WHERE zoid = %(zoid)s",
                {"zoid": int(rid)},
            )
            row = cur.fetchone()
        if row is None or row["path"] is None:
            raise KeyError(rid)
        return row["path"]

    def getrid(self, path, default=None):
        """Return the record ID (ZOID) for a path.

        Replaces ZCatalog's ``_catalog.uids.get(path)`` BTree lookup.
        Used by ``plone.app.vocabularies`` for content validation.
        Returns ``default`` if the path is not found.
        """
        conn = self._get_pg_read_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state WHERE path = %(path)s",
                {"path": path},
            )
            row = cur.fetchone()
        return row["zoid"] if row else default

    # -- ZMI helpers ---------------------------------------------------------

    _ZMI_PAGE_SIZE = 20

    def manage_get_catalog_summary(self):
        """Return summary dict for the Catalog tab header."""
        conn = self._get_pg_read_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM object_state WHERE idx IS NOT NULL"
            )
            row = cur.fetchone()
        object_count = row["cnt"] if row else 0

        registry = get_registry()
        backend = get_backend()
        has_bm25 = isinstance(backend, BM25Backend)
        return {
            "object_count": object_count,
            "index_count": len(registry),
            "metadata_count": len(registry.metadata),
            "backend_name": "BM25" if has_bm25 else "Tsvector",
            "has_bm25": has_bm25,
            "bm25_languages": list(getattr(backend, "languages", [])),
        }

    def manage_get_catalog_objects(self, batch_start=0, filterpath=""):
        """Return paginated list of cataloged objects for the Catalog tab."""
        batch_start = int(batch_start)
        conn = self._get_pg_read_connection()

        params = {}
        where = "idx IS NOT NULL"
        if filterpath:
            where += " AND path LIKE %(prefix)s"
            # Escape LIKE wildcards in user input, then add trailing %
            safe = (
                filterpath.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            )
            params["prefix"] = safe + "%"

        sql = (
            "SELECT zoid, path, idx->>'portal_type' AS portal_type, "
            "COUNT(*) OVER() AS _total "
            f"FROM object_state WHERE {where} "
            "ORDER BY path "
            f"LIMIT {self._ZMI_PAGE_SIZE} OFFSET %(offset)s"
        )
        params["offset"] = batch_start

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        total = rows[0]["_total"] if rows else 0
        objects = [
            {
                "zoid": r["zoid"],
                "path": r["path"],
                "portal_type": r["portal_type"] or "",
            }
            for r in rows
        ]
        return {"objects": objects, "total": total, "batch_start": batch_start}

    def manage_get_object_detail(self, zoid):
        """Return detail dict for a single cataloged object.

        Returns idx_items as a pre-sorted list of {"key", "value"} dicts
        so the DTML template doesn't need isinstance/sorted (restricted).
        """
        zoid = int(zoid)
        conn = self._get_pg_read_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT path, idx, "
                "searchable_text IS NOT NULL AS has_searchable_text, "
                "left(searchable_text::text, 200) AS searchable_text_preview "
                "FROM object_state WHERE zoid = %(zoid)s",
                {"zoid": zoid},
            )
            row = cur.fetchone()
        if row is None:
            return None
        idx = row["idx"] or {}
        idx_items = []
        for k in sorted(idx):
            v = idx[k]
            if isinstance(v, list):
                display = ", ".join(str(i) for i in v)
            elif v is None:
                display = ""
            elif isinstance(v, bool):
                display = "True" if v else "False"
            else:
                display = str(v)
            idx_items.append({"key": k, "value": display, "is_none": v is None})
        return {
            "path": row["path"],
            "idx_items": idx_items,
            "has_searchable_text": row["has_searchable_text"],
            "searchable_text_preview": row["searchable_text_preview"] or "",
        }

    def manage_get_indexes_and_metadata(self):
        """Return indexes and metadata from the IndexRegistry for ZMI display.

        Returns a dict with:
        - indexes: sorted list of dicts with name, index_type, storage info
        - metadata: sorted list of metadata column names
        - index_count / metadata_count: totals
        """
        registry = get_registry()
        indexes = []
        for name, (idx_type, idx_key, source_attrs) in sorted(registry.items()):
            indexes.append(
                {
                    "name": name,
                    "index_type": idx_type.value,
                    "idx_key": idx_key or "",
                    "is_special": idx_key is None,
                    "storage": "dedicated column" if idx_key is None else "idx JSONB",
                    "source_attrs": ", ".join(source_attrs),
                }
            )
        metadata = sorted(registry.metadata)
        return {
            "indexes": indexes,
            "metadata": metadata,
            "index_count": len(indexes),
            "metadata_count": len(metadata),
        }

    # -- Helpers (delegated to extraction.py) --------------------------------

    def _wrap_object(self, obj):
        """Wrap an object with IIndexableObject for plone.indexer."""
        return wrap_object(obj, self)

    @staticmethod
    def _obj_to_zoid(obj):
        """Extract the integer zoid from a persistent object's _p_oid."""
        return obj_to_zoid(obj)

    def _extract_idx(self, wrapper, idxs=None):
        """Extract all idx values from a wrapped indexable object."""
        return extract_idx(wrapper, idxs=idxs)

    def _extract_from_translators(self, wrapper, idx, idxs=None):
        """Call IPGIndexTranslator.extract() for all registered translators."""
        return extract_from_translators(wrapper, idx, idxs=idxs)

    @staticmethod
    def _extract_searchable_text(wrapper):
        """Extract SearchableText from a wrapped indexable object."""
        return extract_searchable_text(wrapper)


# Attach unsupported method stubs (security already declared in class body)
for _name, _msg in _UNSUPPORTED.items():
    setattr(PlonePGCatalogTool, _name, _make_unsupported(_name, _msg))

InitializeClass(PlonePGCatalogTool)
