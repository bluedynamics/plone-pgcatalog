"""Configuration and connection pool discovery for plone.pgcatalog.

The PostgreSQL connection pool is discovered from:
1. The ZODB storage's pool (if using zodb-pgjsonb)
2. Environment variable PGCATALOG_DSN (creates a fallback pool)

Also contains the CatalogStateProcessor which integrates with
zodb-pgjsonb's state processor infrastructure to write catalog
index data atomically alongside the object state.
"""

from transaction.interfaces import IDataManagerSavepoint
from transaction.interfaces import ISavepointDataManager
from zope.interface import implementer

import logging
import os
import threading
import transaction


log = logging.getLogger(__name__)


def _install_orjson_loader():
    """Register orjson as psycopg's JSONB deserializer if available."""
    try:
        from psycopg.types.json import set_json_loads

        import orjson

        set_json_loads(orjson.loads)
    except ImportError:
        pass


_install_orjson_loader()


_fallback_pool = None

# Thread-local store for pending catalog data.
# Keyed by zoid (int) → dict (catalog data) or None (uncatalog sentinel).
# Using thread-local avoids CMFEditions version-copy duplication:
# the annotation is NOT stored on the object's __dict__ (which gets
# cloned), but in this thread-local registry that only the original
# zoid is registered in.
_local = threading.local()


def _get_pending():
    """Return the thread-local pending catalog data dict."""
    try:
        return _local.pending
    except AttributeError:
        _local.pending = {}
        return _local.pending


def set_pending(zoid, data):
    """Register pending catalog data for a zoid.

    Args:
        zoid: ZODB OID as int
        data: dict with catalog columns, or None for uncatalog sentinel
    """
    _get_pending()[zoid] = data
    _ensure_joined()


def pop_pending(zoid):
    """Pop pending catalog data for a zoid, or return sentinel if absent.

    Returns:
        dict (catalog data), None (uncatalog), or _MISSING (no data).
    """
    return _get_pending().pop(zoid, _MISSING)


_MISSING = object()  # Sentinel for "no pending data"


@implementer(IDataManagerSavepoint)
class PendingSavepoint:
    """Snapshot of pending catalog data for savepoint rollback."""

    def __init__(self, snapshot):
        self._snapshot = snapshot

    def rollback(self):
        pending = _get_pending()
        pending.clear()
        pending.update(self._snapshot)


@implementer(ISavepointDataManager)
class PendingDataManager:
    """Participates in ZODB transaction to make pending data savepoint-aware.

    Joins lazily on first ``set_pending()`` call.  Clears pending on
    abort / tpc_finish / tpc_abort.
    """

    transaction_manager = None

    def __init__(self, txn):
        self._txn = txn
        self._joined = True

    def savepoint(self):
        return PendingSavepoint(dict(_get_pending()))

    def abort(self, transaction):
        _get_pending().clear()
        self._joined = False  # AbortSavepoint may have unjoined us

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_vote(self, transaction):
        pass

    def tpc_finish(self, transaction):
        _get_pending().clear()

    def tpc_abort(self, transaction):
        _get_pending().clear()

    def sortKey(self):
        return "~plone.pgcatalog.pending"


def _ensure_joined():
    """Ensure a PendingDataManager is joined to the current transaction."""
    txn = transaction.get()
    try:
        dm = _local._pending_dm
        if dm._txn is txn and dm._joined:
            return
    except AttributeError:
        pass
    dm = PendingDataManager(txn)
    _local._pending_dm = dm
    txn.join(dm)


def get_request_connection(pool):
    """Get or create a request-scoped connection from the pool.

    Reuses the same connection for the duration of a Zope request,
    avoiding pool lock overhead for pages with multiple catalog queries.
    The connection is returned to the pool by ``release_request_connection()``,
    which is called by the IPubEnd subscriber at request end.

    Falls back to normal pool getconn/putconn when no request-scoped
    connection is active (e.g. in tests or background tasks).
    """
    conn = getattr(_local, "pgcat_conn", None)
    if conn is not None and not conn.closed:
        return conn
    conn = pool.getconn()
    _local.pgcat_conn = conn
    _local.pgcat_pool = pool
    return conn


def release_request_connection(event=None):
    """Return the request-scoped connection to the pool.

    Called by the IPubEnd subscriber at the end of each Zope request.
    Safe to call when no request-scoped connection is active (no-op).
    """
    conn = getattr(_local, "pgcat_conn", None)
    pool = getattr(_local, "pgcat_pool", None)
    if conn is not None and pool is not None:
        try:
            pool.putconn(conn)
        except Exception:
            log.warning("Failed to return connection to pool", exc_info=True)
    _local.pgcat_conn = None
    _local.pgcat_pool = None


def get_pool(context=None):
    """Discover the PostgreSQL connection pool.

    Args:
        context: persistent object with _p_jar (e.g. Plone site or tool)

    Returns:
        psycopg_pool.ConnectionPool

    Raises:
        RuntimeError: if no pool can be found
    """
    # 1. From ZODB storage (zodb-pgjsonb)
    if context is not None:
        pool = _pool_from_storage(context)
        if pool is not None:
            return pool

    # 2. Fallback: create pool from env var
    pool = _pool_from_env()
    if pool is not None:
        return pool

    raise RuntimeError(
        "Cannot find PG connection pool. Use zodb-pgjsonb storage or set PGCATALOG_DSN."
    )


def _pool_from_storage(context):
    """Extract connection pool from the ZODB storage backend."""
    try:
        storage = context._p_jar.db().storage
        return getattr(storage, "_instance_pool", None)
    except (AttributeError, TypeError):
        return None


def _pool_from_env():
    """Lazy-create a fallback pool from PGCATALOG_DSN env var."""
    global _fallback_pool
    if _fallback_pool is not None:
        return _fallback_pool

    dsn = os.environ.get("PGCATALOG_DSN")
    if not dsn:
        return None

    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    _fallback_pool = ConnectionPool(
        dsn,
        min_size=1,
        max_size=4,
        kwargs={"row_factory": dict_row},
        open=True,
    )
    return _fallback_pool


# Keep get_dsn for setuphandlers.py (DDL needs its own connection, not pool)
def get_dsn(context=None):
    """Discover the PostgreSQL DSN string.

    Args:
        context: persistent object with _p_jar

    Returns:
        DSN string or None
    """
    dsn = os.environ.get("PGCATALOG_DSN")
    if dsn:
        return dsn

    if context is not None:
        try:
            storage = context._p_jar.db().storage
            if hasattr(storage, "_dsn"):
                return storage._dsn
        except (AttributeError, TypeError):
            pass

    return None


# ── State Processor for zodb-pgjsonb ────────────────────────────────

# Annotation key set by catalog_object() on persistent objects.
# The processor pops it from the JSON state before writing to PG.
ANNOTATION_KEY = "_pgcatalog_pending"


class CatalogStateProcessor:
    """Extracts ``_pgcatalog_pending`` from object state → extra PG columns.

    Works with zodb-pgjsonb's state processor infrastructure.
    When ``_pgcatalog_pending`` is a dict, catalog data is written.
    When it is ``None`` (sentinel), all catalog columns are NULLed (uncatalog).
    """

    def get_extra_columns(self):
        from zodb_pgjsonb import ExtraColumn

        return [
            ExtraColumn("path", "%(path)s"),
            ExtraColumn("idx", "%(idx)s"),
            ExtraColumn(
                "searchable_text",
                "to_tsvector('simple'::regconfig, %(searchable_text)s)",
            ),
        ]

    def get_schema_sql(self):
        """Return DDL for catalog columns, functions, and indexes.

        Applied by PGJsonbStorage.register_state_processor() using
        the storage's own connection — no REPEATABLE READ lock conflicts.
        Includes rrule_plpgsql functions for DateRecurringIndex support.
        """
        from plone.pgcatalog.schema import CATALOG_COLUMNS
        from plone.pgcatalog.schema import CATALOG_FUNCTIONS
        from plone.pgcatalog.schema import CATALOG_INDEXES
        from plone.pgcatalog.schema import RRULE_FUNCTIONS

        return CATALOG_COLUMNS + CATALOG_FUNCTIONS + CATALOG_INDEXES + RRULE_FUNCTIONS

    def process(self, zoid, class_mod, class_name, state):
        # Look up pending data from the thread-local store (set by
        # catalog_object / uncatalog_object via set_pending).
        pending = pop_pending(zoid)
        if pending is _MISSING:
            # Also check state dict for backward compat / direct use
            if isinstance(state, dict) and ANNOTATION_KEY in state:
                pending = state.pop(ANNOTATION_KEY)
            else:
                return None

        log.debug(
            "CatalogStateProcessor.process: zoid=%d class=%s.%s",
            zoid,
            class_mod,
            class_name,
        )

        if pending is None:
            # Uncatalog sentinel: NULL all catalog columns
            return {
                "path": None,
                "idx": None,
                "searchable_text": None,
            }

        # Normal catalog: return column values
        from psycopg.types.json import Json

        idx = pending.get("idx")
        return {
            "path": pending.get("path"),
            "idx": Json(idx) if idx else None,
            "searchable_text": pending.get("searchable_text"),
        }


def _get_main_storage(db):
    """Unwrap the main PGJsonbStorage from a ZODB.DB."""
    storage = db.storage
    # MVCC: db.storage may be the main storage or a wrapper
    main = getattr(storage, "_main", storage)
    return main


def register_catalog_processor(event):
    """IDatabaseOpenedWithRoot subscriber: register the processor.

    Called once at Zope startup when the database is opened.
    Registers the CatalogStateProcessor on the PGJsonbStorage.
    The processor's ``get_schema_sql()`` provides DDL which is applied
    by the storage using its own connection (no REPEATABLE READ lock
    conflicts).

    Finally, syncs the IndexRegistry from each Plone site's
    portal_catalog so dynamic indexes are available before
    the first request.
    """
    db = event.database
    storage = _get_main_storage(db)
    if hasattr(storage, "register_state_processor"):
        processor = CatalogStateProcessor()
        storage.register_state_processor(processor)
        log.info("Registered CatalogStateProcessor on %s", storage)
        _sync_registry_from_db(db)
    else:
        log.debug("Storage %s does not support state processors", storage)


def _register_dri_translators(catalog):
    """Discover DateRecurringIndex instances and register IPGIndexTranslator utilities.

    Called during startup after sync_from_catalog.  Reads per-index config
    (recurdef_attr, until_attr) from the ZCatalog index objects and registers
    a DateRecurringIndexTranslator utility for each.
    """
    try:
        from plone.pgcatalog.dri import DateRecurringIndexTranslator
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import provideUtility
    except ImportError:
        return  # missing dependencies — skip

    try:
        indexes = catalog._catalog.indexes
    except AttributeError:
        return

    for name, index_obj in indexes.items():
        if getattr(index_obj, "meta_type", None) != "DateRecurringIndex":
            continue
        translator = DateRecurringIndexTranslator(
            date_attr=name,
            recurdef_attr=getattr(index_obj, "attr_recurdef", ""),
            until_attr=getattr(index_obj, "attr_until", ""),
        )
        provideUtility(translator, IPGIndexTranslator, name=name)
        log.info(
            "Registered DRI translator for index %r (recurdef=%r)",
            name,
            translator.recurdef_attr,
        )


def _sync_registry_from_db(db):
    """Populate the IndexRegistry from portal_catalog at startup.

    Opens a temporary ZODB connection, traverses the root to find
    Plone sites with portal_catalog, and syncs the registry from
    each catalog's registered indexes and metadata.
    """
    from plone.pgcatalog.columns import get_registry

    registry = get_registry()
    conn = db.open()
    try:
        root = conn.root()
        app = root.get("Application", root)
        for obj in app.values():
            catalog = getattr(obj, "portal_catalog", None)
            if catalog is not None and hasattr(catalog, "_catalog"):
                try:
                    registry.sync_from_catalog(catalog)
                    _register_dri_translators(catalog)
                    log.info(
                        "IndexRegistry synced from %s/portal_catalog (%d indexes, %d metadata)",
                        getattr(obj, "getId", lambda: "?")(),
                        len(registry),
                        len(registry.metadata),
                    )
                except Exception:
                    log.warning(
                        "Failed to sync IndexRegistry from portal_catalog",
                        exc_info=True,
                    )
    except Exception:
        log.debug("Could not sync IndexRegistry from ZODB", exc_info=True)
    finally:
        conn.close()
