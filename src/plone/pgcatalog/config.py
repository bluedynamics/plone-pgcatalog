"""Configuration and connection pool discovery for plone.pgcatalog.

The PostgreSQL connection pool is discovered from:
1. The ZODB storage's pool (if using zodb-pgjsonb)
2. Environment variable PGCATALOG_DSN (creates a fallback pool)

Also contains the CatalogStateProcessor which integrates with
zodb-pgjsonb's state processor infrastructure to write catalog
index data atomically alongside the object state.
"""

import logging
import os


log = logging.getLogger(__name__)


_fallback_pool = None


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
        "Cannot find PG connection pool. "
        "Use zodb-pgjsonb storage or set PGCATALOG_DSN."
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
        """
        from plone.pgcatalog.schema import CATALOG_COLUMNS
        from plone.pgcatalog.schema import CATALOG_FUNCTIONS
        from plone.pgcatalog.schema import CATALOG_INDEXES

        return CATALOG_COLUMNS + CATALOG_FUNCTIONS + CATALOG_INDEXES

    def process(self, zoid, class_mod, class_name, state):
        if not isinstance(state, dict) or ANNOTATION_KEY not in state:
            return None

        log.info("CatalogStateProcessor.process: zoid=%d class=%s.%s — found annotation", zoid, class_mod, class_name)
        pending = state.pop(ANNOTATION_KEY)

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


