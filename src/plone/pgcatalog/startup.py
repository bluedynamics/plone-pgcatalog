"""Startup initialization for plone.pgcatalog.

IDatabaseOpenedWithRoot subscriber that:
1. Detects the best search backend (BM25 or tsvector)
2. Registers the CatalogStateProcessor on the PGJsonbStorage
3. Syncs the IndexRegistry from each Plone site's portal_catalog
4. Creates GIN expression indexes for dynamically discovered TEXT indexes
5. Registers IPGIndexTranslator utilities for DateRecurringIndex instances
6. Registers IPGIndexTranslator utilities for DateRangeInRangeIndex instances
"""

from plone.pgcatalog.backends import detect_and_set_backend
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.columns import validate_identifier
from plone.pgcatalog.dri import DateRecurringIndexTranslator
from plone.pgcatalog.driri import DateRangeInRangeIndexTranslator
from plone.pgcatalog.interfaces import IPGIndexTranslator
from plone.pgcatalog.processor import CatalogStateProcessor
from zope.component import provideUtility

import logging
import os
import psycopg
import transaction


__all__ = ["register_catalog_processor"]


log = logging.getLogger(__name__)


def _get_main_storage(db):
    """Unwrap the main PGJsonbStorage from a ZODB.DB."""
    storage = db.storage
    # MVCC: db.storage may be the main storage or a wrapper
    main = getattr(storage, "_main", storage)
    return main


def _get_bm25_languages(db):
    """Read BM25 language configuration.

    Reads from ``PGCATALOG_BM25_LANGUAGES`` env var:
    - Comma-separated language codes (e.g. "en,de,fr,zh")
    - ``"auto"`` to detect from portal_languages at startup
    - Default: ``"en"`` (backward compatible with Phase 2)

    Returns:
        list of ISO 639-1 language codes, or None for default.
    """
    env_val = os.environ.get("PGCATALOG_BM25_LANGUAGES", "").strip()
    if not env_val:
        return None  # default to ["en"] in BM25Backend

    if env_val.lower() == "auto":
        return _detect_languages_from_db(db)

    return [lang.strip() for lang in env_val.split(",") if lang.strip()]


def _detect_languages_from_db(db):
    """Read supported languages from portal_languages in the ZODB.

    Opens a temporary connection, finds Plone sites, reads their
    supported languages.  Falls back to None (default) on failure.
    """
    try:
        conn = db.open()
        try:
            root = conn.root()
            app = root.get("Application", root)
            for obj in app.values():
                lang_tool = getattr(obj, "portal_languages", None)
                if lang_tool is not None:
                    langs = list(lang_tool.getSupportedLanguages())
                    if langs:
                        log.info(
                            "Auto-detected BM25 languages from %s: %s",
                            getattr(obj, "getId", lambda: "?")(),
                            langs,
                        )
                        return langs
        finally:
            # Abort the implicit transaction before closing -- traversal
            # may have joined the connection to a transaction, and ZODB
            # refuses to close joined connections.
            try:
                transaction.abort()
            except Exception:
                pass
            conn.close()
    except Exception:
        log.debug("Could not auto-detect BM25 languages from ZODB", exc_info=True)
    return None


def register_catalog_processor(event):
    """IDatabaseOpenedWithRoot subscriber: register the processor.

    Called once at Zope startup when the database is opened.
    Detects the best search backend (BM25 or tsvector), then registers
    the CatalogStateProcessor on the PGJsonbStorage.
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
        # Detect search backend before registering (affects schema DDL)
        dsn = getattr(storage, "_dsn", None)
        languages = _get_bm25_languages(db)
        detect_and_set_backend(dsn, languages=languages)

        processor = CatalogStateProcessor()
        storage.register_state_processor(processor)
        log.info("Registered CatalogStateProcessor on %s", storage)
        _sync_registry_from_db(db)
        _ensure_text_indexes(storage)
    else:
        log.debug("Storage %s does not support state processors", storage)


def _ensure_text_indexes(storage):
    """Create GIN expression indexes for dynamically discovered TEXT indexes.

    For each TEXT-type index with idx_key != None (not SearchableText),
    creates a GIN expression index on to_tsvector('simple', idx->>'{key}')
    if it doesn't already exist.  Uses an autocommit connection to avoid
    REPEATABLE READ lock conflicts.
    """
    registry = get_registry()
    text_indexes = [
        (name, idx_key)
        for name, (idx_type, idx_key, _) in registry.items()
        if idx_type == IndexType.TEXT and idx_key is not None
    ]
    if not text_indexes:
        return

    dsn = getattr(storage, "_dsn", None)
    if not dsn:
        return

    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for name, idx_key in text_indexes:
                validate_identifier(idx_key)
                idx_name = f"idx_os_cat_{idx_key.lower()}_tsv"
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} "
                    f"ON object_state USING gin ("
                    f"to_tsvector('simple'::regconfig, "
                    f"COALESCE(idx->>'{idx_key}', ''))) "
                    f"WHERE idx IS NOT NULL"
                )
                log.info("Ensured GIN text index %s for %s", idx_name, name)
    except Exception:
        log.warning("Failed to create text expression indexes", exc_info=True)


def _register_dri_translators(catalog):
    """Discover DateRecurringIndex instances and register IPGIndexTranslator utilities.

    Called during startup after sync_from_catalog.  Reads per-index config
    (recurdef_attr, until_attr) from the ZCatalog index objects and registers
    a DateRecurringIndexTranslator utility for each.
    """
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


def _register_driri_translators(catalog):
    """Discover DateRangeInRangeIndex instances and register IPGIndexTranslator utilities.

    Called during startup after sync_from_catalog.  Reads startindex/endindex
    config from the ZCatalog index objects and registers a
    DateRangeInRangeIndexTranslator utility for each.
    """
    try:
        indexes = catalog._catalog.indexes
    except AttributeError:
        return

    for name, index_obj in indexes.items():
        if getattr(index_obj, "meta_type", None) != "DateRangeInRangeIndex":
            continue
        startindex = getattr(index_obj, "startindex", None)
        endindex = getattr(index_obj, "endindex", None)
        if not startindex or not endindex:
            log.warning(
                "DateRangeInRangeIndex %r missing startindex/endindex config",
                name,
            )
            continue
        translator = DateRangeInRangeIndexTranslator(
            startindex=startindex,
            endindex=endindex,
        )
        provideUtility(translator, IPGIndexTranslator, name=name)
        log.info(
            "Registered DRIRI translator for index %r (start=%r, end=%r)",
            name,
            startindex,
            endindex,
        )


def _sync_registry_from_db(db):
    """Populate the IndexRegistry from portal_catalog at startup.

    Opens a temporary ZODB connection, traverses the root to find
    Plone sites with portal_catalog, and syncs the registry from
    each catalog's registered indexes and metadata.
    """
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
                    _register_driri_translators(catalog)
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
        # Abort the implicit transaction before closing -- traversal
        # may have joined the connection to a transaction, and ZODB
        # refuses to close joined connections.
        try:
            transaction.abort()
        except Exception:
            pass
        conn.close()
