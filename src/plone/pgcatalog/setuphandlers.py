"""GenericSetup install handler for plone.pgcatalog.

Replaces ``portal_catalog`` with ``PlonePGCatalogTool`` via a
snapshot-then-replace strategy:

1. **Snapshot** existing index definitions and metadata columns
2. **Replace** ``portal_catalog`` with a fresh ``PlonePGCatalogTool``
3. **Ensure** core Plone indexes via profile re-import (creates lexicons)
4. **Restore** addon indexes from the snapshot (skips already-existing ones)
5. **Remove** orphaned ZCTextIndex lexicons

This preserves addon-provided indexes that would otherwise be lost when
the old catalog object is replaced.

DDL (ALTER TABLE for catalog columns, functions, indexes) is applied
by ``PGJsonbStorage.register_state_processor()`` using the storage's
own connection — no REPEATABLE READ lock conflicts.
"""

import logging


log = logging.getLogger(__name__)


class _Extra:
    """Simple namespace for index constructor extra parameters."""

    pass


def install(context):
    """GenericSetup import step: replace portal_catalog and restore indexes.

    Handles three cases:
    - Already PlonePGCatalogTool: just ensure core indexes
    - Foreign catalog class: snapshot → replace → ensure → restore
    - No catalog: replace → ensure core indexes
    """
    if context.readDataFile("install_pgcatalog.txt") is None:
        return

    site = context.getSite()
    catalog = getattr(site, "portal_catalog", None)

    # Already our class — just ensure core indexes exist
    if catalog is not None:
        from plone.pgcatalog.catalog import PlonePGCatalogTool

        if isinstance(catalog, PlonePGCatalogTool):
            _ensure_catalog_indexes(site)
            _remove_lexicons(site)
            return

    # Snapshot existing index definitions + metadata columns
    snapshot = _snapshot_catalog(catalog) if catalog is not None else None

    # Replace with PlonePGCatalogTool
    _replace_catalog(site)

    # Restore core indexes (from profiles — also creates lexicons needed
    # by ZCTextIndex constructors)
    _ensure_catalog_indexes(site)

    # Restore addon indexes from snapshot (skip already-existing ones)
    if snapshot:
        _restore_from_snapshot(site, snapshot)

    # Clean up lexicons (unused — pgcatalog uses PG tsvector)
    _remove_lexicons(site)


def _snapshot_catalog(catalog):
    """Snapshot index definitions and metadata columns before replacement.

    Returns a dict with:
    - ``indexes``: {name: {meta_type, source_attrs, ...extra_attrs}}
    - ``metadata``: [column_name, ...]
    """
    snapshot = {"indexes": {}, "metadata": []}

    # Indexes
    try:
        for name, index_obj in catalog._catalog.indexes.items():
            entry = {
                "meta_type": getattr(index_obj, "meta_type", None),
            }
            # Source attributes
            if hasattr(index_obj, "getIndexSourceNames"):
                try:
                    entry["source_attrs"] = list(index_obj.getIndexSourceNames())
                except Exception:
                    entry["source_attrs"] = [name]
            else:
                entry["source_attrs"] = [name]

            # Type-specific extra attributes
            # DateRangeIndex
            if hasattr(index_obj, "getSinceField"):
                entry["since_field"] = index_obj.getSinceField()
                entry["until_field"] = index_obj.getUntilField()
            # ZCTextIndex
            if hasattr(index_obj, "lexicon_id"):
                entry["lexicon_id"] = index_obj.lexicon_id
            if hasattr(index_obj, "index_type"):
                entry["index_type"] = index_obj.index_type
            # DateRecurringIndex (plone.app.event)
            for attr in ("attr_recurdef", "attr_until"):
                val = getattr(index_obj, attr, None)
                if val is not None:
                    entry[attr] = val
            # DateRangeInRangeIndex (plone.app.event)
            for attr in ("startindex", "endindex"):
                val = getattr(index_obj, attr, None)
                if val is not None:
                    entry[attr] = val

            snapshot["indexes"][name] = entry
    except AttributeError:
        pass

    # Metadata columns
    try:
        snapshot["metadata"] = list(catalog._catalog.schema.keys())
    except AttributeError:
        pass

    return snapshot


def _replace_catalog(site):
    """Replace portal_catalog with a fresh PlonePGCatalogTool."""
    from plone.pgcatalog.catalog import PlonePGCatalogTool

    if "portal_catalog" in site.objectIds():
        site._delObject("portal_catalog")

    new_catalog = PlonePGCatalogTool()
    site._setObject("portal_catalog", new_catalog)
    log.info("Replaced portal_catalog with PlonePGCatalogTool")


def _restore_from_snapshot(site, snapshot):
    """Restore addon index definitions and metadata columns from snapshot.

    Skips indexes and metadata that already exist in the fresh catalog
    (e.g. those created by ``_ensure_catalog_indexes``).
    """
    catalog = site.portal_catalog

    # Get currently existing indexes (from _ensure_catalog_indexes)
    try:
        existing_indexes = set(catalog.indexes())
    except Exception:
        existing_indexes = set()

    try:
        existing_metadata = set(catalog._catalog.schema.keys())
    except Exception:
        existing_metadata = set()

    # Restore indexes not already present
    for name, entry in snapshot["indexes"].items():
        if name in existing_indexes:
            continue  # already restored by core profiles
        meta_type = entry.get("meta_type")
        if meta_type is None:
            continue

        extra = _build_extra(entry)
        try:
            catalog.addIndex(name, meta_type, extra)
            log.info("Restored index %r (%s) from snapshot", name, meta_type)
        except Exception:
            log.warning(
                "Could not restore index %r (%s) from snapshot",
                name,
                meta_type,
                exc_info=True,
            )

    # Restore metadata columns not already present
    for col_name in snapshot.get("metadata", []):
        if col_name not in existing_metadata:
            try:
                catalog.addColumn(col_name)
                log.info("Restored metadata column %r from snapshot", col_name)
            except Exception:
                log.warning(
                    "Could not restore metadata column %r",
                    col_name,
                    exc_info=True,
                )


def _build_extra(entry):
    """Build an extra namespace object from a snapshot entry for addIndex().

    Different index types expect different attributes on the ``extra`` object:
    - FieldIndex/KeywordIndex/etc.: ``indexed_attrs``
    - DateRangeIndex: ``since_field``, ``until_field``
    - ZCTextIndex: ``lexicon_id``, ``index_type``, ``doc_attr``
    """
    extra = _Extra()

    # indexed_attrs: FieldIndex, KeywordIndex, BooleanIndex, UUIDIndex, GopipIndex
    source_attrs = entry.get("source_attrs")
    if source_attrs:
        extra.indexed_attrs = ",".join(source_attrs)

    # DateRangeIndex
    if "since_field" in entry:
        extra.since_field = entry["since_field"]
        extra.until_field = entry["until_field"]

    # ZCTextIndex
    if "lexicon_id" in entry:
        extra.lexicon_id = entry["lexicon_id"]
    if "index_type" in entry:
        extra.index_type = entry["index_type"]
    # ZCTextIndex: doc_attr from source_attrs
    if entry.get("meta_type") == "ZCTextIndex" and source_attrs:
        extra.doc_attr = source_attrs[0]

    return extra


def _ensure_catalog_indexes(site):
    """Re-apply Plone catalog indexes on a fresh catalog.

    When the catalog is replaced with PlonePGCatalogTool, the ZCatalog
    indexes (UID, Title, etc.) are lost.  Re-importing the ``catalog``
    step from Plone's core profiles restores them.  This also creates
    lexicons needed by ZCTextIndex constructors.
    """
    catalog = getattr(site, "portal_catalog", None)
    if catalog is None:
        return

    # Only re-apply if essential Plone indexes are missing.
    try:
        existing = set(catalog.indexes())
        if "UID" in existing and "portal_type" in existing:
            log.debug("Catalog has essential indexes, skipping re-apply")
            return
    except Exception:
        pass

    setup = getattr(site, "portal_setup", None)
    if setup is None:
        log.warning("No portal_setup — cannot re-apply catalog indexes")
        return

    for profile_id in [
        "profile-Products.CMFPlone:plone",
        "profile-Products.CMFEditions:CMFEditions",
        "profile-plone.app.contenttypes:default",
        "profile-plone.app.event:default",
    ]:
        try:
            # run_dependencies=False: skip componentregistry → toolset
            # cascade which would replace our PlonePGCatalogTool and
            # purge IFactory registrations for content types.
            setup.runImportStepFromProfile(
                profile_id,
                "catalog",
                run_dependencies=False,
            )
            log.info("Re-applied catalog config from %s", profile_id)
        except Exception:
            log.debug("Could not apply catalog from %s", profile_id, exc_info=True)


def _remove_lexicons(site):
    """Remove ZCTextIndex lexicons — unused with PG-backed text search.

    Plone's catalog.xml creates htmltext_lexicon, plaintext_lexicon, and
    plone_lexicon for ZCTextIndex stemming/splitting.  With pgcatalog,
    full-text search uses PostgreSQL tsvector/BM25, so these are orphaned.
    """
    catalog = getattr(site, "portal_catalog", None)
    if catalog is None:
        return

    for name in ("htmltext_lexicon", "plaintext_lexicon", "plone_lexicon"):
        if name in catalog.objectIds():
            catalog._delObject(name)
            log.info("Removed orphaned lexicon: %s", name)
