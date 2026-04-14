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

from Acquisition import aq_base

import logging


log = logging.getLogger(__name__)


def importToolset(context):
    """Override of Products.GenericSetup.tool.importToolset.

    Prevents the toolset step from deleting portal_catalog when it is a
    PlonePGCatalogTool.  CMFPlone's toolset.xml declares portal_catalog
    with class Products.CMFPlone.CatalogTool.CatalogTool; since
    PlonePGCatalogTool is a different class, the default importToolset
    deletes it and tries to recreate it.  The deletion fires
    IObjectModifiedEvent → reindexOnModify → queryUtility(ICatalogTool)
    which fails because the catalog was just deleted.

    This wrapper temporarily removes portal_catalog from the toolset
    registry's required tools, runs the original importToolset, then
    restores it.
    """
    from Products.GenericSetup.tool import importToolset as _orig

    site = context.getSite()
    existing = getattr(aq_base(site), "portal_catalog", None)

    if existing is None:
        # No catalog yet — let the original handler create it
        return _orig(context)

    from plone.pgcatalog.catalog import PlonePGCatalogTool

    if not isinstance(existing, PlonePGCatalogTool):
        # Not our class — let the original handler deal with it
        return _orig(context)

    # Our catalog is installed — suppress toolset replacement.
    # Read the XML, remove portal_catalog from required tools,
    # then delegate to the original handler.
    from Products.GenericSetup.tool import TOOLSET_XML

    xml = context.readDataFile(TOOLSET_XML)
    if xml is None:
        return

    setup_tool = context.getSetupTool()
    toolset = setup_tool.getToolsetRegistry()
    toolset.parseXML(xml, context.getEncoding())

    # Remove portal_catalog from required tools before processing
    had_catalog = False
    if "portal_catalog" in {info["id"] for info in toolset.listRequiredToolInfo()}:
        had_catalog = True
        # Clear and re-add without portal_catalog
        new_required = [
            info
            for info in toolset.listRequiredToolInfo()
            if info["id"] != "portal_catalog"
        ]
        toolset._required.clear()
        for info in new_required:
            toolset.addRequiredTool(info["id"], info["class"])

    setup_tool._p_changed = True

    # Now process all tools except portal_catalog
    existing_ids = site.objectIds()
    for tool_id in toolset.listForbiddenTools():
        if tool_id in existing_ids:
            site._delObject(tool_id)

    for info in toolset.listRequiredToolInfo():
        tool_id = str(info["id"])
        from Products.GenericSetup.utils import _resolveDottedName

        tool_class = _resolveDottedName(info["class"])
        if tool_class is None:
            continue
        existing_tool = getattr(aq_base(site), tool_id, None)
        if existing_tool is None:
            try:
                new_tool = tool_class()
            except TypeError:
                new_tool = tool_class(tool_id)
            else:
                new_tool._setId(tool_id)
            site._setObject(tool_id, new_tool)
        elif type(aq_base(existing_tool)) is not tool_class:
            site._delObject(tool_id)
            site._setObject(tool_id, tool_class())

    # Re-add portal_catalog to the registry (for record-keeping) without
    # actually touching the object
    if had_catalog:
        toolset.addRequiredTool(
            "portal_catalog", "plone.pgcatalog.catalog.PlonePGCatalogTool"
        )

    log.info("Toolset imported (portal_catalog protected)")


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

    # Rebuild: index all existing content objects into PG.
    # Content created before pgcatalog was installed was indexed by the
    # old ZCatalog BTree catalog.  That data is lost when the catalog is
    # replaced.  A full rebuild traverses the portal tree and calls
    # _set_pg_annotation() for every content object, ensuring path/idx
    # are populated in object_state.  Without this, navigation and
    # search return empty results.
    new_catalog = getattr(site, "portal_catalog", None)
    if new_catalog is not None and hasattr(new_catalog, "clearFindAndRebuild"):
        try:
            new_catalog.clearFindAndRebuild()
            log.info("Rebuilt catalog: indexed all existing content into PG")
        except Exception:
            log.warning(
                "Could not rebuild catalog after replacement — "
                "run portal_catalog.clearFindAndRebuild() manually",
                exc_info=True,
            )


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
    from Products.CMFCore.interfaces import ICatalogTool
    from zope.component import getSiteManager

    if "portal_catalog" in site.objectIds():
        site._delObject("portal_catalog")

    new_catalog = PlonePGCatalogTool()
    site._setObject("portal_catalog", new_catalog)

    # Register as ICatalogTool immediately so that importCatalogTool
    # (called by _ensure_catalog_indexes via runImportStepFromProfile)
    # can find it via queryUtility(ICatalogTool).
    sm = getSiteManager(site)
    sm.registerUtility(site.portal_catalog, ICatalogTool)
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
    - DateRecurringIndex: ``recurdef``, ``until`` (NOT ``attr_recurdef`` —
      the index reads ``extra.recurdef`` in its constructor and stores it
      on ``self.attr_recurdef``; the snapshot captures the stored attribute
      so we have to translate the key back here).
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

    # DateRecurringIndex (plone.app.event / bda.aaf.site etc.):
    # the constructor expects extra.recurdef / extra.until — without
    # these, DateRecurringIndex.__init__ raises AttributeError and the
    # index silently fails to restore.  Default to empty strings so the
    # index creates even when the snapshot didn't capture one of them
    # (rare but possible for hand-written catalog.xml imports).
    if entry.get("meta_type") == "DateRecurringIndex":
        extra.recurdef = entry.get("attr_recurdef", "")
        extra.until = entry.get("attr_until", "")

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
            log.info("Applied catalog indexes from %s", profile_id)
        except Exception:
            log.warning("Could not apply catalog from %s", profile_id, exc_info=True)


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
