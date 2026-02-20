"""GenericSetup install handler for plone.pgcatalog.

Re-applies Plone's ZCatalog index configuration after toolset.xml
replaces portal_catalog with a fresh PlonePGCatalogTool.

DDL (ALTER TABLE for catalog columns, functions, indexes) is applied
by ``PGJsonbStorage.register_state_processor()`` using the storage's
own connection — no REPEATABLE READ lock conflicts.
"""

import logging


log = logging.getLogger(__name__)


def install(context):
    """GenericSetup import step: restore Plone catalog indexes.

    toolset.xml creates a fresh PlonePGCatalogTool with no ZCatalog
    indexes.  This step re-applies Plone's catalog.xml from the core
    profiles to restore UID, Title, allowedRolesAndUsers, etc.
    """
    if context.readDataFile("install_pgcatalog.txt") is None:
        return

    site = context.getSite()
    _ensure_catalog_indexes(site)


def _ensure_catalog_indexes(site):
    """Re-apply Plone catalog indexes when toolset created a fresh catalog.

    When toolset.xml replaces portal_catalog with PlonePGCatalogTool,
    the ZCatalog indexes (UID, Title, etc.) are lost.  Re-importing
    the ``catalog`` step from Plone's core profiles restores them.
    """
    catalog = getattr(site, "portal_catalog", None)
    if catalog is None:
        return

    # Only re-apply if essential Plone indexes are missing.
    # Check for core indexes rather than "any indexes exist" — an addon
    # may have added custom indexes before our step runs, but the Plone
    # defaults (UID, portal_type, etc.) still need to be applied.
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
