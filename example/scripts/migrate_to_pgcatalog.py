"""Migrate an existing Plone site to plone.pgcatalog.

Run via zconsole (after installing plone-pgcatalog into the venv)::

    .venv/bin/zconsole run instance/etc/zope.conf scripts/migrate_to_pgcatalog.py

This script:

1. Installs the ``plone.pgcatalog:default`` GenericSetup profile
   (replaces portal_catalog with PlonePGCatalogTool via toolset.xml)
2. Rebuilds the catalog (``clearFindAndRebuild``)
3. Verifies that all content is indexed and searchable
"""

import sys
import time

import transaction
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.users import SimpleUser
from plone import api
from zope.component.hooks import setSite

SITE_ID = "Plone"


def install_pgcatalog(site):
    """Install the plone.pgcatalog GenericSetup profile."""
    setSite(site)

    catalog = api.portal.get_tool("portal_catalog")
    print(f"Before: catalog class = {catalog.__class__.__name__}")
    print(f"Before: {len(catalog)} objects indexed")

    print("\nInstalling plone.pgcatalog:default profile ...")
    setup = api.portal.get_tool("portal_setup")
    setup.runAllImportStepsFromProfile("profile-plone.pgcatalog:default")
    transaction.commit()

    # Re-fetch catalog (it was replaced by toolset.xml)
    catalog = api.portal.get_tool("portal_catalog")
    print(f"After:  catalog class = {catalog.__class__.__name__}")
    n_indexes = len(list(catalog.indexes()))
    print(f"After:  {n_indexes} ZCatalog indexes registered")

    return catalog


def rebuild_catalog(site):
    """Run clearFindAndRebuild to populate PG catalog from content."""
    setSite(site)
    catalog = api.portal.get_tool("portal_catalog")

    print("\nRebuilding catalog (clearFindAndRebuild) ...")
    t0 = time.time()
    catalog.clearFindAndRebuild()
    transaction.commit()
    elapsed = time.time() - t0
    print(f"  Rebuild completed in {elapsed:.1f}s")


def verify_search(site):
    """Run test queries to verify the migration worked."""
    setSite(site)
    catalog = api.portal.get_tool("portal_catalog")

    print("\nVerification:")

    # Count by type
    all_results = catalog()
    docs = catalog(portal_type="Document")
    folders = catalog(portal_type="Folder")
    print(f"  Total indexed: {len(all_results)}")
    print(f"  Documents: {len(docs)}")
    print(f"  Folders: {len(folders)}")

    # Full-text search
    volcano = catalog(SearchableText="volcano")
    print(f"  SearchableText='volcano': {len(volcano)} hits")

    # Path search
    en_library = catalog(path={"query": "/Plone/en/library", "depth": 1})
    print(f"  path=/Plone/en/library depth=1: {len(en_library)} hits")

    if len(docs) > 0 and len(volcano) > 0:
        print("\nMigration successful! All content is indexed and searchable.")
    else:
        print("\nWARNING: Migration may be incomplete!")
        sys.exit(1)


def main(app):
    if SITE_ID not in app.objectIds():
        print(f"Error: Site '{SITE_ID}' not found.", file=sys.stderr)
        sys.exit(1)

    site = app[SITE_ID]
    setSite(site)

    admin = SimpleUser("admin", "", ["Manager"], [])
    newSecurityManager(None, admin)

    install_pgcatalog(site)

    # Re-fetch site after profile install (catalog was replaced)
    site = app[SITE_ID]
    rebuild_catalog(site)

    site = app[SITE_ID]
    verify_search(site)


main(app)  # noqa: F821
