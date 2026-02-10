"""Minimal test: can we replace portal_catalog with PlonePGCatalogTool?

Run: .venv/bin/zconsole run instance/etc/zope.conf sources/plone-pgcatalog/benchmarks/_test_catalog_swap.py
"""
import transaction
from Acquisition import aq_base
from zope.component.hooks import setSite
from Products.CMFCore.interfaces import ICatalogTool
from plone.pgcatalog.catalog import PlonePGCatalogTool

app = globals()["app"]
site = app.Plone  # adjust if your site has a different id

setSite(site)
sm = site.getSiteManager()

print(f"BEFORE: {type(aq_base(site.portal_catalog)).__name__}")

# Approach: manage_delObjects + _setObject
sm.unregisterUtility(provided=ICatalogTool)
site.manage_delObjects(["portal_catalog"])

new_catalog = PlonePGCatalogTool()
site._setObject("portal_catalog", new_catalog)
sm.registerUtility(site.portal_catalog, ICatalogTool)

# Re-apply catalog indexes
setup_tool = site.portal_setup
for profile_id in [
    "profile-Products.CMFPlone:plone",
    "profile-plone.app.contenttypes:default",
]:
    try:
        setup_tool.runImportStepFromProfile(profile_id, "catalog")
        print(f"  Applied catalog step from {profile_id}")
    except Exception as e:
        print(f"  FAILED {profile_id}: {e}")

print(f"AFTER: {type(aq_base(site.portal_catalog)).__name__}")
print(f"Indexes: {list(site.portal_catalog.indexes())[:5]}...")

# Test: can we still create content?
try:
    site.invokeFactory("Document", "test-swap-doc", title="Test")
    print("Content creation: OK")
    site.manage_delObjects(["test-swap-doc"])
except Exception as e:
    print(f"Content creation FAILED: {e}")

# Don't commit - this is just a test
transaction.abort()
print("Done (aborted)")
