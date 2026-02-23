<!-- diataxis: tutorial -->

# Tutorial: Migrate from ZCatalog to plone.pgcatalog

## What You Will Do

Take an existing Plone 6 site that uses the standard ZCatalog and migrate all
catalog data to PostgreSQL.  After this tutorial, your site will use
plone.pgcatalog for all catalog operations while the standard Plone API
continues to work unchanged.

## Prerequisites

- An existing Plone 6 site
- The site must already use [zodb-pgjsonb](https://github.com/bluedynamics/zodb-pgjsonb) as its ZODB storage backend
- PostgreSQL 14+ accessible (tested with 17)
- Admin access to the Plone site

:::{note}
plone.pgcatalog requires zodb-pgjsonb because it stores catalog data as extra
columns in the same `object_state` table that holds ZODB objects.  If your site
uses FileStorage or RelStorage with MySQL, you will need to migrate to
zodb-pgjsonb first.
:::

## Step 1: Back Up Your Database

Before making any changes, create a full backup of your PostgreSQL database.
You will use this to roll back if anything goes wrong.

```bash
pg_dump -h localhost -p 5433 -U zodb zodb > backup_before_pgcatalog.sql
```

Adjust the host, port, and credentials to match your environment.

## Step 2: Install plone.pgcatalog

```bash
uv pip install plone.pgcatalog
```

The package is auto-discovered by Plone via `z3c.autoinclude`.  No ZCML slug
or manual include is needed.

## Step 3: Apply the GenericSetup Profile

You have two options: use the Plone web UI, or run a zconsole script.  The
result is the same.

### Option A: Via the Plone Add-on Installer

1. Log in as a Manager user
2. Go to **Site Setup** > **Add-ons**
3. Find **plone.pgcatalog** in the list and click **Install**

### Option B: Via a zconsole Script

Create a file called `migrate.py`:

```python
import transaction
from zope.component.hooks import setSite
from plone import api

SITE_ID = "Plone"  # adjust to your site ID

site = app[SITE_ID]  # noqa: F821 -- app injected by zconsole
setSite(site)

catalog = api.portal.get_tool("portal_catalog")
print(f"Before: catalog class = {catalog.__class__.__name__}")
print(f"Before: {len(catalog)} objects indexed")

setup = api.portal.get_tool("portal_setup")
setup.runAllImportStepsFromProfile("profile-plone.pgcatalog:default")
transaction.commit()

# Re-fetch -- the catalog object was replaced by the install step
catalog = api.portal.get_tool("portal_catalog")
print(f"After:  catalog class = {catalog.__class__.__name__}")
print(f"After:  {len(list(catalog.indexes()))} ZCatalog indexes registered")
```

Run it:

```bash
.venv/bin/zconsole run instance/etc/zope.conf migrate.py
```

### What the Profile Does

The GenericSetup profile performs these changes:

1. **Snapshots existing indexes** -- captures all index definitions and metadata
   columns from the current catalog, including any addon-provided indexes
2. **Replaces `portal_catalog`** -- replaces the standard catalog tool with
   `PlonePGCatalogTool` (based on `UniqueObject + Folder`, not ZCatalog)
3. **Restores catalog indexes** -- re-applies essential Plone indexes, then
   restores addon indexes from the snapshot so no index definitions are lost
4. **Removes orphaned ZCTextIndex lexicons** -- no longer needed since
   full-text search is handled by PostgreSQL tsvector
5. **Applies DDL schema** -- creates the necessary columns, GIN indexes, and
   PostgreSQL functions on the `object_state` table

:::{tip}
The old ZCatalog's BTree data (the actual indexed values) becomes unreferenced
in ZODB after migration.  Run a ZODB pack after migration to reclaim the space.
:::

## Step 4: Rebuild the Catalog

The old ZCatalog BTree data is now irrelevant.  You need to populate the
PostgreSQL catalog columns from your existing content objects.

Create a file called `rebuild.py`:

```python
import time
import transaction
from zope.component.hooks import setSite
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.users import SimpleUser
from plone import api

SITE_ID = "Plone"  # adjust to your site ID

site = app[SITE_ID]  # noqa: F821
setSite(site)

admin = SimpleUser("admin", "", ["Manager"], [])
newSecurityManager(None, admin)

catalog = api.portal.get_tool("portal_catalog")

print("Rebuilding catalog (clearFindAndRebuild) ...")
t0 = time.time()
catalog.clearFindAndRebuild()
transaction.commit()
elapsed = time.time() - t0
print(f"  Completed in {elapsed:.1f}s")
print(f"  {len(catalog)} objects indexed")
```

Run it:

```bash
.venv/bin/zconsole run instance/etc/zope.conf rebuild.py
```

:::{note}
Expected timing: approximately 15 ms per object.  A site with 10,000 objects
takes about 2.5 minutes.  Large sites with 100,000+ objects may take 25 minutes
or more.
:::

## Step 5: Verify the Migration

### Check Object Counts

Start Zope and visit `portal_catalog` in the ZMI (Zope Management Interface).
The **Catalog** tab should show:

- The correct number of indexed objects
- All expected indexes listed
- All expected metadata columns listed

### Test Search from Python

Create a file called `verify.py`:

```python
from zope.component.hooks import setSite
from plone import api

SITE_ID = "Plone"

site = app[SITE_ID]  # noqa: F821
setSite(site)

catalog = api.portal.get_tool("portal_catalog")

results = catalog(portal_type="Document")
print(f"Found {len(results)} documents")

results = catalog(SearchableText="test")
print(f"Full-text search found {len(results)} results")

# Verify brains have expected attributes
if results:
    brain = results[0]
    print(f"  Title: {brain.Title}")
    print(f"  Path:  {brain.getPath()}")
```

Run it:

```bash
.venv/bin/zconsole run instance/etc/zope.conf verify.py
```

### Compare with PostgreSQL

Connect to PostgreSQL directly and confirm the counts match:

```sql
SELECT COUNT(*) FROM object_state WHERE path IS NOT NULL;
```

This count should match the object count shown in the ZMI.

## Rollback Strategy

If anything goes wrong, you can restore the original state.

1. **Restore the database backup:**

   ```bash
   psql -h localhost -p 5433 -U zodb zodb < backup_before_pgcatalog.sql
   ```

2. **Uninstall plone.pgcatalog:**

   ```bash
   uv pip uninstall plone-pgcatalog
   ```

3. **Restart Zope** -- the original `CatalogTool` class will be restored from
   the database, and the site will operate as before.

## What You Learned

- Migration requires zodb-pgjsonb as the ZODB backend (same PostgreSQL
  database)
- The GenericSetup profile replaces the catalog tool class and applies DDL
  schema
- `clearFindAndRebuild()` populates PostgreSQL from existing content objects
- The standard Plone catalog API works unchanged after migration
- A database backup provides a safe rollback path

## Next Steps

- {doc}`multilingual-search` to set up language-aware search
- {doc}`quickstart-demo` to try plone.pgcatalog with example content
