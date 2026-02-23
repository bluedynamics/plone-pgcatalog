<!-- diataxis: how-to -->

# Install plone.pgcatalog

## Install the Package

```bash
pip install plone.pgcatalog
# or
uv pip install plone.pgcatalog
```

Requirements: Python 3.12+, PostgreSQL 14+ (tested with 17), zodb-pgjsonb >= 1.1.

## Configure Zope

Your `zope.conf` must use zodb-pgjsonb as the ZODB storage.
plone.pgcatalog discovers its PostgreSQL connection from the storage layer.

```
%import zodb_pgjsonb

<zodb_db main>
  <pgjsonb>
    dsn dbname=zodb host=localhost port=5432 user=zodb password=zodb
  </pgjsonb>
</zodb_db>
```

plone.pgcatalog is auto-discovered via `z3c.autoinclude` -- no `%import` needed for the catalog itself.

## Apply the GenericSetup Profile

Install via Plone's Add-on installer (Site Setup > Add-ons), or programmatically:

```python
setup_tool = portal.portal_setup
setup_tool.runAllImportStepsFromProfile("profile-plone.pgcatalog:default")
```

The profile replaces `portal_catalog` with `PlonePGCatalogTool`.
The install step automatically:

- **Snapshots** existing index definitions and metadata columns (preserving addon indexes)
- Replaces `portal_catalog` with a fresh `PlonePGCatalogTool`
- Re-applies essential Plone catalog indexes (UID, portal_type, etc.)
- **Restores** addon-provided indexes from the snapshot
- Removes orphaned ZCTextIndex lexicons (no longer needed with PG-backed search)
- Applies DDL schema to PostgreSQL (columns, functions, indexes)

## Verify Installation

- Visit ZMI > portal_catalog -- the class should show `PlonePGCatalogTool` and the meta type `PG Catalog Tool`.
- Run a test query:

  ```python
  portal.portal_catalog(portal_type="Document")
  ```
