# Clean Break from ZCatalog + eea.facetednavigation Adapter

**Status: IMPLEMENTED** — 857 tests passing (11 BM25 skipped).

## Overview

Two-part refactoring of `PlonePGCatalogTool`:

1. **Clean break**: Change base class from `CatalogTool(CMFPlone)` →
   `UniqueObject + SimpleItem`. Implement needed API, block unsupported
   methods, provide `_catalog` compat shim.
2. **eea.facetednavigation**: PG-backed `IFacetedCatalog` adapter in
   `addons_compat/` subpackage with fully conditional imports and ZCML.

---

## Part 1: Clean Break from ZCatalog

### New Base Class

```python
from OFS.SimpleItem import SimpleItem
from Products.CMFCore.utils import UniqueObject

@implementer(IPGCatalogTool, IPloneCatalogTool)
class PlonePGCatalogTool(UniqueObject, SimpleItem):
    id = "portal_catalog"
    meta_type = "PG Catalog Tool"
```

- `SimpleItem` provides: Persistent, Acquisition, RoleManager, Item (ZMI basics).
- `UniqueObject` provides: `getId()` returning `self.id`.

### _catalog Compat Shim

`ZCatalogIndexes._getOb()` reads `aq_parent(self)._catalog.indexes` (line 73
of ZCatalogIndexes.py). `_setOb()` and `_delOb()` write there too.
`CMFCore.CatalogTool.reindexObject` checks `self._catalog.indexes`.
eea.facetednavigation CountableWidget calls `ctool._catalog.getIndex(index_id)`.
Provide a minimal persistent object:

```python
class _CatalogCompat(Persistent):
    """Minimal _catalog providing index object storage."""

    def __init__(self):
        self.indexes = PersistentMapping()

    def getIndex(self, name):
        return self.indexes[name]

    def clear(self):
        pass  # no BTrees to clear — PG data cleared separately
```

In `__init__`:
```python
def __init__(self):
    self._catalog = _CatalogCompat()
```

For **existing ZODB instances**: the old `_catalog` (full `Catalog` object)
persists in ZODB and already has `.indexes`. Our code only reads `.indexes`
and `.getIndex()`, so it works without migration. New instances get
`_CatalogCompat`.

### PGCatalogIndexes — No Change Needed

`PGCatalogIndexes(ZCatalogIndexes)` in pgindex.py is unchanged.
`ZCatalogIndexes._getOb()` reads from `aq_parent(self)._catalog.indexes` —
our `_CatalogCompat` provides that dict.

### Methods Copied from CMFPlone.CatalogTool

One method must be copied (with its imports):

| Method | Source | Why |
|--------|--------|-----|
| `_listAllowedRolesAndUsers(user)` | CMFPlone/CatalogTool.py ~L170 | Group-based security filtering (`user:groupname` entries) |

### getCounter — PG Sequence, No Persistent Counter

The original `getCounter()` / `_increment_counter()` uses a `Length` object
stored on the catalog tool — every `catalog_object()` call dirties the tool
in ZODB. This is unnecessary with PG.

**Consumers** (only 2 actual callers):
- `plone.app.textfield/transform.py` — cache invalidation for image URLs in richtext
- `plone.app.layout/sitemap.py` — cache key for sitemap XML generation

Both just compare `counter == cached_counter` to detect catalog changes.

**PG replacement**: Dedicated PG sequence incremented only when catalog data
is actually written. This avoids false invalidations from non-catalog ZODB
commits (portlet edits, registry changes, etc.) that would occur with
`max(tid) FROM transaction_log`.

**Schema** (in `schema.py` / `install_catalog_schema()`):
```sql
CREATE SEQUENCE IF NOT EXISTS pgcatalog_counter;
```

**Increment** in `CatalogStateProcessor.tpc_vote()` — the exact point where
idx writes happen. One `nextval` per transaction that writes catalog data:
```python
# In processor.py, inside tpc_vote(), after batch write:
cur.execute("SELECT nextval('pgcatalog_counter')")
```

**Read**:
```python
def getCounter(self):
    """Return a value that changes when catalog data is written."""
    conn = self._get_pg_read_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT last_value FROM pgcatalog_counter")
        row = cur.fetchone()
    return row["last_value"] or 0
```

No `_increment_counter()` method needed. No `_counter` attribute. No
dirtying the catalog object on writes. Counter advances precisely when
catalog data is committed, matching original CatalogTool semantics.

### Methods to Implement (PG-backed)

| Method | Behavior | Used By |
|--------|----------|---------|
| `indexes()` | `list(self._catalog.indexes.keys())` | CMFPlone search (sort validation), GenericSetup, plone.app.querystring, plone.app.event, plone.app.content |
| `schema()` | `list(get_registry().metadata)` | GenericSetup, plone.app.vocabularies (MetaDataFieldsVocabulary), plone.base utils |
| `addIndex(name, type, extra)` | Resolve type → create index object → store in `_catalog.indexes` → sync IndexRegistry | GenericSetup import, plone.app.event setuphandlers |
| `delIndex(name)` | Remove from `_catalog.indexes` → sync IndexRegistry | GenericSetup import |
| `addColumn(name, default)` | `get_registry().add_metadata(name)` | GenericSetup import, plone.app.event setuphandlers |
| `delColumn(name)` | `get_registry().metadata.discard(name)` | GenericSetup import |
| `getIndexObjects()` | Wrap each index via PGCatalogIndexes path | GenericSetup export, eea.facetednavigation vocabularies |
| `getIndexDataForRID(rid)` | `SELECT idx FROM object_state WHERE zoid = %(rid)s` → return dict | plone.app.event (occurrence expansion) |
| `manage_catalogClear()` | Call `clear_catalog_data(conn)` | Called by `clearFindAndRebuild()` |

`addIndex` type resolution — reuse `Products.PluginIndexes` factory pattern
(same as ZCatalog.addIndex):
```python
from Products.PluginIndexes.interfaces import IPluggableIndex
if IPluggableIndex.providedBy(index_type):
    self._catalog.indexes[name] = index_type
else:
    # Resolve type string to class via PluggableIndex registry
    ...
```

### clearFindAndRebuild — Reimplementation

No more `super().clearFindAndRebuild()`. Self-contained:
```python
def clearFindAndRebuild(self):
    with self._pg_connection() as conn:
        clear_catalog_data(conn)
    portal = aq_parent(aq_inner(self))
    portal.ZopeFindAndApply(
        portal, search_sub=True,
        apply_func=lambda obj, path: self.catalog_object(obj, path),
    )
```

### manage_options

Define directly (no more filtering parent's options):
```python
manage_options = (
    {"action": "manage_catalogView", "label": "Catalog"},
    {"action": "manage_catalogAdvanced", "label": "Advanced"},
    {"action": "manage_catalogIndexesAndMetadata", "label": "Indexes & Metadata"},
)
```

### searchResults — show_inactive

Currently inherits `allow_inactive()` from CMFPlone. The current
`searchResults()` implementation already handles `show_inactive` via
`sm.checkPermission()` (catalog.py line 553-558). No dependency on
`allow_inactive()` — our impl is self-contained.

### Deprecated Proxy Methods

These are thin wrappers that emit `DeprecationWarning` to nudge callers
toward the canonical API:

| Method | Implementation |
|--------|---------------|
| `search(query, sort_index=None, reverse=0, limit=None, merge=1)` | Emits `DeprecationWarning("portal_catalog.search() is deprecated, use searchResults() instead")`, then proxies to `self.searchResults(query)`. |
| `uniqueValuesFor(name)` | Emits `DeprecationWarning("portal_catalog.uniqueValuesFor() is deprecated, use catalog.Indexes[name].uniqueValues() instead")`, then returns `tuple(self.Indexes._getOb(name).uniqueValues())`. |

### Methods to Block (NotImplementedError)

These exist on ZCatalog but return wrong/empty data with PG-only storage.
Each becomes a method that raises `NotImplementedError(msg)`:

| Method | Message |
|--------|---------|
| `getAllBrains()` | Use searchResults() or direct PG queries |
| `searchAll()` | Use searchResults() or direct PG queries |
| `getobject()` | Use brain.getObject() instead |
| `getMetadataForUID()` | Metadata is in idx JSONB — use searchResults |
| `getMetadataForRID()` | Metadata is in idx JSONB — use searchResults |
| `getIndexDataForUID()` | Use getIndexDataForRID(zoid) instead |
| `index_objects()` | Use getIndexObjects() instead |

### Files Modified (Part 1)

| File | Change |
|------|--------|
| `src/plone/pgcatalog/catalog.py` | Change base class, add `_CatalogCompat`, copy `_listAllowedRolesAndUsers`, implement PG-backed API, PG-backed `getCounter`, block methods, rewrite `clearFindAndRebuild`, own `manage_options` |
| `src/plone/pgcatalog/interfaces.py` | Add `IPloneCatalogTool` to imports |
| `src/plone/pgcatalog/schema.py` | Add `CREATE SEQUENCE IF NOT EXISTS pgcatalog_counter` to `install_catalog_schema()` |
| `src/plone/pgcatalog/processor.py` | Add `nextval('pgcatalog_counter')` call in `tpc_vote()` after batch write |

### Unchanged Files

| File | Why |
|------|-----|
| `pgindex.py` | `PGCatalogIndexes(ZCatalogIndexes)` reads `_catalog.indexes` — still works with `_CatalogCompat` |
| `configure.zcml` | Utility registration stays the same |

### Key Imports to Add

```python
from OFS.SimpleItem import SimpleItem
from Persistence import Persistent
from persistent.mapping import PersistentMapping
from Products.CMFCore.utils import UniqueObject
from plone.base.interfaces import IPloneCatalogTool
```

### Key Import to Remove

```python
# DELETE: from Products.CMFPlone.CatalogTool import CatalogTool
```

---

## Part 2: eea.facetednavigation Adapter

### Problem

eea.facetednavigation's `CountableWidget.count()` (widget.py:326-386) does
facet counting by calling `IFacetedCatalog.apply_index(context, index, value)`
which invokes `index._apply_index()` on ZCatalog BTree indexes. Since we
never populate BTrees, facet counts always return 0.

### Solution

Register a PG-backed `IFacetedCatalog` utility that overrides `apply_index()`
to query PG's `idx` JSONB column. Fully conditional — only active when
`eea.facetednavigation` is installed.

### File Structure

```
src/plone/pgcatalog/addons_compat/
    __init__.py                              # empty
    eeafacetednavigation.py                  # PGFacetedCatalog
    eeafacetednavigation_overrides.zcml      # utility override
```

### PGFacetedCatalog

Subclasses `eea.facetednavigation.search.catalog.FacetedCatalog` to preserve
`__call__()` (Collection/Topic query merging, `QueryWillBeExecutedEvent`
firing). Only overrides `apply_index()`.

```python
from eea.facetednavigation.search.catalog import FacetedCatalog
from eea.facetednavigation.search.interfaces import IFacetedCatalog

@implementer(IFacetedCatalog)
class PGFacetedCatalog(FacetedCatalog):

    def apply_index(self, context, index, value):
        catalog = getToolByName(context, "portal_catalog")
        if not IPGCatalogTool.providedBy(catalog):
            return super().apply_index(context, index, value)

        index_id = index.getId()
        conn = catalog._get_pg_read_connection()
        zoids = self._pg_apply_index(conn, index_id, index, value)
        return zoids, (index_id,)
```

**SQL dispatch by IndexType** (uses `get_registry()` + `META_TYPE_MAP` fallback):

| IndexType | SQL |
|-----------|-----|
| FIELD, GOPIP, UUID | `idx @> %(match)s::jsonb` with `Json({key: value})` |
| KEYWORD (single) | `idx @> %(match)s::jsonb` with `Json({key: [value]})` |
| KEYWORD (multi) | `idx->'{key}' ?| %(vals)s` |
| BOOLEAN | `idx @> %(match)s::jsonb` with `Json({key: bool(value)})` |
| DATE | `idx @> %(match)s::jsonb` (ISO string match) |
| Special/unknown | `frozenset()` (not used for facet counting) |

**IPGIndexTranslator fallback**: if a translator is registered for the index
(e.g. DateRecurringIndex), use `translator.query()` → wrap SQL fragment in
`SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {fragment}`.

Returns `frozenset` of integer ZOIDs — matches `PGCatalogBrain.getRID()`
(brain.py:108) for correct intersection in CountableWidget.count().

### ZCML Registration

`addons_compat/eeafacetednavigation_overrides.zcml`:
```xml
<configure xmlns="http://namespaces.zope.org/zope">
  <utility
      factory=".eeafacetednavigation.PGFacetedCatalog"
      provides="eea.facetednavigation.search.interfaces.IFacetedCatalog"
      />
</configure>
```

Addition to main `configure.zcml`:
```xml
<includeOverrides
    xmlns:zcml="http://namespaces.zope.org/zcml"
    zcml:condition="installed eea.facetednavigation"
    package=".addons_compat"
    file="eeafacetednavigation_overrides.zcml"
    />
```

Uses `<includeOverrides>` to resolve ZCML conflict with eea's own
`<utility>` registration (both provide unnamed `IFacetedCatalog`).

---

## Testing

### `tests/test_clean_break.py`

- `PlonePGCatalogTool` does NOT inherit from `ZCatalog`
- Implements `IPGCatalogTool`, `IPloneCatalogTool`
- `_catalog.indexes` is dict-like
- `indexes()` returns index names from `_catalog.indexes`
- `schema()` returns metadata from IndexRegistry
- `addIndex()` / `delIndex()` — round-trip
- `addColumn()` / `delColumn()` — round-trip with registry.metadata
- `getIndexObjects()` returns wrapped index objects
- `getIndexDataForRID(rid)` returns idx dict from PG
- `getCounter()` / `_increment_counter()` — counter management
- `search()` proxies to `searchResults()` with `DeprecationWarning`
- `uniqueValuesFor()` proxies to index with `DeprecationWarning`
- Blocked methods raise `NotImplementedError`
- `_listAllowedRolesAndUsers` includes groups
- `manage_options` defined directly (no query report tabs)

### `tests/test_faceted_compat.py`

Guarded by `pytest.mark.skipif(not HAS_EEA, ...)`.

- `PGFacetedCatalog` implements `IFacetedCatalog`
- `PGFacetedCatalog` subclasses `FacetedCatalog`
- `_handle_field`: FieldIndex exact match returns correct zoids
- `_handle_keyword`: single + shared value containment
- `_handle_boolean`: True/False matching
- Intersection: `frozenset` from handler ∩ brain RIDs works correctly
- IPGIndexTranslator fallback: registered translator is used

Test data pattern (from existing conftest.py):
```python
insert_object(conn, zoid=1)
catalog_object(conn, zoid=1, path="/plone/doc", idx={...})
conn.commit()
```

### Run

```bash
pytest tests/test_clean_break.py tests/test_faceted_compat.py -v
pytest tests/ -v  # full suite, regression check
```

---

## Implementation Order

1. Create `_CatalogCompat` class in `catalog.py`
2. Change base class, update imports, declare interfaces
3. Copy `_listAllowedRolesAndUsers` from CMFPlone
4. Add `pgcatalog_counter` sequence to `schema.py`
5. Add `nextval('pgcatalog_counter')` to `processor.py` tpc_vote
6. Implement PG-backed `getCounter()` (reads sequence)
7. Implement: `indexes`, `schema`, `addIndex`, `delIndex`, `addColumn`,
   `delColumn`, `getIndexObjects`, `getIndexDataForRID`, `manage_catalogClear`
8. Reimplement `clearFindAndRebuild` (no super)
9. Define `manage_options` directly
10. Add blocked methods (NotImplementedError)
11. Write `tests/test_clean_break.py`
12. Create `addons_compat/` package
13. Write `eeafacetednavigation.py` + overrides ZCML
14. Update `configure.zcml` with conditional `<includeOverrides>`
15. Write `tests/test_faceted_compat.py`
16. Run full test suite

---

## Key Files Reference

| File | Role |
|------|------|
| `src/plone/pgcatalog/catalog.py` | Main refactoring target |
| `src/plone/pgcatalog/pgindex.py` | PGCatalogIndexes — unchanged, reads `_catalog.indexes` |
| `src/plone/pgcatalog/columns.py` | `get_registry()`, `IndexType`, `META_TYPE_MAP`, `validate_identifier()` |
| `src/plone/pgcatalog/pool.py` | `get_storage_connection()`, `get_pool()`, `get_request_connection()` |
| `src/plone/pgcatalog/indexing.py` | `catalog_object()` — test data helper |
| `src/plone/pgcatalog/interfaces.py` | `IPGCatalogTool`, `IPGIndexTranslator` |
| `.venv/.../CMFPlone/CatalogTool.py` | Source for `_listAllowedRolesAndUsers` |
| `.venv/.../eea/facetednavigation/search/catalog.py` | FacetedCatalog to subclass |
| `.venv/.../eea/facetednavigation/widgets/widget.py:326-386` | CountableWidget.count() — defines the contract |
| `.venv/.../Products/ZCatalog/ZCatalogIndexes.py` | Shows `_catalog.indexes` access pattern |
