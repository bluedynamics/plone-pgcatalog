<!-- diataxis: reference -->

# ZCatalog Compatibility

`PlonePGCatalogTool` does not inherit from `ZCatalog` or `CatalogTool`.
It implements the same public API so existing Plone code continues to work.
This page documents the compatibility surface.

## Supported Methods

These methods behave identically to their ZCatalog counterparts:

| Method | Permission | Description |
|---|---|---|
| `searchResults(**kw)` | Search ZCatalog | Query the catalog with security filters |
| `__call__(**kw)` | Search ZCatalog | Alias for `searchResults()` |
| `unrestrictedSearchResults(**kw)` | Private | Query without security filters |
| `catalog_object(obj, uid, idxs)` | Manage ZCatalog Entries | Index an object |
| `uncatalog_object(uid)` | Manage ZCatalog Entries | Remove an object from the catalog |
| `refreshCatalog(clear)` | Manage ZCatalog Entries | Re-catalog all objects |
| `reindexIndex(name)` | Manage ZCatalog Entries | Re-apply a single index |
| `clearFindAndRebuild()` | Manage ZCatalog Entries | Clear and rebuild from content |
| `getpath(rid)` | Search ZCatalog | Physical path for a record ID (ZOID) |
| `getrid(path)` | Search ZCatalog | Record ID (ZOID) for a path |
| `getIndexDataForRID(rid)` | Search ZCatalog | `idx` JSONB dict for a record |
| `indexes()` | Search ZCatalog | List of index names |
| `schema()` | Search ZCatalog | List of metadata column names |
| `addIndex(name, type, extra)` | Manage ZCatalogIndex Entries | Register a new index |
| `delIndex(name)` | Manage ZCatalogIndex Entries | Remove an index |
| `addColumn(name)` | Manage ZCatalogIndex Entries | Register a metadata column |
| `delColumn(name)` | Manage ZCatalogIndex Entries | Remove a metadata column |
| `getIndexObjects()` | Manage ZCatalogIndex Entries | List of index objects |

## Blocked Methods

These ZCatalog methods are not supported.
They raise `NotImplementedError` with a guidance message.

| Method | Migration Path |
|---|---|
| `getAllBrains()` | Use `searchResults()` or direct PostgreSQL queries |
| `searchAll()` | Use `searchResults()` or direct PostgreSQL queries |
| `getobject()` | Use `brain.getObject()` instead |
| `getMetadataForUID()` | Metadata is in the `idx` JSONB column -- use `searchResults()` |
| `getMetadataForRID()` | Metadata is in the `idx` JSONB column -- use `searchResults()` |
| `getIndexDataForUID()` | Use `getIndexDataForRID(zoid)` instead |
| `index_objects()` | Use `getIndexObjects()` instead |

All blocked methods are assigned the `Search ZCatalog` permission so callers
receive `NotImplementedError`, not `Unauthorized`.

## Deprecated Methods

These methods work but emit `DeprecationWarning`:

| Method | Replacement |
|---|---|
| `search(*args, **kw)` | `searchResults(*args, **kw)` |
| `uniqueValuesFor(name)` | `catalog.Indexes[name].uniqueValues()` |

## _CatalogCompat Shim

`PlonePGCatalogTool._catalog` is a `_CatalogCompat` instance providing
the minimal API that ZCatalog-aware code expects:

| Attribute / Method | Type | Description |
|---|---|---|
| `_catalog.indexes` | `PersistentMapping` | Map of index name to index object |
| `_catalog.schema` | `PersistentMapping` | Map of metadata column names |
| `_catalog.getIndex(name)` | method | Return index object by name |

For existing ZODB instances that were migrated from ZCatalog, the old
`Catalog` object persists in ZODB and already has `.indexes` and `.schema`
attributes.  The code only reads these attributes, so it works without
an additional migration step.

## PGCatalogIndexes and PGIndex

`catalog.Indexes` returns a `PGCatalogIndexes` wrapper.  Accessing an
index by name returns a `PGIndex` proxy:

```python
index = catalog.Indexes["portal_type"]
index.uniqueValues()           # SELECT DISTINCT from PostgreSQL
index._index.get("Document")   # PG query returning matching ZOIDs
```

`PGIndex._index` is a `_PGIndexMapping` that translates dict-style
`get(value)` calls into PostgreSQL queries.

## Brain Attribute Resolution

`PGCatalogBrain` attribute access follows these rules:

| Attribute Type | Behavior |
|---|---|
| Known index or metadata (in `IndexRegistry`) | Returns value from `idx` JSONB, or `None` if missing |
| Unknown attribute | Raises `AttributeError` |

The `AttributeError` for unknown attributes is intentional:
`CatalogContentListingObject.__getattr__()` catches it and falls back to
`getObject()`, loading the real content object.  Returning `None` instead
would cause `plone.restapi` and listing views to display `None` values.
