<!-- diataxis: reference -->

# Permissions

`PlonePGCatalogTool` declares Zope `AccessControl` permissions on all
public methods.  The permission model mirrors ZCatalog so existing role
assignments carry over after migration.

## Permission Tiers

### Search ZCatalog

Default roles: **Anonymous**, **Manager**

| Method | Notes |
|---|---|
| `searchResults(**kw)` | Primary search method |
| `__call__(**kw)` | Alias for `searchResults()` |
| `indexes()` | List index names |
| `schema()` | List metadata column names |
| `getpath(rid)` | Path for record ID (ZOID) |
| `getrid(path)` | Record ID for path |
| `getIndexDataForRID(rid)` | `idx` JSONB for record |
| `uniqueValuesFor(name)` | Unique values for an index |
| `search(*args, **kw)` | Deprecated proxy |
| `all_meta_types()` | Available index types |
| `getAllBrains()` | Blocked (NotImplementedError) |
| `searchAll()` | Blocked (NotImplementedError) |
| `getobject()` | Blocked (NotImplementedError) |
| `getMetadataForUID()` | Blocked (NotImplementedError) |
| `getMetadataForRID()` | Blocked (NotImplementedError) |
| `getIndexDataForUID()` | Blocked (NotImplementedError) |
| `index_objects()` | Blocked (NotImplementedError) |

Blocked methods use the same permission as ZCatalog so callers receive
`NotImplementedError`, not `Unauthorized`.

### Manage ZCatalog Entries

Default roles: **Manager**

| Method | Notes |
|---|---|
| `catalog_object(obj, uid, idxs)` | Index an object |
| `uncatalog_object(uid)` | Remove from catalog |
| `refreshCatalog(clear)` | Re-catalog all objects |
| `reindexIndex(name)` | Re-apply a single index |
| `clearFindAndRebuild()` | Clear and rebuild |
| `manage_catalogClear()` | Clear all catalog data |
| `manage_catalogView` | ZMI Catalog tab |
| `manage_catalogAdvanced` | ZMI Advanced tab |
| `manage_objectInformation` | ZMI object detail |
| `manage_catalogIndexesAndMetadata` | ZMI Indexes & Metadata tab |
| `manage_get_catalog_summary()` | Catalog tab header data |
| `manage_get_catalog_objects()` | Paginated object list |
| `manage_get_object_detail()` | Single object detail |
| `manage_get_indexes_and_metadata()` | Index/metadata registry |

### Manage ZCatalogIndex Entries

Default roles: **Manager**

| Method | Notes |
|---|---|
| `addIndex(name, type, extra)` | Register a new index |
| `delIndex(name)` | Remove an index |
| `addColumn(name)` | Register metadata column |
| `delColumn(name)` | Remove metadata column |
| `getIndexObjects()` | List index objects |

### Private (Python-only)

These methods are declared `private` and cannot be called
through-the-web or via URL traversal:

| Method | Description |
|---|---|
| `unrestrictedSearchResults()` | Search without security filters |
| `_unrestrictedSearchResults()` | Search without security or queue processing |
| `_listAllowedRolesAndUsers()` | Roles + groups for security filtering |
| `_increment_counter()` | Increment the catalog counter |
| `indexObject()` | Queue-aware index |
| `unindexObject()` | Queue-aware unindex |
| `reindexObject()` | Queue-aware reindex |
| `_indexObject()` | Direct index (IndexQueue processor) |
| `_unindexObject()` | Direct unindex (IndexQueue processor) |
| `_reindexObject()` | Direct reindex (IndexQueue processor) |

## ZCatalog Parity

Permissions are identical to those declared by ZCatalog and CMFPlone's
`CatalogTool`.  After migrating from ZCatalog, any custom role-permission
mappings defined in your site's `portal_catalog` Security tab carry over
unchanged.
