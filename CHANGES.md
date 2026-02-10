# Changelog

## 1.0.0b1 Initial release (2026-02-10)

### Changed

- **ZCatalog BTree write elimination**: Removed `super()` delegation in
  `indexObject()`, `reindexObject()`, `catalog_object()`, and
  `uncatalog_object()`.  All catalog data now flows exclusively to
  PostgreSQL via `CatalogStateProcessor` — no BTree/Bucket objects are
  written to ZODB.  Content creation dropped from 175 ms/doc to
  68.5 ms/doc (2.5x faster), making PGCatalog 1.13x faster than
  RelStorage+ZCatalog for writes.

### Added

- **Dynamic IndexRegistry**: Replaced static `KNOWN_INDEXES` dict with a
  dynamic `IndexRegistry` that discovers indexes from ZCatalog at startup
  via `sync_from_catalog()`. Addons that add indexes via `catalog.xml`
  profiles are now automatically supported without code changes.

  - `META_TYPE_MAP` maps ZCatalog meta_types (FieldIndex, KeywordIndex,
    DateIndex, etc.) to `IndexType` enum values.
  - `SPECIAL_INDEXES` (`SearchableText`, `effectiveRange`, `path`) have
    dedicated PG columns and are excluded from idx JSONB extraction.
  - Registry entries are 3-tuples: `(IndexType, idx_key, source_attrs)`,
    where `source_attrs` supports `indexed_attr` differing from index name.
  - Startup sync via `_sync_registry_from_db()` populates the registry
    from each Plone site's `portal_catalog` before the first request.

- **IPGIndexTranslator utility**: Named utility interface for custom index
  types not covered by `META_TYPE_MAP`. Wired into `query.py` (query +
  sort fallback) and `catalog.py` (extraction fallback).

- **DateRecurringIndex support**: Built-in translator for
  `Products.DateRecurringIndex` (Plone's `start` / `end` event indexes).
  Stores base date + RFC 5545 RRULE string in idx JSONB; queries use
  [rrule_plpgsql](https://github.com/sirrodgepodge/rrule_plpgsql) (pure
  PL/pgSQL, no C extensions) for recurrence expansion at query time.
  Translators are auto-discovered from ZCatalog at startup -- no manual
  configuration needed. Container-friendly: works on standard `postgres:17`
  images without additional extensions.

- **DDL via `get_schema_sql()`**: `CatalogStateProcessor` now provides DDL
  through the `get_schema_sql()` method, applied by `PGJsonbStorage` using
  its own connection — no REPEATABLE READ lock conflicts during startup.

- **Transactional catalog writes**: `catalog_object()` sets a
  `_pgcatalog_pending` annotation on persistent objects. The
  `CatalogStateProcessor` extracts this annotation during ZODB commit and
  writes catalog columns (`path`, `parent_path`, `path_depth`, `idx`,
  `searchable_text`) atomically alongside the object state.

- **PlonePGCatalogTool**: PostgreSQL-backed `portal_catalog` replacement
  for Plone, inheriting from `Products.CMFPlone.CatalogTool`. Registered
  via GenericSetup `toolset.xml`.

- **plone.restapi compatibility**: `CatalogSearchResults` inherits
  `ZTUtils.Lazy.Lazy` for serialization; `PGCatalogBrain` implements
  `ICatalogBrain` for `IContentListingObject` adaptation.
