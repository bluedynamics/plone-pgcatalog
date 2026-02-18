# Changelog

## 1.0.0b6 (unreleased)

### Added

- Relevance-ranked search results: SearchableText queries now automatically
  return results ordered by relevance when no explicit `sort_on` is specified.
  Title matches rank highest (weight A), followed by Description (weight B),
  then body text (weight D). Uses PostgreSQL's built-in `ts_rank_cd()` with
  cover density ranking. No extensions required.
  **Note:** Requires a full catalog reindex after upgrade.

## 1.0.0b5

### Added

- Add partial idx JSONB updates for lightweight reindex. [#6]

  - When `reindexObject(idxs=[...])` is called with specific index names (e.g. during `reindexObjectSecurity`), extract only the requested values and register a JSONB merge patch (`idx || patch`) instead of full ZODB serialization + full idx column replacement
  - Avoids `_p_changed = True` and the associated pickle-JSON round-trip for every object in a subtree
  - Uses the new `finalize(cursor)` hook from zodb-pgjsonb to apply partial JSONB merges atomically in the same PG transaction

## 1.0.0b4

### Added

- **Language-aware full-text search**: SearchableText now uses per-object
  language for stemming. The `pgcatalog_lang_to_regconfig()` PL/pgSQL function
  maps Plone language codes (ISO 639-1, 30 languages) to PostgreSQL text search
  configurations (e.g. `"de"` → `german`). Falls back to `'simple'` for
  unmapped or missing languages. Non-multilingual sites are unaffected.

  Python mirror: `columns.language_to_regconfig()` for testing/validation.

- **Title/Description text search**: Title and Description queries now use
  tsvector word-level matching instead of exact JSONB containment.
  `catalog(Title="Hello")` now correctly matches `"Hello World"`.
  Backed by GIN expression indexes with `'simple'` config (no stemming).

- **Automatic addon ZCTextIndex support**: Addon-registered ZCTextIndex fields
  are automatically discovered at startup. GIN expression indexes are created
  dynamically by `_ensure_text_indexes()`, and queries use tsvector matching --
  zero addon code needed.

### Fixed

- **Title/Description query broken**: Previously, querying Title or Description
  as ZCTextIndex used JSONB exact containment (`idx @> '{"Title":"Hello"}'`),
  which only matched exact values, not words within text. Now uses
  `to_tsvector`/`plainto_tsquery` for proper word-level matching.

## 1.0.0b3

### Fixed

- **Snapshot consistency**: Catalog read queries now route through the ZODB
  storage instance's PG connection, sharing the same REPEATABLE READ snapshot
  as `load()` calls. Previously, catalog queries used a separate autocommit
  connection that could see a different database state than ZODB object loads
  within the same request.

  New internal API:
  - `config.get_storage_connection(context)` — retrieves the PG connection
    from `context._p_jar._storage.pg_connection`.
  - `PlonePGCatalogTool._get_pg_read_connection()` — prefers storage
    connection, falls back to pool for non-ZODB contexts (tests, scripts).

  `CatalogSearchResults` now accepts a `conn` parameter (was `pool`) for
  lazy idx batch loading, using the same connection directly.

## 1.0.0b2

### Security

- **SQL identifier validation**: Added `validate_identifier()` in `columns.py`
  to reject unsafe SQL identifiers. All `idx_key` values in `IndexRegistry`
  and `date_attr` in `DateRecurringIndexTranslator` are now validated.

- **Access control declarations**: Added `declareProtected` for management
  methods (`refreshCatalog`, `reindexIndex`, `clearFindAndRebuild`) and
  `declarePrivate` for `unrestrictedSearchResults` on `PlonePGCatalogTool`.

- **API safety**: Renamed `execute_query()` to `_execute_query()` to mark as
  internal API. Capped path query list size to 100 (DoS prevention).
  Documented security contract for `IPGIndexTranslator` implementations.

### Fixed

- **Savepoint-aware pending store**: The thread-local pending catalog data
  now participates in ZODB's transaction lifecycle via `ISavepointDataManager`.
  Fixes two bugs: pending data not reverting on savepoint rollback, and
  stale pending data leaking across transactions after abort.

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
