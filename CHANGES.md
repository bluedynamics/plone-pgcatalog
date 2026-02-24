# Changelog

## 1.0.0b11 (unreleased)

### Fixed

- Fix example `requirements.txt`: use local editable path for
  `pgcatalog-example` instead of bare package name (not on PyPI).
  Fixes #18.

- Fix ZMI "Update Catalog" and "Clear and Rebuild" buttons returning 404.
  Added missing `manage_catalogReindex` and `manage_catalogRebuild` methods.
  Fixes #19.

- Fix `clearFindAndRebuild` indexing non-content objects (e.g. `acl_users`).
  Now filters for contentish objects only (those with a `reindexObject` method),
  matching Plone's `CatalogTool` behavior.
  Fixes #20.

### Changed

- `uniqueValuesFor(name)` is now a supported API (no longer deprecated).
  It delegates to `catalog.Indexes[name].uniqueValues()`.

## 1.0.0b10

### Changed

- **Clean break from ZCatalog**: `PlonePGCatalogTool` no longer inherits
  from `Products.CMFPlone.CatalogTool` (and transitively `ZCatalog`,
  `ObjectManager`, etc.).  The new base classes are `UniqueObject + Folder`,
  providing a minimal OFS container for index objects and lexicons while
  eliminating the deep inheritance chain.

  This improves query performance by ~2x across most scenarios (reduced
  Python-side overhead from attribute lookups, security checks, and
  Acquisition wrapping) and write performance by ~5% (lighter commit path).

  A `_CatalogCompat` persistent object provides `_catalog.indexes` and
  `_catalog.schema` for backward compatibility with code that accesses
  ZCatalog internal data structures.  Existing ZODB instances with the old
  `_catalog` (full `Catalog` object) continue to work without migration.

- **ZCML override for eea.facetednavigation**: Moved from `<includeOverrides>`
  inside `configure.zcml` to a proper `overrides.zcml` at the package root,
  loaded by Zope's `five:loadProductsOverrides`.  Fixes ZCML conflict errors
  when both eea.facetednavigation and plone.pgcatalog are installed.

### Added

- **eea.facetednavigation adapter**: `PGFacetedCatalog` in
  `addons_compat/eeafacetednavigation.py` -- PG-backed `IFacetedCatalog`
  that queries `idx` JSONB directly for faceted counting.  Dispatches by
  `IndexType` (FIELD, KEYWORD, BOOLEAN, UUID, DATE) with `IPGIndexTranslator`
  fallback.  Falls back to the default BTree-based implementation when the
  catalog is not `IPGCatalogTool`.  Conditionally loaded only when
  `eea.facetednavigation` is installed.

- **Deprecated proxy methods**: `search()` proxies to `searchResults()` and
  `uniqueValuesFor()` proxies to `Indexes[name].uniqueValues()`, both
  emitting `DeprecationWarning`.

- **Blocked methods**: `getAllBrains`, `searchAll`, `getobject`,
  `getMetadataForUID`, `getMetadataForRID`, `getIndexDataForUID`,
  `index_objects` raise `NotImplementedError` with descriptive messages.

- **AccessControl security declarations**: Comprehensive Zope security
  matching ZCatalog's permission model.  `Search ZCatalog` on read
  methods (`searchResults`, `__call__`, `getpath`, `getrid`, etc.),
  `Manage ZCatalog Entries` on write methods (`catalog_object`,
  `uncatalog_object`, `refreshCatalog`, etc.), `Manage ZCatalogIndex
  Entries` on index management (`addIndex`, `delIndex`, `addColumn`,
  `delColumn`, `getIndexObjects`).  `setPermissionDefault` assigns
  default roles (`Anonymous` for search, `Manager` for management).
  Private helpers (`indexObject`, `reindexObject`, etc.) declared
  private.

- **DateRangeInRangeIndex support**: Native `IPGIndexTranslator` for
  `Products.DateRangeInRangeIndex` overlap queries. Translates
  `catalog({'my_idx': {'start': dt1, 'end': dt2}})` into a single SQL
  overlap clause (`obj_start <= q_end AND obj_end >= q_start`).
  Supports recurring events: when the underlying start index is a
  DateRecurringIndex with RRULE, uses `rrule."between"()` with duration
  offset for occurrence-level overlap detection.  Auto-discovered at
  startup — no configuration needed.  Allows dropping the
  `Products.DateRangeInRangeIndex` addon while keeping the same query API.

### Fixed

- **Addon index preservation**: Installing plone.pgcatalog on a site with
  addon-provided catalog indexes (e.g. from `collective.taxonomy`,
  `plone.app.multilingual`, etc.) no longer silently drops those index
  definitions. The install step now snapshots all existing index definitions
  and metadata columns before replacing `portal_catalog`, then restores
  addon indexes after re-applying core Plone profiles. Removed `toolset.xml`
  in favour of a setuphandler-controlled replacement for correct timing.

## 1.0.0b9

### Changed

- **ZMI polish**: All ZMI tabs now use Bootstrap 4 cards/tables matching
  Zope 5's modern look (was old-style `<table>` layout with `section-bar`).

- **Catalog tab** (`manage_catalogView`): Replaced inherited ZCatalog
  BTree-based view with PG-backed version. Shows catalog summary (object
  count, index/metadata count, search backend with BM25/Tsvector status),
  path filter, and server-side paginated object table (20/page) with
  Previous/Next navigation. Object detail shows full idx JSONB and
  searchable text preview.

- **Advanced tab** (`manage_catalogAdvanced`): Simplified to only show
  Update Catalog and Clear and Rebuild actions. Removed ZCatalog-specific
  features (subtransactions, progress logging, standalone Clear Catalog)
  that don't apply to PostgreSQL.

- **Indexes & Metadata tab** (`manage_catalogIndexesAndMetadata`): Merged
  the separate Indexes and Metadata tabs into one read-only view showing
  all registered indexes (name, type, PG storage location, source attrs)
  and metadata columns.  Reflects the IndexRegistry rather than BTree
  counts (which were always 0).

- **Removed tabs**: Query Report, Query Plan (BTree timing), and the
  separate Indexes / Metadata tabs are hidden — replaced by PG-aware
  equivalents.

- **Lexicon cleanup**: `setuphandlers.install()` now removes orphaned
  ZCTextIndex lexicons (`htmltext_lexicon`, `plaintext_lexicon`,
  `plone_lexicon`) created by Plone's `catalog.xml` — unused with
  PG-backed text search.

## 1.0.0b8

### Changed

- **Module split**: `config.py` has been split into four focused modules:
  `pending.py` (thread-local pending store + savepoint support),
  `pool.py` (connection pool discovery + request-scoped connections),
  `processor.py` (`CatalogStateProcessor`),
  `startup.py` (`IDatabaseOpenedWithRoot` subscriber + registry sync).
  `config.py` is now a deprecation stub.

- **Shared `ensure_date_param()`**: Deduplicated date coercion utility from
  `query.py` and `dri.py` into `columns.ensure_date_param()`.

- **`__all__` exports**: Added explicit `__all__` to `pending.py`, `pool.py`,
  `processor.py`, `startup.py`, `columns.py`, `backends.py`, `interfaces.py`.

- **Top-level imports**: Removed unnecessary deferred imports across
  `catalog.py`, `processor.py`, `startup.py`.

### Added

- `verifyClass`/`verifyObject` tests for `IPGIndexTranslator` implementations.

- Shared `query_zoids()` test helper in `conftest.py`.

### Security

Security review fixes (addresses #11):

- **CAT-C1:** Replace f-string DDL in `BM25Backend.install_schema()` with
  `psycopg.sql.SQL`/`Identifier`/`Literal` composition. Validate language
  codes against `LANG_TOKENIZER_MAP` allowlist + `validate_identifier()` on
  all generated column/index/tokenizer names.
- **CAT-H1:** Clamp `sort_limit`/`b_size` to `_MAX_LIMIT` (10,000) and
  `b_start` to `_MAX_OFFSET` (1,000,000) to prevent resource exhaustion.
- **CAT-H2:** Validate RRULE strings in `DateRecurringIndexTranslator.extract()`
  against RFC 5545 pattern and `_MAX_RRULE_LENGTH` (1,000) before storing.
- **CAT-H3:** Truncate full-text search queries to `_MAX_SEARCH_LENGTH` (1,000)
  to prevent excessive tsvector parsing.
- **CAT-M1:** Replace f-string SQL in `clear_catalog_data()` with
  `psycopg.sql.Identifier` for extra column names.
- **CAT-M2:** Add `conn.closed` guard in `release_request_connection()` to
  handle already-closed connections; document pool leak recovery in docstring.
- **CAT-M3:** Add defensive `validate_identifier(index_name)` in
  `DateRecurringIndexTranslator.query()`.
- **CAT-L1:** Simplify error messages to not expose internal limit values.
- **CAT-L2:** Add rate limiting guidance note in `searchResults()` docstring.
- **CAT-L3:** Normalize double slashes in `_validate_path()`.

## 1.0.0b7

### Fixed

- `sort_on` now accepts a list of index names for multi-column sorting,
  matching ZCatalog's API. `sort_order` can also be a list (one direction
  per sort key) or a single string applied to all keys.

- `PGCatalogBrain.__getattr__` now distinguishes known catalog fields from
  unknown attributes. Known indexes and metadata columns return `None` when
  absent from idx (matching ZCatalog's Missing Value behavior), while unknown
  attributes raise `AttributeError`. This enables
  `CatalogContentListingObject.__getattr__` to fall back to `getObject()`
  for non-catalog attributes (e.g. `content_type`), and fixes PAM's
  `get_alternate_languages()` viewlet crash on `brain.Language`.

- `reindexIndex` now accepts `pghandler` keyword argument for compatibility
  with ZCatalog's `manage_reindexIndex` and plone.distribution. The argument
  is accepted but ignored (PG-based reindexing doesn't need progress
  reporting). [#9]

- `clearFindAndRebuild` now properly rebuilds the catalog by traversing all
  content objects after clearing PG data. Previously only cleared without
  rebuilding.

- `refreshCatalog` now properly re-catalogs objects by resolving them from
  ZODB and re-extracting index values. Added missing `pghandler` parameter
  for ZCatalog API compatibility.

- Fixed `ConnectionStateError` on Zope restart when a Plone site already
  exists in the database. `_sync_registry_from_db` and
  `_detect_languages_from_db` now abort the transaction before closing
  their temporary ZODB connections.

- `_ensure_catalog_indexes` now checks for essential Plone indexes (UID,
  portal_type) instead of any indexes, preventing addon indexes from
  blocking re-application of Plone defaults.

- ZCatalog internal API compatibility: `getpath(rid)`, `getrid(path)`,
  `Indexes["UID"]._index.get(uuid)`, and `uniqueValues(withLengths=True)`
  now work with PG-backed data. Uses ZOID as the record ID. This fixes
  `plone.api.content.get(UID=...)`, `plone.app.vocabularies` content
  validation, and dexterity type counting in the control panel.

## 1.0.0b6

### Added

- Relevance-ranked search results: SearchableText queries now automatically
  return results ordered by relevance when no explicit `sort_on` is specified.
  Title matches rank highest (weight A), followed by Description (weight B),
  then body text (weight D). Uses PostgreSQL's built-in `ts_rank_cd()` with
  cover density ranking. No extensions required.
  **Note:** Requires a full catalog reindex after upgrade.

- Optional BM25 ranking via VectorChord-BM25 extension. When `vchord_bm25`
  and `pg_tokenizer` extensions are detected at startup, search results are
  automatically ranked using BM25 (IDF, term saturation, length normalization)
  instead of `ts_rank_cd`. Title matches are boosted via combined text.
  Vanilla PostgreSQL installations continue using weighted tsvector
  ranking with no changes needed.
  **Requires:** `vchord_bm25` + `pg_tokenizer` PostgreSQL extensions.
  **Note:** Full catalog reindex required after enabling.

- Per-language BM25 columns: each configured language gets its own
  `bm25vector` column with a language-specific tokenizer. Supports
  30 Snowball stemmers (Arabic to Yiddish), jieba (Chinese), and
  lindera (Japanese/Korean). Configure via `PGCATALOG_BM25_LANGUAGES`
  environment variable (comma-separated codes, or `auto` to detect from
  portal_languages). Fallback column for unconfigured languages ensures
  BM25 ranking benefits for all content.
  **Note:** Changing languages requires full catalog reindex.

- `SearchBackend` abstraction: thin interface for swappable search/ranking
  backends. `TsvectorBackend` (always available) and `BM25Backend` (optional).
  Backend auto-detected at Zope startup.

- `LANG_TOKENIZER_MAP` in `backends.py` maps ISO 639-1 codes to pg_tokenizer
  configurations. Regional variants (pt-br, zh-CN) are normalized to base
  codes automatically.

- Estonian (`et`) added to language-to-regconfig mapping (supported by PG 17).

- Multilingual example: `create_site.py` zconsole script creates a Plone
  site with `plone.app.multilingual` (EN, DE, ZH), installs plone.pgcatalog,
  and imports ~800+ Wikipedia geography articles across all three languages
  with PAM translation linking. `fetch_wikipedia.py` fetches articles from
  en/de/zh Wikipedia with cross-language links. See `example/README.md`.

### Fixed

- `reindexObjectSecurity` now works for newly created objects.
  `unrestrictedSearchResults` extends PG results with objects from the
  thread-local pending store (not yet committed to PG) for path queries.
  Previously, newly created objects were invisible to the path search in
  `CMFCatalogAware.reindexObjectSecurity`, so their security indexes
  (e.g. `allowedRolesAndUsers`) were never updated during workflow
  transitions in the same transaction.

- `CatalogSearchResults` now implements `IFiniteSequence`, enabling
  `IContentListing` adaptation in Plone's search view.

- `PGCatalogBrain` now provides `getId` (property) and `pretty_title_or_id()`
  for compatibility with Plone's Classic UI navigation and search templates.
  `getId` is a property (not a method) so `brain.getId` returns a string,
  matching standard ZCatalog brain behavior.

- `PGCatalogBrain.__getattr__` returns `None` for missing idx keys instead
  of raising `AttributeError`, matching ZCatalog's Missing Value behavior.
  Fixes PAM's `get_alternate_languages()` viewlet crash on `brain.Language`.

- Unknown catalog indexes (e.g. `Language`, `TranslationGroup` from
  plone.app.multilingual) now fall back to JSONB field queries instead of
  being silently skipped. This enables PAM's translation registration and
  lookup queries to work correctly.

- CJK tokenizer TOML format fixed: jieba (Chinese) and lindera
  (Japanese/Korean) now use the correct table syntax for pg_tokenizer's
  `pre_tokenizer` configuration.

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
  - `pool.get_storage_connection(context)` — retrieves the PG connection
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
