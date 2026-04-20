# Changelog

## 1.0.0b56

### Fixed

- The v1->v2 upgrade step was silently no-op on production sites.  GenericSetup
  invokes upgrade handlers with the ``portal_setup`` tool as the context, but
  ``_resolve_compat`` only understood the ``ImportContext`` shape (``getSite()``),
  so the setup-tool call path hit the ``return None, None`` branch and logged
  ``migrate_catalog_indexes: no _CatalogCompat found; skipping`` — while
  GenericSetup happily bumped the profile version to 2.  Net effect: the
  persisted ``_CatalogCompat`` kept its legacy ``indexes`` attribute and the
  new ``indexes`` property then raised ``AttributeError`` for ``_raw_indexes``,
  which Acquisition swallowed and replaced with the tool's ``indexes()``
  method — surfacing as ``'function' object has no attribute 'keys'`` from
  ``catalog.indexes.keys()`` and ``'method' object is not subscriptable`` from
  ``catalog.indexes[name]``.

  ``_resolve_compat`` now also walks ``aq_parent(context)`` to reach the Plone
  site when the context is the setup tool, so the normal ZMI
  ``manage_upgrades`` path migrates the state as intended.

- Made ``_CatalogCompat.indexes`` self-healing: if the legacy ``indexes``
  attribute is still in ``__dict__`` (unmigrated or fresh-install site that
  skipped v1), the property moves it to ``_raw_indexes`` on first access and
  marks the instance dirty.  This avoids the Acquisition-swallowed
  ``AttributeError`` failure mode even when the upgrade step never ran.

  Closes #139.

## 1.0.0b55

### Fixed

- Plone and addon code commonly reaches into the catalog via the non-API-
  conform pattern ``catalog._catalog.indexes[name]`` / ``.get(name)`` /
  ``.items()``.  Previously this returned the raw ZCatalog index objects
  with empty BTrees, so queries against them silently returned no results.
  ``_CatalogCompat.indexes`` is now a property returning a transient view
  that wraps each index with ``PGIndex`` (same behavior as
  ``catalog.Indexes[name]``).  Custom ``PATH``-type indexes and other
  special indexes (``idx_key=None``) continue to be returned raw, since
  they have dedicated typed columns and don't need PG-backed wrapping.

  **Migration:** GenericSetup profile bumped from v1 to v2.  The upgrade
  step renames the persisted ``indexes`` attribute to ``_raw_indexes``
  and sets ``__parent__`` on the compat so ``aq_parent`` can reach the
  catalog tool through bare attribute access.  Run
  *Plone Site Setup → Add-ons → plone.pgcatalog → Upgrade* on existing
  sites, or let the next ``runAllImportSteps`` on the default profile
  pick it up.

  Likely-affected callers include
  ``plone.base.utils.check_id`` (reserved-name check),
  ``plone.restapi.search.query.Query.get_index``,
  ``plone.app.discussion``, ``plone.app.referenceablebehavior``,
  ``plone.volto``, ``collective.collectionfilter``, and
  ``collective.exportimport`` — per-package verification is
  recommended after upgrade.

  Based on prior prototyping by @thet on ``thet/indexes-wrapper``.
  Closes #137.

## 1.0.0b54

### Changed

- Stop duplicating `path`, `path_parent`, and `path_depth` between the typed
  columns on `object_state` and the `idx` JSONB.  These three fields now live
  exclusively in their typed columns (`path`, `parent_path`, `path_depth`) —
  previously identical values were stored in both places, wasting ~10 % of
  JSONB storage and (more importantly) blocking the planner from collecting
  selectivity statistics on path-subtree filters.  Indexes and extended
  statistics on these fields have been migrated to reference the typed columns
  directly.  Custom `PATH`-type indexes (e.g. `tgpath`) are unaffected and
  continue to store their data in `idx`.

  **Migration:** Schema and writer changes are picked up automatically on
  startup (the eight affected indexes and three extended-statistics objects
  are reissued with idempotent `DROP … IF EXISTS` / `CREATE … IF NOT EXISTS`
  pairs).  To strip the obsolete keys from existing JSONB on large catalogs,
  run:

  ```python
  from plone.pgcatalog.migrations.strip_path_keys import run
  run(conn, batch_size=5000)
  ```

  Safe to run online, idempotent, batched.  Issue #132.

## 1.0.0b53

### Fixed

- Migration install handler silently dropped every
  `DateRecurringIndex` (DRI) — e.g. `plone.app.event`'s / `bda.aaf.site`'s
  `general_start` / `general_end` — when replacing a foreign
  `portal_catalog` with `PlonePGCatalogTool`.  `_snapshot_catalog`
  correctly captured the stored `attr_recurdef` / `attr_until`
  attributes, but `_build_extra` had no DRI branch, so the restored
  `extra` namespace lacked the `recurdef` / `until` keys that
  `DateRecurringIndex.__init__` reads.  The constructor raised
  `AttributeError`, the outer `try/except` in `_restore_from_snapshot`
  swallowed it as a warning, and the index was never created — which
  meant `extract_idx` never indexed those fields, the IndexRegistry
  had no entry for them, and every site-wide Collection filtering on
  `general_end` returned zero results.

  Added the DRI translation in `_build_extra`, plus a roundtrip test
  that actually instantiates `DateRecurringIndex` with the built extra
  — the kind of assertion that would have caught this before it ever
  shipped.  Issue #126.

  Existing deployments that migrated on an affected build have to
  re-add the missing indexes manually (the upgrade can't recover them
  without the original catalog snapshot).  Run:

  ```python
  catalog = portal.portal_catalog
  class _Extra: pass
  extra = _Extra()
  extra.recurdef = "recurrence"
  extra.until = ""
  for name in ("general_start", "general_end"):
      if name not in catalog._catalog.indexes:
          catalog.addIndex(name, "DateRecurringIndex", extra)
  catalog.reindexIndex("general_start")
  catalog.reindexIndex("general_end")
  ```

### Added

- Slow-query suggestions now produce covering composite indexes for the
  common `portal_type + effectiveRange + sort_on=effective` pattern
  (issue #122).  The suggestion engine splits the legacy
  `_NON_IDX_FIELDS` into purpose-specific constants, expands
  `effectiveRange` to its `effective` date contributor, and appends the
  query's `sort_on` field as a trailing btree composite column so the
  planner can skip the ORDER BY sort step.

## 1.0.0b52

### Fixed

- `CatalogStateProcessor._enqueue_tika_jobs` indexed result rows by
  integer position (`row[0]`, `row[1]`), but the request-scoped
  connection pool uses a `dict_row` factory, so every content save
  that produced an unresolved blob ref raised `KeyError: 0` during
  `tpc_vote` (e.g. uploading a Dexterity Image).  Switched to
  column-name access.

  Existing tests didn't catch this because the integration tests
  opened their cursor with `tuple_row` and the unit tests mocked
  `fetchall()` with tuple rows — both diverged from production.
  Tests updated to use `dict_row` to match the real pool.

## 1.0.0b51

### Added

- Extended PostgreSQL statistics for every default composite catalog
  index on `object_state`, so the planner has accurate joint-selectivity
  estimates for the expression pairs we actually index.  `mcv +
  dependencies` for low-cardinality pairs (`type + state`, `parent +
  type`, `type + effective`, `type + expires`), `dependencies` only
  for path-pairs (high-cardinality paths make `mcv` wasteful; CMS
  content structure is typically wide-shallow, so dependency signal
  is what matters).

  Without these, PG's per-column histograms treat the expressions as
  independent and underestimate joint selectivity, so the planner
  picks a composite-index scan and heap-filters thousands of tuples
  instead of doing a Bitmap-AND with the available GIN indexes.  On a
  published-Event navigation query observed in production, this dropped
  query time from 911 ms to sub-100 ms.

  On existing installations a one-shot `ANALYZE object_state` runs on
  the first write transaction after upgrade so the new statistics take
  effect immediately rather than waiting for autovacuum.  Idempotent
  via `pg_stats_ext` skip check.

  Issue #122 (PR 1 of 3 — engine refactor and EXPLAIN-driven coverage
  to follow).

## 1.0.0b50

### Fixed

- ``release_request_connection`` now issues an explicit
  ``conn.rollback()`` before returning the connection to the pool.
  Otherwise an implicit transaction opened by a prior ``SELECT`` on
  the pool fallback path stays alive, holding a ``virtualxid`` that
  blocks ``CREATE INDEX CONCURRENTLY``.  Companion fix to
  bluedynamics/zodb-pgjsonb#58 (the storage-conn path).  Closes #118.

- Suggested Indexes UI: detect already-applied suggestions with
  mixed-case field names (e.g. `Language`) by matching index names
  case-insensitively — PostgreSQL folds unquoted identifiers to
  lowercase.  Also strengthen expression normalization (whitespace
  around `->>`, iterative paren collapse, WHERE-anchored extraction)
  so generated and PG-stored `indexdef` forms compare equal.
  `apply_index` is now idempotent when a valid index with the same
  name already exists — returns success no-op instead of propagating
  the `DuplicateTable` error.  Closes #119.

- Tika enqueue: resolve Dexterity `NamedBlobFile` / `NamedBlobImage`
  wrapper OIDs via a second-hop lookup through `object_state`, so the
  queue receives jobs for modern Dexterity File/Image content.
  Previously `_enqueue_tika_jobs()` only looked up the OIDs it found
  in the content's state — which are the wrapper OIDs, not the inner
  `ZODB.blob.Blob` OIDs.  The direct lookup returned zero rows and the
  enqueue silently skipped.  Flat-state content (legacy/Archetypes-
  style, where the content state carries a direct `ZODB.blob.Blob`
  `@ref`) is unchanged.  Closes #115.

- `_handle_uuid` now accepts list/tuple queries (uses `= ANY(...)`),
  matching `_handle_field` semantics.  Previously a list query such as
  `catalog.searchResults(UID=['f852...'])` was stringified as
  `str(['f852...'])` → `"['f852...']"` and the JSONB `->>` comparison
  never matched, so `@@getVocabulary?name=plone.app.vocabularies.Catalog`
  with a `plone.app.querystring.operation.list.contains` criterion on
  UID returned an empty vocabulary.

- `catalog._catalog.getIndex(name)` now returns a `PGIndex` wrapper
  with PG-backed `_index` and `uniqueValues()`, same as
  `catalog.Indexes[name]`.  Previously it returned the raw ZCatalog
  index with empty BTrees, which broke:

  - `plone.app.vocabularies.KeywordsVocabulary` (empty Subject/Tags
    dropdowns).
  - `Products.CMFPlone.browser.search.Search.types_list()` (empty
    "Item type" filter in `@@search`).
  - `plone.app.event.setuphandlers` (DateIndex detection).
  - Other Plone code paths that bypass `catalog.Indexes[name]`.

  Special indexes registered with `idx_key=None` (SearchableText,
  path, effectiveRange) are returned unwrapped so dedicated columns
  are used for them.

## 1.0.0b49

### Added

- Log all catalog queries for debugging via
  `PGCATALOG_LOG_ALL_QUERIES=1` (also accepts `true`/`yes`).  Enabled
  queries are logged at INFO level with duration, SQL, params, and
  query keys.  Params are truncated at 2000 chars to bound log size.
  The env var is re-checked on every query, so the setting can be
  toggled at runtime without a restart.  See
  `docs/how-to/debug-queries.md` for details and a production-safety
  warning about logging user-supplied query values.

- Slow-query log format changed slightly: the prefix is now
  `Slow SQL catalog query (%.2f ms)` instead of
  `Slow catalog query (%.1f ms)` — log-aggregation grep patterns may
  need an update.

### Fixed

- `clearFindAndRebuild()` now works on fresh installs and after refactorings
  that leave `object_state.path` empty.  Previously the rebuild relied on a
  PG snapshot of `WHERE path IS NOT NULL`, so when the column was not yet
  populated only the Plone root was re-indexed.  The rebuild now walks the
  `ISiteRoot` breadth-first regardless of PG state.

  The new traversal is still memory-flat: the BFS queue holds only path
  strings (not objects), and objects are ghosted by `cacheMinimize()`
  after every 500 commits.  Also yields discussion items via the
  `IConversation` adapter when `plone.app.discussion` is installed, so
  comments on content are included in the rebuild.

- Stringify Boolean query values to JSON notation (`'true'`/`'false'`) so
  queries against JSONB `->>` comparisons match.  Previously `str(True)`
  produced `'True'` which never matched JSONB's lowercase form, causing
  queries to return no results.  Fix applied in `query.py` (all field
  handlers), `pgindex.py` (ZCatalog `_index.get()` compat),
  `addons_compat/eeafacetednavigation.py` (faceted search dispatch),
  and `backends.py` (text search backends).  Helper renamed from
  `_to_json_string` to `_bool_to_lower_str` to match what it actually does.

- Gracefully handle missing `meta` column in `_load_idx_batch()` (#105).
  Falls back to `SELECT zoid, idx` if the column does not exist yet,
  preventing `UndefinedColumn` crash on first read after upgrade.
  Root cause fix is in zodb-pgjsonb 1.10.4 (`poll_invalidations` now
  applies deferred DDL before the read snapshot).

- Show index creation errors in red instead of green in ZMI (#104).
  `manage_apply_index` / `manage_drop_index` now redirect with
  `index_error` param on failure, rendered as Bootstrap `alert-danger`.

## 1.0.0b48

### Fixed

- Fix startup warnings "security declaration for nonexistent method" for
  unsupported ZCatalog stubs (`getAllBrains`, `searchAll`, etc.).
  `ObjectManager.__class_init__` calls `InitializeClass` at class creation
  time, so stub methods must be defined in the class body, not via post-hoc
  `setattr`.

### Changed

- Extract `@meta`, `object_provides`, and `allowedRolesAndUsers` from
  `idx` JSONB into dedicated columns via generic `ExtraIdxColumn` mechanism.
  Reduces `idx` size by ~85% (from ~3.2 KB to ~400 B avg, below TOAST
  threshold). Run `clear_and_rebuild` after upgrading. (#98)
- `object_provides` queries now use a dedicated `TEXT[]` column with GIN
  index instead of JSONB containment.
- Removed `_backfill_allowed_roles` startup function (superseded by
  generic extraction mechanism).

## 1.0.0b47

### Fixed

- Fix `ValueError: Invalid path: ''` when path query receives empty
  string. Empty/blank paths are now silently filtered, matching
  ZCatalog behavior.

## 1.0.0b46

### Fixed

- Query cache: use catalog-specific change counter instead of
  `MAX(tid)` (#94). The cache was invalidated on every ZODB write
  (~2500/hour from ScalesDict alone), making it nearly useless (~28%
  hit rate). Now uses `pgcatalog_change_seq` which only increments
  on actual catalog writes (catalog_object, uncatalog, reindex, move).
  Expected hit rate 90%+ on typical sites.

## 1.0.0b45

### Fixed

- Fix Tika queue never populated: `content_type` always None (#90).
  Removed broken `extract_content_type()` — `IPrimaryFieldInfo` can't
  adapt the indexer wrapper and Dexterity items have no top-level
  `content_type` attribute. MIME type is now read directly from
  `idx["mime_type"]` (the standard Plone catalog index), which is
  reliably extracted by the IndexRegistry.

- Fix suggestion index existence check + dedicated KEYWORD fields (#92).
  `_check_covered()` now compares by index name (reliable) with
  normalized expression fallback. `object_provides` and `Subject`
  added to `_DEDICATED_FIELDS` — their existing GIN indexes make new
  suggestions useless.

## 1.0.0b44

### Added

- Smart index suggestions in ZMI Slow Queries tab (#86). Replaces the
  naive `_suggest_index()` with field-type-aware suggestions using the
  IndexRegistry. Generates correct DDL per IndexType (btree expression,
  GIN, tsvector, composites). Detects already-covered fields (dedicated
  columns, existing indexes). Manual "Apply" button creates indexes via
  `CREATE INDEX CONCURRENTLY`. "Drop" button for removing suggestion
  indexes (`idx_os_sug_*`). On-demand EXPLAIN plans for slow queries.
  New `suggestions.py` module with pure suggestion engine + DB helpers.

## 1.0.0b43

### Fixed

- Fix Tika enqueue: `_collect_ref_oids()` and the `ANNOTATION_KEY`
  fallback in `CatalogStateProcessor.process()` now handle JSON
  string state (from `decode_zodb_record_for_pg_json`). Previously
  `state` was assumed to be a dict, but the fast codec path returns a
  JSON string — so `@ref` markers were never found, no extraction
  jobs were enqueued, and Tika sat idle.

## 1.0.0b42

### Fixed

- Handle Unix epoch floats/ints in `ensure_date_param()`. Callers
  like `plone.app.textfield` pass `time.time()` values as date query
  params. Now converts to `datetime.fromtimestamp(value, tz=UTC)`.
  Fixes #82.

- Skip missing attributes instead of storing null in idx JSONB.
  Matches ZCatalog semantics: missing attribute = not indexed (key
  omitted), not "indexed as null". Fixes #81.

- Use `@>` containment for single-value KeywordIndex queries instead
  of `?|` overlap. The GIN index handles `@>` much better.
  `object_provides` queries: 2.4s to 650ms. Fixes #80.

## 1.0.0b41

### Fixed

- Register refs prefetch expression for cataloged content objects.
  Uses `CASE WHEN idx IS NOT NULL THEN refs END` so only content
  objects (with catalog data) trigger prefetch. Requires
  zodb-pgjsonb >= 1.9.2.

## 1.0.0b40

### Added

- Brain object prefetch via `storage.load_multiple()`. When iterating
  search results and calling `getObject()`, the first call in each batch
  prefetches up to 100 objects in a single SQL query, warming the storage
  cache for subsequent calls. Configurable via `PGCATALOG_PREFETCH_BATCH`
  environment variable (default 100, set to 0 to disable).

## 1.0.0b39

### Fixed

- Log query cache TID lookup failures instead of silently swallowing
  them. Diagnoses why the query cache may not be populating.

## 1.0.0b38

### Fixed

- Extract `allowed_roles` backfill from schema DDL into batched startup
  step (#65 Phase 2). Previously the backfill ran as a single UPDATE
  on 4.4M rows inside ACCESS EXCLUSIVE, blocking the entire database.
  Now processes 5000 rows per batch with autocommit and `lock_timeout`.
  Safe to re-run, idempotent, logs progress.

## 1.0.0b37

### Fixed

- Prevent database lockup during rolling deployments (#65).
  `_ensure_text_indexes()` and `_ensure_field_indexes()` now set
  `lock_timeout = '5s'` to prevent indefinite blocking on ACCESS
  EXCLUSIVE locks from concurrent REPEATABLE READ sessions.
  Log level changed from `error` to `warning` since indexes are
  retried on next startup.

## 1.0.0b36

### Added

- Process-wide query result cache with TID-based invalidation. Caches
  catalog query results in memory. Invalidated when `MAX(tid)` changes
  (any ZODB commit). Cost-based eviction keeps expensive queries in
  cache. Configurable via `PGCATALOG_QUERY_CACHE_SIZE` (default 200)
  and `PGCATALOG_QUERY_CACHE_TTR` (default 60s). Fixes #74.

- ZMI: Cache Status section on the Slow Queries tab showing hit/miss
  rate, entries, invalidations, TTR, and top cached queries by cost.

### Fixed

- `getCounter()` fix: `SELECT MAX(tid)` returns column `max`, not
  `tid`. Added `AS tid` alias. Also fixes the test.

## 1.0.0b35

### Performance

- Add partial index `idx_os_cat_nav_visible` for navigation listings
  (`exclude_from_nav=false` is only ~1.6% of rows). Verified on
  production: 261ms → 20ms (13×).
- Add partial index `idx_os_cat_events_upcoming` for calendar/event
  queries (`portal_type=Event` + `show_in_sidecalendar=true` + end
  date). Verified on production: 728ms → 33ms (22×).
- Mark `pgcatalog_to_timestamptz()` as `PARALLEL SAFE` to allow
  parallel query execution.

### Fixed

- `getCounter()` now returns `MAX(tid)` from PostgreSQL instead of a
  persistent counter that was never incremented (always returned 0).
  This enables Plone's cache invalidation (`plone.memoize`) for
  catalog-dependent caches like navigation trees. ~0.2ms via
  Index Only Scan, no ZODB write overhead.

## 1.0.0b34

### Fixed

- DateRecurringIndex fields (e.g. `start`, `end`) now get auto-created
  btree expression indexes and appear in the ZMI Indexes tab. Added
  `DateRecurringIndex` to `META_TYPE_MAP`. Query builder now checks
  `IPGIndexTranslator` before IndexRegistry, so rrule query logic
  takes priority. Fixes #71.

## 1.0.0b33

### Fixed

- Revert path_parent IN subquery for bounded-depth queries (#68).
  The subquery caused Nested Loop plans where PG repeated the
  `allowed_roles` GIN scan per parent path (615ms). Reverts to
  `LIKE + path_depth` which uses the `path_depth_type` composite
  index (85-300ms depending on cache state).

## 1.0.0b32

### Fixed

- Optimize bounded-depth path queries: rewrite `path LIKE + path_depth`
  to `path_parent IN (subquery)` so PG can use the composite
  `(path_parent, portal_type)` index. Navigation tree queries drop
  from 630ms to ~77ms. Fixes #66.

## 1.0.0b31

### Added

- Denormalize `allowedRolesAndUsers` into dedicated `allowed_roles
TEXT[]` column with GIN index. Security filter queries now use
  `allowed_roles && ARRAY[...]` instead of JSONB decompression.
  Includes automatic backfill migration for existing databases.
  Navigation queries 85ms to 5-15ms, all queries benefit. Fixes #63.

## 1.0.0b30

### Fixed

- Add dedicated GIN indexes for `allowedRolesAndUsers`,
  `object_provides`, and `Subject` keyword fields. The full-idx GIN
  index is too broad for `?|` queries on individual keyword arrays.
  Dedicated indexes are much smaller and faster. `object_provides`
  queries drop from 850ms to sub-millisecond.

## 1.0.0b29

### Fixed

- Use btree-friendly expressions instead of GIN containment for
  FieldIndex single-value, BooleanIndex, and UUIDIndex queries.
  Root cause of 3-4 second navigation queries on large sites.
  Navigation queries drop from 3900ms to <1ms.

- Fix Slow Queries ZMI tab crash with KeyError when
  `PGCATALOG_SLOW_QUERY_MS` is not set. Fixes #58.

- Python 3.14 CI compatibility. Fixes #57.

## 1.0.0b28

### Added

- Auto-create btree expression indexes for custom CatalogIndex fields
  at startup. Only standard Plone fields have hardcoded indexes; custom
  fields (like `general_end`) now get indexes automatically based on
  the IndexRegistry. Date fields use `pgcatalog_to_timestamptz()`
  wrapper. Fixes #49.

## 1.0.0b27

### Fixed

- Add composite indexes for common catalog query patterns. Without
  these, PG picks a single-column index and sequentially filters all
  indexed rows (3+ seconds per query, 30+ second page loads). With
  composite indexes: sub-millisecond. Fixes #50.

  New indexes:
  - `(path_parent, portal_type)` — folder listings, navigation
  - `(path pattern, portal_type)` — collections, search
  - `(path pattern, path_depth, portal_type)` — navigation tree
  - `(portal_type, review_state)` — workflow-filtered listings

  Indexes are created automatically on startup (idempotent DDL).

### Added

- Slow catalog query logging: queries exceeding `PGCATALOG_SLOW_QUERY_MS`
  (default: 10ms) are logged as warnings and recorded in the
  `pgcatalog_slow_queries` table. Fixes #52.

- ZMI "Slow Queries" tab on portal_catalog: shows aggregated slow query
  patterns (count, avg/max duration, last seen) with suggested composite
  index DDL for frequent patterns. Includes a "Clear Stats" button.

## 1.0.0b26

### Added

- ZMI: Tika status card on the Advanced tab showing URL, worker mode,
  configured content types, extraction queue stats (pending/processing/
  done/failed), and IFile transform override status. Fixes #47.

## 1.0.0b25

### Fixed

- `reindexIndex(name)` now re-extracts index values from ZODB objects
  instead of reshuffling existing JSONB values. Iterates all cataloged
  paths, loads via `unrestrictedTraverse`, extracts the requested index,
  and writes a JSONB merge update. Batched commits for memory. Fixes #43.

### Added

- ZMI [reindex] button per index on the Indexes & Metadata tab with
  confirmation dialog. Calls new `manage_reindexIndex` endpoint.

- ZMI: confirmation dialogs on Advanced tab for "Update Catalog" and
  "Clear and Rebuild" buttons. Warns that operations may take a while
  and that Clear and Rebuild destroys catalog data temporarily.
  Fixes #44.

## 1.0.0b24

### Changed

- `clearFindAndRebuild` now uses PG-driven iteration instead of
  `ZopeFindAndApply`. Queries `object_state` directly, filtering out
  known non-content classes (~96% of rows). No acquisition parent
  chains on the call stack means `cacheMinimize()` can ghost all
  objects — flat memory on large sites. Fixes #39.

### Added

- Skip `portal_transforms` text extraction for `IFile` when
  `PGCATALOG_TIKA_URL` is set. The async Tika worker handles blob
  text extraction — no more synchronous pdftotext/wv calls or BFS
  graph traversal of the transform registry during indexing.
  Custom types with blob fields need their own override (see docs).
  Fixes #41.

## 1.0.0b23

### Fixed

- Fix Tika worker queue never being populated. Content objects
  (File/Image) and their Blob sub-objects have different ZODB oids.
  The enqueue logic now extracts `@ref` oids from the content state
  to resolve the actual blob zoid in `blob_state`. The queue stores
  both `zoid` (content, for searchable_text update) and `blob_zoid`
  (for blob data fetch). Fixes #37.

## 1.0.0b22

### Fixed

- Fix high memory usage during catalog rebuild. `clearFindAndRebuild`
  and `refreshCatalog` now commit every 500 objects, flushing dirty
  ZODB objects and pending catalog data so `cacheMinimize()` can
  actually reclaim memory. Previously, `_p_changed = True` on every
  indexed object prevented deactivation until the end of the
  (single) transaction.

## 1.0.0b21

### Fixed

- Reduce memory usage during catalog rebuild. `clearFindAndRebuild` and
  `refreshCatalog` now deactivate ZODB objects after indexing and
  periodically call `cacheMinimize()` to keep RAM usage flat on large
  sites. Folderish objects are kept active during tree traversal to
  avoid redundant reloads.

## 1.0.0b20

### Fixed

- Fix UID expression index using wrong case (`idx->>'uid'` instead of
  `idx->>'UID'`). JSONB keys are case-sensitive, so the old index was
  never used. Also add `CREATE STATISTICS` for UID selectivity so the
  query planner picks the correct index on large tables. Existing
  databases are migrated automatically on next startup. Fixes #28.

## 1.0.0b19

### Removed

- Remove "Blob Storage" ZMI tab from portal_catalog. Blob storage
  statistics are now provided by zodb-pgjsonb >= 1.5.2 in the Zope
  Control Panel under Database management.

## 1.0.0b18

### Fixed

- Fix computed index extraction (`is_folderish`, `is_default_page`,
  `sortable_title`, etc.) always returning `null`. `IPGCatalogTool`
  extended both `ICatalogTool` and `IPloneCatalogTool`, causing
  `ICatalogTool` to come first in the interface resolution order.
  CMFCore's `IndexableObjectWrapper` (which does not resolve
  plone.indexer adapters) won over the plone.indexer wrapper.
  Fixed by extending `IPloneCatalogTool` only — `ICatalogTool` is
  already provided via `IZCatalog`.

## 1.0.0b17

### Security

- **CAT-Q1:** Validate unknown query keys before SQL interpolation in
  `_process_index()` fallback path. Unregistered index names are now
  checked with `validate_identifier()` before being interpolated into
  JSONB field query expressions, preventing potential SQL injection via
  crafted query dict keys.

- **CAT-S1:** Replace f-string DDL in `_ensure_text_indexes()` with
  `psycopg.sql.SQL`/`Identifier`/`Literal` composition for defense-in-depth.

### Changed

- **CAT-P1:** `reindex_index()` now uses a server-side cursor with batched
  fetches instead of loading all rows into memory at once. Progress is
  logged after each batch.

### Fixed

- **CAT-O1:** Index/metadata extraction failures in `extraction.py` now
  emit `log.debug()` messages with field name and exception info instead
  of silently passing. Translator extraction failures are also logged.

- **CAT-O2:** Startup degradation (failed registry sync, failed text index
  creation) now logs at `ERROR` level with actionable context messages
  instead of `WARNING`/`DEBUG`.

- **CAT-L1:** Fallback connection pool (`_fallback_pool` from
  `PGCATALOG_DSN` env var) now registers an `atexit` close hook for
  clean shutdown.

- Install step now runs `clearFindAndRebuild()` after catalog replacement
  to index all existing content into PostgreSQL. Previously, content
  created before pgcatalog was installed (e.g. during Plone site creation)
  had no `path`/`idx` data, causing empty navigation and search results.

## 1.0.0b16

### Added

- Add "Blob Storage" ZMI tab to portal_catalog showing blob statistics
  (total count, size, per-tier breakdown for PG/S3), a logarithmic size
  distribution histogram, and S3 tiering threshold visualization.

## 1.0.0b15

### Fixed

- Protect PlonePGCatalogTool from being replaced during GenericSetup
  profile imports. CMFPlone's baseline `toolset.xml` declares
  `portal_catalog` with `CatalogTool`; since `PlonePGCatalogTool` is a
  different class, the default `importToolset` deletes it, triggering an
  `IObjectModifiedEvent` cascade that raises `KeyError: 'portal_catalog'`.
  Added `importToolset` wrapper in `overrides.zcml` that skips
  `portal_catalog` when it is already a `PlonePGCatalogTool`.

## 1.0.0b14

### Fixed

- Fix new objects not being indexed in PostgreSQL.
  ZODB assigns object IDs (`_p_oid`) during `Connection.commit()`, which runs
  _after_ `before_commit` hooks (where the IndexQueue flushes). All new objects
  therefore have `_p_oid=None` at `catalog_object()` call time, causing the
  catalog to silently skip them. The fix stores pending catalog data directly
  in `obj.__dict__` under the `_pgcatalog_pending` key when no OID is available
  yet; `CatalogStateProcessor.process()` pops and uses it during `store()` so
  the annotation is never persisted to the database. Fixes #27.

## 1.0.0b13

### Fixed

- Preserve original Python types for metadata columns (e.g. `brain.effective`
  now returns a Zope `DateTime` object instead of an ISO string).
  Non-JSON-native metadata values (DateTime, datetime, date, etc.) are
  encoded via the Rust codec into `idx["@meta"]` at write time and restored
  on brain attribute access with per-brain caching. JSON-native values
  (str, int, float, bool, None) remain in top-level `idx` unchanged.
  Backward compatible — old data without `@meta` still works.
  Fixes #23.

## 1.0.0b12

### Fixed

- Fix `clearFindAndRebuild` producing wrong paths (missing portal id prefix,
  e.g. `/news` instead of `/Plone/news`), indexing `portal_catalog` itself,
  and not re-indexing the portal root object.
  Now uses `getPhysicalPath()` for authoritative paths, `aq_base()` for
  identity comparison through Acquisition wrappers, and explicitly indexes
  the portal root before traversal (matching Plone's `CatalogTool`).
  Fixes #21.

## 1.0.0b11

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
  `ObjectManager`, etc.). The new base classes are `UniqueObject + Folder`,
  providing a minimal OFS container for index objects and lexicons while
  eliminating the deep inheritance chain.

  This improves query performance by ~2x across most scenarios (reduced
  Python-side overhead from attribute lookups, security checks, and
  Acquisition wrapping) and write performance by ~5% (lighter commit path).

  A `_CatalogCompat` persistent object provides `_catalog.indexes` and
  `_catalog.schema` for backward compatibility with code that accesses
  ZCatalog internal data structures. Existing ZODB instances with the old
  `_catalog` (full `Catalog` object) continue to work without migration.

- **ZCML override for eea.facetednavigation**: Moved from `<includeOverrides>`
  inside `configure.zcml` to a proper `overrides.zcml` at the package root,
  loaded by Zope's `five:loadProductsOverrides`. Fixes ZCML conflict errors
  when both eea.facetednavigation and plone.pgcatalog are installed.

### Added

- **eea.facetednavigation adapter**: `PGFacetedCatalog` in
  `addons_compat/eeafacetednavigation.py` -- PG-backed `IFacetedCatalog`
  that queries `idx` JSONB directly for faceted counting. Dispatches by
  `IndexType` (FIELD, KEYWORD, BOOLEAN, UUID, DATE) with `IPGIndexTranslator`
  fallback. Falls back to the default BTree-based implementation when the
  catalog is not `IPGCatalogTool`. Conditionally loaded only when
  `eea.facetednavigation` is installed.

- **Deprecated proxy methods**: `search()` proxies to `searchResults()` and
  `uniqueValuesFor()` proxies to `Indexes[name].uniqueValues()`, both
  emitting `DeprecationWarning`.

- **Blocked methods**: `getAllBrains`, `searchAll`, `getobject`,
  `getMetadataForUID`, `getMetadataForRID`, `getIndexDataForUID`,
  `index_objects` raise `NotImplementedError` with descriptive messages.

- **AccessControl security declarations**: Comprehensive Zope security
  matching ZCatalog's permission model. `Search ZCatalog` on read
  methods (`searchResults`, `__call__`, `getpath`, `getrid`, etc.),
  `Manage ZCatalog Entries` on write methods (`catalog_object`,
  `uncatalog_object`, `refreshCatalog`, etc.), `Manage ZCatalogIndex
Entries` on index management (`addIndex`, `delIndex`, `addColumn`,
  `delColumn`, `getIndexObjects`). `setPermissionDefault` assigns
  default roles (`Anonymous` for search, `Manager` for management).
  Private helpers (`indexObject`, `reindexObject`, etc.) declared
  private.

- **DateRangeInRangeIndex support**: Native `IPGIndexTranslator` for
  `Products.DateRangeInRangeIndex` overlap queries. Translates
  `catalog({'my_idx': {'start': dt1, 'end': dt2}})` into a single SQL
  overlap clause (`obj_start <= q_end AND obj_end >= q_start`).
  Supports recurring events: when the underlying start index is a
  DateRecurringIndex with RRULE, uses `rrule."between"()` with duration
  offset for occurrence-level overlap detection. Auto-discovered at
  startup — no configuration needed. Allows dropping the
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
  and metadata columns. Reflects the IndexRegistry rather than BTree
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
  `uncatalog_object()`. All catalog data now flows exclusively to
  PostgreSQL via `CatalogStateProcessor` — no BTree/Bucket objects are
  written to ZODB. Content creation dropped from 175 ms/doc to
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
