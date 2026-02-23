<!-- diataxis: explanation -->

# Architecture

plone.pgcatalog replaces ZCatalog's BTree-based indexes with SQL queries against
PostgreSQL. Rather than maintaining thousands of BTree objects in ZODB, catalog data
lives in columns on the `object_state` table -- the same table that zodb-pgjsonb
uses to store ZODB object pickles. This means catalog writes are atomic with object
writes: there is no window where an object is committed but its catalog entry is stale.

This page explains how data flows through the system on writes and reads, how the
pieces fit together, and the reasoning behind key design choices.

## Key files

| File | Purpose |
|---|---|
| `catalog.py` | `PlonePGCatalogTool(UniqueObject, Folder)` -- Plone's `portal_catalog` replacement |
| `query.py` | Query translation: ZCatalog dict -> SQL WHERE + ORDER BY |
| `columns.py` | `IndexRegistry`, `IndexType` enum, `convert_value()`, `ensure_date_param()` |
| `indexing.py` | SQL write operations (`catalog_object`, `uncatalog_object`, `reindex_object`) |
| `pending.py` | Thread-local pending store + `PendingDataManager` (savepoint support) |
| `pool.py` | Connection pool discovery + request-scoped connection reuse |
| `processor.py` | `CatalogStateProcessor` for zodb-pgjsonb integration |
| `startup.py` | `IDatabaseOpenedWithRoot` subscriber, registry sync, DRI/DRIRI translator registration |
| `schema.py` | DDL for catalog columns, functions, and indexes |
| `brain.py` | `PGCatalogBrain` + lazy `CatalogSearchResults` |
| `pgindex.py` | `PGIndex`, `PGCatalogIndexes` -- ZCatalog internal API wrappers |
| `backends.py` | `SearchBackend` ABC, `TsvectorBackend`, `BM25Backend` |
| `dri.py` | `DateRecurringIndexTranslator` for recurring events |
| `driri.py` | `DateRangeInRangeIndexTranslator` for overlap queries |
| `interfaces.py` | `IPGCatalogTool`, `IPGIndexTranslator` |
| `setuphandlers.py` | GenericSetup install: snapshot, replace, restore indexes |
| `addons_compat/` | Addon compatibility adapters (eea.facetednavigation) |

## Overview

The high-level data flow looks like this:

```{mermaid}
flowchart LR
    A[Plone object] -->|catalog_object / reindexObject| B[PlonePGCatalogTool]
    B -->|extract index data| C[set_pending]
    C --> D[Thread-local PendingStore]
    D -->|tpc_vote| E[CatalogStateProcessor]
    E -->|UPDATE object_state| F[(PostgreSQL)]
```

Plone content changes flow through the catalog tool, which extracts index values,
stashes them in a thread-local store, and marks the object dirty. When ZODB commits,
the state processor picks up the pending data and writes it as extra columns on the
same row in `object_state`.

## Write path

### Step by step

1. **`catalog_object()` or `reindexObject()` is called.** This happens when Plone's
   event subscribers fire on content creation, modification, or workflow transitions.
   The IndexQueue may batch these calls, but the annotation is set immediately (not
   deferred to `before_commit`).

2. **Index data is extracted from the object.** `PlonePGCatalogTool._extract_idx()`
   iterates the dynamic `IndexRegistry` (populated at startup from each Plone site's
   ZCatalog indexes). For each index, it reads the configured `source_attrs` from the
   `plone.indexer`-wrapped object. Custom index types not in the registry are handled
   by `IPGIndexTranslator` named utilities.

3. **Data is stored in the thread-local pending store.** `set_pending(zoid, data)`
   writes the extracted index dict, path, and searchable text into a
   `threading.local()` dict keyed by ZOID. This is the critical design choice -- see
   "Why thread-local?" below.

4. **The object is marked `_p_changed = True`.** This tells ZODB that the object
   needs to be serialized and stored during the next transaction commit. Without this,
   the state processor would never see the object.

5. **During ZODB `tpc_vote`, `CatalogStateProcessor.process()` runs.** The zodb-pgjsonb
   storage calls registered state processors for every object being stored. The
   catalog processor pops the pending data for the object's ZOID and returns column
   values (path, idx JSONB, searchable_text tsvector, and any backend-specific
   columns like BM25 vectors).

6. **zodb-pgjsonb writes catalog columns atomically.** The `ExtraColumn` values
   returned by the processor are included in the same `UPDATE object_state` statement
   that writes the object's pickle. One SQL statement, one transaction, zero
   consistency gaps.

```{mermaid}
sequenceDiagram
    participant P as Plone
    participant C as PlonePGCatalogTool
    participant PS as PendingStore
    participant SP as CatalogStateProcessor
    participant PG as PostgreSQL

    P->>C: catalog_object(obj)
    C->>C: Extract index data
    C->>PS: set_pending(zoid, data)
    C->>P: obj._p_changed = True
    Note over P,PG: ZODB transaction commit
    P->>SP: process(zoid, state)
    SP->>PS: pop_pending(zoid)
    SP->>PG: UPDATE object_state SET path=, idx=, searchable_text=
```

### Why thread-local instead of object annotations?

The original design stored pending catalog data as an annotation on the persistent
object itself (its `__dict__`). This caused a real problem with CMFEditions: when
Plone creates a version snapshot, it clones the object's state -- including any
annotations. The cloned annotation would then be processed during commit,
producing duplicate or incorrect catalog entries.

Thread-local storage avoids this entirely. The pending data lives in
`threading.local()` and is keyed by ZOID. Only the state processor reads it, and
it pops each entry exactly once during `tpc_vote`. Cloned objects get a different
ZOID and have no pending entry.

The `PendingDataManager` joins the ZODB transaction to participate in savepoints
and cleanup: if the transaction is aborted, pending data is cleared. If a
savepoint is rolled back, pending data reverts to its snapshot.

### Partial reindex

When `reindexObject(idxs=["review_state"])` is called with a specific list of
indexes, plone.pgcatalog avoids the overhead of full re-extraction. Instead:

- `_partial_reindex()` extracts only the requested index values.
- `set_partial_pending(zoid, idx_updates)` registers a JSONB merge update.
- The processor's `finalize()` method executes these as
  `UPDATE object_state SET idx = idx || %(patch)s::jsonb` -- a lightweight JSONB
  merge using the `||` operator.
- The object is NOT marked `_p_changed`. No ZODB serialization happens.

This matters for frequent, targeted reindexes like workflow state changes, where
re-serializing the entire object and re-extracting all 30+ indexes would be wasteful.

Special indexes with `idx_key=None` (SearchableText, effectiveRange, path) cannot be
partially updated because they use dedicated columns, not idx JSONB keys. When any
requested index is special, `_partial_reindex()` returns False and the full write path
runs instead.

**Interaction with full pending:** If a full `set_pending()` already exists for a zoid
(e.g., from a `catalog_object` call in the same transaction), the partial update merges
into the full pending's `idx` dict. Conversely, a subsequent `set_pending()` removes any
partial pending for the same zoid -- full always supersedes partial.

**Savepoint safety:** `set_partial_pending()` uses non-mutating merges (`{**old, **new}`)
because `PendingSavepoint` snapshots are shallow copies. Mutating shared dicts would
corrupt rollback state.

### Uncataloging

When an object is deleted, `uncatalog_object()` registers a `None` sentinel in the
pending store. The state processor sees this sentinel and NULLs all catalog columns
(path, idx, searchable_text, and any backend-specific columns). The base
`object_state` row is preserved -- ZODB still tracks the object's lifecycle.

### Pending-store lookup for security reindex

`unrestrictedSearchResults` extends PG results with objects from the thread-local
pending store when the query includes a `path` filter. This is needed because
`CMFCatalogAware.reindexObjectSecurity` searches
`catalog.unrestrictedSearchResults(path=path)` to find all objects in a subtree and
reindex their `allowedRolesAndUsers`. Newly created objects exist only in the pending
store (not yet committed to PG), so without this merging step security indexes would
never be updated for new objects during workflow transitions.

`_pending_brains_for_path()` scans the pending store, matches paths against the
query, and returns lightweight `_PendingBrain` instances with just enough interface
(`getPath()`, `_unrestrictedGetObject()`) for `reindexObjectSecurity` to work.

## Read path

### Step by step

1. **`catalog.searchResults(query_dict)` is called.** This is the standard Plone
   catalog search entry point, triggered by collection views, search forms, listing
   tiles, and REST API endpoints.

2. **Security filters are injected.** `apply_security_filters()` adds
   `allowedRolesAndUsers` (the current user's roles) and `effectiveRange` (the
   current timestamp for publication date filtering). These are added to the query
   dict before SQL translation, ensuring every search respects Plone's security model.

3. **`build_query()` translates the query dict to parameterized SQL.** Each key in
   the query dict is resolved against the `IndexRegistry` to determine its type
   (FieldIndex, KeywordIndex, DateIndex, etc.) and the corresponding SQL handler.
   Unknown indexes fall back to `IPGIndexTranslator` utilities, then to simple JSONB
   field queries. The result is a WHERE clause, ORDER BY expression, LIMIT, OFFSET,
   and a params dict.

4. **Each index key is routed to its handler.** The `_QueryBuilder` dispatches to
   type-specific methods: `_handle_field` for FieldIndex/GopipIndex, `_handle_keyword`
   for KeywordIndex (using JSONB `?|` overlap), `_handle_date` for DateIndex (using
   `pgcatalog_to_timestamptz()` expression indexes), `_handle_text` for
   ZCTextIndex (delegating to the active search backend), `_handle_path` for
   ExtendedPathIndex (with support for subtree, depth, and navtree queries), and
   so on.

5. **SQL is executed via psycopg with prepared statement support.** `_run_search()`
   assembles the final SELECT and executes it with `prepare=True`, which saves PG
   parse overhead when the same query shape is repeated within a connection.

6. **Results are wrapped in `CatalogSearchResults` with `PGCatalogBrain` objects.**
   Each brain is a lightweight wrapper around a PG row dict. It implements
   `ICatalogBrain` for Plone compatibility and supports attribute access into the
   `idx` JSONB for catalog metadata.

## Lazy loading

The initial catalog query selects only `zoid` and `path` -- not the full `idx`
JSONB column. This is the biggest single performance optimization in the read path.

When brain attribute access first touches a metadata field (e.g., `brain.Title`,
`brain.portal_type`), the brain delegates to its parent `CatalogSearchResults`, which
calls `_load_idx_batch()`. This issues a single `SELECT zoid, idx FROM object_state
WHERE zoid = ANY(%(zoids)s)` for ALL brains in the result set, populating every
brain's `idx` in one round-trip.

Why this matters:

- Many search results pages never access brain metadata at all (e.g., count-only
  queries, batched listings where only the first page is rendered).
- When metadata IS accessed, a single batch query is far cheaper than selecting idx
  for every row in the initial query (JSONB decompression is expensive for wide rows).
- The batch load uses the same PG connection and thus the same REPEATABLE READ
  snapshot as the original search, guaranteeing consistency.

## Connection routing

plone.pgcatalog obtains its PostgreSQL connection through a deliberate preference
order:

1. **Storage connection (preferred).** `get_storage_connection()` reads
   `context._p_jar._storage.pg_connection` -- the same connection that the
   zodb-pgjsonb storage instance uses for ZODB object loads. Since this connection
   is inside a REPEATABLE READ transaction, catalog queries see exactly the same
   data snapshot as object traversals. No phantom reads, no inconsistencies.

2. **Request-scoped pool connection (fallback).** When no storage connection is
   available (e.g., the catalog tool has not yet been traversed through a ZODB
   connection), `get_request_connection()` borrows a connection from the pool and
   stores it in `threading.local()`. Subsequent catalog queries within the same
   Zope request reuse this connection, avoiding pool lock overhead. The connection
   is returned by an `IPubEnd` subscriber when the request ends.

3. **Pool borrow (last resort).** For scripts, tests, and maintenance operations
   that run outside a Zope request, a connection is borrowed from the pool and
   returned in a context manager.

## Dynamic index registration

The `IndexRegistry` is the bridge between ZCatalog's index definitions and
plone.pgcatalog's SQL query builder. Here is how it gets populated:

1. At Zope startup, the `IDatabaseOpenedWithRoot` subscriber fires.
2. `_sync_registry_from_db()` opens a temporary ZODB connection and traverses the
   root to find Plone sites.
3. For each `portal_catalog`, `registry.sync_from_catalog(catalog)` reads
   `catalog._catalog.indexes` and maps each index's `meta_type` to an `IndexType`
   enum value. The `getIndexSourceNames()` method provides the attribute names to
   extract at indexing time.
4. DateRecurringIndex and DateRangeInRangeIndex instances are auto-discovered and
   registered as `IPGIndexTranslator` utilities.
5. For each TEXT-type index with a JSONB key (e.g., Title, Description, addon
   ZCTextIndex fields), `_ensure_text_indexes()` creates GIN expression indexes
   using `to_tsvector('simple', idx->>'{key}')`.

The registry is a module-level singleton. Once populated, it is used by both the
write path (`_extract_idx()`) and the read path (`build_query()`).

## Base class architecture

`PlonePGCatalogTool` inherits from `UniqueObject + Folder` -- not from ZCatalog.
This deliberate "clean break" eliminates the deep inheritance chain
(`CatalogTool -> ZCatalog -> ObjectManager -> ...`, roughly 15 classes) and the
associated overhead from attribute lookups, security checks, and Acquisition
wrapping in the query and write hot paths.  Benchmarks show ~2x improvement in
query latency after the clean break.

`Folder` provides `ObjectManager` containment for ZCatalog index objects and
lexicons (needed by `PGCatalogIndexes._getOb()` and GenericSetup's
`ZCatalogXMLAdapter`).  `UniqueObject` provides the standard `getId()` method.

A `_CatalogCompat(Persistent)` shim provides `_catalog.indexes` and
`_catalog.schema` for backward compatibility with code that reads ZCatalog
internal data structures.  Existing ZODB instances with the old `_catalog` (a
full `Catalog` object from before the clean break) continue to work without
migration because the code only reads `.indexes` and `.schema` attributes.

## ZCatalog compatibility layer

Plone add-ons and core code access ZCatalog internal data structures directly.
Since plone.pgcatalog stores no BTree index data in ZODB, these are replaced
with PG-backed implementations:

- **`PGCatalogIndexes`** replaces the `Indexes` container. When code accesses
  `catalog.Indexes["UID"]`, it returns a `PGIndex` proxy instead of the raw ZCatalog
  index object.

- **`PGIndex`** wraps each ZCatalog index and overrides `_index` with a
  `_PGIndexMapping` that translates `_index.get(value)` into a PG query on the `idx`
  JSONB column. It also overrides `uniqueValues()` with a `SELECT DISTINCT` query.

- **`getpath(rid)` / `getrid(path)`** use ZOID as the record ID. ZCatalog assigns
  sequential integer record IDs; plone.pgcatalog uses the object's ZODB OID (already
  an integer primary key in `object_state`), eliminating the need for a separate
  mapping table.

- **Brain attribute resolution** distinguishes known from unknown fields. Known
  catalog fields (registered indexes or metadata) return `None` when absent from the
  `idx` JSONB -- matching ZCatalog's Missing Value behavior. Unknown fields raise
  `AttributeError`, which triggers the `getObject()` fallback in
  `CatalogContentListingObject.__getattr__()`.

- **Blocked methods**: ZCatalog methods that would return wrong/empty data
  (`getAllBrains`, `searchAll`, `getobject`, etc.) raise `NotImplementedError`.

- **Deprecated proxies**: `search()` and `uniqueValuesFor()` emit
  `DeprecationWarning` and delegate to their PG-backed equivalents.
