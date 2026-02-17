# Architecture

Internal design of plone.pgcatalog.

## Overview

plone.pgcatalog extends the `object_state` table (owned by zodb-pgjsonb) with catalog columns:

| Column | Type | Purpose |
|---|---|---|
| `path` | TEXT | Physical path for brain construction |
| `idx` | JSONB | All index values as a single JSON object |
| `searchable_text` | TSVECTOR | Full-text search vector |

All catalog data lives in these columns -- no BTree/Bucket objects are written to ZODB.

## Key Files

| File | Purpose |
|---|---|
| `catalog.py` | `PlonePGCatalogTool` -- Plone's `portal_catalog` replacement |
| `query.py` | Query translation: ZCatalog dict -> SQL WHERE + ORDER BY |
| `columns.py` | `IndexRegistry`, `IndexType` enum, `convert_value()` |
| `indexing.py` | SQL write operations (`catalog_object`, `uncatalog_object`, `reindex_object`) |
| `config.py` | `CatalogStateProcessor`, pool discovery, DRI translator registration |
| `schema.py` | DDL for catalog columns, functions, and indexes |
| `brain.py` | `PGCatalogBrain` + lazy `CatalogSearchResults` |
| `dri.py` | `DateRecurringIndexTranslator` for recurring events |
| `interfaces.py` | `IPGCatalogTool`, `IPGIndexTranslator` |
| `setuphandlers.py` | GenericSetup integration |

## Dynamic Index Registration

Indexes are discovered from ZCatalog at startup, not hardcoded.

### Discovery Flow

1. Zope fires `IDatabaseOpenedWithRoot`
2. `register_catalog_processor()` subscriber registers `CatalogStateProcessor` on the storage
3. `_sync_registry_from_db()` opens a ZODB connection, finds `portal_catalog`
4. `IndexRegistry.sync_from_catalog()` reads `catalog._catalog.indexes`
5. Each index's `meta_type` is mapped via `META_TYPE_MAP` to an `IndexType` enum
6. `_register_dri_translators()` discovers `DateRecurringIndex` instances and registers `IPGIndexTranslator` utilities
7. `_ensure_text_indexes()` creates GIN expression indexes for any dynamically discovered `TEXT`-type indexes with `idx_key is not None` (Title, Description, addon ZCTextIndex fields)

### IndexType Enum

```
FIELD       -- FieldIndex: exact match, range, NOT
KEYWORD     -- KeywordIndex: contains any/all
DATE        -- DateIndex: timestamptz comparison
BOOLEAN     -- BooleanIndex: true/false
DATE_RANGE  -- DateRangeIndex: composite (effective + expires)
UUID        -- UUIDIndex: exact match
TEXT        -- ZCTextIndex: full-text via tsvector
PATH        -- ExtendedPathIndex/PathIndex: hierarchical
GOPIP       -- GopipIndex: integer position
```

### META_TYPE_MAP

Maps ZCatalog's `meta_type` string to `IndexType`:

```python
META_TYPE_MAP = {
    "FieldIndex": IndexType.FIELD,
    "KeywordIndex": IndexType.KEYWORD,
    "DateIndex": IndexType.DATE,
    "BooleanIndex": IndexType.BOOLEAN,
    "DateRangeIndex": IndexType.DATE_RANGE,
    "UUIDIndex": IndexType.UUID,
    "ZCTextIndex": IndexType.TEXT,
    "ExtendedPathIndex": IndexType.PATH,
    "PathIndex": IndexType.PATH,
    "GopipIndex": IndexType.GOPIP,
}
```

### Special Indexes

Three indexes have dedicated columns/logic instead of simple JSONB keys:

- `SearchableText` -- uses `searchable_text` TSVECTOR column
- `effectiveRange` -- uses `effective`/`expires` JSONB keys with composite logic
- `path` -- uses `path` column (brain SELECT) + `idx` JSONB keys (`path`, `path_parent`, `path_depth`) for queries

These have `idx_key=None` in the registry.

### Registry Entry

Each index is stored as a 3-tuple: `(IndexType, idx_key, source_attrs)`

- `idx_key`: JSONB key in the `idx` column (usually the index name), or `None` for special indexes
- `source_attrs`: list of object attribute names for extraction (from `getIndexSourceNames()`)

## Custom Index Types (IPGIndexTranslator)

For index types not in `META_TYPE_MAP`, addons register an `IPGIndexTranslator` named utility. The utility name must match the index name.

### Interface

```python
class IPGIndexTranslator(Interface):
    def extract(obj, index_name):
        """Return dict to merge into idx JSONB."""

    def query(index_name, query_value, query_options):
        """Return (sql_fragment, params_dict)."""

    def sort(index_name):
        """Return SQL expression for ORDER BY, or None."""
```

### Wiring

- `catalog.py`: `_extract_from_translators()` calls `extract()` for all registered translators during indexing
- `query.py`: `_process_index()` falls back to `_lookup_translator()` when index not in registry
- `query.py`: `_process_sort()` falls back to translator's `sort()` for ORDER BY

### Example

```python
@implementer(IPGIndexTranslator)
class MyCustomTranslator:
    def extract(self, obj, index_name):
        return {"my_key": getattr(obj, "my_attr", None)}

    def query(self, index_name, query_value, query_options):
        return ("idx->>'my_key' = %(my_val)s", {"my_val": query_value})

    def sort(self, index_name):
        return "idx->>'my_key'"
```

Register via ZCML:

```xml
<utility
    factory=".translators.MyCustomTranslator"
    provides="plone.pgcatalog.interfaces.IPGIndexTranslator"
    name="MyCustomIndex" />
```

## DateRecurringIndex (DRI)

`Products.DateRecurringIndex` is supported via `DateRecurringIndexTranslator` in `dri.py`. Plone uses it for `start` and `end` event indexes (both configured with `recurdef="recurrence"`).

### Auto-discovery

At startup, `_register_dri_translators()` iterates ZCatalog indexes, finds those with `meta_type == "DateRecurringIndex"`, reads their config attributes (`attr_recurdef`, `attr_until`), and registers translator utilities via `provideUtility()`.

### Storage

Base date and RRULE string stored in idx JSONB:

```json
{
  "start": "2025-01-06T10:00:00+00:00",
  "start_recurrence": "FREQ=WEEKLY;BYDAY=MO;COUNT=52"
}
```

Non-recurring events omit the `_recurrence` key.

### Query Strategy

Queries use [rrule_plpgsql](https://github.com/sirrodgepodge/rrule_plpgsql) (pure PL/pgSQL, vendored in `rrule_schema.sql`) for query-time recurrence expansion. No C extensions required.

| Range | Recurring SQL | Non-recurring SQL |
|---|---|---|
| `min:max` | `EXISTS (SELECT 1 FROM rrule."between"(...))` | `date BETWEEN min AND max` |
| `min` | `EXISTS (SELECT 1 FROM rrule."after"(..., 1))` | `date >= min` |
| `max` | `date <= max` (base date check) | `date <= max` |
| exact | `EXISTS (SELECT 1 FROM rrule."between"(date, date))` | `date = value` |

The `CASE WHEN` dispatches between recurring and non-recurring at query time based on whether `{name}_recurrence` is present in the JSONB.

### Sort

Sorting is by base date: `pgcatalog_to_timestamptz(idx->>'start')`.

## Query Translation

`query.py` translates ZCatalog query dicts into parameterized SQL.

### Flow

1. `build_query(query_dict)` creates a `_QueryBuilder`
2. For each index key in the query dict, `_process_index()` dispatches to the appropriate handler
3. Handlers generate SQL fragments with `%(param)s` placeholders
4. All values go through psycopg parameterized queries (no string formatting)

### Handler Dispatch

```python
_HANDLERS = {
    IndexType.FIELD: "_handle_field",
    IndexType.KEYWORD: "_handle_keyword",
    IndexType.DATE: "_handle_date",
    IndexType.BOOLEAN: "_handle_boolean",
    IndexType.DATE_RANGE: "_handle_date_range",
    IndexType.UUID: "_handle_uuid",
    IndexType.TEXT: "_handle_text",
    IndexType.PATH: "_handle_path",
    IndexType.GOPIP: "_handle_field",
}
```

### SQL Patterns

**FieldIndex**: `idx @> '{"portal_type": "Document"}'::jsonb` (exact), `idx->>'key' = ANY(array)` (multi), range operators for min/max.

**KeywordIndex**: `idx->'Subject' ?| array` (OR), `idx @> '{"Subject": [...]}'::jsonb` (AND).

**DateIndex**: `pgcatalog_to_timestamptz(idx->>'modified') >= %(param)s` with an immutable SQL wrapper function for expression indexes.

**PathIndex**: `idx->>'path' LIKE '/plone/folder/%'` (subtree), `idx->>'path_parent' = '/plone/folder'` (children), navtree breadcrumb queries.

**TextIndex (SearchableText)**: `searchable_text @@ plainto_tsquery(pgcatalog_lang_to_regconfig(%(lang)s)::regconfig, %(text)s)` -- language-aware stemming via the per-object `Language` field. Falls back to `'simple'` when no language is set.

**TextIndex (Title/Description/addon)**: `to_tsvector('simple'::regconfig, COALESCE(idx->>'Title', '')) @@ plainto_tsquery('simple'::regconfig, %(text)s)` -- word-level matching on idx JSONB values, backed by GIN expression indexes. Uses `'simple'` config (no stemming) because expression indexes require a fixed regconfig.

## Transactional Writes

Catalog data flows through ZODB's transaction lifecycle:

1. `PlonePGCatalogTool.catalog_object()` extracts index values and calls `set_pending(zoid, data)` (thread-local store)
2. During ZODB commit, `CatalogStateProcessor.process()` pops pending data and returns extra column values
3. `PGJsonbStorage._batch_write_objects()` writes catalog columns atomically alongside object state

This ensures catalog data is always consistent with object state -- no separate transaction needed.

### Annotation-based vs Thread-local

The thread-local `set_pending()` approach avoids issues with CMFEditions, which clones objects (including annotations) during versioning. Thread-local storage ensures only the original object's catalog data is written.

### Partial idx Updates

When `reindexObject(idxs=['allowedRolesAndUsers'])` is called with specific index names (e.g. during `reindexObjectSecurity`), a lightweight path avoids full ZODB serialization:

1. `PlonePGCatalogTool._partial_reindex()` extracts only the requested index values
2. Calls `set_partial_pending(zoid, idx_updates)` -- stores a JSONB patch in a separate thread-local dict
3. Does NOT set `_p_changed` -- no ZODB pickle-JSON round-trip
4. During `tpc_vote`, `CatalogStateProcessor.finalize(cursor)` applies patches via `UPDATE object_state SET idx = idx || patch`

This uses the `finalize(cursor)` hook from zodb-pgjsonb's state processor protocol, which runs after batch object writes in the same PG transaction.

**Fallback to full reindex**: Special indexes with `idx_key=None` (SearchableText, effectiveRange, path) cannot be partially updated because they use dedicated columns, not idx JSONB keys. When any requested index is special, `_partial_reindex()` returns False and the full path runs.

**Savepoint safety**: `set_partial_pending()` uses non-mutating merges (`{**old, **new}`) because `PendingSavepoint` snapshots are shallow copies. Mutating shared dicts would corrupt rollback state.

**Interaction with full pending**: If a full `set_pending()` already exists for a zoid, the partial update merges into its `idx` dict. Conversely, a subsequent `set_pending()` removes any partial pending for the same zoid (full supersedes partial).

## Schema

DDL is applied via `CatalogStateProcessor.get_schema_sql()` at Zope startup, using the storage's own connection to avoid REPEATABLE READ lock conflicts.

Includes:

- `ALTER TABLE object_state ADD COLUMN IF NOT EXISTS ...` for catalog columns (`path`, `idx`, `searchable_text`)
- `pgcatalog_to_timestamptz()` immutable wrapper for expression indexes
- `pgcatalog_lang_to_regconfig()` maps Plone language codes (ISO 639-1) to PG text search configurations (e.g. `'de'` → `'german'`). Used at both write time (`to_tsvector`) and query time (`plainto_tsquery`). Returns `'simple'` for NULL, empty, or unmapped languages.
- GIN index on `idx` JSONB
- B-tree expression indexes on `idx` JSONB for path queries (`path`, `path_parent`, `path_depth`)
- B-tree expression indexes for common sort/filter fields (modified, created, effective, expires, sortable_title, portal_type, review_state, UID)
- Full-text GIN index on `searchable_text`
- GIN expression indexes for Title/Description tsvector matching (`to_tsvector('simple', COALESCE(idx->>'Title', ''))`)
- Dynamic GIN expression indexes for addon ZCTextIndex fields (created at startup by `_ensure_text_indexes()`)
- rrule_plpgsql schema and functions (for DateRecurringIndex)

## Full-Text Search

Three tiers of text search, each with different characteristics:

### SearchableText (Language-Aware)

Uses the dedicated `searchable_text` TSVECTOR column with per-object language stemming:

- **Write path**: `to_tsvector(pgcatalog_lang_to_regconfig(idx->>'Language')::regconfig, text)` -- language extracted from the object's `Language` field in idx JSONB
- **Query path**: `searchable_text @@ plainto_tsquery(pgcatalog_lang_to_regconfig(%(lang)s)::regconfig, %(text)s)` -- language from the query's `Language` filter
- **Index**: GIN on `searchable_text` column
- **Stemming**: Yes, for the 30 supported languages (falls back to `'simple'` for unknown/empty)

### Title / Description (Word-Level)

Uses tsvector expression matching on idx JSONB values:

- **Write path**: Values stored as plain text in `idx->>'Title'` / `idx->>'Description'`
- **Query path**: `to_tsvector('simple', COALESCE(idx->>'Title', '')) @@ plainto_tsquery('simple', %(text)s)`
- **Index**: GIN expression indexes (pre-created in DDL)
- **Stemming**: No (`'simple'` config) -- expression indexes require a fixed regconfig. Language-aware stemmed search for titles is available via SearchableText (which includes title text).

### Addon ZCTextIndex Fields

Any addon that registers a ZCTextIndex in ZCatalog (via `catalog.xml`) is automatically supported:

1. `sync_from_catalog()` discovers the index → registered as `(IndexType.TEXT, idx_key, source_attrs)`
2. `_ensure_text_indexes()` creates a GIN expression index at startup: `to_tsvector('simple', COALESCE(idx->>'{idx_key}', ''))`
3. Value extracted into idx JSONB during indexing (idx_key is not None)
4. `_handle_text()` generates tsvector expression matching -- zero addon code needed

## Query Optimizations

1. **orjson**: Registered as psycopg's JSONB deserializer for faster JSON parsing
2. **Lazy idx loading**: `_run_search` selects only `zoid, path`; idx fetched on demand via `_load_idx_batch()` when brain metadata is accessed
3. **Prepared statements**: `prepare=True` on execute for repeated query patterns
4. **Request-scoped connections**: Thread-local connection reuse via `get_request_connection()`, released by IPubEnd subscriber
