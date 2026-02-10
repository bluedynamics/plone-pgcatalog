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

**TextIndex**: `searchable_text @@ plainto_tsquery('simple', %(text)s)` for SearchableText; other text indexes treated as field match.

## Transactional Writes

Catalog data flows through ZODB's transaction lifecycle:

1. `PlonePGCatalogTool.catalog_object()` extracts index values and calls `set_pending(zoid, data)` (thread-local store)
2. During ZODB commit, `CatalogStateProcessor.process()` pops pending data and returns extra column values
3. `PGJsonbStorage._batch_write_objects()` writes catalog columns atomically alongside object state

This ensures catalog data is always consistent with object state -- no separate transaction needed.

### Annotation-based vs Thread-local

The thread-local `set_pending()` approach avoids issues with CMFEditions, which clones objects (including annotations) during versioning. Thread-local storage ensures only the original object's catalog data is written.

## Schema

DDL is applied via `CatalogStateProcessor.get_schema_sql()` at Zope startup, using the storage's own connection to avoid REPEATABLE READ lock conflicts.

Includes:

- `ALTER TABLE object_state ADD COLUMN IF NOT EXISTS ...` for catalog columns (`path`, `idx`, `searchable_text`)
- `pgcatalog_to_timestamptz()` immutable wrapper for expression indexes
- GIN index on `idx` JSONB
- B-tree expression indexes on `idx` JSONB for path queries (`path`, `path_parent`, `path_depth`)
- B-tree expression indexes for common sort/filter fields (modified, created, effective, expires, sortable_title, portal_type, review_state, UID)
- Full-text GIN index on `searchable_text`
- rrule_plpgsql schema and functions (for DateRecurringIndex)

## Query Optimizations

1. **orjson**: Registered as psycopg's JSONB deserializer for faster JSON parsing
2. **Lazy idx loading**: `_run_search` selects only `zoid, path`; idx fetched on demand via `_load_idx_batch()` when brain metadata is accessed
3. **Prepared statements**: `prepare=True` on execute for repeated query patterns
4. **Request-scoped connections**: Thread-local connection reuse via `get_request_connection()`, released by IPubEnd subscriber
