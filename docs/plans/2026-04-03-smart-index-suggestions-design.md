# Smart Index Suggestions — Phase 1 Design

**Issue:** https://github.com/bluedynamics/plone-pgcatalog/issues/86
**Parent:** https://github.com/bluedynamics/plone-pgcatalog/issues/85

## Goal

Replace the naive `_suggest_index()` with field-type-aware suggestions
that generate correct DDL, detect already-covered fields, offer on-demand
EXPLAIN plans, and let admins apply or drop indexes from the ZMI.

## Architecture

New module `src/plone/pgcatalog/suggestions.py` owns all suggestion logic.
`catalog.py` delegates to it from ZMI action methods.  The suggestion
engine is a pure function (no DB access) — DB helpers are separate
functions for EXPLAIN, apply, and drop.

## Module: `suggestions.py`

### `suggest_indexes(query_keys, registry, existing_indexes) -> list[dict]`

Pure function.  No DB access.

**Input:**

- `query_keys`: list of catalog query field names (from `pgcatalog_slow_queries.query_keys`)
- `registry`: the `IndexRegistry` instance
- `existing_indexes`: dict `{index_name: index_def}` from `get_existing_indexes()`

**Logic per field:**

0. Filter out query-meta keys that are not index fields: `sort_on`,
   `sort_order`, `b_size`, `b_start`, `SearchableText`,
   `effectiveRange`, `path` (same set as existing `_NON_IDX_FIELDS`).
1. Look up in IndexRegistry → determine IndexType.
   Fields not in the registry are skipped (unknown index type).
2. Dedicated columns (`allowed_roles`, `searchable_text`) → status "already_covered".
3. Map IndexType to DDL expression:

| IndexType | DDL Expression | Composite-eligible? |
|-----------|---------------|---------------------|
| FIELD     | `(idx->>'field')` | yes |
| DATE      | `pgcatalog_to_timestamptz(idx->>'field')` | yes |
| BOOLEAN   | `(idx->>'field')::boolean` | yes |
| UUID      | `(idx->>'field')` | yes |
| KEYWORD   | GIN — `(idx->'field')` | no (own GIN index) |
| TEXT      | tsvector GIN | no (handled by `_ensure_text_indexes`) |
| PATH      | `(idx->>'field') text_pattern_ops` | yes |

4. Composite formation: all btree-eligible fields (max 3), sorted by
   estimated selectivity: UUID > FIELD > DATE > BOOLEAN > PATH.
5. Duplicate check: compare generated DDL expression against
   `existing_indexes` values.  If the expression is already present
   in any existing index definition → status "already_covered".

**Output:** list of suggestion dicts:

```python
{
    "fields": ["portal_type", "Creator"],
    "field_types": ["FIELD", "FIELD"],
    "ddl": "CREATE INDEX CONCURRENTLY idx_os_sug_portal_type_Creator ON ...",
    "status": "new" | "already_covered",
    "reason": "Composite btree for 2 FIELD columns",
}
```

### Naming convention

- Base indexes (from `schema.py`): `idx_os_cat_*`
- Suggestion-system indexes: `idx_os_sug_*`

Only `idx_os_sug_*` indexes can be dropped via ZMI.
`EXPECTED_INDEXES` from `schema.py` serves as protection list.

### `get_existing_indexes(conn) -> dict[str, str]`

```sql
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'object_state'
```

Returns `{"idx_os_cat_portal_type": "CREATE INDEX ...", ...}`.
Called on-demand in ZMI, not cached.

### `explain_query(conn, sql, params) -> dict`

Runs `EXPLAIN (FORMAT JSON) <sql>` with stored params.  The `params`
dict comes from `pgcatalog_slow_queries.params` (JSONB column) and is
passed directly to `conn.execute()` as keyword params.  Returns the
top-level plan node with `total_cost` and `node_type` (Seq Scan,
Index Scan, Bitmap Heap Scan, etc.).  No `ANALYZE` — planner estimate
only, no query execution.

### `apply_index(conn, ddl) -> tuple[bool, str, float]`

1. Sets `conn.autocommit = True` (required for CONCURRENTLY).
2. `log.info("Creating index: %s", ddl)`
3. Executes DDL, measures duration.
4. `log.info("Index created in %.1fs: %s", duration, index_name)`
5. Returns `(success, message, duration_seconds)`.
6. On error: `(False, error_message, 0.0)`.

### `drop_index(conn, index_name) -> tuple[bool, str, float]`

Same pattern as `apply_index` but with `DROP INDEX CONCURRENTLY`.
Validates that `index_name` starts with `idx_os_sug_` and is NOT in
`EXPECTED_INDEXES`.  Refuses to drop protected indexes.

## Integration in `catalog.py`

### `manage_get_slow_query_stats()` — modified

Currently calls `_suggest_index(keys)`.  Changed to:

```python
from plone.pgcatalog.suggestions import suggest_indexes, get_existing_indexes

existing = get_existing_indexes(pg_conn)
registry = get_registry()
# per row:
row["suggestions"] = suggest_indexes(keys, registry, existing)
```

The old `_suggest_index()` function is removed.

### `manage_explain_slow_query(query_id, REQUEST=None)` — new

ZMI action.  Takes a `query_id` from `pgcatalog_slow_queries`, reads
`query_text` + `params`, calls `explain_query()`, returns the plan for
DTML display.

### `manage_apply_index(ddl, REQUEST=None)` — new

ZMI action.  Receives DDL from form, validates it starts with
`CREATE INDEX CONCURRENTLY` and targets `object_state`.  Calls
`apply_index()`.  Redirects with `manage_tabs_message`:

- Success: `"Index idx_os_sug_portal_type_Creator created in 2.3s"`
- Error: `"Index creation failed: ..."`

### `manage_drop_index(index_name, REQUEST=None)` — new

ZMI action.  Validates `index_name` starts with `idx_os_sug_` and is
not in `EXPECTED_INDEXES`.  Calls `drop_index()`.  Redirects with
status message.

## ZMI Slow Queries Tab — changes

### Current display

Table: query_keys, count, avg_ms, max_ms, last_seen, suggested_index (text).

### New display

Per slow-query row:

| query_keys | count | avg/max ms | Suggestions |
|-----------|-------|-----------|-------------|
| portal_type, Creator | 42 | 3.2 / 12.1 | **FIELD + FIELD → Composite btree** `CREATE INDEX ...` [Apply] |
| allowedRolesAndUsers | 15 | 8.5 / 45.0 | **KEYWORD → Already covered** (allowed_roles GIN) |

- Fields with status "already_covered": no Apply button, shows which
  existing index covers them.
- Fields with status "new": Apply button (POST form, DDL as hidden field).
- [EXPLAIN] link per row → separate page `manage_explainQuery?id=123`
  showing the JSON query plan.

### Managed indexes section

Below the slow queries table, a new section lists all `idx_os_sug_*`
indexes currently on `object_state`:

| Index name | Definition | [Drop] |
|-----------|-----------|--------|
| idx_os_sug_portal_type_Creator | CREATE INDEX ... | [Drop] |

Only `idx_os_sug_*` indexes appear here.  Each has a [Drop] button.

## Non-goals (Phase 2+)

- Automatic index creation (Phase 2: #87)
- EXPLAIN ANALYZE comparison before/after (Phase 2)
- Background index creation (Phase 2)
- Leader election for multi-pod (Phase 3: #88)
- Index budget / auto-rollback (Phase 3)

## Testing

- `test_suggestions.py`: unit tests for `suggest_indexes()` — pure
  function, no DB needed.  Test each IndexType mapping, composite
  formation, max-3-fields limit, KEYWORD/TEXT exclusion from composites,
  duplicate detection, dedicated column detection.
- `test_suggestions_db.py`: integration tests (requires PG) for
  `get_existing_indexes()`, `explain_query()`, `apply_index()`,
  `drop_index()`.
- Existing slow query tests remain unchanged.
