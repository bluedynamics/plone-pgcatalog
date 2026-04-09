# pgcatalog: Extract `@meta` and `object_provides` from `idx` JSONB

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the `idx` JSONB column size by ~85% (from 424 MB to ~50 MB in AAF prod) by extracting `@meta` and `object_provides` into dedicated PostgreSQL columns.

**Architecture:** Introduce a generic `ExtraIdxColumn` declaration mechanism in `plone.pgcatalog` that allows specific idx keys to be transparently extracted into dedicated PG columns. The extraction happens at write time (in `CatalogStateProcessor.process()` and `indexing.py`), queries are redirected at query time (in `query.py`), and brain attribute access reads from the new columns. The mechanism must be declarative and reusable — not hardcoded per field.

**Tech Stack:** Python, PostgreSQL, psycopg, zodb-pgjsonb `ExtraColumn` infrastructure, plone.pgcatalog

**Repo:** https://github.com/bluedynamics/plone-pgcatalog

---

## Motivation / Measurements (AAF prod, 2026-04-09)

| Metric | Value |
|---|---|
| Total rows in `object_state` | 2,600,888 |
| Rows with `idx` | 137,327 |
| **Total `idx` size (TOAST-compressed)** | **424 MB** |
| `@meta` (mainly `image_scales`) | 368 MB (87%) |
| `object_provides` | 214 MB (50%) |
| Everything else | ~50 MB |

Top offenders inside `@meta`:

| Key | avg | total est |
|---|---|---|
| `image_scales` | 3,400 B | 315 MB |
| dates (created, modified, etc.) | ~93 B each | ~40 MB |

## Background: Existing Architecture

### How idx is written today

1. `extraction.py:extract_idx()` builds an `idx` dict from IndexRegistry + metadata
2. Non-JSON-native metadata → pickled via `zodb_json_codec` → stored as `idx["@meta"]`
3. `catalog.py` sets `_pgcatalog_pending` annotation with `{"path": ..., "idx": ..., "searchable_text": ...}`
4. `CatalogStateProcessor.process()` pops this annotation → returns `{"path": ..., "idx": Json(idx), "searchable_text": ...}`
5. `zodb_pgjsonb` writes these as `ExtraColumn` values via `_batch_write_objects()`

### How idx is queried today

- `query.py:_QueryBuilder` dispatches per IndexType
- `_handle_keyword()` for `object_provides`: `idx->'object_provides' ?| %(p)s`
- `allowedRolesAndUsers` also uses `_handle_keyword()`: `idx->'allowedRolesAndUsers' ?| %(p)s`
- GIN index `idx_os_cat_provides_gin` on `idx->'object_provides'`

### How brain metadata is read today

- `brain.py:_resolve_from_idx()`: checks `idx["@meta"]` first (decoded via Rust codec), then top-level `idx[name]`
- Lazy mode: `CatalogSearchResults._load_idx_batch()` fetches `idx` for all brains in one query

### ExtraColumn mechanism (zodb-pgjsonb)

`ExtraColumn(name, value_expr, update_expr=None)` — declares a PG column written alongside object state. The `CatalogStateProcessor` already uses this for `path`, `idx`, and `searchable_text`. DDL is applied via `get_schema_sql()`.

### Note on existing `allowed_roles` column

The DB has a `allowed_roles text[]` column with GIN index, but it is **not declared or used by pgcatalog**. It appears to be from a previous experiment. The `allowedRolesAndUsers` index currently lives in `idx` JSONB and is queried via `_handle_keyword()`. This plan does NOT touch `allowed_roles` — it can be addressed in a follow-up.

---

## Design Decisions

### 1. Generic `ExtraIdxColumn` mechanism (not hardcoded)

Instead of hardcoding `object_provides` and `@meta` extraction, introduce a declarative registry:

```python
@dataclasses.dataclass
class ExtraIdxColumn:
    """Declare an idx key to extract into a dedicated PG column."""
    idx_key: str          # key in the idx dict (e.g. "object_provides", "@meta")
    column_name: str      # PG column name (e.g. "object_provides", "meta")
    column_type: str      # PG type (e.g. "JSONB", "TEXT[]")
    value_expr: str       # SQL value expression for INSERT (e.g. "%(object_provides)s")
    gin_index: bool       # whether to create a GIN index
```

This allows future extractions (e.g. `allowedRolesAndUsers`) without code changes.

### 2. `@meta` → `meta JSONB` column

- Stores the entire `@meta` sub-dict as-is (already JSON, encoded via `zodb_json_codec`)
- Brain `_resolve_from_idx()` checks the new `meta` column in addition to `idx["@meta"]`
- Backward compatible: if `idx["@meta"]` still exists (pre-migration data), it still works

### 3. `object_provides` → `object_provides TEXT[]` column

- Stored as native PG array (same pattern as the existing `allowed_roles` column)
- Query rewritten from `idx->'object_provides' ?| ...` to `object_provides ?| ...` (native GIN on `text[]`)
- Faster than JSONB GIN for array containment/overlap queries

### 4. Migration strategy

- DDL via `get_schema_sql()` — columns added with `IF NOT EXISTS`
- Old data still works (idx still contains the keys until full reindex)
- Query layer checks column first, falls back to idx for pre-migration rows
- Full `clear_and_rebuild` populates the new columns for all objects

---

## File Structure

All changes in the `plone.pgcatalog` package (https://github.com/bluedynamics/plone-pgcatalog):

| File | Responsibility | Change |
|---|---|---|
| `columns.py` | `ExtraIdxColumn` dataclass + registry | **New code**: dataclass, module-level registry, registration API |
| `schema.py` | DDL for new PG columns + indexes | **Modify**: add DDL strings for `meta` and `object_provides` columns |
| `processor.py` | `CatalogStateProcessor` | **Modify**: declare new `ExtraColumn`s, extract keys from idx dict in `process()` |
| `extraction.py` | `extract_idx()` | **Modify**: pop extracted keys from idx dict before returning |
| `query.py` | `_QueryBuilder` | **Modify**: `_handle_keyword()` checks if idx_key has a dedicated column |
| `brain.py` | `PGCatalogBrain` | **Modify**: `_resolve_from_idx()` checks `meta` column; `_load_idx_batch()` also fetches `meta` |
| `search.py` | `_run_search()` | **Modify**: include `meta` in SELECT columns |
| `indexing.py` | `catalog_object()` / `reindex_object()` | **Modify**: extract keys before writing idx |
| `tests/test_extra_idx_columns.py` | Tests for the new mechanism | **New file** |

---

## Tasks

### Task 1: `ExtraIdxColumn` dataclass and registry in `columns.py`

**Files:**
- Modify: `src/plone/pgcatalog/columns.py`
- Create: `tests/test_extra_idx_columns.py`

- [ ] **Step 1: Write failing test for ExtraIdxColumn dataclass**

```python
# tests/test_extra_idx_columns.py
import pytest
from plone.pgcatalog.columns import ExtraIdxColumn
from plone.pgcatalog.columns import get_extra_idx_columns
from plone.pgcatalog.columns import register_extra_idx_column


class TestExtraIdxColumn:
    def test_dataclass_fields(self):
        col = ExtraIdxColumn(
            idx_key="object_provides",
            column_name="object_provides",
            column_type="TEXT[]",
            value_expr="%(object_provides)s",
            gin_index=True,
        )
        assert col.idx_key == "object_provides"
        assert col.column_name == "object_provides"
        assert col.column_type == "TEXT[]"
        assert col.value_expr == "%(object_provides)s"
        assert col.gin_index is True

    def test_register_and_retrieve(self):
        # Clear registry for test isolation
        from plone.pgcatalog import columns
        old = columns._extra_idx_columns.copy()
        columns._extra_idx_columns.clear()
        try:
            col = ExtraIdxColumn(
                idx_key="test_key",
                column_name="test_col",
                column_type="TEXT[]",
                value_expr="%(test_col)s",
                gin_index=False,
            )
            register_extra_idx_column(col)
            result = get_extra_idx_columns()
            assert len(result) == 1
            assert result[0].idx_key == "test_key"
        finally:
            columns._extra_idx_columns = old

    def test_lookup_by_idx_key(self):
        from plone.pgcatalog.columns import get_extra_idx_column_for_key
        from plone.pgcatalog import columns
        old = columns._extra_idx_columns.copy()
        columns._extra_idx_columns.clear()
        try:
            col = ExtraIdxColumn(
                idx_key="object_provides",
                column_name="object_provides",
                column_type="TEXT[]",
                value_expr="%(object_provides)s",
                gin_index=True,
            )
            register_extra_idx_column(col)
            assert get_extra_idx_column_for_key("object_provides") is col
            assert get_extra_idx_column_for_key("nonexistent") is None
        finally:
            columns._extra_idx_columns = old
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_extra_idx_columns.py -v`
Expected: ImportError — `ExtraIdxColumn` not defined yet

- [ ] **Step 3: Implement ExtraIdxColumn in columns.py**

Add at the end of `src/plone/pgcatalog/columns.py`:

```python
import dataclasses


@dataclasses.dataclass(frozen=True)
class ExtraIdxColumn:
    """Declare an idx key to be extracted into a dedicated PG column.

    When registered, the key is popped from the idx dict at write time
    and stored in its own column.  Queries and brain attribute access
    are redirected transparently.
    """
    idx_key: str        # key in the idx dict (e.g. "object_provides")
    column_name: str    # PG column name
    column_type: str    # PG type (e.g. "TEXT[]", "JSONB")
    value_expr: str     # SQL value expression for psycopg (e.g. "%(object_provides)s")
    gin_index: bool = False


# Module-level registry
_extra_idx_columns: list[ExtraIdxColumn] = []


def register_extra_idx_column(col):
    """Register an ExtraIdxColumn for extraction."""
    _extra_idx_columns.append(col)


def get_extra_idx_columns():
    """Return all registered ExtraIdxColumn declarations."""
    return list(_extra_idx_columns)


def get_extra_idx_column_for_key(idx_key):
    """Look up an ExtraIdxColumn by its idx_key. Returns None if not found."""
    for col in _extra_idx_columns:
        if col.idx_key == idx_key:
            return col
    return None
```

Also add to `__all__`:

```python
"ExtraIdxColumn",
"register_extra_idx_column",
"get_extra_idx_columns",
"get_extra_idx_column_for_key",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_extra_idx_columns.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/columns.py tests/test_extra_idx_columns.py
git commit -m "feat: add ExtraIdxColumn dataclass and registry"
```

---

### Task 2: Register default extra columns (`@meta`, `object_provides`)

**Files:**
- Modify: `src/plone/pgcatalog/columns.py` (or create a `defaults.py` — depending on package conventions)
- Modify: `src/plone/pgcatalog/startup.py`
- Extend: `tests/test_extra_idx_columns.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_extra_idx_columns.py — add to file

class TestDefaultRegistrations:
    def test_meta_column_registered(self):
        from plone.pgcatalog.columns import get_extra_idx_column_for_key
        col = get_extra_idx_column_for_key("@meta")
        assert col is not None
        assert col.column_name == "meta"
        assert col.column_type == "JSONB"

    def test_object_provides_column_registered(self):
        from plone.pgcatalog.columns import get_extra_idx_column_for_key
        col = get_extra_idx_column_for_key("object_provides")
        assert col is not None
        assert col.column_name == "object_provides"
        assert col.column_type == "TEXT[]"
        assert col.gin_index is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_extra_idx_columns.py::TestDefaultRegistrations -v`
Expected: FAIL — columns not registered yet

- [ ] **Step 3: Register defaults at module level in columns.py**

At the bottom of `columns.py`, after the registry functions:

```python
# -- Default extra idx columns ------------------------------------------------

_DEFAULT_EXTRA_IDX_COLUMNS = [
    ExtraIdxColumn(
        idx_key="@meta",
        column_name="meta",
        column_type="JSONB",
        value_expr="%(meta)s",
        gin_index=False,
    ),
    ExtraIdxColumn(
        idx_key="object_provides",
        column_name="object_provides",
        column_type="TEXT[]",
        value_expr="%(object_provides)s",
        gin_index=True,
    ),
]

for _col in _DEFAULT_EXTRA_IDX_COLUMNS:
    register_extra_idx_column(_col)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_extra_idx_columns.py::TestDefaultRegistrations -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/columns.py tests/test_extra_idx_columns.py
git commit -m "feat: register @meta and object_provides as default extra idx columns"
```

---

### Task 3: DDL for new columns in `schema.py`

**Files:**
- Modify: `src/plone/pgcatalog/schema.py`
- Extend: `tests/test_extra_idx_columns.py`

- [ ] **Step 1: Write failing test**

```python
class TestSchema:
    def test_catalog_columns_includes_meta(self):
        from plone.pgcatalog.schema import CATALOG_COLUMNS
        assert "ADD COLUMN IF NOT EXISTS meta JSONB" in CATALOG_COLUMNS

    def test_catalog_columns_includes_object_provides(self):
        from plone.pgcatalog.schema import CATALOG_COLUMNS
        assert "ADD COLUMN IF NOT EXISTS object_provides TEXT[]" in CATALOG_COLUMNS

    def test_catalog_indexes_includes_object_provides_gin(self):
        from plone.pgcatalog.schema import CATALOG_INDEXES
        assert "idx_os_object_provides" in CATALOG_INDEXES
        assert "gin (object_provides)" in CATALOG_INDEXES
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_extra_idx_columns.py::TestSchema -v`
Expected: FAIL

- [ ] **Step 3: Add DDL to schema.py**

In `CATALOG_COLUMNS`, append:

```sql
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS meta JSONB;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS object_provides TEXT[];
```

In `CATALOG_INDEXES`, append:

```sql
-- GIN index on object_provides array (native text[] overlap/containment)
CREATE INDEX IF NOT EXISTS idx_os_object_provides
    ON object_state USING gin (object_provides) WHERE object_provides IS NOT NULL;
```

Add `"meta"` and `"object_provides"` to `EXPECTED_COLUMNS`:

```python
EXPECTED_COLUMNS = {
    "path": "text",
    "parent_path": "text",
    "path_depth": "integer",
    "idx": "jsonb",
    "searchable_text": "tsvector",
    "meta": "jsonb",
    "object_provides": "text[]",
}
```

Add `"idx_os_object_provides"` to `EXPECTED_INDEXES`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_extra_idx_columns.py::TestSchema -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/schema.py tests/test_extra_idx_columns.py
git commit -m "feat: DDL for meta JSONB and object_provides TEXT[] columns"
```

---

### Task 4: Extract keys from idx in `CatalogStateProcessor.process()`

**Files:**
- Modify: `src/plone/pgcatalog/processor.py`
- Extend: `tests/test_extra_idx_columns.py`

- [ ] **Step 1: Write failing test**

```python
class TestProcessorExtraction:
    def test_process_extracts_meta_from_idx(self):
        from plone.pgcatalog.processor import CatalogStateProcessor
        from plone.pgcatalog.pending import set_pending
        from psycopg.types.json import Json

        processor = CatalogStateProcessor()

        meta_data = {"image_scales": {"foo": "bar"}, "created": "2026-01-01"}
        idx = {
            "Title": "Test",
            "portal_type": "Document",
            "@meta": meta_data,
            "object_provides": ["IFolderish", "IContentish"],
        }
        set_pending(42, {"path": "/plone/test", "idx": idx, "searchable_text": None})

        result = processor.process(42, "my.module", "MyClass", {})

        # @meta extracted into "meta" key
        meta_result = result["meta"]
        assert isinstance(meta_result, Json)
        assert meta_result.obj == meta_data

        # object_provides extracted into "object_provides" key
        assert result["object_provides"] == ["IFolderish", "IContentish"]

        # Keys removed from idx
        idx_result = result["idx"]
        assert isinstance(idx_result, Json)
        assert "@meta" not in idx_result.obj
        assert "object_provides" not in idx_result.obj
        assert idx_result.obj["Title"] == "Test"

    def test_process_handles_missing_keys(self):
        from plone.pgcatalog.processor import CatalogStateProcessor
        from plone.pgcatalog.pending import set_pending

        processor = CatalogStateProcessor()
        idx = {"Title": "Test", "portal_type": "Document"}
        set_pending(43, {"path": "/plone/test2", "idx": idx, "searchable_text": None})

        result = processor.process(43, "my.module", "MyClass", {})

        assert result["meta"] is None
        assert result["object_provides"] is None

    def test_uncatalog_nulls_extra_columns(self):
        from plone.pgcatalog.processor import CatalogStateProcessor
        from plone.pgcatalog.pending import set_pending

        processor = CatalogStateProcessor()
        set_pending(44, None)  # uncatalog sentinel

        result = processor.process(44, "my.module", "MyClass", {})
        assert result["meta"] is None
        assert result["object_provides"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_extra_idx_columns.py::TestProcessorExtraction -v`
Expected: FAIL — extra keys not in result

- [ ] **Step 3: Implement extraction in processor.py**

In `CatalogStateProcessor.get_extra_columns()`, add the new columns:

```python
def get_extra_columns(self):
    from plone.pgcatalog.columns import get_extra_idx_columns
    extra = []
    for col in get_extra_idx_columns():
        extra.append(ExtraColumn(col.column_name, col.value_expr))
    return [
        ExtraColumn("path", "%(path)s"),
        ExtraColumn("idx", "%(idx)s"),
        *extra,
        *get_backend().get_extra_columns(),
    ]
```

In `CatalogStateProcessor.process()`, extract registered keys from idx before wrapping as Json:

```python
def process(self, zoid, class_mod, class_name, state):
    # ... existing pending lookup code ...

    if pending is None:
        # Uncatalog sentinel: NULL all catalog columns
        result = {
            "path": None,
            "idx": None,
            "searchable_text": None,
        }
        # NULL extra idx columns too
        for col in get_extra_idx_columns():
            result[col.column_name] = None
        result.update(get_backend().uncatalog_extra())
        return result

    # ... existing Tika code ...

    # Normal catalog: extract registered extra idx columns
    idx = pending.get("idx")
    extra_values = {}
    if idx:
        for col in get_extra_idx_columns():
            value = idx.pop(col.idx_key, None)
            if value is not None:
                if col.column_type == "JSONB":
                    extra_values[col.column_name] = Json(value)
                else:
                    extra_values[col.column_name] = value
            else:
                extra_values[col.column_name] = None

    result = {
        "path": pending.get("path"),
        "idx": Json(idx) if idx else None,
        "searchable_text": pending.get("searchable_text"),
        **extra_values,
    }
    result.update(get_backend().process_search_data(pending))
    return result
```

Add import at top:

```python
from plone.pgcatalog.columns import get_extra_idx_columns
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_extra_idx_columns.py::TestProcessorExtraction -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/processor.py tests/test_extra_idx_columns.py
git commit -m "feat: extract registered extra idx columns in CatalogStateProcessor"
```

---

### Task 5: Redirect `object_provides` queries to dedicated column in `query.py`

**Files:**
- Modify: `src/plone/pgcatalog/query.py`
- Extend: `tests/test_extra_idx_columns.py`

- [ ] **Step 1: Write failing test**

```python
class TestQueryRedirection:
    def test_object_provides_queries_column_not_idx(self):
        """object_provides query should use the dedicated column, not idx JSONB."""
        from plone.pgcatalog.query import build_query
        # Need registry to have object_provides as KeywordIndex
        from plone.pgcatalog.columns import get_registry
        registry = get_registry()
        if "object_provides" not in registry:
            registry.register(
                "object_provides",
                IndexType.KEYWORD,
                "object_provides",
                ["object_provides"],
            )

        result = build_query({
            "object_provides": {
                "query": ["IFolderish", "IContentish"],
                "operator": "or",
            }
        })

        # Should query the column directly, not idx->'object_provides'
        assert "object_provides ?|" in result["where"]
        assert "idx->'object_provides'" not in result["where"]

    def test_object_provides_and_operator(self):
        from plone.pgcatalog.query import build_query
        from plone.pgcatalog.columns import get_registry, IndexType
        registry = get_registry()
        if "object_provides" not in registry:
            registry.register(
                "object_provides",
                IndexType.KEYWORD,
                "object_provides",
                ["object_provides"],
            )

        result = build_query({
            "object_provides": {
                "query": ["IFolderish", "IContentish"],
                "operator": "and",
            }
        })

        # AND: column @> ARRAY[...]
        assert "object_provides @>" in result["where"]
        assert "idx" not in result["where"] or "idx IS NOT NULL" == result["where"].split(" AND ")[0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_extra_idx_columns.py::TestQueryRedirection -v`
Expected: FAIL — still generates `idx->'object_provides'`

- [ ] **Step 3: Modify `_handle_keyword()` in query.py**

In `_handle_keyword()`, check if `idx_key` has a dedicated column:

```python
def _handle_keyword(self, name, idx_key, spec):
    query_val = spec.get("query")
    if query_val is None:
        return

    operator = spec.get("operator", "or")
    query_val = [query_val] if isinstance(query_val, str) else list(query_val)

    # Check for dedicated column
    from plone.pgcatalog.columns import get_extra_idx_column_for_key
    extra_col = get_extra_idx_column_for_key(idx_key)

    if extra_col is not None and extra_col.column_type == "TEXT[]":
        # Query the dedicated TEXT[] column directly
        p = self._pname(name)
        if operator == "and":
            self.clauses.append(f"{extra_col.column_name} @> %({p})s")
            self.params[p] = query_val
        else:
            self.clauses.append(f"{extra_col.column_name} ?| %({p})s")
            self.params[p] = query_val
    elif operator == "and":
        # All values must be present → JSONB containment
        p = self._pname(name)
        self.clauses.append(f"idx @> %({p})s::jsonb")
        self.params[p] = Json({idx_key: query_val})
    else:
        # Any value present → ?| overlap
        p = self._pname(name)
        self.clauses.append(f"idx->'{idx_key}' ?| %({p})s")
        self.params[p] = query_val
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_extra_idx_columns.py::TestQueryRedirection -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/query.py tests/test_extra_idx_columns.py
git commit -m "feat: redirect keyword queries to dedicated columns when available"
```

---

### Task 6: Update brain metadata access for `meta` column

**Files:**
- Modify: `src/plone/pgcatalog/brain.py`
- Modify: `src/plone/pgcatalog/search.py`
- Extend: `tests/test_extra_idx_columns.py`

- [ ] **Step 1: Write failing test**

```python
class TestBrainMetaColumn:
    def test_brain_reads_meta_from_column(self):
        """Brain should read @meta from the dedicated 'meta' column."""
        from plone.pgcatalog.brain import PGCatalogBrain
        from plone.pgcatalog.columns import get_registry

        # Register a metadata field so _resolve_from_idx doesn't raise
        registry = get_registry()
        registry.add_metadata("image_scales")

        # Simulate a row with meta in a separate column (not in idx)
        meta_data = {"image_scales": {"preview": {"width": 400, "height": 300}}}
        row = {
            "zoid": 1,
            "path": "/plone/test",
            "idx": {"Title": "Test", "portal_type": "Document"},
            "meta": meta_data,  # separate column
        }
        brain = PGCatalogBrain(row)

        # Codec-encoded @meta needs decoding — but for JSON-native values
        # stored directly in the meta column, they should be accessible.
        # The actual production meta column stores codec-encoded dicts,
        # so brain access should try decode_meta on the column value.
        assert brain.image_scales == {"preview": {"width": 400, "height": 300}}

    def test_brain_falls_back_to_idx_meta(self):
        """For pre-migration data, idx['@meta'] should still work."""
        from plone.pgcatalog.brain import PGCatalogBrain

        row = {
            "zoid": 2,
            "path": "/plone/old",
            "idx": {
                "Title": "Old",
                "@meta": {"image_scales": {"old": True}},
            },
            "meta": None,  # not yet migrated
        }
        brain = PGCatalogBrain(row)
        # Should fall back to idx["@meta"] and decode
        # (exact behavior depends on whether image_scales is JSON-native)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_extra_idx_columns.py::TestBrainMetaColumn -v`
Expected: FAIL — brain doesn't check `meta` column

- [ ] **Step 3: Modify brain.py**

In `_resolve_from_idx()`, check the `meta` column first:

```python
def _resolve_from_idx(self, name, idx):
    row = object.__getattribute__(self, "_row")

    # Check dedicated meta column first (new path)
    meta_col = row.get("meta")
    if meta_col is not None and name in meta_col:
        decoded = self._decode_meta_col(meta_col)
        return decoded.get(name)

    if idx is not None:
        # Check idx["@meta"] fallback (pre-migration data)
        meta = idx.get("@meta")
        if meta is not None and name in meta:
            decoded = self._decode_meta(idx)
            return decoded.get(name)
        if name in idx:
            return idx[name]

    registry = get_registry()
    if name in registry or name in registry.metadata:
        return None
    raise AttributeError(name)


def _decode_meta_col(self, meta_col):
    """Decode the dedicated meta column (same codec as @meta)."""
    row = object.__getattribute__(self, "_row")
    cached = row.get("_meta_decoded")
    if cached is not None:
        return cached
    decoded = decode_meta(meta_col)
    row["_meta_decoded"] = decoded
    return decoded
```

In `__contains__()`, also check `meta` column:

```python
def __contains__(self, name):
    # ... existing idx load logic ...
    meta_col = self._row.get("meta")
    if meta_col is not None and name in meta_col:
        return True
    if idx:
        if name in idx:
            return True
        meta = idx.get("@meta")
        if meta is not None and name in meta:
            return True
    return name in ("path", "zoid", "getPath", "getURL", "getRID")
```

In `search.py`, include `meta` in SELECT columns:

```python
_SELECT_COLS_LAZY = "zoid, path"
_SELECT_COLS_LAZY_COUNTED = "zoid, path, COUNT(*) OVER() AS _total_count"
_SELECT_COLS_EAGER = "zoid, path, idx, meta"
_SELECT_COLS_EAGER_COUNTED = "zoid, path, idx, meta, COUNT(*) OVER() AS _total_count"
```

In `CatalogSearchResults._load_idx_batch()`:

```python
cur.execute(
    "SELECT zoid, idx, meta FROM object_state WHERE zoid = ANY(%(zoids)s)",
    {"zoids": zoids},
    prepare=True,
)
for row in cur:
    brain = brain_map.get(row["zoid"])
    if brain is not None:
        brain._row["idx"] = row["idx"]
        brain._row["meta"] = row["meta"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_extra_idx_columns.py::TestBrainMetaColumn -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/brain.py src/plone/pgcatalog/search.py tests/test_extra_idx_columns.py
git commit -m "feat: brain reads @meta from dedicated column with idx fallback"
```

---

### Task 7: Update `indexing.py` for direct SQL writes

**Files:**
- Modify: `src/plone/pgcatalog/indexing.py`
- Extend: `tests/test_extra_idx_columns.py`

The `indexing.py` functions (`catalog_object`, `uncatalog_object`, `reindex_object`) write directly via SQL (used for non-batch operations). They need to extract registered keys from idx and write them to dedicated columns.

- [ ] **Step 1: Write failing test**

```python
class TestIndexingExtraction:
    def test_catalog_object_extracts_meta(self, pg_conn):
        """catalog_object should pop @meta from idx and write to meta column."""
        from plone.pgcatalog.indexing import catalog_object

        idx = {
            "Title": "Test",
            "@meta": {"image_scales": {"preview": {}}},
            "object_provides": ["IContentish"],
        }
        catalog_object(pg_conn, zoid=100, path="/plone/test", idx=idx)

        row = pg_conn.execute(
            "SELECT idx, meta, object_provides FROM object_state WHERE zoid = 100"
        ).fetchone()

        # @meta should NOT be in idx anymore
        assert "@meta" not in row["idx"]
        assert "object_provides" not in row["idx"]

        # Should be in dedicated columns
        assert row["meta"] == {"image_scales": {"preview": {}}}
        assert row["object_provides"] == ["IContentish"]
```

Note: This test requires a PG fixture (`pg_conn`). Adapt to the project's existing test infrastructure.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_extra_idx_columns.py::TestIndexingExtraction -v`
Expected: FAIL

- [ ] **Step 3: Modify indexing.py**

Add a helper and modify `catalog_object()`:

```python
from plone.pgcatalog.columns import get_extra_idx_columns


def _extract_extra_columns(idx):
    """Pop registered extra idx keys from idx dict, return column values."""
    if not idx:
        return {}
    extra = {}
    for col in get_extra_idx_columns():
        value = idx.pop(col.idx_key, None)
        if value is not None:
            if col.column_type == "JSONB":
                extra[col.column_name] = Json(value)
            else:
                extra[col.column_name] = value
        else:
            extra[col.column_name] = None
    return extra


def catalog_object(conn, zoid, path, idx, searchable_text=None, language="simple"):
    parent_path, path_depth = compute_path_info(path)
    idx.setdefault("path", path)
    idx.setdefault("path_parent", parent_path)
    idx.setdefault("path_depth", path_depth)

    extra = _extract_extra_columns(idx)
    extra_set_clauses = "".join(
        f",\n                {col} = %({col})s" for col in extra
    )

    params = {
        "zoid": zoid,
        "path": path,
        "parent_path": parent_path,
        "path_depth": path_depth,
        "idx": Json(idx),
        **extra,
    }

    if searchable_text is not None:
        tsvector_sql = _WEIGHTED_TSVECTOR.format(
            idx_expr="%(idx)s::jsonb",
            lang_expr="%(lang)s",
            text_expr="%(text)s",
        )
        params["text"] = searchable_text
        params["lang"] = language
        conn.execute(
            f"""
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s,
                searchable_text = {tsvector_sql}{extra_set_clauses}
            WHERE zoid = %(zoid)s
            """,
            params,
        )
    else:
        conn.execute(
            f"""
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s,
                searchable_text = NULL{extra_set_clauses}
            WHERE zoid = %(zoid)s
            """,
            params,
        )
```

Apply same pattern to `uncatalog_object()` (NULL the extra columns) and `reindex_object()` (extract from `idx_updates` if present).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_extra_idx_columns.py::TestIndexingExtraction -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/indexing.py tests/test_extra_idx_columns.py
git commit -m "feat: extract extra idx columns in direct SQL write path"
```

---

### Task 8: Schema DDL in `get_schema_sql()` (processor integration)

**Files:**
- Modify: `src/plone/pgcatalog/schema.py`
- Modify: `src/plone/pgcatalog/processor.py`

The `CATALOG_COLUMNS` and `CATALOG_INDEXES` strings in `schema.py` (modified in Task 3) are already included in `CatalogStateProcessor.get_schema_sql()`. Verify the DDL is applied on startup.

- [ ] **Step 1: Write test that DDL includes new columns**

```python
class TestProcessorDDL:
    def test_schema_sql_includes_meta_column(self):
        from plone.pgcatalog.processor import CatalogStateProcessor
        processor = CatalogStateProcessor()
        ddl = processor.get_schema_sql()
        assert "meta JSONB" in ddl
        assert "object_provides TEXT[]" in ddl
        assert "idx_os_object_provides" in ddl
```

- [ ] **Step 2: Run test to verify it passes (should pass if Task 3 was done correctly)**

Run: `pytest tests/test_extra_idx_columns.py::TestProcessorDDL -v`
Expected: PASS (DDL strings are composed from `CATALOG_COLUMNS + CATALOG_INDEXES`)

- [ ] **Step 3: Commit (if any adjustments were needed)**

```bash
git commit -m "test: verify DDL includes extra idx columns"
```

---

### Task 9: Integration test with full write-query-read cycle

**Files:**
- Extend: `tests/test_extra_idx_columns.py` (or dedicated integration test file)

- [ ] **Step 1: Write integration test**

```python
class TestIntegration:
    """Full cycle: write object → query → read brain metadata."""

    def test_full_cycle_object_provides(self, pg_conn):
        """Write with object_provides, query it, verify brain access."""
        from plone.pgcatalog.indexing import catalog_object
        from plone.pgcatalog.query import build_query

        idx = {
            "Title": "Integration Test",
            "portal_type": "Document",
            "object_provides": ["IFolderish", "IContentish"],
            "@meta": {"image_scales": {"preview": {"w": 400}}},
        }
        catalog_object(pg_conn, zoid=200, path="/plone/integration", idx=idx)

        # Query by object_provides
        qr = build_query({
            "object_provides": {"query": ["IFolderish"], "operator": "or"}
        })
        row = pg_conn.execute(
            f"SELECT zoid, path, idx, meta, object_provides "
            f"FROM object_state WHERE {qr['where']}",
            qr["params"],
        ).fetchone()

        assert row is not None
        assert row["zoid"] == 200

        # Verify idx is clean
        assert "@meta" not in row["idx"]
        assert "object_provides" not in row["idx"]

        # Verify dedicated columns
        assert row["object_provides"] == ["IFolderish", "IContentish"]
        assert row["meta"]["image_scales"]["preview"]["w"] == 400

    def test_pre_migration_data_still_works(self, pg_conn):
        """Objects with @meta and object_provides still in idx should still be queryable."""
        # Simulate pre-migration row (written before this feature)
        pg_conn.execute(
            """UPDATE object_state SET
                idx = '{"Title":"Old","object_provides":["IContentish"],"@meta":{"image_scales":{}}}'::jsonb,
                meta = NULL,
                object_provides = NULL
            WHERE zoid = 201"""
        )
        # Query should still find it via idx fallback
        # (This depends on whether we add OR-fallback in queries — see design notes)
```

- [ ] **Step 2: Run and iterate**

Run: `pytest tests/test_extra_idx_columns.py::TestIntegration -v`

- [ ] **Step 3: Commit**

```bash
git add tests/test_extra_idx_columns.py
git commit -m "test: integration test for full write-query-read cycle"
```

---

## Migration Notes

### Existing data

After deploying this code, existing rows will have:
- `meta` = NULL, `object_provides` = NULL (new columns empty)
- `idx` still contains `@meta` and `object_provides` (old data)

The brain fallback (`_resolve_from_idx` checking `idx["@meta"]`) handles this transparently.

For queries: `object_provides` queries on old rows won't match the dedicated column. Two options:

**Option A (recommended):** Run `clear_and_rebuild` after deployment — rewrites all rows with the new column layout. This is the standard approach for pgcatalog schema changes.

**Option B (gradual):** Add an OR-fallback in `_handle_keyword()`: `(object_provides ?| ... OR idx->'object_provides' ?| ...)`. Remove after full reindex. More complex, only needed if downtime for reindex is unacceptable.

### Cleanup of old `allowed_roles` column

The existing `allowed_roles text[]` column is unused by pgcatalog. After this feature is stable, consider:
1. Registering `allowedRolesAndUsers` as an `ExtraIdxColumn` (same pattern)
2. Dropping the old `allowed_roles` column (it uses a different name than the idx key)
3. Or renaming the column and wiring it up

This is explicitly out of scope for this plan.
