# Strip `path`/`path_parent`/`path_depth` from `idx` JSONB — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop duplicating `path`, `path_parent`, and `path_depth` between typed columns and the `idx` JSONB on `object_state`. Migrate index DDL, extended-statistics DDL, the query builder, and existing data to use the typed columns exclusively.

**Architecture:** This is a 4-axis change — writers, schema (indexes + stats), query builder, and a one-shot data migration — all converging on a single invariant: *the typed columns `path`, `parent_path`, `path_depth` are the only source of truth; `idx` JSONB never carries these three keys for the built-in `path` index.* Custom path indexes (e.g. `tgpath`) keep their JSONB layout. Each axis is testable in isolation; they ship together because the writers and queries assume the same shape.

**Tech Stack:** Python 3.13, psycopg3, PostgreSQL 18, pytest, ZODB/Plone catalog API, zodb-pgjsonb state-processor plugin model.

**Tracking issue:** [bluedynamics/plone-pgcatalog#132](https://github.com/bluedynamics/plone-pgcatalog/issues/132)

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `src/plone/pgcatalog/query.py` | SQL fragment generation for path queries | **Modify** lines 517–522: dispatch built-in path index to typed columns |
| `src/plone/pgcatalog/indexing.py` | Direct catalog write path (`catalog_object()`) | **Modify** lines 43–46: stop adding path keys to `idx` |
| `src/plone/pgcatalog/catalog.py` | Plone catalog tool, sets pending annotation | **Modify** lines 501–504: stop adding path keys to `idx` |
| `src/plone/pgcatalog/processor.py` | State processor — read pending → typed cols + run bulk move | **Modify** lines 232–234 (read source), 287–294 (bulk move SQL) |
| `src/plone/pgcatalog/schema.py` | DDL constants for indexes + extended statistics | **Modify**: replace 7 indexes + 3 statistics objects to use typed columns. **Add**: idempotent `DROP INDEX IF EXISTS` for the obsolete JSONB versions |
| `src/plone/pgcatalog/migrations/__init__.py` | New migrations sub-package | **Create**: house migration helpers |
| `src/plone/pgcatalog/migrations/strip_path_keys.py` | One-shot UPDATE to remove keys from existing rows | **Create**: batched, idempotent, safe to re-run |
| `tests/test_strip_path_from_idx.py` | Test suite for this whole change | **Create**: write/read parity, query builder dispatch, schema state, migration |
| `CHANGES.md` | Changelog | **Modify**: top-of-file entry |

**Out of scope (separate follow-ups):**
- `id`/`getId` JSONB duplication (also 100% identical, ~4.5 MB) — different code path, different question (which key is canonical for catalog readers?). File as separate issue.
- The `overview_image` JSONB key (16 MB total) — needs investigation; not a duplication, just large.
- `idx_os_cat_events_upcoming` (0 scans ever) — separate cleanup, listed in #131.

---

## Pre-flight (one-time)

- [ ] **Step 0: Decide on worktree**

This change spans 4+ files in `sources/plone-pgcatalog/`. Use the `superpowers:using-git-worktrees` skill to create an isolated worktree if not already in one. Branch suggestion: `feat/strip-path-from-idx`.

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/plone-pgcatalog
git checkout -b feat/strip-path-from-idx
```

- [ ] **Step 0.1: Confirm test environment**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/plone-pgcatalog
.venv/bin/pytest tests/test_indexing.py -x --tb=short 2>&1 | tail -20
```

Expected: tests pass on current main. If they don't, stop and investigate.

---

## Task 1: Test scaffold for the whole change (TDD start)

**Files:**
- Create: `tests/test_strip_path_from_idx.py`

- [ ] **Step 1.1: Create the failing test file**

This is one big test module that covers all four axes of the change. Each test should fail on current main; together they prove the cleanup is complete when they all pass.

```python
"""Tests for stripping path/parent_path/path_depth from idx JSONB.

See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md
Issue: bluedynamics/plone-pgcatalog#132
"""

import pytest

from plone.pgcatalog.query import QueryBuilder


PATH_KEYS_IN_IDX = ("path", "path_parent", "path_depth")


# ── 1. Writer side: idx must not contain the path keys ────────────────────

class TestWriterDoesNotDuplicatePath:
    """After the cleanup, writers must NOT put path/path_parent/path_depth
    into idx JSONB.  Typed columns carry these values.
    """

    def test_catalog_object_strips_path_keys(self, pg_conn, sample_zoid):
        """plone.pgcatalog.indexing.catalog_object must not write path keys."""
        from plone.pgcatalog.indexing import catalog_object

        idx_in = {"portal_type": "Document", "Title": "T"}
        catalog_object(pg_conn, sample_zoid, "/Plone/doc", idx_in)

        row = pg_conn.execute(
            "SELECT path, parent_path, path_depth, idx FROM object_state "
            "WHERE zoid = %s",
            (sample_zoid,),
        ).fetchone()

        assert row.path == "/Plone/doc"
        assert row.parent_path == "/Plone"
        assert row.path_depth == 2
        for key in PATH_KEYS_IN_IDX:
            assert key not in row.idx, (
                f"{key!r} must not be written to idx JSONB after cleanup"
            )

    def test_pgcatalog_tool_set_pg_annotation_strips_path_keys(self, pg_conn, plone_obj):
        """PlonePGCatalogTool._set_pg_annotation must not write path keys to idx."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool, ANNOTATION_KEY

        tool = PlonePGCatalogTool()
        tool._set_pg_annotation(plone_obj, "/Plone/doc")

        pending = plone_obj.__dict__[ANNOTATION_KEY]
        for key in PATH_KEYS_IN_IDX:
            assert key not in pending["idx"], (
                f"{key!r} must not be in pending idx after cleanup"
            )

    def test_processor_reads_typed_cols_not_idx(self, pg_conn, sample_pending):
        """CatalogStateProcessor.process must source parent_path/path_depth
        from compute_path_info(path), not from idx."""
        from plone.pgcatalog.processor import CatalogStateProcessor

        # pending["path"] = "/a/b/c" → parent_path="/a/b", depth=3
        # idx must NOT carry path_parent/path_depth (test isolation)
        processor = CatalogStateProcessor()
        result = processor.process(sample_pending, "fake_state_dict")

        assert result["path"] == "/a/b/c"
        assert result["parent_path"] == "/a/b"
        assert result["path_depth"] == 3


# ── 2. Bulk move (rename of subtree) keeps typed cols in sync, leaves idx alone ──

class TestBulkMoveDoesNotTouchIdxPathKeys:
    def test_bulk_move_updates_typed_only(self, pg_conn, two_objects_at):
        """After bulk move SQL, typed cols reflect new path; idx still has no path keys."""
        # two_objects_at fixture: pre-creates /Plone/old/a and /Plone/old/b
        from plone.pgcatalog.processor import CatalogStateProcessor

        processor = CatalogStateProcessor()
        # Register a move /Plone/old → /Plone/new
        from plone.pgcatalog.move import register_move
        register_move("/Plone/old", "/Plone/new", depth_delta=0)

        cursor = pg_conn.cursor()
        processor.finalize(cursor)
        pg_conn.commit()

        for old_path in ("/Plone/old/a", "/Plone/old/b"):
            row = pg_conn.execute(
                "SELECT path, parent_path, idx FROM object_state WHERE path = %s",
                (old_path.replace("/old/", "/new/"),),
            ).fetchone()
            assert row is not None, f"row not found at new path"
            assert row.parent_path == "/Plone/new"
            for key in PATH_KEYS_IN_IDX:
                assert key not in row.idx


# ── 3. Query builder: built-in path index → typed cols, custom → JSONB ──

class TestQueryBuilderDispatchesPathToTypedColumns:
    def _build(self, query):
        qb = QueryBuilder(query, language="simple")
        qb.build()
        return qb.sql, qb.params

    def test_builtin_path_index_uses_typed_columns(self):
        sql, params = self._build({"path": {"query": "/Plone/x", "depth": -1}})
        # Must NOT use idx->>'path' for the built-in path index
        assert "idx->>'path'" not in sql
        assert "(idx->>'path_depth')::integer" not in sql
        # MUST use typed columns
        assert " path " in sql or "path " in sql or " path)" in sql

    def test_builtin_path_navtree_uses_typed_parent_path(self):
        sql, params = self._build(
            {"path": {"query": "/Plone/a/b", "navtree": True, "depth": 1}}
        )
        assert "idx->>'path_parent'" not in sql
        assert "parent_path" in sql

    def test_builtin_path_depth_uses_typed_path_depth(self):
        sql, params = self._build({"path": {"query": "/Plone", "depth": 2}})
        assert "(idx->>'path_depth')::integer" not in sql
        assert "path_depth" in sql

    def test_custom_path_index_keeps_jsonb_keys(self):
        # tgpath (or any non-builtin path index): idx_key="tgpath", uses JSONB
        sql, params = self._build(
            {"tgpath": {"query": "/Plone/x", "depth": -1}}
        )
        assert "idx->>'tgpath'" in sql


# ── 4. Schema state: indexes + statistics on typed cols, JSONB versions gone ──

class TestSchemaUsesTypedColumns:
    """Verify the resulting schema after migrations."""

    EXPECTED_TYPED_COL_INDEXES = {
        "idx_os_cat_path",          # → btree(path)
        "idx_os_cat_path_pattern",  # → btree(path text_pattern_ops)
        "idx_os_cat_path_parent",   # → btree(parent_path)
        "idx_os_cat_path_depth",    # → btree(path_depth)
        "idx_os_cat_parent_type",   # → btree(parent_path, idx->>'portal_type')
        "idx_os_cat_path_type",     # → btree(path text_pattern_ops, idx->>'portal_type')
        "idx_os_cat_path_depth_type",
        "idx_os_cat_nav_visible",
    }

    def test_path_indexes_reference_typed_columns(self, pg_conn):
        rows = pg_conn.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'object_state'
              AND indexname = ANY(%s)
        """, (list(self.EXPECTED_TYPED_COL_INDEXES),)).fetchall()
        for r in rows:
            # The path expression in any of these indexes must be the typed column,
            # not the JSONB extract.
            assert "idx ->> 'path'" not in r.indexdef, (
                f"{r.indexname}: still uses idx->>'path' — migration incomplete"
            )
            assert "idx ->> 'path_parent'" not in r.indexdef
            assert "idx ->> 'path_depth'" not in r.indexdef

    def test_extended_statistics_reference_typed_columns(self, pg_conn):
        rows = pg_conn.execute("""
            SELECT stxname, pg_get_statisticsobjdef(oid) AS stxdef
            FROM pg_statistic_ext
            WHERE stxname IN (
                'stts_os_parent_type',
                'stts_os_path_type',
                'stts_os_path_depth_type'
            )
        """).fetchall()
        for r in rows:
            assert "idx ->> 'path'" not in r.stxdef
            assert "idx ->> 'path_parent'" not in r.stxdef
            assert "idx ->> 'path_depth'" not in r.stxdef


# ── 5. Migration: existing rows get their path keys stripped, idempotently ──

class TestMigrationStripsPathKeys:
    def test_strip_removes_keys_idempotently(self, pg_conn, dirty_rows):
        """dirty_rows fixture inserts 5 rows with path/path_parent/path_depth
        present in idx — simulating pre-migration state."""
        from plone.pgcatalog.migrations.strip_path_keys import run

        run(pg_conn, batch_size=2)  # forces multiple batches

        rows = pg_conn.execute(
            "SELECT idx FROM object_state WHERE zoid = ANY(%s)",
            (dirty_rows,),
        ).fetchall()
        for r in rows:
            for key in PATH_KEYS_IN_IDX:
                assert key not in r.idx

    def test_strip_is_idempotent(self, pg_conn, dirty_rows):
        from plone.pgcatalog.migrations.strip_path_keys import run

        run(pg_conn)
        # Second run must be a no-op (zero rows updated)
        result = run(pg_conn)
        assert result["rows_updated"] == 0
```

- [ ] **Step 1.2: Run the new tests; they must all fail**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/plone-pgcatalog
.venv/bin/pytest tests/test_strip_path_from_idx.py -v 2>&1 | tail -50
```

Expected: every test fails (most likely import errors or fixture errors first; that's fine — we'll define fixtures as we go).

- [ ] **Step 1.3: Sketch fixtures** (do not implement — just identify gaps)

Look at `tests/conftest.py` to find existing fixtures (`pg_conn`, etc.). Likely missing fixtures we'll need:
- `sample_zoid` — a zoid pre-created in `object_state`
- `plone_obj` — a Plone content stub with `getPhysicalPath()`
- `sample_pending` — a pending dict shaped like `_pgcatalog_pending`
- `two_objects_at` — pre-creates 2 catalog rows under a path
- `dirty_rows` — pre-creates rows with the path keys still in `idx`

Add minimal versions of these to `tests/conftest.py` as needed. Don't over-engineer — only add what each test consumes.

- [ ] **Step 1.4: Commit the failing tests**

```bash
git add tests/test_strip_path_from_idx.py tests/conftest.py
git commit -m "test: failing test scaffold for stripping path keys from idx (#132)"
```

---

## Task 2: Query builder — dispatch built-in `path` to typed columns

**Files:**
- Modify: `src/plone/pgcatalog/query.py:517-522`

- [ ] **Step 2.1: Confirm exactly which path tests fail**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestQueryBuilderDispatchesPathToTypedColumns -v 2>&1 | tail -30
```

Expected: 3 failures (`test_builtin_path_*`); 1 pass (`test_custom_path_index_keeps_jsonb_keys` — current code already uses JSONB).

- [ ] **Step 2.2: Modify `_handle_path` to dispatch on the built-in path index**

Open `src/plone/pgcatalog/query.py`. Replace lines 517–522 with:

```python
        # Dispatch: the built-in "path" index lives in typed columns
        # (path, parent_path, path_depth).  Custom path indexes
        # (e.g. "tgpath") still store their data in idx JSONB.
        # See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md
        if idx_key is None and name == "path":
            expr_path = "path"
            expr_parent = "parent_path"
            expr_depth = "path_depth"
        else:
            key = name if idx_key is None else idx_key
            expr_path = f"idx->>'{key}'"
            expr_parent = f"idx->>'{key}_parent'"
            expr_depth = f"(idx->>'{key}_depth')::integer"
```

- [ ] **Step 2.3: Run the QB tests**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestQueryBuilderDispatchesPathToTypedColumns -v
```

Expected: all 4 pass.

- [ ] **Step 2.4: Run the broader path test suite to catch regressions**

```bash
.venv/bin/pytest tests/test_path.py tests/test_query.py tests/test_query_integration.py -v 2>&1 | tail -40
```

Expected: all pass. If any fail because they assert on `idx->>'path'` literally (rather than behavior), update them — they were testing implementation, not contract.

- [ ] **Step 2.5: Commit**

```bash
git add src/plone/pgcatalog/query.py tests/test_path.py tests/test_query.py
git commit -m "feat(query): use typed path columns for built-in path index (#132)"
```

---

## Task 3: Writer — `indexing.catalog_object` stops adding path keys to `idx`

**Files:**
- Modify: `src/plone/pgcatalog/indexing.py:43-46`

- [ ] **Step 3.1: Run the relevant test, confirm failure**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestWriterDoesNotDuplicatePath::test_catalog_object_strips_path_keys -v
```

Expected: FAIL — `'path' must not be written to idx JSONB after cleanup`.

- [ ] **Step 3.2: Remove the three `setdefault` calls**

In `src/plone/pgcatalog/indexing.py`, replace lines 43–46:

```python
    parent_path, path_depth = compute_path_info(path)

    # Store path data in idx JSONB for unified path queries
    idx.setdefault("path", path)
    idx.setdefault("path_parent", parent_path)
    idx.setdefault("path_depth", path_depth)
```

with:

```python
    parent_path, path_depth = compute_path_info(path)

    # Path data lives in typed columns only (path, parent_path, path_depth).
    # See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md
```

- [ ] **Step 3.3: Run, confirm pass**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestWriterDoesNotDuplicatePath::test_catalog_object_strips_path_keys -v
```

Expected: PASS.

- [ ] **Step 3.4: Run the full indexing tests for regressions**

```bash
.venv/bin/pytest tests/test_indexing.py -v 2>&1 | tail -30
```

Expected: all pass.

- [ ] **Step 3.5: Commit**

```bash
git add src/plone/pgcatalog/indexing.py
git commit -m "feat(indexing): stop dual-writing path keys to idx JSONB (#132)"
```

---

## Task 4: Writer — `catalog._set_pg_annotation` stops adding path keys

**Files:**
- Modify: `src/plone/pgcatalog/catalog.py:501-504`

- [ ] **Step 4.1: Run the test, confirm failure**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestWriterDoesNotDuplicatePath::test_pgcatalog_tool_set_pg_annotation_strips_path_keys -v
```

Expected: FAIL.

- [ ] **Step 4.2: Remove the three assignments**

In `src/plone/pgcatalog/catalog.py`, replace lines 499–510:

```python
        zoid = self._obj_to_zoid(obj)

        wrapper = self._wrap_object(obj)
        idx = self._extract_idx(wrapper)
        searchable_text = self._extract_searchable_text(wrapper)
        parent_path, path_depth = compute_path_info(uid)

        # Store built-in path data in idx JSONB for unified path queries
        idx["path"] = uid
        idx["path_parent"] = parent_path
        idx["path_depth"] = path_depth

        pending_data = {
            "path": uid,
            "idx": idx,
            "searchable_text": searchable_text,
        }
```

with:

```python
        zoid = self._obj_to_zoid(obj)

        wrapper = self._wrap_object(obj)
        idx = self._extract_idx(wrapper)
        searchable_text = self._extract_searchable_text(wrapper)

        # Path data lives in typed columns only (path, parent_path, path_depth).
        # The processor computes parent/depth from `path` via compute_path_info.
        # See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md
        pending_data = {
            "path": uid,
            "idx": idx,
            "searchable_text": searchable_text,
        }
```

(The `compute_path_info` import on the previous lines is no longer needed in this function — leave it if other functions in the file use it; otherwise remove.)

- [ ] **Step 4.3: Verify import cleanup**

```bash
.venv/bin/python -c "import ast; tree = ast.parse(open('src/plone/pgcatalog/catalog.py').read()); print('OK')"
```

If `compute_path_info` becomes unused, remove its import. Run pyflakes if available:

```bash
.venv/bin/python -m pyflakes src/plone/pgcatalog/catalog.py
```

- [ ] **Step 4.4: Run the test**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestWriterDoesNotDuplicatePath::test_pgcatalog_tool_set_pg_annotation_strips_path_keys -v
```

Expected: PASS.

- [ ] **Step 4.5: Run broader catalog tool tests**

```bash
.venv/bin/pytest tests/test_catalog_plone.py tests/test_indexing.py -v 2>&1 | tail -30
```

Expected: all pass.

- [ ] **Step 4.6: Commit**

```bash
git add src/plone/pgcatalog/catalog.py
git commit -m "feat(catalog): stop dual-writing path keys to idx JSONB (#132)"
```

---

## Task 5: Processor — read parent/depth from `path`, not `idx`

**Files:**
- Modify: `src/plone/pgcatalog/processor.py:227-238`

- [ ] **Step 5.1: Run the test, confirm failure**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestWriterDoesNotDuplicatePath::test_processor_reads_typed_cols_not_idx -v
```

Expected: FAIL — current code reads `idx.get("path_parent")` and `idx.get("path_depth")`, which after Task 3 + 4 are now `None`.

- [ ] **Step 5.2: Replace the source of `parent_path` and `path_depth`**

In `src/plone/pgcatalog/processor.py`, replace lines 227–240:

```python
        # Normal catalog: extract registered extra idx columns
        idx = pending.get("idx")
        extra_values = extract_extra_idx_columns(idx)

        result = {
            "path": pending.get("path"),
            "parent_path": idx.get("path_parent") if idx else None,
            "path_depth": idx.get("path_depth") if idx else None,
            "idx": Json(idx) if idx else None,
            "searchable_text": pending.get("searchable_text"),
            **extra_values,
        }
        result.update(get_backend().process_search_data(pending))
        return result
```

with:

```python
        # Normal catalog: extract registered extra idx columns
        idx = pending.get("idx")
        extra_values = extract_extra_idx_columns(idx)

        # Path data lives in typed columns only.  Compute parent/depth
        # from the canonical `path` field — not from idx.
        # See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md
        path = pending.get("path")
        if path:
            from plone.pgcatalog.columns import compute_path_info
            parent_path, path_depth = compute_path_info(path)
        else:
            parent_path, path_depth = None, None

        result = {
            "path": path,
            "parent_path": parent_path,
            "path_depth": path_depth,
            "idx": Json(idx) if idx else None,
            "searchable_text": pending.get("searchable_text"),
            **extra_values,
        }
        result.update(get_backend().process_search_data(pending))
        return result
```

(If `compute_path_info` is already imported at module level, drop the local import.)

- [ ] **Step 5.3: Run the test**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestWriterDoesNotDuplicatePath::test_processor_reads_typed_cols_not_idx -v
```

Expected: PASS.

- [ ] **Step 5.4: Commit**

```bash
git add src/plone/pgcatalog/processor.py
git commit -m "feat(processor): derive parent_path/path_depth from path, not idx (#132)"
```

---

## Task 6: Processor — bulk move SQL stops touching `idx` for path keys

**Files:**
- Modify: `src/plone/pgcatalog/processor.py:281-303`

- [ ] **Step 6.1: Run the test, confirm failure**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestBulkMoveDoesNotTouchIdxPathKeys -v
```

Expected: FAIL — current bulk move SQL writes the three keys back into idx.

- [ ] **Step 6.2: Simplify the bulk move SQL**

In `src/plone/pgcatalog/processor.py`, replace lines 281–304:

```python
        # Execute bulk path moves (one SQL per moved subtree)
        moves = pop_all_pending_moves()
        for old_prefix, new_prefix, depth_delta in moves:
            cursor.execute(
                """
                UPDATE object_state SET
                    path = %(new)s || substring(path FROM length(%(old)s) + 1),
                    parent_path = %(new)s || substring(parent_path FROM length(%(old)s) + 1),
                    path_depth = path_depth + %(dd)s,
                    idx = idx || jsonb_build_object(
                        'path',
                        %(new)s || substring(idx->>'path' FROM length(%(old)s) + 1),
                        'path_parent',
                        %(new)s || substring(idx->>'path_parent' FROM length(%(old)s) + 1),
                        'path_depth',
                        (idx->>'path_depth')::int + %(dd)s
                    )
                WHERE path LIKE %(like)s
                  AND idx IS NOT NULL
                """,
                {
                    "old": old_prefix,
                    "new": new_prefix,
                    "dd": depth_delta,
                    "like": old_prefix + "/%",
                },
            )
```

with:

```python
        # Execute bulk path moves (one SQL per moved subtree).
        # Touches typed columns only — idx no longer carries path keys.
        # See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md
        moves = pop_all_pending_moves()
        for old_prefix, new_prefix, depth_delta in moves:
            cursor.execute(
                """
                UPDATE object_state SET
                    path = %(new)s || substring(path FROM length(%(old)s) + 1),
                    parent_path = %(new)s || substring(parent_path FROM length(%(old)s) + 1),
                    path_depth = path_depth + %(dd)s
                WHERE path LIKE %(like)s
                """,
                {
                    "old": old_prefix,
                    "new": new_prefix,
                    "dd": depth_delta,
                    "like": old_prefix + "/%",
                },
            )
```

(The `AND idx IS NOT NULL` is no longer needed — the move now updates only typed columns, and rows with a `path` value have catalog data by definition.)

- [ ] **Step 6.3: Run the test + integration tests**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestBulkMoveDoesNotTouchIdxPathKeys tests/test_move_integration.py -v 2>&1 | tail -30
```

Expected: all pass.

- [ ] **Step 6.4: Commit**

```bash
git add src/plone/pgcatalog/processor.py
git commit -m "feat(processor): bulk move updates typed columns only (#132)"
```

---

## Task 7: Schema — replace JSONB-extract indexes with typed-column indexes

**Files:**
- Modify: `src/plone/pgcatalog/schema.py` (DDL constants for `CATALOG_INDEXES` and `CATALOG_STATISTICS`)

This task replaces 7 indexes and 3 extended-statistics objects. The strategy: explicit `DROP INDEX IF EXISTS` followed by `CREATE INDEX IF NOT EXISTS` so the swap is idempotent and self-healing on next startup.

- [ ] **Step 7.1: Run the schema test, confirm failure**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestSchemaUsesTypedColumns -v
```

Expected: FAIL — current schema definitions still use `idx->>'path'`.

- [ ] **Step 7.2: Replace the path-related indexes in `schema.py`**

In `src/plone/pgcatalog/schema.py`, locate the `CATALOG_INDEXES` constant.

For each of the 7 indexes below, **replace** the existing `CREATE INDEX IF NOT EXISTS …` block with a `DROP INDEX IF EXISTS …; CREATE INDEX IF NOT EXISTS …` pair using the typed columns:

```sql
-- Replace idx_os_cat_path (was: btree on idx->>'path')
DROP INDEX IF EXISTS idx_os_cat_path;
CREATE INDEX IF NOT EXISTS idx_os_cat_path
    ON object_state (path) WHERE path IS NOT NULL;

-- Replace idx_os_cat_path_pattern
DROP INDEX IF EXISTS idx_os_cat_path_pattern;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_pattern
    ON object_state USING btree (path text_pattern_ops) WHERE path IS NOT NULL;

-- Replace idx_os_cat_path_parent
DROP INDEX IF EXISTS idx_os_cat_path_parent;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_parent
    ON object_state (parent_path) WHERE parent_path IS NOT NULL;

-- Replace idx_os_cat_path_depth
DROP INDEX IF EXISTS idx_os_cat_path_depth;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_depth
    ON object_state (path_depth) WHERE path_depth IS NOT NULL;

-- Replace idx_os_cat_parent_type
DROP INDEX IF EXISTS idx_os_cat_parent_type;
CREATE INDEX IF NOT EXISTS idx_os_cat_parent_type
    ON object_state (parent_path, (idx->>'portal_type'))
    WHERE parent_path IS NOT NULL;

-- Replace idx_os_cat_path_type
DROP INDEX IF EXISTS idx_os_cat_path_type;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_type
    ON object_state (path text_pattern_ops, (idx->>'portal_type'))
    WHERE path IS NOT NULL;

-- Replace idx_os_cat_path_depth_type
DROP INDEX IF EXISTS idx_os_cat_path_depth_type;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_depth_type
    ON object_state (path text_pattern_ops, path_depth, (idx->>'portal_type'))
    WHERE path IS NOT NULL;

-- Replace idx_os_cat_nav_visible
DROP INDEX IF EXISTS idx_os_cat_nav_visible;
CREATE INDEX IF NOT EXISTS idx_os_cat_nav_visible
    ON object_state (path text_pattern_ops, (idx->>'portal_type'))
    WHERE path IS NOT NULL AND (idx->>'exclude_from_nav')::boolean = false;
```

- [ ] **Step 7.3: Replace the extended statistics in `schema.py`**

In the same file, locate the `CATALOG_STATISTICS` (or equivalent) constant and replace the three path-related statistics objects:

```sql
-- Replace stts_os_parent_type
DROP STATISTICS IF EXISTS stts_os_parent_type;
CREATE STATISTICS IF NOT EXISTS stts_os_parent_type
    (mcv, dependencies)
    ON parent_path, (idx->>'portal_type')
    FROM object_state;

-- Replace stts_os_path_type
DROP STATISTICS IF EXISTS stts_os_path_type;
CREATE STATISTICS IF NOT EXISTS stts_os_path_type
    (dependencies)
    ON path, (idx->>'portal_type')
    FROM object_state;

-- Replace stts_os_path_depth_type
DROP STATISTICS IF EXISTS stts_os_path_depth_type;
CREATE STATISTICS IF NOT EXISTS stts_os_path_depth_type
    (dependencies)
    ON path, path_depth, (idx->>'portal_type')
    FROM object_state;
```

- [ ] **Step 7.4: Verify the schema applies cleanly to a fresh test DB**

```bash
.venv/bin/pytest tests/test_schema.py -v 2>&1 | tail -20
```

Expected: pass. If a test asserts on the *old* index definitions, update it (those tests were locking down the obsolete shape).

- [ ] **Step 7.5: Run the schema invariant tests**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestSchemaUsesTypedColumns -v
```

Expected: PASS.

- [ ] **Step 7.6: Commit**

```bash
git add src/plone/pgcatalog/schema.py tests/test_schema.py
git commit -m "feat(schema): index path/parent_path/path_depth on typed columns (#132)"
```

---

## Task 8: Migration helper — strip keys from existing rows

**Files:**
- Create: `src/plone/pgcatalog/migrations/__init__.py`
- Create: `src/plone/pgcatalog/migrations/strip_path_keys.py`

- [ ] **Step 8.1: Run the migration test, confirm failure**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestMigrationStripsPathKeys -v
```

Expected: FAIL with `ImportError`.

- [ ] **Step 8.2: Create the migrations package**

```bash
mkdir -p src/plone/pgcatalog/migrations
touch src/plone/pgcatalog/migrations/__init__.py
```

- [ ] **Step 8.3: Implement the migration**

Create `src/plone/pgcatalog/migrations/strip_path_keys.py`:

```python
"""One-shot migration: remove path/path_parent/path_depth from idx JSONB.

After issue #132, these three keys live exclusively in typed columns
(path, parent_path, path_depth).  This script removes them from existing
rows that were written before the cleanup.

Idempotent and batched.  Safe to re-run; safe to interrupt.

Usage (from a shell with a configured psycopg connection):

    from plone.pgcatalog.migrations.strip_path_keys import run
    result = run(conn, batch_size=5000)
    print(result)  # {"batches": N, "rows_updated": M}
"""

import logging


log = logging.getLogger(__name__)

# Rows that still have any of the three keys present in idx
_DIRTY_PREDICATE = "idx ?| ARRAY['path', 'path_parent', 'path_depth']"

_BATCH_SQL = f"""
    WITH batch AS (
        SELECT zoid
        FROM object_state
        WHERE zoid > %(after_zoid)s
          AND idx IS NOT NULL
          AND {_DIRTY_PREDICATE}
        ORDER BY zoid
        LIMIT %(batch_size)s
    )
    UPDATE object_state os
       SET idx = idx - 'path' - 'path_parent' - 'path_depth'
      FROM batch
     WHERE os.zoid = batch.zoid
    RETURNING os.zoid
"""


def run(conn, batch_size: int = 5000) -> dict:
    """Strip path keys from idx in batches.

    Args:
        conn: psycopg connection (autocommit will be enabled per batch
              by issuing COMMIT after each batch — caller-managed
              transaction is committed first).
        batch_size: rows per batch.  Default 5000 keeps each UPDATE under
                    ~10 MB WAL and ~1 s on a typical pod.

    Returns: {"batches": int, "rows_updated": int}
    """
    if not conn.autocommit:
        # Ensure each batch commits independently — avoids a multi-GB
        # single transaction and lets autovacuum reclaim space mid-run.
        conn.commit()
        conn.autocommit = True

    after_zoid = -1
    batches = 0
    total = 0

    while True:
        with conn.cursor() as cur:
            cur.execute(_BATCH_SQL, {
                "after_zoid": after_zoid,
                "batch_size": batch_size,
            })
            zoids = [r[0] for r in cur.fetchall()]

        if not zoids:
            break

        batches += 1
        total += len(zoids)
        after_zoid = max(zoids)
        log.info(
            "strip_path_keys: batch %d, %d rows, last zoid=%d, total=%d",
            batches, len(zoids), after_zoid, total,
        )

    log.info("strip_path_keys: done. %d batches, %d rows updated.", batches, total)
    return {"batches": batches, "rows_updated": total}
```

- [ ] **Step 8.4: Run the migration tests**

```bash
.venv/bin/pytest tests/test_strip_path_from_idx.py::TestMigrationStripsPathKeys -v
```

Expected: both tests PASS.

- [ ] **Step 8.5: Commit**

```bash
git add src/plone/pgcatalog/migrations/__init__.py src/plone/pgcatalog/migrations/strip_path_keys.py
git commit -m "feat(migrations): strip_path_keys removes obsolete idx duplicates (#132)"
```

---

## Task 9: Run the full test suite

- [ ] **Step 9.1: Run everything**

```bash
.venv/bin/pytest tests/ -v 2>&1 | tail -60
```

Expected: all 700-ish tests pass. Pay special attention to:
- `test_path.py`, `test_query.py`, `test_query_integration.py`
- `test_indexing.py`, `test_catalog_plone.py`
- `test_move_integration.py`
- `test_schema.py`
- `test_strip_path_from_idx.py` (the new module — every test in it must pass)

If anything fails, investigate. Common cases:
- A test asserted on `idx->>'path'` in a generated SQL string — update the assertion to check the *behavior*, not the implementation.
- A fixture seeded `idx` with the path keys explicitly — remove that seeding.

- [ ] **Step 9.2: Lint pass**

```bash
.venv/bin/python -m pyflakes src/plone/pgcatalog/ 2>&1
```

Expected: no warnings about unused imports introduced by the cleanup.

- [ ] **Step 9.3: Commit any test fixups**

```bash
git add tests/
git commit -m "test: align assertions with typed-column path storage (#132)"
```

---

## Task 10: CHANGES.md

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 10.1: Add changelog entry**

Open `CHANGES.md`. Under the next-release header (or add one if needed — check `gh release list -L 5` first per user preference, do NOT infer the version from CHANGES.md alone), insert:

```markdown
### Changed

- Stop duplicating `path`, `path_parent`, and `path_depth` keys in the `idx`
  JSONB column. These three fields now live exclusively in their typed columns
  on `object_state`. Previously identical values were stored in both places,
  wasting ~10% of JSONB storage and blocking the planner from getting accurate
  selectivity stats on path-subtree filters. Indexes and extended statistics
  on these fields have been migrated to reference the typed columns directly.
  Custom `PATH`-type indexes (e.g. `tgpath`) are unaffected.

  **Migration:** Schema and writer changes are picked up automatically on
  startup (idempotent `DROP/CREATE` of affected indexes). To strip the
  obsolete keys from existing JSONB on large catalogs, run:

  ```python
  from plone.pgcatalog.migrations.strip_path_keys import run
  run(conn, batch_size=5000)
  ```

  Safe to run online and idempotent.

  See: bluedynamics/plone-pgcatalog#132.
```

- [ ] **Step 10.2: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog entry for #132 path JSONB dedup"
```

---

## Task 11: Manual smoke verification (optional, before opening PR)

These are quick sanity checks against a local test DB to confirm the pipeline works end-to-end. Skip if CI covers this and you trust it.

- [ ] **Step 11.1: Spin up a fresh test DB and verify schema bootstraps clean**

```bash
docker exec -it zodb-pgjsonb-dev psql -U zodb -d zodb_test -c "DROP TABLE IF EXISTS object_state CASCADE;"
.venv/bin/pytest tests/test_indexing.py::test_first_object -v
```

Then:

```bash
docker exec -it zodb-pgjsonb-dev psql -U zodb -d zodb_test \
    -c "\d object_state" | grep -E "(path|parent|depth)"
```

Expected: typed columns present; indexes named `idx_os_cat_path*` and `idx_os_cat_parent_type` reference `path`/`parent_path`/`path_depth` columns directly (not `idx ->> 'path'`).

- [ ] **Step 11.2: Verify a write does not produce path keys in idx**

```bash
docker exec -it zodb-pgjsonb-dev psql -U zodb -d zodb_test \
    -c "SELECT zoid, path, idx ? 'path' AS has_path_key FROM object_state LIMIT 5;"
```

Expected: `has_path_key = f` for every row written by the new code.

---

## Task 12: PR

- [ ] **Step 12.1: Push the branch**

```bash
git push -u origin feat/strip-path-from-idx
```

- [ ] **Step 12.2: Open the PR**

Use `gh pr create`. PR body should:
- Reference issue #132 (`Closes #132`)
- Summarize the four axes of the change
- Note the migration step (mention the `strip_path_keys.run()` helper)
- Link to this plan
- Mention the prod numbers measured during diagnosis (~21 MB JSONB freed, ~140 MB index storage freed if the duplicate `idx_os_path` is also dropped — though that drop is *not* in this PR; it's existing schema)

---

## Self-Review

Done after writing the plan above. Findings:

1. **Spec coverage:** Issue #132 lists three phases (writer, queries/indexes, bonus check). Phase 1 → Tasks 3–6. Phase 2 → Tasks 2 + 7. Phase 3 → addressed in pre-flight audit (clean). Migration is added as Task 8 (issue says "background migration job is available" — Task 8 provides it). ✅

2. **Placeholder scan:** No "TBD" / "implement later" / "similar to Task N". All code blocks contain real, runnable code. Checked. ✅

3. **Type consistency:**
   - `expr_path`, `expr_parent`, `expr_depth` — same names used in Task 2 dispatch and in helpers `_path_subtree`, `_path_exact`, etc. (existing in query.py). ✅
   - `compute_path_info(path) -> (parent_path, path_depth)` — used identically in Tasks 5 (processor) and references real signature in catalog.py / indexing.py. ✅
   - Migration `run(conn, batch_size=5000) -> dict` — signature matches both test calls and the changelog snippet. ✅
   - Index names retained verbatim from the existing schema; no rename collisions. ✅

4. **Open question from issue (`idx->>'path'` consumed elsewhere?):** The exploration agent confirmed only `query.py` reads these keys in production code. No REST/serialiser path. ZCatalog readers go through the typed-column-backed `path` brain attribute, not the JSONB. Documented in plan as resolved. ✅

5. **Risk: planner regression on un-migrated rows.** During the brief window after deploy but before `strip_path_keys.run()` is invoked, queries will use the new typed-column indexes. Old rows still have the JSONB keys but the new query builder doesn't read them — so old rows behave identically to new rows from the query side. Migration only reclaims JSONB storage; it is not required for correctness. ✅

---

## Future Work (not in this plan)

- **`id` / `getId` 100% duplication inside idx (~4.5 MB):** different code path (Plone-side index registration; both indexes register and both end up in idx via the catalog extraction loop). File a separate issue. Likely fix: filter one out at extraction time, or alias them via an SQL view.
- **`overview_image` is the largest single JSONB key (16 MB total):** turns out to be a path-component array, not image data. Worth investigating what consumes it and whether it justifies the size.
- **Drop `idx_os_path` (typed-column duplicate of `idx_os_cat_path`):** after this PR lands, both indexes have identical content. Delete the redundant one in a follow-up to free another ~27 MB.
- **`idx_os_cat_events_upcoming` (0 scans ever):** unrelated cleanup, already noted in #131.
