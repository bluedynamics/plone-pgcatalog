# Suggested Indexes Fix — Implementation Plan

**Goal:** Fix three detection/idempotency bugs in the Suggested Indexes flow: (A) case-sensitive name lookup in `_check_covered`, (B) too-weak expression normalization in `_normalize_idx_expr`, (D) non-idempotent `apply_index` when a valid index already exists.

**Architecture:** Single-file change in `src/plone/pgcatalog/suggestions.py` plus test additions in `tests/test_suggestions.py`. No schema, no API change.

**Tech Stack:** Python 3.12+, psycopg 3 (mock-only for unit tests), pytest.

**Spec:** `docs/superpowers/specs/2026-04-13-suggestions-covered-detection-design.md`

---

## File Map

- Modify: `src/plone/pgcatalog/suggestions.py`
- Modify: `tests/test_suggestions.py`
- Modify: `CHANGES.md` (append to Unreleased section)

---

### Task 1: Failing tests for mixed-case name and PG-stored expression forms

**Files:**
- Modify: `tests/test_suggestions.py`

- [ ] **Step 1: Add two failing tests to `TestSuggestIndexes`**

Insert these new methods at the end of `TestSuggestIndexes` (after `test_gopip_skipped` at ~line 234).  These reproduce the two observed detection failures.

```python
    def test_mixed_case_name_already_covered(self):
        """Mixed-case field name should match existing lowercased PG index.

        Regression for #119: `_check_covered` Check 1 was case-sensitive
        but PostgreSQL folds unquoted identifiers to lowercase in
        `pg_indexes.indexname`.
        """
        registry = _reg(Language=IndexType.FIELD)
        # PG stores unquoted identifiers lowercased in pg_indexes.
        existing = {
            "idx_os_sug_language": (
                "CREATE INDEX idx_os_sug_language ON public.object_state "
                "USING btree (((idx ->> 'Language'::text))) "
                "WHERE (idx IS NOT NULL)"
            )
        }
        result = suggest_indexes(["Language"], registry, existing)
        assert all(s["status"] == "already_covered" for s in result)

    def test_composite_already_covered_by_pg_normalized_indexdef(self):
        """Composite suggestion detects equivalent PG-stored indexdef.

        Regression for #119: `_normalize_idx_expr` did not normalize
        whitespace around `->>`, so the generated form and the
        PG-stored form didn't compare as equal even after the existing
        normalization passes.
        """
        registry = _reg(
            Language=IndexType.FIELD,
            portal_type=IndexType.FIELD,
            end=IndexType.DATE,
        )
        # Real indexdef text captured from pg_indexes after a successful
        # apply of this exact composite suggestion.
        existing = {
            "idx_os_sug_language_portal_type_end": (
                "CREATE INDEX idx_os_sug_language_portal_type_end "
                "ON public.object_state USING btree ("
                "((idx ->> 'Language'::text)), "
                "((idx ->> 'portal_type'::text)), "
                "pgcatalog_to_timestamptz((idx ->> 'end'::text))"
                ") WHERE (idx IS NOT NULL)"
            )
        }
        result = suggest_indexes(
            ["Language", "portal_type", "end"], registry, existing
        )
        assert all(s["status"] == "already_covered" for s in result)
```

Both tests exist to fail before the fix and pass after.

- [ ] **Step 2: Verify both tests fail against current code**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_suggestions.py -k "mixed_case_name_already_covered or composite_already_covered_by_pg_normalized_indexdef" -v`

Expected: both FAIL with `assert all(...) == already_covered` — current status is `new`.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_suggestions.py
git commit -m "test: failing regressions for mixed-case + PG-normalized detection (#119)"
```

---

### Task 2: Apply fix A — lowercase name match in `_check_covered`

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` — function `_check_covered` (~line 230)

- [ ] **Step 1: Update Check 1 to lowercase the extracted name**

Replace the existing function body:

```python
def _check_covered(ddl, existing_indexes):
    """Check if the suggested index already exists.

    Two checks:
    1. Exact name match (catches re-apply of same suggestion).
       Name is lowercased because PostgreSQL folds unquoted
       identifiers to lowercase in pg_indexes.indexname.
    2. Normalized expression match (catches existing idx_os_cat_*
       indexes that cover the same columns with different naming).
    """
    # Check 1: case-insensitive index name match
    m = re.search(r"(?:CREATE INDEX\s+(?:CONCURRENTLY\s+)?)(\S+)", ddl, re.I)
    if m and m.group(1).lower() in existing_indexes:
        return "already_covered"

    # Check 2: normalize and compare column expressions
    norm = _normalize_idx_expr(ddl)
    if norm:
        for _name, idx_def in existing_indexes.items():
            if norm in _normalize_idx_expr(idx_def):
                return "already_covered"
    return "new"
```

- [ ] **Step 2: Verify the mixed-case name test passes now (composite one may still fail)**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_suggestions.py::TestSuggestIndexes::test_mixed_case_name_already_covered -v`

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py
git commit -m "fix(suggestions): _check_covered uses case-insensitive name match (#119)"
```

---

### Task 3: Apply fix B — strengthen `_normalize_idx_expr`

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` — function `_normalize_idx_expr` (~line 252)

- [ ] **Step 1: Replace the function body**

```python
def _normalize_idx_expr(ddl):
    """Extract and normalize the column expression from a CREATE INDEX DDL.

    Produces a canonical form that compares equal across:
    - whitespace differences (including around ``->>``)
    - ``::text`` casts that PG adds on ingest
    - redundant paren wrappers PG adds around each expression
    """
    m = re.search(r"\((.+)\)\s*(?:WHERE|$)", ddl, re.I)
    if not m:
        return ""
    expr = m.group(1)
    # Strip PG's explicit ::text casts
    expr = re.sub(r"::text\b", "", expr)
    # Squeeze whitespace around JSON arrow operators — generated form
    # has no spaces (idx->>'x'), PG-stored form has them (idx ->> 'x').
    expr = re.sub(r"\s*(->>?|#>>?|#>)\s*", r"\1", expr)
    # Collapse runs of whitespace
    expr = re.sub(r"\s+", " ", expr).strip()
    # Iteratively collapse redundant paren wrappers — PG stores
    # ((expr)) where the generator emits (expr), and a single-pass
    # regex with a non-greedy group misses nested cases.
    while True:
        new = re.sub(r"\(\s*\(([^()]+)\)\s*\)", r"(\1)", expr)
        if new == expr:
            break
        expr = new
    return expr
```

- [ ] **Step 2: Add explicit unit tests for the normalizer**

Append a new `TestNormalizeIdxExpr` class at the end of `tests/test_suggestions.py`:

```python
class TestNormalizeIdxExpr:
    """Unit tests for _normalize_idx_expr — comparison canonicalization."""

    def test_generated_and_pg_stored_composite_equal(self):
        """Same index in generated and PG-stored form normalize equal."""
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        generated = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_Language_portal_type_end "
            "ON object_state ("
            "(idx->>'Language'), (idx->>'portal_type'), "
            "(pgcatalog_to_timestamptz(idx->>'end'))"
            ") WHERE idx IS NOT NULL"
        )
        stored = (
            "CREATE INDEX idx_os_sug_language_portal_type_end "
            "ON public.object_state USING btree ("
            "((idx ->> 'Language'::text)), "
            "((idx ->> 'portal_type'::text)), "
            "pgcatalog_to_timestamptz((idx ->> 'end'::text))"
            ") WHERE (idx IS NOT NULL)"
        )
        assert _normalize_idx_expr(generated) == _normalize_idx_expr(stored)

    def test_arrow_whitespace_normalized(self):
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        with_spaces = "CREATE INDEX i ON t ((idx ->> 'x')) WHERE idx IS NOT NULL"
        without_spaces = "CREATE INDEX i ON t ((idx->>'x')) WHERE idx IS NOT NULL"
        assert _normalize_idx_expr(with_spaces) == _normalize_idx_expr(without_spaces)

    def test_text_cast_stripped(self):
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        with_cast = "CREATE INDEX i ON t ((idx->>'x'::text)) WHERE idx IS NOT NULL"
        without_cast = "CREATE INDEX i ON t ((idx->>'x')) WHERE idx IS NOT NULL"
        assert _normalize_idx_expr(with_cast) == _normalize_idx_expr(without_cast)

    def test_nested_paren_collapse(self):
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        triple = "CREATE INDEX i ON t (((x))) WHERE idx IS NOT NULL"
        single = "CREATE INDEX i ON t ((x)) WHERE idx IS NOT NULL"
        # After normalization both collapse to the same canonical form.
        assert _normalize_idx_expr(triple) == _normalize_idx_expr(single)

    def test_no_where_clause(self):
        """DDL without WHERE still normalizes — pattern uses `|$` fallback."""
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        # Graceful: regex falls through to $ anchor
        result = _normalize_idx_expr("CREATE INDEX i ON t ((idx->>'x'))")
        assert "idx->>'x'" in result
```

- [ ] **Step 3: Run both the new unit tests and the composite regression**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_suggestions.py -v`

Expected: all tests pass, including:
- `test_composite_already_covered_by_pg_normalized_indexdef` (from Task 1) now PASS
- all 5 in `TestNormalizeIdxExpr`

- [ ] **Step 4: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "fix(suggestions): normalize ->> whitespace + iterative paren collapse (#119)"
```

---

### Task 4: Apply fix D — idempotent `apply_index` when valid index exists

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` — function `apply_index` (~line 320, specifically the INVALID-index-drop block around line 366)

- [ ] **Step 1: Replace the INVALID-only pre-flight with a valid-or-invalid decision**

Locate this existing block:

```python
        # Drop INVALID index from a previously aborted CONCURRENTLY build.
        # An INVALID index blocks new CREATE INDEX on the same name and
        # wastes disk space.  idx_name is validated above.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_index i "
                "JOIN pg_class c ON c.oid = i.indexrelid "
                "WHERE c.relname = %s AND NOT i.indisvalid",
                (idx_name,),
            )
            if cur.fetchone():
                log.warning(
                    "Dropping INVALID index %s (aborted previous build)",
                    idx_name,
                )
                conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {idx_name}")
```

Replace it with:

```python
        # Pre-flight: query pg_index for any index with this name.
        # Three cases:
        #   - valid index exists: idempotent success no-op (#119)
        #   - INVALID index from aborted CIC: drop and retry
        #   - no index: proceed to CREATE INDEX
        # relname is always lowercase in pg_class; match case-insensitively.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT i.indisvalid FROM pg_index i "
                "JOIN pg_class c ON c.oid = i.indexrelid "
                "WHERE c.relname = %s",
                (idx_name.lower(),),
            )
            row = cur.fetchone()
        if row is not None:
            # psycopg returns dict_row or tuple_row depending on the
            # caller's factory — handle both.
            is_valid = (
                row["indisvalid"] if hasattr(row, "keys") else row[0]
            )
            if is_valid:
                log.info(
                    "Index %s already exists and is valid — no-op",
                    idx_name,
                )
                return (
                    True,
                    f"Index {idx_name} already exists (no-op)",
                    0.0,
                )
            log.warning(
                "Dropping INVALID index %s (aborted previous build)",
                idx_name,
            )
            conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {idx_name}")
```

- [ ] **Step 2: Add unit tests for `apply_index` pre-flight with a mock conn**

Append a new `TestApplyIndexPreflight` class at the end of `tests/test_suggestions.py`:

```python
class TestApplyIndexPreflight:
    """Unit tests for apply_index idempotency (#119)."""

    def _make_conn(self, preflight_row):
        """Return a mock conn whose pg_index pre-flight returns the
        given row (tuple or None).
        """
        from unittest import mock

        conn = mock.MagicMock()
        conn.autocommit = False
        cur = mock.MagicMock()
        cur.fetchone.return_value = preflight_row
        # Support context-manager protocol on conn.cursor()
        ctx = mock.MagicMock()
        ctx.__enter__.return_value = cur
        ctx.__exit__.return_value = None
        conn.cursor.return_value = ctx
        return conn, cur

    def test_valid_pre_existing_index_is_noop(self):
        from plone.pgcatalog.suggestions import apply_index

        # pg_index returns indisvalid=True
        conn, cur = self._make_conn(preflight_row=(True,))
        ddl = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_foo "
            "ON object_state ((idx->>'foo')) WHERE idx IS NOT NULL"
        )
        success, msg, duration = apply_index(conn, ddl)

        assert success is True
        assert "already exists" in msg
        assert duration == 0.0
        # No CIC issued — conn.execute is only called for the pre-flight
        # SELECT (via cur.execute), never for the CREATE INDEX.
        executed_sqls = [c.args[0] for c in conn.execute.call_args_list]
        assert not any("CREATE INDEX" in s for s in executed_sqls)

    def test_invalid_pre_existing_index_is_dropped_then_built(self):
        from plone.pgcatalog.suggestions import apply_index

        # pg_index returns indisvalid=False
        conn, cur = self._make_conn(preflight_row=(False,))
        ddl = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_foo "
            "ON object_state ((idx->>'foo')) WHERE idx IS NOT NULL"
        )
        success, msg, _duration = apply_index(conn, ddl)

        assert success is True
        executed_sqls = [c.args[0] for c in conn.execute.call_args_list]
        # Both DROP INDEX CONCURRENTLY and CREATE INDEX CONCURRENTLY
        assert any("DROP INDEX CONCURRENTLY IF EXISTS" in s for s in executed_sqls)
        assert any("CREATE INDEX CONCURRENTLY" in s for s in executed_sqls)

    def test_no_pre_existing_index_proceeds_to_create(self):
        from plone.pgcatalog.suggestions import apply_index

        conn, cur = self._make_conn(preflight_row=None)
        ddl = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_foo "
            "ON object_state ((idx->>'foo')) WHERE idx IS NOT NULL"
        )
        success, msg, _duration = apply_index(conn, ddl)

        assert success is True
        executed_sqls = [c.args[0] for c in conn.execute.call_args_list]
        # No DROP, just CREATE
        assert not any(
            "DROP INDEX CONCURRENTLY IF EXISTS" in s for s in executed_sqls
        )
        assert any("CREATE INDEX CONCURRENTLY" in s for s in executed_sqls)
```

Note: `apply_index` is currently marked `# pragma: no cover` so these unit tests also raise its coverage.  Remove the pragma in the next step.

- [ ] **Step 3: Remove `# pragma: no cover` from `apply_index`**

In `src/plone/pgcatalog/suggestions.py`, change:

```python
def apply_index(conn, ddl, timeout=_DEFAULT_INDEX_TIMEOUT):  # pragma: no cover
```

to:

```python
def apply_index(conn, ddl, timeout=_DEFAULT_INDEX_TIMEOUT):
```

- [ ] **Step 4: Run the new tests**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_suggestions.py::TestApplyIndexPreflight -v`

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "fix(suggestions): apply_index is idempotent on valid pre-existing index (#119)"
```

---

### Task 5: Full suite regression + changelog

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Run the full suggestions test file**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_suggestions.py -v`

Expected: all original 24 tests + 2 Task-1 regressions + 5 TestNormalizeIdxExpr + 3 TestApplyIndexPreflight = 34 passed.

- [ ] **Step 2: Add changelog entry**

Read `CHANGES.md` header section (`## Unreleased` if present, otherwise create it directly after `# Changelog`).  Append:

```markdown
### Fixed

- Suggested Indexes UI: detect already-applied suggestions with
  mixed-case field names (e.g. ``Language``) by matching index names
  case-insensitively — PostgreSQL folds unquoted identifiers to
  lowercase.  Also strengthen expression normalization (whitespace
  around ``->>``, iterative paren collapse) so generated and
  PG-stored ``indexdef`` forms compare equal.  ``apply_index`` is
  now idempotent when a valid index with the same name already
  exists — returns success no-op instead of propagating the
  ``DuplicateTable`` error.  Closes #119.
```

- [ ] **Step 3: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog for suggestions detection + idempotency fix (#119)"
```

---

### Task 6: Push + open PR

- [ ] **Step 1: Push branch**

```bash
git push -u origin fix/suggestions-covered-detection
```

- [ ] **Step 2: Create PR**

```bash
gh pr create --repo bluedynamics/plone-pgcatalog --base main \
  --head fix/suggestions-covered-detection \
  --title "Suggested Indexes: fix apply-loop detection + idempotency (#119)" \
  --body "$(cat <<'EOF'
## Summary

Three targeted fixes in \`suggestions.py\` so applied suggestions stop reappearing and reclicks don't error:

- **Check 1 (case)**: \`_check_covered\` now lowercases the extracted index name before lookup — PG folds unquoted identifiers lowercase in \`pg_indexes.indexname\`, so mixed-case field names like \`Language\` previously missed.
- **Check 2 (normalization)**: \`_normalize_idx_expr\` now squeezes whitespace around JSON arrow operators (\`->>\`, \`->\`, \`#>>\`, \`#>\`) and iteratively collapses redundant paren wrappers — so the generator form \`(idx->>'x')\` and the PG-stored form \`((idx ->> 'x'::text))\` compare equal.
- **Idempotency**: \`apply_index\` pre-flights \`pg_index\` by (lowercased) name and returns a success no-op when a valid index with the same name already exists.  INVALID index remnants are still dropped and rebuilt as before.

Closes #119.

## Tests

- 2 regression tests for Check 1 + Check 2 (mixed-case name, composite PG-normalized indexdef).
- 5 unit tests for \`_normalize_idx_expr\` canonicalization.
- 3 unit tests for \`apply_index\` pre-flight branches (valid → no-op, invalid → drop+build, missing → build).
- \`apply_index\` loses its \`# pragma: no cover\`.

All pre-existing suggestions tests still pass.  No schema change, no API change.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Report the PR URL.
