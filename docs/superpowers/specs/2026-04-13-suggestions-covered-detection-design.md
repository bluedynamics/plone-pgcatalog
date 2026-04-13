# Suggested Indexes: Fix "Apply" Loop and Spurious 500 — Design Spec

**Issue:** https://github.com/bluedynamics/plone-pgcatalog/issues/119
**Date:** 2026-04-13

## Problem

In the ZMI Catalog → Slow Queries → "Suggested Indexes" view, applying a
suggestion creates the index successfully, but on page reload the same
suggestion is still listed as `new` with an Apply button.  Clicking
Apply again hits `CREATE INDEX CONCURRENTLY` → `relation already exists`
→ HTTP 500.

Two independent detection bugs in `suggestions.py:_check_covered()`
cause the re-listing, and a missing pre-flight in `apply_index()` lets
the duplicate-create error propagate as a 500.

## Root cause

### Bug 1 — case-sensitive name match (Check 1)

[`suggestions.py:239-241`](src/plone/pgcatalog/suggestions.py):

```python
m = re.search(r"(?:CREATE INDEX\s+(?:CONCURRENTLY\s+)?)(\S+)", ddl, re.I)
if m and m.group(1) in existing_indexes:
    return "already_covered"
```

DDL generator (`_add_btree_suggestions`, `suggestions.py:209`) builds:

```python
name = "idx_os_sug_" + "_".join(field_names)
```

If a field is registered with mixed case (e.g. `Language`), the name is
`idx_os_sug_Language_portal_type_end`.  PostgreSQL folds unquoted
identifiers to lowercase, so `pg_indexes.indexname` returns
`idx_os_sug_language_portal_type_end`.  `existing_indexes` is keyed by
that lowercased form (`get_existing_indexes` queries `pg_indexes`
directly), so the case-sensitive `in` lookup misses.

### Bug 2 — expression normalization too weak (Check 2)

[`suggestions.py:252-266`](src/plone/pgcatalog/suggestions.py):

```python
def _normalize_idx_expr(ddl):
    m = re.search(r"\((.+)\)\s*(?:WHERE|$)", ddl)
    if not m:
        return ""
    expr = m.group(1)
    expr = re.sub(r"::text", "", expr)
    expr = re.sub(r"\s+", " ", expr).strip()
    expr = re.sub(r"\(\((.+?)\)\)", r"(\1)", expr)
    return expr
```

Generated DDL: `(idx->>'Language')` (no spaces around `->>`).
PG-stored `indexdef`:  `((idx ->> 'Language'::text))` (spaces around
`->>`, `::text` cast, extra paren level).

`_normalize_idx_expr` handles `::text` and whitespace collapse, but
leaves `idx->>'x'` vs `idx ->> 'x'` unequal — the whitespace collapse
only normalizes *runs* of whitespace, not insertion of whitespace
between `idx` and `->>`.  Substring match fails, Check 2 returns False.

### Bug 3 — `apply_index` is not idempotent on duplicate

[`suggestions.py:320-397`](src/plone/pgcatalog/suggestions.py) only
drops `INVALID` index remnants from aborted CIC builds; a *valid*
pre-existing index with the same name raises `psycopg.errors.DuplicateTable`
which is caught but returned as a generic failure string.  Called from
`manage_apply_index` ([`catalog.py:1263`](src/plone/pgcatalog/catalog.py)),
that failure surfaces correctly as an `index_error=...` redirect — so
strictly speaking, Bug 3 is *not* a 500 (the issue reporter's log
evidence is consistent with a success that's misread).  But it still
produces a "failure" toast for what is effectively a no-op, confusing
the operator.

Making `apply_index` return a *success* no-op when a valid index with
the same name already exists is the right idempotency contract.

## Fix

### Bug 1 — lowercase the extracted name before lookup

```python
m = re.search(r"(?:CREATE INDEX\s+(?:CONCURRENTLY\s+)?)(\S+)", ddl, re.I)
if m and m.group(1).lower() in existing_indexes:
    return "already_covered"
```

`existing_indexes` keys are already lowercased by PG, so no change on
that side.  This is the authoritative signal — our DDL generator emits
deterministic names, and PG stores them deterministically-lowercased —
so once the lookup is case-insensitive, Bug 1 is fully covered.

### Bug 2 — strengthen `_normalize_idx_expr`

Add two canonical rewrites before the final whitespace collapse:

1. Normalize whitespace around `->>` (and `->`): squeeze out all
   whitespace around JSON arrow operators.  This makes `idx ->> 'x'`
   match `idx->>'x'` regardless of which form the source uses.
2. Iteratively collapse *all* redundant paren wrappers, not just one
   level.  Replace the single-pass `re.sub(r"\(\((.+?)\)\)", r"(\1)", expr)`
   with a loop that runs until no more changes happen, so
   `(((x)))` → `(x)`.

Rewritten function:

```python
def _normalize_idx_expr(ddl):
    m = re.search(r"\((.+)\)\s*(?:WHERE|$)", ddl, re.I)
    if not m:
        return ""
    expr = m.group(1)
    # Strip PG's explicit ::text casts
    expr = re.sub(r"::text\b", "", expr)
    # Squeeze whitespace around JSON arrow operators
    expr = re.sub(r"\s*(->>?|#>>?|#>)\s*", r"\1", expr)
    # Collapse runs of whitespace
    expr = re.sub(r"\s+", " ", expr).strip()
    # Iteratively collapse doubled parens until stable
    while True:
        new = re.sub(r"\(\s*\(([^()]+)\)\s*\)", r"(\1)", expr)
        if new == expr:
            break
        expr = new
    return expr
```

### Bug 3 — idempotent `apply_index`

In `apply_index`, before the CIC build, check `pg_index` for a
**valid** index with the same name.  If present, short-circuit with
success:

```python
with conn.cursor() as cur:
    cur.execute(
        "SELECT i.indisvalid FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE c.relname = %s",
        (idx_name.lower(),),
    )
    row = cur.fetchone()
    if row is not None:
        is_valid = row[0] if isinstance(row, tuple) else row["indisvalid"]
        if is_valid:
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

This replaces the current INVALID-only check with a two-state decision
that also handles the valid-pre-existing case.  Note `idx_name.lower()`
when querying `pg_class.relname` — same case-folding issue as Bug 1.

## Testing

### `_check_covered` — mixed case (Bug 1)

Extend `TestSuggestIndexes`:

- Registry with a field named `Language` (mixed case).
- `existing_indexes` keyed with the lowercased PG-stored name
  `idx_os_sug_language_portal_type_end`, plus the PG-normalized
  `indexdef` as value.
- Assert the suggestion's status is `already_covered`.

### `_normalize_idx_expr` — expression forms (Bug 2)

New unit tests for `_normalize_idx_expr`:

- Generated form: `((idx->>'Language'), (idx->>'portal_type'), (pgcatalog_to_timestamptz(idx->>'end')))`
- PG-stored form: `((idx ->> 'Language'::text), (idx ->> 'portal_type'::text), pgcatalog_to_timestamptz((idx ->> 'end'::text)))`

  (the actual `pg_indexes.indexdef` structure for our composite suggestions)
- Both must normalize to the *same* string.
- Add a case for triple-nested parens `(((x)))` → normalized `(x)`.

### `_check_covered` — end-to-end with both forms (Bug 1 + 2)

Use the real DDL the generator produces for a mixed-case composite
suggestion, plus the `indexdef` as PostgreSQL actually stores it
(captured verbatim from the issue), and assert detection works.

### `apply_index` — idempotency (Bug 3)

Unit test with a mock `conn`:

- `pg_index` returns a valid index with the same name → `apply_index`
  returns `(True, "...already exists (no-op)", 0.0)` **without**
  executing `CREATE INDEX`.
- `pg_index` returns an INVALID index → behavior unchanged: drop
  + retry (existing path).
- `pg_index` returns nothing → behavior unchanged: proceed to CIC.

## Non-goals

- Approach B' (query `pg_get_indexdef(indexrelid)` and canonical-parse
  through PG) — overkill; the string-level normalization with the two
  added rewrites is sufficient for our deterministic DDL templates.
- Approach C (cache invalidation after apply) — with detection fixed,
  the reload path shows the correct state immediately.  Real-time UI
  update is a future polish, not part of this fix.
- Refactoring `_check_covered` / `_normalize_idx_expr` signatures —
  keep existing API.

## Rollout

- Single file change: `src/plone/pgcatalog/suggestions.py`
- Test-only changes: `tests/test_suggestions.py`
- No schema change, no migration, no API change
