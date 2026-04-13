# Suggestions Engine — PR 2: Field Categorization + Covering Composites

**Status:** Approved design
**Date:** 2026-04-13
**Issue:** [bluedynamics/plone-pgcatalog#122](https://github.com/bluedynamics/plone-pgcatalog/issues/122)
**Precedes:** PR 3 (EXPLAIN-driven coverage + ZMI JSON/JS UI)
**Follows:** PR 1 (extended statistics, merged as v1.0.0b51)

## Problem

The Slow Queries page produces **no index suggestions** for queries whose keys are all members of the catch-all `_NON_IDX_FIELDS` set or `_DEDICATED_FIELDS` map. Two concrete gaps:

1. **`effectiveRange`** (a DateRangeIndex virtual key that Plone expands into floor/ceiling date comparisons at query time) is skipped entirely — yet it's one of the most common filters in real traffic (published-content listings). The actual underlying columns (`effective`, `expires`) are regular `DATE` indexes but the engine never sees them because the query key it observes is the virtual `effectiveRange`.
2. **`sort_on`** is skipped as non-filterable, but ORDER BY is often the dominant cost in slow queries. When the planner can use a composite whose trailing column matches `ORDER BY`, it skips the sort step entirely (covering index).

These two gaps coincide on the canonical slow query in production: `portal_type=… & effectiveRange=now & sort_on=effective`.

## Scope

In scope for PR 2:

- Split `_NON_IDX_FIELDS` into three purpose-specific constants.
- Expand `effectiveRange` into `effective` for composite suggestions (narrow expansion — not `expires`).
- Extract the sort field from query params and append it as trailing composite column when eligible.
- Plumb the full params dict through `suggest_indexes()` so the sort field is available (and PR 3's EXPLAIN coverage will need it anyway).
- Extend `manage_get_slow_query_stats` SQL to return one representative params blob per query-key group.

Out of scope (deferred to PR 3):

- EXPLAIN-driven coverage check (replaces name-based `_check_covered`).
- Path-specific composite suggestions — path coverage is already comprehensive (`idx_os_cat_path`, `idx_os_cat_path_pattern`, `idx_os_cat_path_type`, `idx_os_cat_path_depth_type`) and PR 1's statistics address the path+portal_type misestimate.
- ZMI template conversion from DTML to JSON+JS.
- `expires` in the effectiveRange expansion (heap-filter on `expires` is cheap once leading conditions are selective; most production rows have `expires IS NULL`).

## Design Decisions

Six clarifying questions were resolved during brainstorming. The answers:

### Q1 — Sort covering approach

**A.** Append the sort field as trailing composite column in the suggested index.

Rationale: Postgres btree composites support ORDER BY skipping only if the ORDER BY column is a trailing column of the index, with equality predicates on the leading columns. This is the standard covering pattern.

### Q2 — `effectiveRange` expansion

Yes — expand the virtual `effectiveRange` key into its date contributor(s) so the suggestion engine can participate.

### Q3 — Which dates to expand to

**B-narrow:** only `effective`. Do not include `expires`.

Rationale: In real Plone catalogs most rows have `expires IS NULL`; the `expires` filter is effectively free once the leading equality predicate is selective. Adding `expires` to the composite bloats the index without noticeable planner benefit. If a future workload proves otherwise we can widen the expansion.

### Q4 — Path suggestions

**Skip.** Keep `path` in the skip set for PR 2. Revisit in PR 3 under EXPLAIN-driven coverage.

### Q5 — Categorization structure

**A.** Three named constants plus a skip set. **Drop `_NON_IDX_FIELDS` entirely** — the name is too vague to reason about.

A single-dict approach (`FIELD_POLICY`) was rejected as premature abstraction.

### Q6 — Params plumbing

**B.** Pass the full params dict into `suggest_indexes()`. PR 3's EXPLAIN-based coverage will need it anyway; plumbing it now avoids a second signature change.

## Section 1 — Architecture

### Signature change

```python
def suggest_indexes(query_keys, params, registry, existing_indexes):
```

`params` is the representative query params dict (same shape as stored in `pgcatalog_slow_queries.params`). It is the **second** positional arg — all existing call sites move to `params=None` when they don't have one.

### Callers

- `manage_get_slow_query_stats` (catalog.py:1175) — single call site in production code. Gets the representative params via a new LATERAL subquery (see Section 4).
- 23 existing test call sites — updated to pass `params=None` (or a dict when the test is exercising sort-covering behavior).

### Untouched

- DDL templates (`_btree_expr`, `_gin_expr`, `_ensure_text_indexes`).
- `_check_covered` / `_normalize_idx_expr` logic (PR 3 replaces these).
- `apply_index` / `drop_index`.
- `pgcatalog_slow_queries` table schema.
- ZMI templates (`manage_slow_queries.dtml` etc.) — stays DTML until PR 3.

## Section 2 — Field Categorization

Replace the existing `_NON_IDX_FIELDS` frozenset with three purpose-specific constants plus a skip set. Semantics are distinct and code paths differ — the split makes each reader obvious.

```python
# Pagination meta — ignored everywhere.
_PAGINATION_META = frozenset({"b_size", "b_start"})

# Sort meta — not a filter, but the VALUE of sort_on/sort_order
# drives covering-composite construction.
_SORT_META = frozenset({"sort_on", "sort_order"})

# Virtual filter fields that expand to real idx keys for composite
# suggestions. Each entry: virtual_key -> [(real_field, IndexType), ...].
# The real fields participate in _add_btree_suggestions as if the
# query had mentioned them directly.
_FILTER_VIRTUAL = {
    "effectiveRange": [("effective", IndexType.DATE)],
}

# Fields we deliberately skip in PR 2 (dedicated coverage elsewhere,
# or deferred to PR 3).
_SKIP_FIELDS = frozenset({"path", "SearchableText"})
```

`_DEDICATED_FIELDS`, `_NON_COMPOSITE_TYPES`, `_SKIP_TYPES`, `_SELECTIVITY_ORDER` — unchanged.

`_NON_IDX_FIELDS` — **removed** (no callers once the new constants land).

### Classification at the top of `suggest_indexes`

Each query key is routed through one pass:

1. Key in `_PAGINATION_META` → drop silently.
2. Key in `_SORT_META` → drop (the value is consumed separately via `_extract_sort_field`).
3. Key in `_FILTER_VIRTUAL` → expand to the real field tuples; they go into the btree candidate set.
4. Key in `_SKIP_FIELDS` → drop (may become a no-op "already covered" note in PR 3).
5. Key in `_DEDICATED_FIELDS` → emit "already_covered" note, don't add to btree candidates.
6. Key in registry → add to btree/gin candidates per IndexType.
7. Unknown key → emit "unknown field, consider registering the index" hint.

## Section 3 — Sort Field Extraction & Composite Builder

### `_extract_sort_field(params, registry)`

Returns `(field_name, IndexType)` or `None`.

1. If `params` is None/empty → return None.
2. Find the params key whose name **contains** `"sort_on"` (Plone produces names like `sort_on`, `p_sort_on_1`, etc. — substring match is the pragmatic fit).
3. Look up the resulting field in the registry.
4. Return `(field_name, idx_type)` if the type is btree-composite-eligible (not in `_NON_COMPOSITE_TYPES`, not in `_SKIP_TYPES`). Otherwise None.

### `_add_btree_suggestions` — covering composite

Signature gains a sort tuple:

```python
def _add_btree_suggestions(btree_fields, sort_field, existing_indexes, suggestions):
    """
    btree_fields: list of (name, IndexType) — filter columns.
    sort_field:   (name, IndexType) or None — trailing covering column.
    """
```

Algorithm:

1. Sort `btree_fields` by `_SELECTIVITY_ORDER` (existing logic).
2. Cap at **3 columns total, sort field included**. If a sort field is present, the filter list is truncated to `3 - 1 = 2` before the sort is appended. No sort → the existing cap of 3 applies unchanged.
3. Dedupe — if `sort_field[0]` already appears in the filter list, drop it from the trailing position (the existing leading column already satisfies the ORDER BY via the composite).
4. Build the DDL expression list from the ordered columns.
5. `_check_covered` gets called as today.
6. Reason string indicates covering, e.g.:

   > *"composite on (portal_type, effective) for slow query; last column covers ORDER BY effective"*

### Cap rationale

Three columns is the empirical sweet spot for Plone catalogs: beyond three, index write amplification dominates read savings. The cap stays a constant (`_MAX_COMPOSITE_COLUMNS = 3`) for PR 3 to revisit.

## Section 4 — Data Flow & Tests

### SQL — `manage_get_slow_query_stats`

Current query groups by `query_keys`. Add a LATERAL subquery that fetches the slowest single row's `params` blob as the representative for each group:

```sql
SELECT
    grp.query_keys,
    grp.n_calls,
    grp.total_ms,
    grp.avg_ms,
    grp.max_ms,
    slowest.params AS representative_params
FROM (
    SELECT query_keys,
           COUNT(*)        AS n_calls,
           SUM(duration_ms) AS total_ms,
           AVG(duration_ms) AS avg_ms,
           MAX(duration_ms) AS max_ms
    FROM pgcatalog_slow_queries
    WHERE  recorded_at > now() - %(window)s::interval
    GROUP BY query_keys
) grp
LEFT JOIN LATERAL (
    SELECT params
    FROM pgcatalog_slow_queries s
    WHERE s.query_keys = grp.query_keys
      AND s.recorded_at > now() - %(window)s::interval
    ORDER BY s.duration_ms DESC
    LIMIT 1
) slowest ON TRUE
ORDER BY grp.total_ms DESC;
```

The returned `representative_params` is passed as the `params` arg to `suggest_indexes()`.

### Tests

**Existing tests** (`tests/test_suggestions.py` — 23 tests) update their call sites from `suggest_indexes(keys, registry, existing)` to `suggest_indexes(keys, None, registry, existing)`. No behavioral change expected.

**New tests** (10):

1. `test_effective_range_expands_to_effective` — `effectiveRange` in keys yields a suggestion mentioning `effective`.
2. `test_effective_range_does_not_expand_to_expires` — expansion is narrow.
3. `test_sort_on_appends_trailing_column` — `params={"sort_on": "effective"}` → suggestion's DDL ends with `effective`.
4. `test_sort_on_deduped_when_already_leading` — sort field already in filter list → not appended twice.
5. `test_sort_on_ignored_for_non_composite_type` — `sort_on=SearchableText` → no sort-covering.
6. `test_sort_on_unknown_field_ignored` — sort field not in registry → no sort-covering, no crash.
7. `test_composite_cap_includes_sort` — 3 filter fields + sort → filter list truncated to 2, sort appended (total 3).
8. `test_params_none_behaves_as_before` — smoke test for the default arg.
9. `test_plone_sort_param_alias` — `params={"p_sort_on_1": "effective"}` (the substring match case).
10. `test_issue_122_pattern` — regression: query keys `{portal_type, effectiveRange}` + `params={"sort_on": "effective"}` produces a non-empty suggestion with DDL `(idx->>'portal_type', pgcatalog_to_timestamptz(idx->>'effective'))`.

### Migration / multi-pod safety

No DDL changes. No migration step. Rolling pod upgrade: old pods keep calling the old signature (4-arg vs 3-arg) — but there's only one production call site in the same package, so pod version skew doesn't cross this boundary. Safe.

## Out of Scope — PR 3 preview

- Replace `_check_covered` / `_normalize_idx_expr` with EXPLAIN JSON parsing: given the representative params we submit a real EXPLAIN (FORMAT JSON) and inspect which indexes were picked. That's authoritative coverage.
- Convert `manage_slow_queries.dtml` to a JSON endpoint + a small vanilla-JS frontend. No heavy JS libs per user constraint.
- Reconsider `path` suggestions using EXPLAIN coverage as the gate.
- Consider widening `_FILTER_VIRTUAL["effectiveRange"]` to include `expires` if EXPLAIN reveals heap-filter cost on real workloads.
