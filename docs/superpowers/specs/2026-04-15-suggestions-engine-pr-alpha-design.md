# Suggestions Engine — PR α: Bundle Data Model + Partial GIN + Hybrid Templates

**Status:** Approved design
**Date:** 2026-04-15
**Issue:** [bluedynamics/plone-pgcatalog#122](https://github.com/bluedynamics/plone-pgcatalog/issues/122) (gap-analysis comment on 2026-04-15)
**Follows:** PR 1 (extended statistics, v1.0.0b51), PR 2 (field categorization + sort covering, v1.0.0b53)
**Precedes:** PR β (EXPLAIN-driven grading + JSON/JS UI) — see companion notes file `2026-04-15-suggestions-engine-pr-beta-notes.md`

## Problem

After PR 1 + PR 2 rolled to production (AAF, ~3.9M objects), a canonical slow-query pattern remains uncovered:

```
portal_type = 'Event' AND review_state = 'published'
AND Subject = 'AT26'
AND effective <= now AND expires >= now
ORDER BY effective
```

Still 900–1600 ms. Three structural reasons:

1. **KEYWORD fields don't get GIN templates.** PR 2 emits only btree composites. `Subject` is a JSONB-array KEYWORD and needs `GIN ((idx->'Subject'))`.
2. **No partial-index templates.** The effective fix is a GIN *scoped by* `portal_type='Event' AND review_state='published'` — smaller, more specific, planner picks it preferentially. The engine has no way to express that WHERE clause.
3. **Engine thinks one-index-per-query.** Real solutions are pairs: btree composite for range+sort axes, partial GIN for the tag filter. The output model is flat — no way to say "apply these two together".

The existing `idx_os_cat_subject_gin` does exist, but the planner bypasses it in favor of `idx_os_allowed_roles` (a BitmapAnd input that scans billions of tuples). Extended statistics from PR 1 don't fix this specific plan shape.

## Goal

Make the suggestion engine express and emit **bundles of indexes** whose members are chosen by **rule-based shape classification** of the slow query, with **data-driven partial-predicate scoping** probing live DB selectivity. Closes the AT26-style gap for any customer, not just AAF — rule operates on `IndexType` shapes, not on field names.

## Design decisions

Seven clarifying questions were resolved during brainstorming.

### Q3 — Approach: scenario catalog vs. data-driven inference

**Hybrid (c).** Rules encode "shape → template family"; live DB stats inform "should this filter be baked into a partial WHERE?"

Reason: Plone's query-shape vocabulary is finite (`FIELD/DATE/BOOL/KEYWORD/TEXT/UUID/PATH` + virtual `effectiveRange` + `path`), so a rule table is tractable. Pure data-driven inference of plan shape is a research project; pure catalog is AAF-specific and rejects the "generic" brief.

### Q4 — Output model

**Bundles (b).** `suggest_indexes()` returns `list[Bundle]` where each `Bundle` carries `name`, `rationale`, `shape_classification`, and `members: list[BundleMember]`. Single-member bundles back-compat with current UI by flattening; multi-member bundles express "these go together".

Reason: hybrid patterns are genuinely sets. Applying half a pair is worse than applying neither (the planner may still pick the wrong index). Bundle is also the natural unit for PR β's EXPLAIN grading.

### Q5 — EXPLAIN's role (PR β scope)

**Two-stage (c):** rules propose bundles; EXPLAIN grades and annotates. PR α ships only the proposer; PR β adds the grader.

### Q6 — Template vocabulary

| Family | DDL | Trigger |
|---|---|---|
| T1 btree composite | `CREATE INDEX … ON object_state (<expr>, …)` | filter shape = `BTREE_ONLY` |
| ~~T2 partial btree~~ | *cut* | *YAGNI — maintenance-cost savings don't justify complexity* |
| T3 plain GIN | `CREATE INDEX … USING gin ((idx->'<k>'))` | single KEYWORD, no scoping opportunity |
| T4 partial GIN | T3 + `WHERE idx->>'<k1>'='<v1>' AND …` | KEYWORD + low-selectivity equality co-filters |
| T5 hybrid bundle | T1 + T3/T4 (multiple members in one bundle) | filter shape = `MIXED` |
| T6 tsvector | existing dedicated path | filter shape = `TEXT_ONLY` — no new DDL |

T2 cut. c/d extensibility deferred (we acknowledge the possibility but time will tell).

### Q7 — EXPLAIN trigger strategy (PR β scope)

**Eager plain + opt-in ANALYZE (c).** JS fetches plain `EXPLAIN (FORMAT JSON)` per slow-query row on tab load. Per-row "deep inspect" button triggers `EXPLAIN (ANALYZE, TIMING off, BUFFERS off)` with 10 s timeout. PR α doesn't implement this; PR β does.

### Q8 — Partial-predicate scoping

**Bake all qualifying equality filters (b).** Every scalar-equality filter whose live selectivity is below the threshold goes into the partial WHERE. Over-specificity is the goal, not a bug — the index is tailored to the observed slow-query shape.

**Probe via MCV-first hybrid (iii).** `pg_stats.most_common_vals` gives the selectivity for common values with no DB round-trip. Miss → fall back to `SELECT COUNT(*) FROM object_state WHERE idx->>'k'='v'`. Cached per request-scoped connection.

Threshold: 10 %. Module constant `_PARTIAL_PREDICATE_SELECTIVITY_THRESHOLD = 0.1`, overridable via `PGCATALOG_PARTIAL_SELECTIVITY_THRESHOLD` env var.

### Q9 — PR decomposition

**Two PRs.** PR α = engine upgrade (this spec). PR β = UI + EXPLAIN grading. PR α is independently releasable and closes the AT26 gap on its own; PR β adds the live-DB insight layer.

## Section 1 — Architecture & Scope

**In scope for PR α:**

- `Bundle` + `BundleMember` dataclasses (frozen).
- `suggest_indexes()` signature change: return type `list[Suggestion]` → `list[Bundle]`. Fifth positional arg `conn` added (`None`-safe for unit tests).
- New templates T3, T4, T5 as defined above. T1 unchanged in DDL, re-wrapped in a Bundle. T6 stays as-is (existing tsvector path).
- `_classify_filter_shape(filter_fields)` → enum.
- `_dispatch_templates(shape, filter_fields, sort_field, probes, existing_indexes)` — pure function.
- `_probe_selectivity(conn, key, value)` — MCV-first hybrid, request-scoped cache.
- `_partial_where_terms(equality_filters, probes)` — threshold enforcement, quote escaping.
- Backward-compatibility flatten in `manage_get_slow_query_stats` so the existing DTML renders without change.

**Not in scope:**

- EXPLAIN grading, JSON endpoint, async JS, HypoPG — all go to PR β.
- Changes to `pgcatalog_slow_queries` schema — PR 2's `representative_params` via LATERAL already provides the input.
- New IndexType support beyond IndexRegistry's existing set.
- ZCA plugin points for customer-specific templates (architected — the dispatcher takes an internal emitter list — but not exposed publicly; that's a one-liner addition when a real use case appears).
- Automatic cleanup of unused `idx_os_sug_*` indexes.

**Multi-pod safety:** Pure Python change. No DDL, no schema migration. Signature of `suggest_indexes()` changes but has a single call site in `catalog.py`. Safe to ship mid-fleet.

**Compat note:** the fifth `conn` argument is `None`-defaulted in tests. When `conn is None` the probe returns `1.0` for every `(key, value)`, which means no partial scoping ever qualifies, which means T4 degrades to T3 and T1 stays as-is — identical to pre-α behavior for any caller that doesn't pass `conn`.

## Section 2 — Bundle data model & template dispatch

### Dataclasses

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class BundleMember:
    """One index in a bundle."""
    ddl: str
    fields: list[str]
    field_types: list[str]   # IndexType.name strings
    status: str              # "new" | "already_covered"
    role: str                # "btree_composite" | "plain_gin" | "partial_gin"
    reason: str


@dataclass(frozen=True)
class Bundle:
    """One or more indexes that together address a slow-query shape."""
    name: str                            # deterministic: shape + sorted filter names
    rationale: str                       # human-readable
    shape_classification: str            # BTREE_ONLY | KEYWORD_ONLY | MIXED | TEXT_ONLY | UNKNOWN
    members: list[BundleMember]          # 1..N indexes
```

### Backward-compat render in `manage_get_slow_query_stats`

Two keys per row (instead of today's single `suggestions`):

- `suggestions` — flat list `[m for b in bundles for m in b.members]` rendered by existing DTML. No UI change in PR α.
- `suggestions_bundles` — the full `[asdict(b) for b in bundles]` structure, present for PR β's JS to consume.

### Dispatcher

```python
def _dispatch_templates(filter_fields, sort_field, registry, probes, existing_indexes):
    shape = _classify_filter_shape(filter_fields)
    if shape == "BTREE_ONLY":
        return [_bundle_btree_composite(filter_fields, sort_field, probes, existing_indexes)]
    if shape == "KEYWORD_ONLY":
        return [_bundle_keyword_gin(filter_fields, probes, existing_indexes)]
    if shape == "MIXED":
        return [_bundle_hybrid(filter_fields, sort_field, probes, existing_indexes)]
    if shape == "TEXT_ONLY":
        return []   # dedicated tsvector handles this; nothing for PR α to add
    return []       # UNKNOWN — silent skip; PR β may still grade via EXPLAIN
```

### Shape classification rules

- All filter IndexTypes ∈ `{FIELD, DATE, BOOL, UUID, PATH}` → `BTREE_ONLY`
- All filter IndexTypes == `KEYWORD` → `KEYWORD_ONLY`
- Mix of btree-eligible and KEYWORD → `MIXED`
- Any `TEXT` in the filter set → `TEXT_ONLY` (dominates)
- Any IndexType the registry doesn't classify → `UNKNOWN`

### Probe resolution order

1. Walk all slow-query groups in the current `manage_get_slow_query_stats` call.
2. Enumerate unique `(key, value)` pairs across all candidate equality filters.
3. Resolve each once via `_probe_selectivity(conn, key, value)`; cache in a dict threaded through dispatcher calls.
4. Bounds DB round-trips to O(unique equality pairs) per ZMI page load, not O(slow queries × filters).

## Section 3 — Classification inputs & partial-scoping algorithm

### Input extraction (per slow-query group)

1. `query_keys` — already normalized by PR 2 (`_FILTER_VIRTUAL` expands `effectiveRange` → `('effective', DATE)`).
2. `representative_params` — per-group params from PR 2's LATERAL. For each key, infer operator:
   - scalar string → `equality`, value candidate for partial WHERE
   - list of scalars with length > 1 → `equality_multi`, *excluded* from partial WHERE (value ambiguous)
   - dict with `range` key → `range`, excluded from partial WHERE
3. Registry lookup: key → `(IndexType, idx_key, source_attrs)`.
4. Sort field: `_extract_sort_field()` from PR 2, unchanged.
5. Partial-scoping candidates: filter entries whose operator is `equality` AND IndexType ∈ `{FIELD, BOOL, PATH, UUID}` (not DATE — DATE values are timestamps, rarely appear in MCV lists, and partial-DATE scoping is usually a range-query anti-pattern).

### Probe algorithm

```python
_request_cache: dict[tuple[str, str], float] = {}   # threaded, not module-global
_pg_stats_cache: dict[str, Optional[dict]] = {}      # per-attname stats row


def _probe_selectivity(conn, key, value):
    cache_key = (key, value)
    if cache_key in _request_cache:
        return _request_cache[cache_key]

    # Step 1: MCV lookup (one fetch per distinct attname per request)
    attname = _derived_attname_for_key(key)
    stats = _pg_stats_cache.setdefault(attname, _fetch_pg_stats(conn, attname))
    if stats and stats["most_common_vals"] is not None and value in stats["most_common_vals"]:
        idx = stats["most_common_vals"].index(value)
        sel = stats["most_common_freqs"][idx]
        _request_cache[cache_key] = sel
        return sel

    # Step 2: fallback live COUNT
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM object_state "
            "WHERE idx IS NOT NULL AND idx->>%s = %s",
            (key, value),
        )
        count = cur.fetchone()["c"]
        cur.execute(
            "SELECT reltuples::bigint AS t FROM pg_class WHERE relname = 'object_state'"
        )
        total = max(cur.fetchone()["t"], 1)
    sel = count / total
    _request_cache[cache_key] = sel
    return sel
```

### Open implementation question (documented, not blocking design)

`_derived_attname_for_key(key)` depends on whether expression-index statistics are reachable via `pg_stats`. Three paths to explore in implementation:

1. If we've named expression-index columns (e.g. `idx_os_cat_portal_type` on `(idx->>'portal_type')`), PG keeps stats under the expression index's implicit attname `idx_expr_*`.
2. If `CREATE STATISTICS` from PR 1 produced queryable per-pair stats, we can read those.
3. If neither works, MCV path is a no-op and we always fall through to COUNT.

COUNT is cheap on a well-indexed column (~ms on 3M rows via the existing btree), so MCV is a "nice fast path" — not a correctness requirement. Spec-level fallback is "ship COUNT-only if MCV turns out infeasible, revisit in β".

### Partial predicate construction

```python
_PARTIAL_PREDICATE_SELECTIVITY_THRESHOLD = 0.1  # 10 %


def _partial_where_terms(equality_filters, probes):
    terms = []
    for (name, value) in equality_filters:
        sel = probes.get((name, value), 1.0)   # missing probe = 1.0 = never qualifies
        if sel < _PARTIAL_PREDICATE_SELECTIVITY_THRESHOLD:
            # Quote escape: single-quote in value → doubled
            safe_value = value.replace("'", "''")
            terms.append(f"idx->>'{name}' = '{safe_value}'")
    return terms
```

### Hybrid bundle construction (T5, shape = MIXED)

- Partition filter_fields into `btree_candidates` (btree-eligible types) and `keyword_candidates` (KEYWORD).
- Build one btree composite member (T1, with PR 2 sort covering logic), using `btree_candidates` + `sort_field`. Do NOT bake any WHERE — the btree composite gains nothing from partial scoping for the cases we see.
- Build one T3/T4 member per KEYWORD filter:
  - Compute `partial_where_terms` from *all* qualifying equality filters (both btree-eligible and KEYWORD equalities qualify, though KEYWORD equalities are rare as scalar values).
  - Empty terms → T3 plain GIN; non-empty → T4 partial GIN with `WHERE idx IS NOT NULL AND <terms AND-joined>`.
- Bundle rationale: machine-built string describing the shape and naming the members.

### Existing-index detection

`_check_covered()` from PR 2 stays unchanged for T1 members. For T3 / T4:

- Add `_normalize_gin_expr()` — canonicalizes `((idx->'<k>'))` and `(idx -> '<k>')` to a single form, handles `::text` stripping similar to `_normalize_idx_expr()`.
- Partial GINs with `WHERE`: the normalized form must also capture the WHERE clause for equality comparison. A second partial GIN with stricter WHERE is not equivalent, so we treat broader WHERE as "covers"; stricter WHERE as "new" (different index). Implemented as WHERE-clause normalization + set-subset check on equality predicates.

Each member's `status` is set per-member. Bundle-level status is not computed in PR α — the UI layer decides how to present per-member statuses (PR β will define "bundle grades").

## Section 4 — Testing, data flow, PR β carryover

### Test strategy

All pure unit tests. Fake `IndexRegistry` from PR 2 reused. Probes injected as deterministic dicts — no live DB.

- **`TestShapeClassifier`** — 6 tests covering each shape enum outcome (including empty + UNKNOWN).
- **`TestPartialWhereTerms`** — threshold enforcement (just-above / just-below / well-above / well-below), multi-filter AND order, single-quote escaping, list-value exclusion, range-value exclusion.
- **`TestBundleDispatch`**:
  - BTREE_ONLY input → 1 bundle, 1 T1 member (existing behavior, new wrapper).
  - KEYWORD_ONLY input with no low-selectivity co-filter → 1 bundle, 1 T3 member.
  - KEYWORD_ONLY + low-selectivity equality on another KEYWORD → T4 (rare but valid).
  - MIXED input → 1 bundle, N+1 members (1 btree composite + N GIN members).
  - TEXT_ONLY → empty bundle list.
  - UNKNOWN IndexType → empty bundle list.
- **`TestProbeSelectivity`** — mock psycopg cursor; verify MCV hit returns frequency, MCV miss falls through to COUNT, request-scoped dict caches both paths, pg_stats fetch happens at most once per attname per request.
- **`TestIssue122AT26Regression`** — the canonical slow query (portal_type, review_state, Subject, effective, expires, sort_on=effective):
  - probes `{('portal_type', 'Event'): 0.05, ('review_state', 'published'): 0.25}` → MIXED bundle with btree `(portal_type, review_state, effective)` + partial GIN on Subject scoped `WHERE portal_type='Event'` only.
  - probes `{('portal_type', 'Event'): 0.05, ('review_state', 'published'): 0.03}` → same bundle but GIN scoped `WHERE portal_type='Event' AND review_state='published'`.
- **Existing PR 2 tests** — update assertions to reach through `bundles[*].members[*]` for `fields`/`ddl`/`status`/`reason`. Helpers `_first_suggestion(result)` / `_all_suggestions(result)` centralize the unwrap so individual tests stay readable.

### Data flow

```
manage_get_slow_query_stats (catalog.py)
  → per row: query_keys, representative_params
  → suggest_indexes(query_keys, params, registry, existing_indexes, conn)
       ↓
       _extract_filter_fields(query_keys, params, registry)
       ↓
       _resolve_probes(conn, filter_fields)           # MCV+COUNT, request-cached
       ↓
       _dispatch_templates(shape, filter_fields, sort_field, probes, existing_indexes)
       ↓ returns list[Bundle]
  row["suggestions"]         = [m for b in bundles for m in b.members]   # legacy DTML
  row["suggestions_bundles"] = [asdict(b) for b in bundles]               # PR β JSON
```

### Out of scope (→ PR β)

- JSON endpoint + async JS fetches.
- EXPLAIN plan parsing + bundle grading.
- HypoPG integration.
- DTML → JSON+JS UI refactor.
- Opt-in ANALYZE button.

### Estimated size

- `src/plone/pgcatalog/suggestions.py`: +~400 lines (dataclasses, classifier, dispatcher, probe, partial-where, hybrid builder, GIN normalizer); existing `_add_btree_suggestions` reorganized into the bundle-aware form.
- `src/plone/pgcatalog/catalog.py`: ~30 line change (threaded `conn` into `suggest_indexes` call, expose both flat + bundle outputs).
- `tests/test_suggestions.py`: +~300 lines (5 new test classes + helpers).
- No schema / DDL / migration. No multi-pod coordination beyond the PR 2 baseline.

---

## PR β carryover

All PR β decisions and open questions live in `2026-04-15-suggestions-engine-pr-beta-notes.md`, committed alongside this spec. That file is the starting point for PR β's brainstorm — it captures everything we decided during this brainstorm but deferred out of PR α's scope, plus the open questions we know need answering then.
