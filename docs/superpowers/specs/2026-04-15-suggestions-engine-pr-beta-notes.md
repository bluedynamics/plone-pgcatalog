# PR β (Suggestions Engine) — Pre-brainstorm Notes

> **This is NOT a design spec.** It captures decisions and open questions agreed during the PR α brainstorm (2026-04-15) that belong to PR β but were deferred. Use this as the starting material for the PR β brainstorm; don't re-derive these answers.

**Parent spec:** `2026-04-15-suggestions-engine-pr-alpha-design.md`
**Issue:** [bluedynamics/plone-pgcatalog#122](https://github.com/bluedynamics/plone-pgcatalog/issues/122)

## Scope of PR β

Build on PR α's Bundle data model to add:

1. **EXPLAIN-driven grading** — live-DB insight per slow-query row and per bundle.
2. **JSON endpoint** — server renders grades + plans as JSON; UI fetches async.
3. **ZMI UI refactor** — DTML → JSON + vanilla JS for the Slow Queries tab (no heavy framework, per project rule).
4. **Opt-in ANALYZE** — user-triggered deep inspection per row.

## Decisions already locked

### Q5 (EXPLAIN's role) — two-stage: rules propose, EXPLAIN grades + annotates

Bundle candidates come from PR α's rule-based dispatcher. For each slow-query group, PR β runs `EXPLAIN (FORMAT JSON)` on the representative row and:

- Extracts the chosen plan (indexes used, top cost nodes, estimated rows, filter predicates).
- Grades each candidate bundle against the baseline plan.
- Attaches a diagnostic panel: the top filter-cost node (node type, estimated Rows Removed by Filter when inferable from plan shape), so users see **why** the query is slow even if no bundle changes it.

### Q7 (EXPLAIN trigger) — eager plain + opt-in ANALYZE

- **Default on tab load:** JS fetches plain `EXPLAIN (FORMAT JSON)` per visible slow-query row. Cheap (~10 ms per plan). Grades computed on the server, returned with the plan.
- **On click:** per-row "deep inspect" button triggers `EXPLAIN (ANALYZE, TIMING off, BUFFERS off)` with `SET LOCAL statement_timeout = '10s'`. UI shows a spinner. On completion, Rows Removed by Filter and actual row counts replace the estimates. Timeout → user sees "query too slow to analyze; here are the estimates we have".

### Grading semantics

Three states per bundle (plus the row-level row-is-in-slow-queries fact):

| Grade | Meaning | Signal |
|---|---|---|
| `already_fast` | Query wouldn't be in `pgcatalog_slow_queries`. Not shown — the row wouldn't be listed. | N/A |
| `covered_but_slow` | Current plan uses indexes that match the bundle's target predicates, but the query is still observed slow (it's in the slow-queries table definitionally). | Planner's chosen indexes cover the filter set; Rows Removed by Filter low / low-ish; something else bites (stats drift, bloat, correlation, or the GIN is too big → partial GIN bundle would help). |
| `uncovered` | Current plan has a Seq Scan on `object_state`, OR chosen indexes don't cover the bundle's target predicates. Bundle would plausibly help. | Seq Scan node present, OR index-set difference between chosen and target non-empty. |

Grade applies per-bundle, not per-row. A single slow query may yield multiple bundles each with its own grade.

### Async JS fetch model

- Page initial load: fast (bundles only, no per-row plans).
- On DOMContentLoaded: JS iterates visible slow-query rows, issues `fetch()` to the per-row plan endpoint. Fills in grades and diagnostic panels as responses arrive.
- Per-row "deep inspect" button triggers a second fetch with `?analyze=true`. Separate endpoint or query-param on the same endpoint — design open.

### HypoPG as optional enhancement, graceful degradation

- At startup, check `SELECT 1 FROM pg_extension WHERE extname = 'hypopg'`.
- If present, bundle grading gains a "what-if plan" mode: `SELECT hypopg_create_index('<ddl without CONCURRENTLY>')`, run `EXPLAIN` again, compare. Gives confidence that applying the bundle would actually change the plan. Wrap in a read-only transaction that gets rolled back so hypothetical index disappears.
- If absent, grade only from the baseline plan + index-set comparison heuristic.

### UI refactor scope

- Replace `manage_slow_queries.dtml` with a small JSON+JS page (or keep DTML as HTML shell + JS doing the rendering — **decide in PR β brainstorm**).
- No heavy framework. Vanilla JS, optional lightweight helpers (alpine.js is acceptable per user's earlier "leichtes ok"; React/Vue are not).
- Per-row expansion → shows plan tree, bundles with grade badges, "deep inspect" button.

### PR α → PR β contract

PR β's UI consumes what PR α's `manage_get_slow_query_stats` puts under `row["suggestions_bundles"]`:

- `bundles[*].name` — stable identifier for UI keyed updates.
- `bundles[*].rationale` — rendered as bundle description.
- `bundles[*].shape_classification` — shown as a small badge.
- `bundles[*].members[*]` — rendered as DDL code blocks with Apply / Drop buttons.

## Open questions deferred to PR β brainstorm

1. **JSON endpoint URL shape.** Per-row endpoint keyed by `query_keys` hash? Single batch endpoint? Separate endpoints for baseline plan vs. ANALYZE? Affects caching.
2. **Plan caching.** Key by `(query_keys, representative_params_hash)`? TTL? Invalidate on index apply/drop? How do we handle plans that are valid for 5 seconds but not 5 hours (stats drift)?
3. **Deep-inspect authorization + UX.** ANALYZE actually runs the slow query. Should the UI show a confirmation dialog ("this may take up to 10 s — proceed?") or silently trigger with a spinner? Who is allowed to trigger — `Manage portal` only, or any ZMI user?
4. **Handling missing `query_text`.** PR 2's slow-query log stores the SQL. If an old row has null `query_text` (logged under a pre-PR-2 schema?), we can't EXPLAIN it. Skip? Reconstruct from `query_keys + params`? How do we reconstruct safely?
5. **`_derived_attname_for_key` resolution for MCV stats** — carried over from PR α. If PR α ships COUNT-only, PR β may want to improve it once we know which pg_stats path is reachable for expression indexes.
6. **Bundle-level grade aggregation.** A bundle has N members, each with their own `status` (new / already_covered). Bundle grade aggregates: what's the rule when some members are new and some already covered? Needed for the UI badge.
7. **Rate limiting.** 50 slow queries × 1 EXPLAIN each on every tab load. At 10 ms each that's 500 ms total. Acceptable. If EXPLAIN is ever slow (stats issues), do we need a global timeout / circuit breaker?
8. **Drop-bundle flow.** If a bundle's members were applied together, should there be a "drop bundle" action (drops all members in sequence)? Or only per-member?
9. **HypoPG production readiness.** Is `hypopg` available in typical Plone PG deployments? Is asking customers to install an extension acceptable? (Extended statistics from PR 1 didn't need an extension; this would be new.)
10. **Auto-apply opt-in.** Far future — a "trust the engine" mode where highly-confident uncovered-grade bundles get applied automatically during low-traffic windows? Definitely not PR β, but worth capturing the ergonomic target.

## Non-goals for PR β

- New templates beyond PR α's T1/T3/T4/T5/T6.
- Changing the shape classifier or the partial-scoping threshold.
- Cross-pod coordination for EXPLAIN caching (each pod EXPLAINs independently; that's fine).
- Writing to `pgcatalog_slow_queries` from PR β (read-only).

## Recommended next actions when PR β becomes the active work

1. Re-invoke `superpowers:brainstorming` with this notes file as context.
2. Answer the open questions above in the same Q-by-Q flow as PR α.
3. Produce a proper design spec (`YYYY-MM-DD-suggestions-engine-pr-beta-design.md`).
4. Feed spec into `superpowers:writing-plans`.
5. Execute via `superpowers:subagent-driven-development`.

All PR α infrastructure (Bundle dataclass, `suggestions_bundles` catalog.py output key, test helpers) is ready for PR β to consume — no re-plumbing needed.
