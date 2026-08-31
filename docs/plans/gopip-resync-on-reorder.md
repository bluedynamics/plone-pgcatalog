# Spec: keep getObjPositionInParent correct after reorderings

Issue: #216 — `getObjPositionInParent` is stored as a snapshot and sorted on
in SQL, but Plone never reindexes siblings after a reordering. Any reorder
(and any delete, which shifts all following siblings) leaves stale positions
behind. Most visible fallout: `folder_contents` shows a stale order and its
drag & drop aborts with *"Client/server ordering mismatch"*.

## Why Plone does not reindex

`plone.folder`'s `GopipIndex` is a fake index: ZCatalog resolves positions at
sort time by traversing to the container and reading its ordering
(`plone/folder/nogopip.py`). Plone therefore has no reason to reindex
siblings when an ordering changes. pgcatalog treats the index like a
FieldIndex and sorts on `(idx->>'getObjPositionInParent')::integer` — the
value captured when the object itself was last indexed.

## Constraints discovered during analysis

- **Only the order is observable, never the value.** ZCatalog's `GopipIndex`
  is a `StubIndex`: `getEntryForObject` returns `[]`, it is not queryable as
  a filter, and `getObjPositionInParent` is not in the metadata set. API
  parity means nothing outside the SQL `ORDER BY` can read the stored value.
  (pgcatalog's `_handle_field` mapping for GOPIP filters is a non-parity
  extra; see open point 2.)
- **The navtree hot path bakes the sort expression into an index.**
  `idx_os_navtree` ends in `((idx->>'getObjPositionInParent')::integer)`, so
  navigation queries get index-ordered scans. Any design that moves the sort
  source out of `idx` loses that.
- **The expensive workload is not the interactive drag & drop.** The
  production trigger is an ordering implementation that *prepends* on add:
  every content creation renumbers the whole folder. Large ordered folders
  (10k+) must not cause O(n) wide-row rewrites per add.
- **No per-child Python reindex, ever.** A naive "reindexObject each
  sibling" does full extraction (SearchableText!) and marks every child
  `_p_changed`. All designs below avoid Python-side reindexing entirely.

## Shared plumbing (identical in every variant)

1. **Subscriber** on `(IOrderableFolder, IContainerModifiedEvent)`
   (`gopip.py`). Every relevant mutation path fires this event on the
   container:
   - `DefaultOrdering.moveObjectsByDelta` — all `moveObjects*` /
     `moveObjectToPosition` funnel through it, including both
     `folder_contents` rearrange views (drag & drop, by-attribute).
   - `OFS.ObjectManager._setObject` / `_delObject` — adds (prepend-style
     orderings) and deletes (shift all following siblings).
   - `OFS.CopySupport` — cut/paste and rename fire
     `notifyContainerModified` on both containers explicitly.

   The subscriber only records the container in a thread-local, deduplicated
   per transaction, and registers one `beforeCommitHook`.

2. **Commit-time snapshot.** The hook runs once per transaction and reads
   each recorded container's *final* `idsInOrder()` (what `nogopip.py`
   itself sorts by). O(n) Python per touched folder per transaction — a few
   ms at 10k. Registered in the pending store (savepoint story is trivial:
   hooks run after the last possible savepoint). Non-`IExplicitOrdering`
   orderings (unordered folders) are skipped.

3. **Apply in `processor.finalize()`** — same PG transaction as the ZODB
   commit, after the bulk path moves so `parent_path` is final. What
   "apply" writes is the fork below.

Known gap in all variants: `IExplicitOrdering.orderObjects` (sort folder by
key) fires no event in `plone.folder`. No Plone UI path calls it — the
`folder_contents` "rearrange by attribute" view goes through
`moveObjectToPosition`, which is covered. Upstream fix: add
`notifyContainerModified` to `DefaultOrdering.orderObjects`.

## The fork: how positions are stored and written

### Variant C — dense in-place resync (baseline)

One UPDATE per touched folder: `jsonb_set` joined against `jsonb_each` of
the `{id: position}` map, guarded with `IS DISTINCT FROM`.

- Reads: unchanged (expression + navtree index intact).
- Writes: every *shifted* sibling is a full wide-tuple rewrite (idx JSONB is
  KBs) plus GIN churn. Prepend-add or move-to-top in a 10k folder ≈ 10k
  tuple rewrites, tens of MB WAL, **per operation**. Delete rewrites all
  following siblings.
- Effort: ~30 lines on top of the plumbing. No migration (existing rows
  heal on a folder's first event, or via maintenance resync).
- Verdict: correct and simple, but write-amplified on exactly the workload
  that motivated the issue.

### Variant D — narrow side table

`gopip(parent_path, child_id, pos)` with PK `(parent_path, child_id)`;
resync = one `INSERT … ON CONFLICT DO UPDATE` from `jsonb_each` plus a
delete of vanished ids. Sort becomes a LEFT JOIN with
`COALESCE(g.pos, (idx->>'…')::int)` fallback (lazy migration).

- Writes: O(shifted) but ~60-byte rows, no GIN — roughly two orders of
  magnitude less WAL than C.
- Reads: sort needs a join in `query.py` (surgery in `_process_sort` and
  the GOPIP filter path), and the navtree index-ordered scan is lost —
  the hottest read path pays for the write fix.
- Also needs: bulk-move handling for the new table's `parent_path`, cleanup
  of orphaned rows, schema DDL.
- Verdict: solid classic design, but it taxes reads to pay for writes and
  has the largest blast radius.

### Variant F — sparse ranks in the existing snapshot (recommended)

Exploit the "only order is observable" constraint: stored values need not
be dense ordinals, only *correctly ordered* integers. `finalize()` reads the
folder's current values (one index scan, no writes), walks the desired
order, and rewrites **only rows that violate the order**, assigning gapped
midpoint ranks (LexoRank-style, step 1024, int4, windowed renumber when a
gap is exhausted).

Per-operation cost in a folder of n:
- append-add: 0 sibling writes (new row gets max+step)
- prepend-add: 1 write (new row gets min−step; negative is fine)
- drag & drop one item: 1 write
- delete: 0 writes (gaps simply remain)
- worst case (full reversal / exhausted gaps): windowed renumber, bounded
  by the number of actually-moved items plus the renumber window

- Reads: **zero change.** Same expression, same indexes (navtree included),
  same query cache. Existing dense values are valid ranks → no migration;
  stale folders heal on their first ordering event (plus an optional
  maintenance resync to heal immediately after upgrade).
- The rank-assignment is a pure function `(current: [(id, rank)], desired:
  [id]) -> {id: new_rank}` — unit-testable without PG.
- Concurrency: two transactions reordering the same folder conflict on the
  ordering `PersistentList` in ZODB and retry serially, so rank writes
  never interleave for one folder.
- Trade-offs: values in PG stop being human-readable ordinals (a
  maintenance/debug view can expose `row_number()` over ranks); the rank
  function needs careful edge-case tests (ties from legacy dense data,
  int4 bounds, gap exhaustion).

### Comparison at 10k siblings, prepend-on-add workload

| | C (dense in-place) | D (side table) | F (sparse ranks) |
|---|---|---|---|
| writes per add | ~10k wide tuples, GIN churn | ~10k narrow rows | **1 row** |
| extra reads per txn | — | — | 1 index scan (n narrow values) |
| sort/read path | unchanged | join, navtree scan lost | **unchanged** |
| schema/query changes | none | table + query.py surgery | none |
| migration | self-healing | lazy via COALESCE | none needed |
| complexity | trivial | large | medium (pure function) |

## Decision

**F** (2026-08-31, confirmed by the package owner): shared plumbing +
rank-diff engine in `finalize()`, with the rank assignment as a standalone
pure function (`gopip.assign_ranks`). C falls out of F for free (a
degenerate rank assigner), so if F's edge cases ever bite, C is the
fallback with identical plumbing and tests. Healing of pre-fix stale
folders ships in the same change: `maintenance.resync_gopip()` walks every
`parent_path` with cataloged children, reads each container's ordering
from ZODB (annotations only — children stay ghosts), and applies the
minimal rank diff; profile upgrade step 2→3 runs it once.

## Implementation map

| Piece | Where |
|---|---|
| rank engine (pure) | `gopip.py: assign_ranks` + `tests/test_gopip_ranks.py` |
| subscriber + commit hook | `gopip.py: container_modified`, ZCML in `configure.zcml`, programmatic registration in `testing.py` (the PG fixture loads no ZCML) |
| pending store | `pending.py: add_pending_gopip / pop_all_pending_gopip` (savepoint-aware like the move store) |
| SQL sync | `gopip.py: sync_folder_ranks`, called from `processor.finalize()` after the bulk path moves |
| healing | `maintenance.py: resync_gopip`, `upgrades/profile_3.py` (2→3) |
| tests | `tests/test_gopip_ranks.py` (unit: rank engine + pending store), `tests/test_pg_integration.py: TestGopipAfterReorder / TestGopipMaintenanceResync` (PG layer, incl. minimal-write and zero-write assertions; lives there because exactly one module may own the `PGCATALOG_PG_FIXTURE` via `fixture.create`) |

## Open points / follow-ups

1. `IndexType.GOPIP` currently maps to `_handle_field` for *filtering* — a
   non-parity extra whose value semantics die with sparse ranks (and were
   already broken by staleness; ZCatalog never supported gopip filters).
   Sorting is unaffected. Candidate follow-up: log-warn on gopip value
   filters; audit `addons_compat/eeafacetednavigation.py` (groups GOPIP
   with FIELD for faceted value queries).
2. Upstream `plone.folder` issue for the `orderObjects` event gap
   (`DefaultOrdering.orderObjects` should call `notifyContainerModified`).
3. Ranks in PG are not human-readable ordinals; if that trips up
   operators, a debug view could expose `row_number() OVER (PARTITION BY
   parent_path ORDER BY rank)`.
