# PGCatalog vs RelStorage+ZCatalog Benchmark

Comparison of **plone-pgcatalog** (PostgreSQL JSONB, single-table)
against the standard Plone stack (**RelStorage + ZCatalog**) — the deployment
used by most large Plone sites.

Both backends store data in PostgreSQL.  The difference is *how catalog
queries work*:

| | **PGJsonbStorage + PGCatalog** | **RelStorage + ZCatalog** |
|---|---|---|
| Object storage | JSONB in `object_state` | pickle bytea in `object_state` |
| Catalog indexes | GIN/B-tree on JSONB columns | BTree persistent objects in ZODB |
| Query engine | PostgreSQL SQL planner | Python index intersection |
| ZODB cache usage | Content objects only | Content + all BTree/Bucket nodes |

## Test Environment

| Component | Version |
|---|---|
| Python | 3.13.9 |
| PostgreSQL | 17.7 |
| ZODB | 6.0.1 |
| Products.ZCatalog | 7.1 |
| RelStorage | 4.1.1 |
| Platform | Linux 6.14 (x86_64) |
| PG connection | localhost:5433 (Docker) |

## Methodology

The benchmark creates a **real Plone 6 site** with real content through
`invokeFactory`, real workflow transitions, and real catalog queries through
`portal_catalog.searchResults()`.  Each backend runs in its own subprocess
with a fresh database.

**ZODB cache-size is set to 400** (vs the default 30,000) to simulate the
cache pressure experienced on large production sites where BTree nodes
compete for limited cache slots.  PGCatalog is unaffected by this setting
because it stores no BTree objects in ZODB.

- **500 documents** created with realistic metadata (titles, descriptions,
  subjects, workflow states, dates)
- **50 query iterations** after 5 warmup iterations
- **Deterministic data** (seeded RNG) for reproducibility
- **Same queries** passed to both backends' `searchResults()`

### Content Distribution

| Field | Distribution |
|---|---|
| portal_type | Document (100%) |
| review_state | published (70%), private (20%), pending (10%) |
| Subject | 0-4 random tags from 15-tag pool |
| SearchableText | Titles + descriptions with random subject keywords |

## Results: 500 Documents

### Setup Performance

| Operation | PGCat | RS+ZCat | Ratio |
|---|---|---|---|
| Site creation | 1,706 ms | 1,281 ms | 1.3x slower |
| Content creation | 68.5 ms/doc | 77.3 ms/doc | **1.13x faster** |
| Content modification | 14.0 ms/doc | 19.4 ms/doc | **1.39x faster** |

PGCatalog writes are now **faster** than RelStorage+ZCatalog at this
scale.  This was achieved through two rounds of optimization:

1. **Write-path optimizations** (batch conflict detection, prepared
   statements, GIL release during Rust decode) — improved from 251 ms/doc
   to 175 ms/doc (30% faster).

2. **ZCatalog BTree elimination** — removing `super().catalog_object()`
   calls that were writing ~140 BTree/Bucket objects per document into
   ZODB, despite PGCatalog never querying them.  This single change
   reduced content creation from 175 ms/doc to 68.5 ms/doc (2.5x faster)
   and shifted PGCatalog from 2.2x slower to 1.13x faster than
   RelStorage+ZCatalog.

### Query Performance (median, 50 iterations, after optimization)

| Query Scenario | PGCat | RS+ZCat | Ratio |
|---|---|---|---|
| Simple field match (`portal_type=Document`) | 5.9 ms | 0.21 ms | 27.5x |
| Complex multi-index (type+state+subject+sort+limit) | 4.1 ms | 0.32 ms | 13.0x |
| Full-text search (`SearchableText`) | 3.7 ms | 0.54 ms | 6.9x |
| Navigation (path depth=1 + sort) | 5.3 ms | 0.68 ms | 7.7x |
| Security filtered (published + anonymous) | 6.6 ms | 0.21 ms | 30.8x |
| Date-sorted with pagination | 5.6 ms | 0.29 ms | 19.7x |

### Where the Time Goes (cProfile, after optimization)

Profiling PGCatalog's 330 query executions (6 scenarios x 55 iterations):

| Component | Time | Share | What |
|---|---|---|---|
| PG network wait | 1.61s | 81% | SQL execution + round-trip to Docker PG |
| Row construction | 0.16s | 8% | psycopg dict_row for 106k rows |
| Other | 0.21s | 11% | Query building, brain construction, pool ops |

In contrast, ZCatalog runs entirely in-process with BTree nodes cached in
ZODB's object cache.  At 500 documents, the entire BTree index fits in
cache-size 400, so every query is a pure in-memory set operation.

## Optimization Results

### Phase 1: orjson JSONB Loader

Replaced stdlib `json.loads` with `orjson.loads` via `psycopg.types.json.set_json_loads()`.

| Component | Before | After orjson | Change |
|---|---|---|---|
| JSON parsing (106k calls) | 1.60s | 0.54s | **-66%** |
| Total query time (330 queries) | 6.21s | 5.00s | **-20%** |
| PG network wait | 3.15s | 3.30s | ~same |

| Query Scenario | Before | After orjson | Change |
|---|---|---|---|
| Simple field match | 25.6 ms | 19.1 ms | -25% |
| Complex multi-index | 4.9 ms | 4.9 ms | 0% |
| Full-text search | 23.1 ms | 17.3 ms | -25% |
| Navigation | 25.8 ms | 19.9 ms | -23% |
| Security filtered | 21.2 ms | 17.0 ms | -20% |
| Date-sorted | 8.4 ms | 7.4 ms | -12% |

Complex query (4.9 ms) already has a small result set (paginated), so
JSON parsing overhead was negligible there.  Queries returning many rows
(Simple, Full-text, Navigation) saw the largest benefit.

### Phase 2: Lazy idx Batch Loading

Changed `_run_search` to SELECT only `zoid, path` (no `idx` JSONB).
When brain metadata is first accessed, `CatalogSearchResults._load_idx_batch()`
fetches all idx values in a single batch query.  The benchmark never accesses
metadata, so idx is never loaded — exactly the `len(results)` / pagination
use case that dominates real Plone page loads.

| Component | Phase 1 | Phase 2 | Change |
|---|---|---|---|
| Total query time (330 queries) | 5.00s | 1.90s | **-62%** |
| PG network wait | 3.30s | 1.63s | **-51%** |
| JSON parsing (orjson) | 0.54s | 0s | **-100%** (deferred) |
| Row construction (dict_row) | 0.10s | 0.06s | -40% |

| Query Scenario | Phase 1 | Phase 2 | Change | vs RS+ZC |
|---|---|---|---|---|
| Simple field match | 19.1 ms | 6.0 ms | -69% | 14x |
| Complex multi-index | 4.9 ms | 4.6 ms | -6% | 6.6x |
| Full-text search | 17.3 ms | 3.9 ms | -77% | 4.2x |
| Navigation | 19.9 ms | 5.9 ms | -70% | 3.2x |
| Security filtered | 17.0 ms | 6.8 ms | -60% | 14x |
| Date-sorted | 7.4 ms | 6.0 ms | -19% | 9.8x |

After Phase 2, PG network wait is 86% of total time — the query is now
I/O bound, not CPU bound.  The remaining gap vs ZCatalog is structural:
network round-trip to Docker PG vs in-process BTree cache.

### Phase 3: Prepared Statements

Added `prepare=True` to `cursor.execute()` in `_run_search` and
`_load_idx_batch`.  Forces prepared statements from the first execution.

**Impact: negligible.**  psycopg3's auto-prepare threshold (5 executions)
already activates during warmup, so explicit `prepare=True` only helps
the first 5 queries per pattern.  Total time: 1.92s vs 1.90s (within noise).

### Phase 4: Connection Reuse per Request

Thread-local connection reuse via `get_request_connection()` / IPubEnd
subscriber.  Eliminates pool lock overhead for pages with multiple
catalog queries (typical Plone page does 3-5 catalog queries).

**Impact on benchmark: negligible.**  The benchmark does one query per
"request", so pool overhead was already minimal (~0.008s total).
The real benefit is in production where a page load does 3-5 catalog
queries — reusing the connection saves the `getconn`/`putconn` lock
per extra query.

### Phase 5: ZCatalog BTree Write Elimination

Removed four `super()` calls in `PlonePGCatalogTool` that delegated to
ZCatalog's write path — `indexObject()`, `reindexObject()`,
`catalog_object()`, and `uncatalog_object()`.  These were writing ~140
BTree/Bucket objects per document into ZODB (FieldIndex entries,
KeywordIndex forward/reverse maps, ZCTextIndex postings, etc.) that
PGCatalog never queries.

| Metric | Before | After | Change |
|---|---|---|---|
| ZODB store() calls per doc | 193 | 40 | **-79%** |
| Content creation | 175 ms/doc | 68.5 ms/doc | **-61%** |
| Content modification | 50.1 ms/doc | 14.0 ms/doc | **-72%** |
| tpc_vote | 19.7 ms | 3.7 ms | **-81%** |
| Commit overhead | 125 ms | 2.7 ms | **-98%** |

This is the single largest optimization.  By eliminating BTree writes,
PGCatalog went from **2.2x slower** to **1.13x faster** than
RelStorage+ZCatalog for content creation, and from **2.8x slower** to
**1.39x faster** for content modification.

### Overall Optimization Summary

**Query path:**

| Metric | Baseline | Phase 1 (orjson) | Phase 2 (lazy idx) | Phase 3+4 |
|---|---|---|---|---|
| Total query time (330) | 6.21s | 5.00s (-20%) | 1.90s (-69%) | 1.98s |
| PG network wait | 3.15s | 3.30s | 1.63s | 1.61s |
| JSON parsing | 1.60s | 0.54s (-66%) | 0s (deferred) | 0s |

The dominant remaining query cost is PG network I/O (81-86% of total
time) — a structural overhead of TCP round-trip to Docker PG vs
ZCatalog's in-process BTree cache.  This gap would narrow significantly
with Unix domain sockets or a colocated PG instance.

**Write path:**

| Metric | Baseline (pre-OPT) | After OPT 1-4 | After Phase 5 (BTree removal) |
|---|---|---|---|
| Content creation | 251 ms/doc | 175 ms/doc (-30%) | **68.5 ms/doc (-73%)** |
| Content modification | — | 50.1 ms/doc | **14.0 ms/doc (-72%)** |
| ZODB store() per doc | ~193 | ~193 | **40 (-79%)** |
| vs RelStorage+ZCatalog | **3.4x slower** | **2.2x slower** | **1.13x faster** |

## Architectural Advantages at Scale

The benchmark at 500 objects shows PGCatalog has significant per-query
overhead from network I/O and JSON parsing.  The architectural advantages
emerge at larger scales:

### Zero ZODB Cache Pressure

ZCatalog with 15 indexes on 50k objects creates ~500k persistent BTree
and Bucket objects.  With a typical `cache-size` of 5,000-30,000, most
BTree nodes are evicted and must be re-fetched from storage on every
query.  Each fetch is a round-trip to PostgreSQL (via RelStorage) to
load and unpickle a single blob.

PGCatalog stores zero catalog objects in ZODB.  The entire cache budget
is available for content objects, improving traversal and rendering
performance across the board.

### Query Planning + LIMIT Pushdown

ZCatalog intersects index result sets sequentially in Python, always in
registration order regardless of selectivity.  It also computes the full
result set before applying `b_size` slicing — no early termination.

PGCatalog delegates to PostgreSQL's cost-based query planner, which
evaluates index selectivity, chooses optimal join strategies, and
can short-circuit via `LIMIT` pushdown.

### JSONB Queryability

Catalog metadata stored as JSONB is queryable from any PostgreSQL
client — reporting tools, admin scripts, monitoring dashboards — without
loading Zope or ZODB.

## Reproducing

```bash
# Requires PostgreSQL on localhost:5433
docker run -d --name zodb-pgjsonb-dev \
  -e POSTGRES_USER=zodb -e POSTGRES_PASSWORD=zodb \
  -e POSTGRES_DB=zodb_test -p 5433:5432 postgres:17

# Run the benchmark (500 docs, ~4 minutes total)
.venv/bin/python sources/plone-pgcatalog/benchmarks/run_benchmarks.py \
  level2 --docs 500 --iterations 50 --warmup 5 \
  --cache-size 400 --output results.json

# Run with profiling (cProfile output to stderr)
.venv/bin/python sources/plone-pgcatalog/benchmarks/run_benchmarks.py \
  level2 --docs 500 --iterations 50 --warmup 5 --profile
```

### CLI Options

```
usage: run_benchmarks.py level2 [-h] [--docs N] [--iterations N]
                                [--warmup N] [--rebuild N]
                                [--cache-size N] [--output FILE]
                                [--profile]

  --docs N          Number of documents to create (default 500)
  --iterations N    Query repetitions (default 50)
  --warmup N        Warmup iterations (default 5)
  --cache-size N    ZODB cache-size for ZCatalog backend (default 400)
  --output FILE     Export full results as JSON
  --profile         Run cProfile on query scenarios
```
