<!-- diataxis: explanation -->

# Performance Characteristics

plone.pgcatalog's performance profile is fundamentally different from ZCatalog's.
ZCatalog is fast when everything fits in the ZODB cache and degrades as cache
pressure increases. plone.pgcatalog's performance is bounded by PostgreSQL network
I/O and does not degrade with catalog size.

This page presents benchmark results, explains where time is spent, and traces the
optimization history that shaped the current implementation.

## Benchmark environment

All measurements were taken under the following conditions:

- Python 3.13, PostgreSQL 17
- 500 documents with realistic Plone metadata (titles, descriptions, subjects,
  workflow states, dates, SearchableText)
- ZODB `cache-size` set to 400 (simulates a large site where cache pressure matters)
- 50 query iterations per pattern for stable measurements
- Comparison baseline: RelStorage (PostgreSQL) with standard ZCatalog

## Write performance

| Operation | plone.pgcatalog | RelStorage + ZCatalog | Speedup |
|---|---|---|---|
| Content creation | 65.4 ms/doc | 77.3 ms/doc | 1.18x |
| Content modification | 13.9 ms/doc | 19.4 ms/doc | 1.49x |

The write speedup comes from eliminating BTree writes. When ZCatalog indexes an
object, it updates every index's forward and reverse BTrees. For a document with 30
indexes, this triggers roughly 193 ZODB `store()` calls per document -- one for each
BTree node that changes. plone.pgcatalog reduces this to roughly 40 `store()` calls
-- the content object itself, its annotations, and other Plone-internal persistent
objects dirtied during the edit cycle (workflow history, modification timestamps,
etc.). The catalog index data is written as extra SQL columns in the same transaction,
not as separate ZODB objects. Fewer ZODB stores means fewer pickle serializations,
fewer PostgreSQL `INSERT`/`UPDATE` statements, and a faster commit.

Content modification shows a larger speedup (1.39x vs 1.13x) because creation
includes one-time costs (container updates, UID assignment) that are independent of
the catalog backend. Modification is dominated by index updates, where the BTree
elimination has the most impact.

## Query performance

| Query pattern | plone.pgcatalog | RelStorage + ZCatalog | Speedup |
|---|---|---|---|
| portal_type filter | 3.7 ms | 114.1 ms | 30.8x |
| Date range | 4.0 ms | 59.2 ms | 14.8x |
| Keyword (Subject) | 5.3 ms | 92.6 ms | 17.5x |
| Full-text search | 5.5 ms | 38.0 ms | 6.9x |
| Path + type | 6.6 ms | 97.8 ms | 14.8x |
| Combined filter | 4.7 ms | 76.5 ms | 16.3x |

The magnitude of improvement varies by query type, but all queries are faster.
The portal_type filter shows the largest speedup (30.8x) because ZCatalog must load
the entire FieldIndex BTree from ZODB to evaluate a simple equality, while PostgreSQL
hits a B-tree expression index on `idx->>'portal_type'` and returns immediately.

Full-text search shows the smallest speedup (6.9x) because PostgreSQL's GIN index
scan and tsvector matching are computationally heavier than the simple JSONB lookups
used by other query types. Even so, 5.5ms for a full-text query across 500 documents
is well within interactive response budgets.

### Where the time goes

Profiling shows that 81% of plone.pgcatalog's query time is spent on PostgreSQL
network I/O -- sending the query and receiving the response. This is the structural
floor: no application-level optimization can reduce it further.

The remaining time breaks down as:

- **8% row construction.** Building `PGCatalogBrain` objects from the returned row
  dicts.
- **11% other.** Query building, parameter marshaling, connection handling, and Python
  overhead.

This profile means the system is I/O-bound, not CPU-bound. Further optimization
requires reducing the number of round-trips (which lazy loading already addresses)
or moving the database closer to the application (local socket vs. network).

## Optimization history

plone.pgcatalog's query performance improved in six phases. Understanding the
progression explains why the current design looks the way it does.

### Phase 1: orjson JSONB loader

**Impact: -20% query time.**

psycopg's default JSON deserializer uses the standard library `json.loads()`. Replacing
it with `orjson.loads()` (a Rust-based JSON parser) reduced the cost of
deserializing the `idx` JSONB column from each row. This is a one-line change in
`pool.py` that applies globally to all psycopg connections.

### Phase 2: Lazy idx batch loading

**Impact: -62% query time (the biggest win).**

The original implementation selected `zoid, path, idx` for every row in the result
set. The `idx` column is a JSONB blob containing all catalog metadata for an object --
typically 1-3 KB per row. For a result set of 100 rows, this meant transferring
100-300 KB of JSONB data on every query, even when the caller only needed the paths
(e.g., for a listing that renders links but no metadata).

The lazy loading optimization splits the query into two phases:

1. The initial search selects only `zoid` and `path`.
2. On first brain metadata access, `_load_idx_batch()` issues a single
   `SELECT zoid, idx WHERE zoid = ANY(...)` for all brains in the result set.

For search results pages that never access metadata (count queries, batched listings
where only a page is rendered), the second query never fires. For pages that do
access metadata, the single batch query is cheaper than including `idx` in every row
of the original query because PostgreSQL can decompress the JSONB values more
efficiently in a targeted lookup than in a full table scan.

### Phase 3: Prepared statements

**Impact: Negligible per benchmark, saves PG parse overhead in production.**

Adding `prepare=True` to the main search query tells psycopg to use a prepared
statement. PostgreSQL parses and plans the query once, then reuses the plan for
subsequent executions with different parameters. In benchmarks with 50 iterations
of the same query shape, the per-query savings are too small to measure reliably.
In production, where the same query patterns repeat thousands of times per
connection lifetime, the cumulative savings are meaningful.

### Phase 4: Request-scoped connections

**Impact: Negligible per benchmark, avoids pool lock contention in production.**

A typical Plone page rendering issues 5-15 catalog queries (navigation, breadcrumbs,
portlets, content listing, related items). Without connection reuse, each query
borrows and returns a connection from the pool, acquiring a lock each time.

Request-scoped connection reuse stores the connection in `threading.local()` for the
duration of a Zope request. Subsequent queries within the same request skip the pool
entirely. The connection is returned by an `IPubEnd` subscriber when the request ends.

Like prepared statements, this does not show up in isolated benchmarks but reduces
contention under concurrent load.

### Phase 5: BTree write elimination

**Impact: -61% write time for creation, -72% for modification.**

This is not an "optimization" in the traditional sense -- it is the fundamental
architectural change. By writing catalog data as PostgreSQL columns instead of ZODB
BTree objects, plone.pgcatalog eliminates the majority of `store()` calls during a
content transaction.

### Phase 6: Clean break from ZCatalog

**Impact: ~2x faster queries across most patterns, modest write improvements.**

`PlonePGCatalogTool` originally inherited from `Products.CMFPlone.CatalogTool`,
which pulls in ZCatalog, ObjectManager, and roughly 15 other classes. Even after
BTree writes were eliminated, every catalog call still traversed this deep MRO for
attribute lookups, security checks, and Acquisition wrapping.

Replacing the base classes with `UniqueObject + Folder` eliminates this overhead.
A `_CatalogCompat` shim provides the `_catalog.indexes` and `_catalog.schema`
attributes that external code expects, so backward compatibility is preserved.

Query benchmarks (median, 50 iterations):

| Query pattern | Before | After | Change |
|---|---|---|---|
| Simple field match | 6.3 ms | 3.2 ms | **-50%** |
| Complex multi-index | 4.2 ms | 2.2 ms | **-46%** |
| Full-text search | 3.8 ms | 5.5 ms | +43% (PG planner variance) |
| Navigation | 6.1 ms | 3.3 ms | **-46%** |
| Security filtered | 7.3 ms | 3.5 ms | **-52%** |
| Date-sorted | 6.9 ms | 3.8 ms | **-44%** |

Write improvements are modest (creation 68.5 → 65.4 ms/doc, modification 14.0 →
13.9 ms/doc) because the write hot path was already dominated by PostgreSQL I/O.

## Scaling expectations

### Linear with content volume

PostgreSQL's query planner benefits from table statistics that are updated by
`ANALYZE` (run automatically by `autovacuum`). As the content volume grows, the
planner's cost estimates become more accurate, and it chooses better execution plans.
Index scans remain logarithmic in the number of rows.

### No cache cliff

ZCatalog has a "cache cliff" -- a point where the working set of BTree nodes exceeds
the ZODB cache size, and performance drops sharply as every query triggers cache
misses. plone.pgcatalog has no equivalent cliff because it does not use the ZODB cache
for catalog data. PostgreSQL has its own `shared_buffers` cache, but it is typically
much larger (hundreds of MB to GB) and shared across all queries, not partitioned per
connection.

### Network I/O is the floor

With 81% of query time spent on network I/O, there is limited room for further
application-level optimization. The practical strategies for reducing latency below
the current floor are:

- **Co-locate the database.** Use a Unix socket instead of TCP when PostgreSQL runs on
  the same host.
- **Reduce round-trips.** The lazy loading design already minimizes the number of
  queries per request.
- **Increase batch sizes.** For bulk operations (reindex, migration), larger batches
  amortize per-statement overhead.
