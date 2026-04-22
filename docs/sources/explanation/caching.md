<!-- diataxis: explanation -->

# Caching

Between a catalog query and the objects it returns, the result passes through
several caches.
Each cache has a different scope, a different lifetime, and a different
invalidation rule.
Understanding how they compose answers two recurring questions: why some
queries are almost free after the first call, and why some things that
could be cached deliberately are not.

This page maps the full chain from query dict to object, explains what
each layer caches and when it lets go, and documents the invariant that
brains do not memoize the resolved object.

## The layers at a glance

The table below lists every cache that a `catalog.searchResults(...)` call
touches, in the order it walks through them.

| # | Layer | Owner | Scope | Lifetime and eviction |
|---|-------|-------|-------|-----------------------|
| 1 | Query result cache | plone.pgcatalog (`cache.py`) | Process | Cost-based LRU evict; whole cache cleared on TID change |
| 2 | Prepared statement cache | psycopg | Connection | Connection lifetime; invalidated by schema changes |
| 3 | Request connection pool | plone.pgcatalog (`pool.py`) | Request | Released at `IPubEnd` |
| 4 | zodb-pgjsonb `LoadCache` | zodb-pgjsonb (`PGJsonbStorageInstance`) | ZODB Connection | LRU by bytes (`cache_local_mb`, default 16 MB); entries invalidated on TID change |
| 5 | ZODB Connection object cache | ZODB | ZODB Connection | `cache-size` / `cache-size-bytes` in `zope.conf`; invalidation messages from storage |
| 6 | PostgreSQL `shared_buffers` | PostgreSQL | Database process | PG lifetime; LRU |

Layers 1, 3, and part of the prefetch path belong to plone.pgcatalog.
Layer 4 belongs to zodb-pgjsonb.
Layers 5 and 6 are standard components that plone.pgcatalog relies on
without controlling.

## What is not cached, and why

Two things you might expect to find cached are not, deliberately.

### Brains

Brains are rebuilt from scratch on every `searchResults` call, even when
the underlying rows come from the query result cache (layer 1).
The rebuild is cheap: a `PGCatalogBrain` holds one dict reference and two
slots (`_row`, `_result_set`).
Keeping brains disposable means no brain ever outlives the request that
created it, which makes staleness across requests impossible by
construction.

### The object returned by `getObject()`

`PGCatalogBrain.getObject()` does not memoize the resolved object.
Every call traverses the ZODB tree again via
`root.unrestrictedTraverse()` and `restrictedTraverse()`.
The traversal is cheap in practice because the ZODB Connection cache
(layer 5) already holds the unpickled instances along the path; all that
a repeat call pays for is a fresh Acquisition wrapper chain.

The reason this memoization is avoided is that the brain, unlike most
short-lived objects, could in principle survive the request that produced
it.
If a caller stashes brains in a session, a `plone.memoize` cache, or any
other request-external container, a memoized object on the brain would
go stale: traversal subscribers fire only during traversal, and some of
the state they set up (security manager, site hook, language) is
request-local.
Keeping brains pure rules out that whole class of bugs.
The place that legitimately caches unpickled instances is the ZODB
Connection, where the cache is scoped to the connection and invalidated
through the normal TID mechanism.

## How the layers compose

The Mermaid diagram below shows the sequence of cache lookups and
misses for a typical request that runs a catalog query and then calls
`getObject()` on one of the brains.

```{mermaid}
:alt: Cache lookup sequence for a catalog query followed by getObject
:caption: Query and getObject walk-through

sequenceDiagram
    participant V as View
    participant C as portal_catalog
    participant Q as Query cache (1)
    participant P as Prepared stmt (2)
    participant PG as PostgreSQL (6)
    participant B as Brain
    participant S as zodb-pgjsonb LoadCache (4)
    participant Z as ZODB Connection cache (5)

    V->>C: searchResults(query)
    C->>Q: get(normalized_query, tid)
    alt Cache hit
        Q-->>C: cached rows
    else Cache miss
        C->>P: execute(sql, params)
        P->>PG: wire protocol
        PG-->>P: rows
        P-->>C: rows
        C->>Q: put(rows, cost_ms, tid)
    end
    C-->>V: CatalogSearchResults(brains)

    V->>B: brain.getObject()
    B->>S: load_multiple(neighbourhood oids)
    note over S: Prefetch warms layer 4
    B->>Z: traverse path
    Z->>S: load(oid) per segment
    alt Bytes cached
        S-->>Z: pickle bytes
    else Bytes missing
        S->>PG: SELECT state FROM object_state
        PG-->>S: rows
        S-->>Z: pickle bytes
    end
    Z-->>B: aq-wrapped object
    B-->>V: object
```

A few properties are worth pulling out of the diagram.

The query path ends at layer 1: on a cache hit no SQL is sent at all,
and the brains are assembled from the cached row dicts.

The `getObject()` path never touches the query cache; it goes through
ZODB and the zodb-pgjsonb storage instance.
The two halves are coupled only through TID-based invalidation: when a
ZODB commit bumps the TID, layer 1 drops all entries and layers 4 and 5
receive invalidation messages for the specific OIDs.

## Prefetch: priming the byte cache

A listing that iterates over brains and calls `getObject()` on each would
cause one `load()` per brain without prefetch, each a separate query to
`object_state`.
Prefetch turns that into a single `load_multiple()` for a neighbourhood
window.

The mechanism is implemented in `CatalogSearchResults._maybe_prefetch_objects`.
When the first `getObject()` call lands on a brain that belongs to a
result set, the result set computes a half-open window
`[i, i + PGCATALOG_PREFETCH_BATCH)` around the brain's position, issues
one `SELECT ... FROM object_state WHERE zoid = ANY(...)`, and inserts
the returned pickle bytes into the zodb-pgjsonb `LoadCache` (layer 4).
Subsequent traversals for OIDs in the window find their bytes already
cached and return without a database round-trip.
A `_prefetched_ranges` set on the result set prevents re-fetching the
same window twice.

What prefetch does and does not do:

- It warms only layer 4 (pickle bytes).
- It does not unpickle, does not wrap with Acquisition, and does not
  traverse.
  The work that turns bytes into an object instance still happens in
  layer 5 when traversal actually accesses the segment.
- It is idempotent: OIDs already present in the `LoadCache` are skipped
  inside `load_multiple()`.
- It degrades gracefully: if the storage has no `load_multiple()` method
  (for example, a non-pgjsonb storage during testing), the prefetch call
  returns silently.

Disable prefetch by setting `PGCATALOG_PREFETCH_BATCH=0`.
The default of 100 matches the most common Plone listing shapes
(navigation trees, folder listings, news overviews) without keeping
material amounts of state in memory.

## Invalidation matrix

The table below ties each write event to the caches it invalidates.

| Event | Layer 1 | Layer 4 | Layer 5 |
|-------|---------|---------|---------|
| Catalog write (`catalog_object`, `reindexObject`, `uncatalog_object`, move) | Cleared when `pgcatalog_change_seq` advances past `_last_tid` | Per-OID invalidate on TID change | Per-OID invalidate on TID change |
| ZODB commit that does not touch the catalog (sessions, scales, annotations) | Not cleared (counter does not advance) | Per-OID invalidate on TID change | Per-OID invalidate on TID change |
| `pack` (history-free or history-preserving) | Not cleared directly; next catalog write triggers clear | Per-OID invalidate as objects reload at new TIDs | Per-OID invalidate as objects reload at new TIDs |
| DDL (new column, index created) | Not cleared | Not cleared | Not cleared |

Two entries deserve extra context.

The query cache uses a counter that only advances on catalog writes,
which means `plone.memoize`-wrapped views that depend on catalog results
keep their hit rate even on busy sites where the ZODB TID increments on
every session write.
This is the same trick that lets the tool expose a stable `getCounter()`
to `plone.memoize.ram`.

DDL does not propagate to any cache automatically.
A column added while a worker is running will not appear in queries
issued by that worker's pooled connections until the prepared statement
cache (layer 2) forgets the old plan, which typically means recycling
the connection.
In practice this only matters during upgrade steps; runtime DDL is not
expected.

## Configuration

The knobs live in environment variables for plone.pgcatalog, in
`zope.conf` sections for ZODB, and in `postgresql.conf` for PostgreSQL.

### plone.pgcatalog environment variables

`PGCATALOG_QUERY_CACHE_SIZE`
:   Maximum number of entries in the query result cache (layer 1).
    Default `200`.
    Set to `0` to disable.

`PGCATALOG_QUERY_CACHE_TTR`
:   Time-to-round, in seconds, for datetime parameters during cache key
    normalization (not a time-to-live).
    Default `60`.
    Two queries with `modified > now()` issued within the same minute
    hash to the same key and share a cache slot.
    Set to `0` to disable rounding.

`PGCATALOG_PREFETCH_BATCH`
:   Window size for `_maybe_prefetch_objects`.
    Default `100`.
    Set to `0` to disable prefetch entirely.

`PGCATALOG_SLOW_QUERY_MS`
:   Threshold in milliseconds above which a query is logged as slow and
    recorded in `pgcatalog_slow_queries`.
    Default `10`.

`PGCATALOG_LOG_ALL_QUERIES`
:   When truthy, log every query (not just slow ones) at `INFO` level.
    Off by default.
    Checked per query, so you can flip it at runtime without a restart.

### zope.conf

`cache-size` and `cache-size-bytes` control the ZODB Connection object
cache (layer 5).
This is the primary performance lever for warm-cache page loads; raising
it is the single biggest win on large sites.
See {doc}`performance` for concrete benchmark numbers.

### zodb-pgjsonb

The `cache_local_mb` option on the `<pgjsonb>` storage section sets the
byte budget for layer 4, per ZODB Connection instance.
Default is 16 MB.
Each worker process typically holds several instances (one per
open connection), so the actual resident memory is
`workers * connections_per_worker * cache_local_mb`.

### PostgreSQL

`shared_buffers` sizes layer 6, and `work_mem` governs per-query sort
and hash memory (which is not a cache but does affect whether a query
spills to disk).
Neither of these is plone.pgcatalog-specific; follow general PostgreSQL
tuning advice for your workload.

## Debugging cache behavior

Cache stats for layer 1 are available through `get_query_cache().stats()`
and in the ZMI under the catalog tool's management tabs.
The output includes `hits`, `misses`, `hit_rate`, `invalidations`, the
top entries by cost, and the `last_tid` the cache is pinned to.

When a query unexpectedly hits PostgreSQL on every call, the most common
causes are: a datetime parameter that is not being rounded (check
`PGCATALOG_QUERY_CACHE_TTR` and whether your query uses a type that
implements `timeTime()`), a non-normalizable object in the query value
(unsortable mixed types in a list), and frequent catalog writes on the
same worker (counter advances faster than hits accumulate).

When `getObject()` is slower than expected for a warm request, first
rule out layer 5 being undersized: if the ZODB cache is full, every
traversal segment re-unpickles from layer 4 bytes.
If that check passes, rule out prefetch being off
(`PGCATALOG_PREFETCH_BATCH=0` or the brain being constructed outside a
result set).

When you suspect cross-request staleness on an object returned by
`getObject()`, remember that brains themselves hold no object state,
and that layer 5 invalidates on TID change.
Staleness in that path usually traces back to either a view that cached
the result of `getObject()` in its own scope across requests, or to a
`_v_` attribute written by a traversal subscriber on a persistent object
that then survived in layer 5 until the next commit.

```{seealso}
{doc}`performance` covers benchmark results and tuning for end-to-end
query and `getObject()` latency.
{doc}`architecture` describes the write path that drives catalog-side
invalidation (`pgcatalog_change_seq`).
```
