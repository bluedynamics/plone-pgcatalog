# Query Result Cache

**Date:** 2026-04-02
**Status:** Approved

## Problem

Catalog queries execute against PostgreSQL on every request, even when
the underlying data has not changed. Navigation tree queries (85-300ms)
fire on every page load. With `getCounter()` now returning `MAX(tid)`,
we have a reliable invalidation signal --- but no cache to invalidate.

## Design

### Overview

A process-wide in-memory cache in `_run_search()`. Cache key = hash of
the normalized query dict. Cache value = raw DB rows + actual_count.
Invalidated when `MAX(tid)` changes (any ZODB commit). Eviction is
cost-based: expensive queries stay in cache, cheap ones are evicted
first when the cache is full.

### New module: `src/plone/pgcatalog/cache.py`

A singleton `QueryCache` on module level.

### Cache key

`hash(repr(normalized_query))` where normalization:

- Sorts dict keys recursively
- Sorts list values (e.g. `portal_type` order is irrelevant)
- Rounds datetime values to a configurable granularity
  (`PGCATALOG_QUERY_CACHE_TTR`, default 60 seconds). This means
  `effectiveRange` queries within the same minute share a cache key.

### Cache value

Raw DB rows (`list[dict]`) + `actual_count` (int or None for
non-LIMIT queries). Brains are rebuilt from cached rows on each hit.
This avoids stale object references across requests.

### Invalidation

On every cache access (hit or miss), compare the stored `last_tid`
with the current `MAX(tid)`. If different, clear the entire cache.
`MAX(tid)` costs ~0.2ms via Index Only Scan and is already called
by `getCounter()`.

### Eviction

When the cache exceeds `max_entries`, evict the entry with the
lowest `cost_ms` (the query duration recorded at insert time).
This keeps expensive queries (navigation 85ms) in cache while
cheap queries (folder listing 10ms) are evicted first.

### Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `PGCATALOG_QUERY_CACHE_SIZE` | `200` | Max cached queries. Set to `0` to disable. |
| `PGCATALOG_QUERY_CACHE_TTR` | `60` | Time-to-round in seconds for datetime values. Controls cache key granularity for effectiveRange queries. |

### Thread safety

A single `threading.Lock` protects the cache dict. The lock is held
only for dict operations (lookup, insert, evict) --- no I/O under lock.

### Integration point

In `_run_search()`:

1. Normalize query, compute cache key
2. Check `MAX(tid)` --- if changed, clear cache
3. Cache lookup --- if hit, rebuild brains from cached rows, return
4. Execute SQL query (cache miss)
5. Store result rows + cost in cache
6. Build brains, return

### What does NOT change

- Lazy metadata loading (batch `idx` fetch on first brain access)
- Brain construction (rebuilt from cached rows per request)
- MVCC snapshot consistency (reads still use REPEATABLE READ)
- Slow query logging (only fires on cache miss)
- `unrestrictedSearchResults` --- also cached (query includes or
  excludes security filters, so cache keys differ naturally)

### Scope

- Cache lives in `cache.py` as a standalone class
- Integration in `search.py` `_run_search()` --- minimal changes
- No changes to `catalog.py`, `query.py`, or `brain.py`

## Expected impact

| Scenario | Before | After |
|----------|--------|-------|
| Navigation (repeat) | 85-300ms | <1ms (cache hit) |
| Folder listing (repeat) | 20ms | <1ms (cache hit) |
| First request after edit | Full query | Cache miss, refill |
| Steady state (no edits) | All queries hit PG | All queries hit cache |

On a typical page with 10-15 catalog queries, the second page load
should be nearly free (~2ms for 10 cache hits + 1 `MAX(tid)` check).

## ZMI visibility

The existing "Slow Queries" tab gets a second section: **Cache Status**.

### Display

- **Hit/Miss ratio**: total hits, misses, hit-rate percentage
- **Entries**: current / max configured
- **Last TID**: current cached TID + number of invalidations since start
- **TTR**: configured rounding granularity (seconds)
- **Top cached queries**: the N most expensive cached entries
  (query keys, cost_ms, hit count)
- **Clear Cache** button (with confirmation dialog)

### Data source

The `QueryCache` singleton exposes a `stats()` method returning a dict
with all the above. The ZMI tab calls `manage_get_cache_stats()` on
the catalog tool, which delegates to `QueryCache.stats()`.

## Implementation steps

1. Create `cache.py` with `QueryCache` class (incl. `stats()` method)
2. Add query normalization (sort keys, round datetimes)
3. Integrate into `_run_search()` with TID check
4. Add env var configuration
5. Add `manage_get_cache_stats()` + `manage_clear_cache()` to catalog
6. Extend `catalogSlowQueries.dtml` with Cache Status section
7. Tests: cache hit/miss, TID invalidation, cost eviction, TTR rounding,
   stats accuracy
8. Changelog + docs
