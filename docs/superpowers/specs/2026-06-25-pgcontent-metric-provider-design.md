# SQL-based plone.observability IMetricProvider (content counts)

**Date:** 2026-06-25
**Issue:** [#174](https://github.com/bluedynamics/plone-pgcatalog/issues/174)
**Status:** Approved (design)

## Summary

Ship a `plone.observability` `IMetricProvider` in plone.pgcatalog that produces the
**content count metrics** (`plone_content_total` by `portal_type`,
`plone_content_by_state` by `review_state`) via SQL `GROUP BY` over the
`object_state` table, for deployments using the pg-based catalog.

The generic provider in plone.observability uses the ZCatalog index API
(`Indexes["portal_type"].uniqueValues(...)`), which `PlonePGCatalogTool` does not
implement, so those metrics are silently absent on pg-catalog sites. pgcatalog owns
the `object_state.idx JSONB` schema and can answer this with two aggregate queries
instead of loading all brains.

## Constraints

- **Soft dependency on plone.observability.** No hard `install_requires`. The adapter
  registers only via `zcml:condition="installed plone.observability"` (same pattern as
  the existing `installed eea.facetednavigation` in `overrides.zcml`). If the package is
  absent, nothing is registered and the provider module is never imported.
- **Migration-safe.** pgcatalog may be installed and loaded while a given Plone site
  still uses a ZCatalog (migration pending). For such a site `object_state.idx` is not
  maintained, so the provider MUST emit **only for sites whose `portal_catalog` actually
  provides `IPGCatalogTool`**. ZCatalog sites are left to the generic provider (whose
  `uniqueValues()` works there). This prevents double-counting and stale/wrong numbers.
- **Label parity** with the generic provider: same metric names, `gauge` type,
  `scope="global"`, same help strings, and the `{..., site=<site-id>}` label.

## Components

### New module `src/plone/pgcatalog/observability.py`

Imports (only loaded when the conditional ZCML fires):
```python
from plone.observability.interfaces import IMetricProvider
from plone.observability.metric import Metric
```

**`_aggregate(conn, site_ids)` — pure helper (testable in isolation).**
Runs the two queries on a psycopg connection, restricted to the given site ids, and
returns a list of `Metric`:

```sql
SELECT split_part(path,'/',2) AS site, idx->>'portal_type' AS pt, count(*) AS n
  FROM object_state
 WHERE idx->>'portal_type' IS NOT NULL
   AND split_part(path,'/',2) = ANY(%(sites)s)
 GROUP BY site, pt;

SELECT split_part(path,'/',2) AS site, idx->>'review_state' AS rs, count(*) AS n
  FROM object_state
 WHERE idx->>'review_state' IS NOT NULL
   AND split_part(path,'/',2) = ANY(%(sites)s)
 GROUP BY site, rs;
```

Emits:
- `plone_content_total{portal_type=<pt>, site=<site>}` — gauge, scope `global`,
  help `"Number of content objects by portal type"`
- `plone_content_by_state{state=<rs>, site=<site>}` — gauge, scope `global`,
  help `"Number of content objects by workflow state"`

**`PGContentMetricProvider` — `@implementer(IMetricProvider)`.**
- `name = "pgcontent"`, `scope = "global"`, `__init__(self, context)`.
- `collect()` (generator):
  1. **Site discovery:** find `IPloneSiteRoot` objects under `self.context` (the app
     root); for each, traverse `portal_catalog` and keep `site.id` only when
     `IPGCatalogTool.providedBy(catalog)`.
  2. If no pg sites → yield nothing.
  3. **TTL cache check** (process-global, see below). On hit, yield cached metrics.
  4. On miss: get the pool via `get_pool(self.context)`, take a connection
     (`with pool.connection() as conn:`), call `_aggregate(conn, pg_site_ids)`, store in
     cache, yield.
  5. Wrap pool/SQL access in try/except: on `RuntimeError` (no pool) or psycopg/DB error,
     `logger.debug(...)` and yield nothing (degrade silently, like the generic provider).

**Process-global TTL cache.** Module-level `_cache` (list|None) + `_cache_ts` (float)
guarded by a `threading.Lock`; TTL from `PLONE_OBSERVABILITY_METRICS_CACHE_TTL`
(default `60`). Cache must be module-level, not instance-level: the `@@metrics` view
calls `getAdapters((app,), IMetricProvider)`, which constructs a fresh provider instance
per request, so an instance cache would never hit across scrapes. Cache key is the sorted
tuple of pg site ids (so it is correct if the set of pg sites changes mid-process, e.g.
after a migration).

### ZCML registration (`configure.zcml`)

Add `xmlns:zcml="http://namespaces.zope.org/zcml"` to the root element and register:

```xml
<adapter
    factory=".observability.PGContentMetricProvider"
    provides="plone.observability.interfaces.IMetricProvider"
    for="OFS.interfaces.IApplication"
    name="pgcontent"
    zcml:condition="installed plone.observability" />
```

### pyproject.toml

Add `plone.observability` to the `test` optional-dependencies (needed so the imports
resolve in the test environment). No change to runtime dependencies.

## Data flow

```
@@metrics request
  → MetricsView calls getAdapters((app,), IMetricProvider)
  → PGContentMetricProvider.collect()
      → discover IPGCatalogTool-backed sites
      → TTL cache (process-global) check
      → get_pool(app).connection() → _aggregate(conn, site_ids)
      → yield Metric(...)
  → PrometheusFormatter renders plone_content_total / plone_content_by_state
```

On a mixed (migrating) deployment:

| Site state | Emitting provider | Mechanism |
|---|---|---|
| migrated (`PlonePGCatalogTool`) | **pgcontent (this)** | SQL, restricted to that site id |
| still ZCatalog | generic content provider | `Indexes[...].uniqueValues()` |

No double counting: the generic provider's `uniqueValues()` raises on pg sites (it steps
aside, plone/plone.observability#25), and this provider restricts its SQL to pg sites
only.

## Error handling

- No pool (`get_pool` raises `RuntimeError`) → debug log, no metrics.
- `object_state` missing / SQL error → debug log, no metrics.
- `portal_catalog` traversal/`providedBy` failure per site → skip that site.

All degrade silently; a metrics endpoint must never raise.

## Testing

`tests/test_observability.py`:

1. **Aggregation (DB fixture `pg_conn_with_catalog`).** Insert objects (via the existing
   `insert_object` helper) with known `path` / `idx.portal_type` / `idx.review_state`
   across two site ids (`/sitea/...`, `/siteb/...`). Call `_aggregate(conn, ["sitea"])`
   and assert: correct `plone_content_total` / `plone_content_by_state` Metric objects
   (names, `gauge`, scope `global`, `{portal_type|state, site}` labels, counts); objects
   under `siteb` and rows with NULL `portal_type`/`review_state` are excluded.
2. **Site selection.** `collect()` yields nothing when no site provides `IPGCatalogTool`
   (stub/empty app context). With a pg-backed site present, only its id is passed to the
   aggregation. (Integration-level via `pgcatalog_layer` where a real pg site exists.)
3. **Degradation.** Monkeypatch `get_pool` to raise `RuntimeError` → `collect()` yields
   nothing, no exception.
4. **Cache.** Two `collect()` cycles within the TTL run the SQL once (assert via a
   call-count spy on `_aggregate` / the connection); past the TTL it re-queries.

`plone.observability` added to the `test` extra so imports resolve.

## Out of scope

- System / ZODB / request metrics (owned by plone.observability's other providers).
- Adding the `z3c.autoinclude` entry point to plone.observability (tracked separately as
  plone/plone.observability#21).
- Any change to the generic provider's step-aside behavior (plone/plone.observability#25).

## CHANGES.md

Add under `## 1.0.0b66 (unreleased)`, section `### Added`:

> - Ship a `plone.observability` `IMetricProvider` (`pgcontent`) that produces
>   `plone_content_total` / `plone_content_by_state` content-count metrics via SQL for
>   pg-catalog sites. Registered only when `plone.observability` is installed
>   (`zcml:condition`) and emits only for sites backed by `IPGCatalogTool` (migration
>   safe). #174
