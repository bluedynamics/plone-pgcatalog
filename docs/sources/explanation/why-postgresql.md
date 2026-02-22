<!-- diataxis: explanation -->

# Why PostgreSQL for the Catalog?

Plone's default ZCatalog stores every index as a BTree inside ZODB. This works well
for small to medium sites, but the architecture has structural limits that become
painful at scale. plone.pgcatalog moves catalog data into PostgreSQL, trading
ZCatalog's self-contained simplicity for the query planner, indexing strategies, and
operational tooling of a dedicated database engine.

This page explains the problems that motivated the move, what PostgreSQL provides in
return, and the trade-offs involved.

## The problem with ZCatalog at scale

### Every index is a BTree stored in ZODB

A typical Plone site has 25-35 catalog indexes. Each index is implemented as one or
more BTree objects (forward index, reverse index, length tracking), and each BTree
is a tree of Bucket objects. A FieldIndex for `portal_type` with 20 distinct values
might have 40+ persistent BTree/Bucket objects. A KeywordIndex for `Subject` with
thousands of tags has proportionally more.

These BTree nodes are ordinary ZODB persistent objects. They are loaded into the ZODB
object cache on demand and evicted under memory pressure, just like any Plone content
object.

### Cache pressure

The ZODB cache has a fixed size (default: 400 objects per connection in Plone). At
scale, catalog BTree nodes compete with application objects for cache slots. Consider
what happens on a site with 50,000 content items:

- A catalog search touches dozens of BTree nodes across multiple indexes.
- These nodes fill the cache, evicting recently loaded content objects.
- The next page render must reload those content objects from the database.
- The content objects evict catalog BTree nodes.
- The next search must reload the BTree nodes.

This is a cascading cache miss pattern. The more indexes you have and the more content
you serve, the worse it gets. Increasing the cache size helps but does not solve the
fundamental problem: catalog data and application data are fighting for the same
limited resource.

### Index intersection in Python

When a ZCatalog query spans multiple indexes (e.g., `portal_type="Document"` AND
`review_state="published"` AND `path="/plone/news"`), each index returns a set of
record IDs. ZCatalog intersects these sets in Python using `IITreeSet.intersection()`.
There is no query planner -- the intersection order is determined by the order indexes
appear in the query dict, not by selectivity.

For a query that matches 10,000 documents by type but only 50 by path, ZCatalog
materializes the 10,000-element set first, then intersects it with the 50-element set.
A SQL query planner would evaluate the path condition first and never touch the other
9,950 records.

## What PostgreSQL gives us

### Zero ZODB cache pressure

Catalog data lives in PostgreSQL columns, not in ZODB persistent objects. A catalog
search does not load a single BTree node into the ZODB cache. Content objects keep
their cache slots undisturbed.

This is not a minor optimization -- it changes the scaling curve. With ZCatalog,
performance degrades as the ratio of catalog objects to cache size grows. With
plone.pgcatalog, the ZODB cache is used exclusively for application objects regardless
of catalog size.

### Query planner

PostgreSQL's query planner selects which indexes to use, in what order, based on
table statistics. It knows the selectivity of each condition and chooses the cheapest
execution plan. When a query combines `portal_type`, `review_state`, and `path`,
PostgreSQL evaluates the most selective condition first and short-circuits early.

This extends to `LIMIT` pushdown: a query for `sort_on="modified"` with
`b_size=20` stops after finding 20 matching rows, rather than materializing the
entire result set and truncating. ZCatalog always materializes the full intersection
before slicing.

### JSONB queryability

All catalog index data is stored in a single `idx` JSONB column. This data is
directly queryable from any PostgreSQL client -- psql, pgAdmin, Grafana, custom
reporting scripts. You do not need to go through Plone to inspect or analyze
catalog data.

```sql
-- How many published Documents exist?
SELECT COUNT(*)
FROM object_state
WHERE idx->>'portal_type' = 'Document'
  AND idx->>'review_state' = 'published';

-- What are the most common subjects?
SELECT jsonb_array_elements_text(idx->'Subject') AS tag, COUNT(*)
FROM object_state
WHERE idx IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
```

This is valuable for debugging, monitoring, and building integrations that do not
require a running Plone instance.

### Transactional consistency

Because plone.pgcatalog writes catalog columns in the same PostgreSQL transaction that
stores the ZODB object pickle (via zodb-pgjsonb's state processor infrastructure),
catalog data is always consistent with object data. There is no window where a content
item has been modified but its catalog entry reflects the old state.

This is actually stronger than ZCatalog's consistency model, where the catalog BTrees
are separate persistent objects that can fail to commit independently of the content
objects (though ZODB's transaction semantics make this rare in practice).

### Operational simplicity

- **`pg_dump` captures everything.** One backup command gets both ZODB objects and
  catalog data. There is no separate catalog export/import step.
- **Standard monitoring works.** `pg_stat_user_tables`, `pg_stat_user_indexes`, and
  `pg_stat_statements` provide visibility into catalog query performance without
  Plone-specific tooling.
- **`REINDEX` and `ANALYZE` are standard operations.** PostgreSQL index maintenance
  uses the same tools and procedures as any other PostgreSQL application.

## Trade-offs

### Network round-trip overhead

ZCatalog's BTrees live in-process memory (the ZODB cache). A BTree lookup is a
pointer dereference -- effectively free once the node is cached. A PostgreSQL query
requires a network round-trip (or at minimum a local socket call), even for the
simplest lookup.

For single-object lookups like `getrid(path)` or `getpath(zoid)`, this overhead is
measurable: roughly 0.5-1ms per call versus nanoseconds for a cached BTree. For
search queries that touch many rows, the overhead is amortized and the query planner's
advantages dominate.

### Additional infrastructure dependency

ZCatalog requires nothing beyond ZODB itself -- a FileStorage or RelStorage backend
is sufficient. plone.pgcatalog requires zodb-pgjsonb as the ZODB storage backend,
which means PostgreSQL is a hard dependency. Sites that use FileStorage or a
non-PostgreSQL RelStorage backend cannot use plone.pgcatalog.

For sites already running on PostgreSQL (which is the recommended RelStorage backend
for production Plone), this is not an additional dependency -- it is leveraging
infrastructure that already exists.

### Small sites may not benefit

A Plone site with a few hundred to a few thousand objects is unlikely to experience
ZCatalog's scaling problems. The BTree nodes fit comfortably in the ZODB cache,
Python-level set intersections are fast on small sets, and the operational simplicity
of having one fewer infrastructure component may outweigh the performance gains.

The crossover point depends on the site's content volume, query complexity, and
traffic patterns, but as a rough guideline: sites with fewer than 5,000 objects and
modest query loads may not see a meaningful improvement.

### Requires zodb-pgjsonb

plone.pgcatalog's write path depends on zodb-pgjsonb's state processor
infrastructure to write catalog columns atomically alongside object pickles. This is
not a generic PostgreSQL integration -- it is specifically designed for zodb-pgjsonb's
`object_state` table layout and transaction lifecycle hooks.

This coupling is intentional: it is what enables the single-transaction atomicity
guarantee. But it means plone.pgcatalog cannot be used with other ZODB storage
backends, even if they happen to use PostgreSQL (e.g., a hypothetical alternative
PostgreSQL storage).
