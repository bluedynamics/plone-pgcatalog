<!-- diataxis: how-to -->

# Deploy in production

## PostgreSQL setup

### Recommended configuration

Key `postgresql.conf` settings for a dedicated PostgreSQL instance:

```
shared_buffers = 256MB          # 25% of RAM for dedicated PG
work_mem = 32MB                 # For complex queries
effective_cache_size = 1GB      # Available OS cache
maintenance_work_mem = 256MB    # For REINDEX/VACUUM
max_connections = 100           # Match Zope thread count + overhead
```

(database-locale)=

### Database locale

The database must not use `C` or `POSIX` as its character classification (`LC_CTYPE`).
With a `C` ctype, PostgreSQL's text search parser treats every non-ASCII character as part of a word, so punctuation such as `»`, `–`, `…` or `©` sticks to the neighbouring word.
Full-text queries then silently return fewer results; on one production site 22 % of the stored search text was affected.
plone.pgcatalog logs an ERROR at startup when it finds such a database.

Check it with:

```sql
SELECT datname, datctype FROM pg_database WHERE datname = current_database();
```

Only `datctype` counts.
A database created with the `builtin` locale provider but `LC_CTYPE 'C'` is affected too, although it looks correctly configured.
The collation (`LC_COLLATE`) can stay `C`; it does not influence tokenization.

`initdb` falls back to `C` when no locale is set in its environment, which is the normal case in container images without `LANG`.
The official `postgres` Docker image sets `en_US.utf8` and is fine.

#### Fixing an affected database

`LC_CTYPE` cannot be changed on an existing database.

1. Create a new database with a proper ctype, keeping the collation if you like:

   ```sql
   CREATE DATABASE zodb_new TEMPLATE template0 ENCODING 'UTF8'
     LC_COLLATE 'C' LC_CTYPE 'C.UTF-8';
   ```

2. Copy the data with `pg_dump` / `pg_restore`.
   The restore rebuilds all indexes, so title and description searches are correct right away.
3. Point Zope at the new database and recatalog all content, because the stored `searchable_text` keeps its broken lexemes through a dump and restore.
   Use the repair script from {doc}`rebuild-catalog` with `--all`: it recatalogs every object without clearing the catalog first, so search keeps working meanwhile.
   With Tika extraction configured, this queues the extraction of every file again.

To see which rows are still mis-tokenized after the move, run in the new database:

```sql
SELECT count(*) FROM object_state os
 WHERE searchable_text IS NOT NULL
   AND EXISTS (
     SELECT 1 FROM unnest(tsvector_to_array(os.searchable_text)) lex
      WHERE lex ~ '[^\x01-\x7f]'
        AND array_length(tsvector_to_array(to_tsvector('simple', lex)), 1)
            IS DISTINCT FROM 1);
```

### Docker

```bash
docker run -d --name plone-pg \
  -e POSTGRES_USER=zodb \
  -e POSTGRES_PASSWORD=zodb \
  -e POSTGRES_DB=zodb \
  -p 5432:5432 \
  postgres:17
```

For BM25 support, use `tensorchord/vchord-suite:pg17-latest` instead.
See {doc}`enable-bm25` for details.

### CloudNativePG (Kubernetes)

plone.pgcatalog is compatible with CloudNativePG.
VectorChord-BM25 supports WAL replication, so read replicas can serve BM25 queries.

CloudNativePG passes no locale to `initdb` unless you set one, so a new cluster gets `LC_CTYPE 'C'` (see {ref}`database-locale`).
Set it when you create the cluster; it cannot be changed later:

```yaml
spec:
  bootstrap:
    initdb:
      localeCType: C.UTF-8
      localeCollate: C
```

## Performance tuning

- Set ZODB `cache-size` high (for example, 10000) -- no BTree pressure means more cache available for application objects.
- PostgreSQL autocreates all necessary indexes at startup via the `CatalogStateProcessor` DDL. No manual index creation is needed.
- Run `ANALYZE object_state` after large bulk imports to update planner statistics.
  `clearFindAndRebuild()` and `repair_uncataloged()` do it themselves.
- Autoanalyze for `object_state`: at startup, plone.pgcatalog sets
  `autovacuum_analyze_scale_factor = 0.01` and `autovacuum_analyze_threshold = 5000` on the table.
  The PostgreSQL default of 10 % is too coarse, because the table holds every ZODB object and a recatalog of all content can stay below it; the planner then ignores the GIN indexes and searches slow down by orders of magnitude.
  If you set any `autovacuum_analyze_*` option on `object_state` yourself, plone.pgcatalog leaves the table alone:

  ```sql
  ALTER TABLE object_state SET (autovacuum_analyze_scale_factor = 0.02);
  ```

- Deploy a reverse proxy (nginx, HAProxy) with rate limiting on search endpoints (`@@search`, `@@search-results`) to protect against query abuse.

### ZODB cache sizing

The default ZODB cache (5000 objects) is too small for production sites.
Increase `cache-size` and `cache-size-bytes` in `zope.conf`:

```ini
<zodb_db main>
  cache-size 70000
  cache-size-bytes 500MB
  <pgjsonb>
    dsn dbname=zodb host=localhost port=5432 user=zodb password=zodb
  </pgjsonb>
</zodb_db>
```

A site with 14,000 events showed 5-6 second warm-cache page loads with
the default, dropping to 0.8 seconds with 70,000 objects. While
plone.pgcatalog eliminates BTree cache pressure for catalog data, Plone
content objects themselves still benefit from a large ZODB cache.

### Query cache and prefetch

plone.pgcatalog includes a process-wide query result cache and a batch
object prefetcher. Both are enabled by default:

- `PGCATALOG_QUERY_CACHE_SIZE=200` -- cached query results per process,
  invalidated on every catalog write. Set to `0` to disable.
- `PGCATALOG_QUERY_CACHE_MAX_ROWS=100000` -- rows over all cached results
  per process (roughly 32 MiB); results over
  `PGCATALOG_QUERY_CACHE_MAX_ENTRY_ROWS=10000` rows are not cached.
- `PGCATALOG_PREFETCH_BATCH=100` -- objects prefetched when
  `brain.getObject()` is called (requires zodb-pgjsonb >= 1.8.0).
  Set to `0` to disable.

See {doc}`../reference/configuration` for all environment variables.

## Monitoring

Key PostgreSQL views:

- `pg_stat_user_tables` -- row counts, sequential vs. index scans, vacuum stats
- `pg_stat_user_indexes` -- index usage and size
- `pg_stat_activity` -- active queries and locks

Enable slow query logging:

```
log_min_duration_statement = 100   # Log queries slower than 100 ms
```

Check catalog object count via ZMI (portal_catalog > Catalog tab) or SQL:

```sql
SELECT COUNT(*) FROM object_state WHERE idx IS NOT NULL;
```

## Backup and recovery

- `pg_dump` captures all catalog data -- it lives in the same `object_state` table as ZODB object data.
- No separate catalog export/import is needed.
- Standard PostgreSQL backup strategies (continuous archiving, `pg_basebackup`, `pgBackRest`) apply without modification.
- After restoring from backup, no catalog rebuild is necessary. The catalog data is transactionally consistent with ZODB state.

## Upgrading plone.pgcatalog

1.
Install the new version of plone.pgcatalog.
2.
Restart Zope.
Schema updates (new columns, functions, indexes) are applied automatically at startup by the `IDatabaseOpenedWithRoot` subscriber.
3.
If release notes mention schema changes that require reindexing: run `clearFindAndRebuild()` from the ZMI Advanced tab or via script.
See {doc}`rebuild-catalog`.
