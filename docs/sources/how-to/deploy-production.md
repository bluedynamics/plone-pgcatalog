<!-- diataxis: how-to -->

# Deploy in Production

## PostgreSQL Setup

### Recommended Configuration

Key `postgresql.conf` settings for a dedicated PostgreSQL instance:

```
shared_buffers = 256MB          # 25% of RAM for dedicated PG
work_mem = 32MB                 # For complex queries
effective_cache_size = 1GB      # Available OS cache
maintenance_work_mem = 256MB    # For REINDEX/VACUUM
max_connections = 100           # Match Zope thread count + overhead
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
No special operator configuration is needed beyond the standard PostgreSQL image.

## Performance Tuning

- Set ZODB `cache-size` high (e.g., 10000) -- no BTree pressure means more cache available for application objects.
- PostgreSQL auto-creates all necessary indexes at startup via the `CatalogStateProcessor` DDL. No manual index creation is needed.
- Run `ANALYZE object_state` after large bulk imports to update planner statistics.
- Configure `autovacuum` for the `object_state` table. With frequent catalog writes, increase `autovacuum_analyze_threshold`:

  ```sql
  ALTER TABLE object_state SET (autovacuum_analyze_threshold = 5000);
  ```

- Deploy a reverse proxy (nginx, HAProxy) with rate limiting on search endpoints (`@@search`, `@@search-results`) to protect against query abuse.

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

## Backup and Recovery

- `pg_dump` captures all catalog data -- it lives in the same `object_state` table as ZODB object data.
- No separate catalog export/import is needed.
- Standard PostgreSQL backup strategies (continuous archiving, `pg_basebackup`, `pgBackRest`) apply without modification.
- After restoring from backup, no catalog rebuild is necessary. The catalog data is transactionally consistent with ZODB state.

## Upgrading plone.pgcatalog

1. Install the new version of plone.pgcatalog.
2. Restart Zope. Schema updates (new columns, functions, indexes) are applied automatically at startup by the `IDatabaseOpenedWithRoot` subscriber.
3. If release notes mention schema changes that require reindexing: run `clearFindAndRebuild()` from the ZMI Advanced tab or via script. See {doc}`rebuild-catalog`.
