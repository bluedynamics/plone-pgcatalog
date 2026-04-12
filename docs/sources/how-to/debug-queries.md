<!-- diataxis: how-to -->

# Debug SQL queries

## Overview

PlonePGCatalog provides multiple ways to debug and monitor SQL queries generated from catalog searches. This is useful when troubleshooting performance issues or understanding how catalog queries are translated to PostgreSQL.

## Environment variables

### PGCATALOG_LOG_ALL_QUERIES

Log all SQL queries to the Python logger at INFO level.

**Usage:**
```bash
export PGCATALOG_LOG_ALL_QUERIES=1
# or
export PGCATALOG_LOG_ALL_QUERIES=true
# or
export PGCATALOG_LOG_ALL_QUERIES=yes
```

**Log output format:**
```
INFO plone.pgcatalog.search SQL catalog query (1.23 ms): SELECT zoid, path FROM object_state WHERE idx IS NOT NULL AND idx @> $1::jsonb | params: {'p_portal_type_1': {'portal_type': 'Document'}} | keys: ['portal_type']
```

**Performance impact:** Minimal when disabled (single environment variable check). When enabled, adds ~0.1ms per query for logging overhead.

### PGCATALOG_SLOW_QUERY_MS

Control the threshold for slow query logging (default: 10ms).

**Usage:**
```bash
export PGCATALOG_SLOW_QUERY_MS=100  # Log queries > 100ms
export PGCATALOG_SLOW_QUERY_MS=0.1  # Log all queries (via slow query system)
```

Slow queries are logged at WARNING level and stored in the `pgcatalog_slow_queries` table for analysis via the ZMI.

## Debugging workflow

### 1. Enable query logging

For comprehensive debugging:
```bash
export PGCATALOG_LOG_ALL_QUERIES=1
```

For slow query analysis only:
```bash
export PGCATALOG_SLOW_QUERY_MS=50  # Adjust threshold as needed
```

### 2. Configure Python logging

Enable INFO level logging for the search module:

```python
import logging
logging.getLogger('plone.pgcatalog.search').setLevel(logging.INFO)
```

Or in your `zope.conf`:
```xml
<logger>
  name plone.pgcatalog.search
  level info
</logger>
```

### 3. Run your problematic query

Execute the catalog search that's causing issues:

```python
# In Python/debug console
results = portal.portal_catalog.searchResults(
    portal_type='Document',
    review_state='published'
)
```

### 4. Analyze the logged SQL

The log output shows:
- **Duration**: Query execution time in milliseconds
- **SQL**: The actual PostgreSQL query
- **Params**: Parameter values used in the query
- **Keys**: Original catalog query keys

Example log entry:
```
INFO plone.pgcatalog.search SQL catalog query (1.23 ms): SELECT zoid, path FROM object_state WHERE idx IS NOT NULL AND idx @> $1::jsonb AND idx @> $2::jsonb | params: {'p_portal_type_1': {'portal_type': 'Document'}, 'p_review_state_2': {'review_state': 'published'}} | keys: ['portal_type', 'review_state']
```

### 5. Test the query directly in PostgreSQL

Copy the SQL and parameters to test directly:

```sql
-- Connect to your database
psql "dbname=zodb host=localhost port=5432 user=zodb"

-- Test the query with actual parameter values
SELECT zoid, path
FROM object_state
WHERE idx IS NOT NULL
  AND idx @> '{"portal_type": "Document"}'::jsonb
  AND idx @> '{"review_state": "published"}'::jsonb;

-- Analyze query performance
EXPLAIN ANALYZE SELECT zoid, path
FROM object_state
WHERE idx IS NOT NULL
  AND idx @> '{"portal_type": "Document"}'::jsonb
  AND idx @> '{"review_state": "published"}'::jsonb;
```

## ZMI interface

Access the **Slow Queries** tab at `/portal_catalog/manage_slowQueries` to:

- View aggregated slow query statistics
- Get index suggestions for common slow queries
- Run EXPLAIN plans on recorded slow queries
- Create suggested indexes to improve performance

## Production considerations

### Recommended settings for production

```bash
# Log only genuinely slow queries
export PGCATALOG_SLOW_QUERY_MS=100

# Disable comprehensive query logging in production
# (omit PGCATALOG_LOG_ALL_QUERIES or set to 0/false)
```

### Debugging specific issues

For temporary debugging in production:

1. Enable query logging with a time limit:
   ```bash
   export PGCATALOG_LOG_ALL_QUERIES=1
   # Restart application
   # Reproduce the issue
   # Disable logging and restart
   ```

2. Use PostgreSQL's built-in logging:
   ```sql
   -- Temporarily log all queries
   ALTER SYSTEM SET log_statement = 'all';
   SELECT pg_reload_conf();

   -- Restore normal logging
   ALTER SYSTEM RESET log_statement;
   SELECT pg_reload_conf();
   ```

## See also

- [Query raw SQL](query-raw-sql.md) - Direct PostgreSQL queries
- [Deploy production](deploy-production.md) - Production configuration