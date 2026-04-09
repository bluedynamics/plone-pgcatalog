<!-- diataxis: how-to -->

# Query the catalog via raw SQL

## Connecting to PostgreSQL

Use psql, pgAdmin, DBeaver, or any PostgreSQL client.
The connection details match the `dsn` in your `zope.conf`:

```bash
psql "dbname=zodb host=localhost port=5432 user=zodb"
```

## The object_state table

All catalog data lives in `object_state` alongside ZODB object pickles:

| Column | Type | Contains |
|---|---|---|
| `zoid` | `BIGINT` | Object ID (primary key) |
| `path` | `TEXT` | Physical path (for example, `/plone/folder/doc`) |
| `parent_path` | `TEXT` | Parent's path (for example, `/plone/folder`) |
| `path_depth` | `INTEGER` | Number of path components |
| `idx` | `JSONB` | Index and metadata values |
| `searchable_text` | `TSVECTOR` | Weighted full-text vector |
| `meta` | `JSONB` | Non-JSON-native metadata (`DateTime`, etc.) |
| `object_provides` | `TEXT[]` | Interface-based lookups |
| `allowed_roles` | `TEXT[]` | Security filter (allowedRolesAndUsers) |

## Common query patterns

### Content by type

```sql
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE idx @> '{"portal_type": "Document"}'::jsonb
  AND path IS NOT NULL;
```

### Full-text search

```sql
SELECT path, idx->>'Title' AS title,
       ts_rank_cd(searchable_text, q) AS rank
FROM object_state, plainto_tsquery('english', 'search term') q
WHERE searchable_text @@ q
ORDER BY rank DESC
LIMIT 20;
```

### Date range query

```sql
SELECT path, idx->>'Title' AS title
FROM object_state
WHERE pgcatalog_to_timestamptz(idx->>'modified')
      BETWEEN '2025-01-01'::timestamptz AND '2025-12-31'::timestamptz
ORDER BY pgcatalog_to_timestamptz(idx->>'modified') DESC;
```

### Security filtering

```sql
SELECT path, idx->>'Title' AS title
FROM object_state
WHERE idx @> '{"portal_type": "Document"}'::jsonb
  AND allowed_roles && ARRAY['Anonymous', 'Member'];
```

### Interface query (`object_provides`)

```sql
SELECT path, idx->>'Title' AS title
FROM object_state
WHERE object_provides && ARRAY['Products.CMFCore.interfaces._content.IContentish'];
```

### Keyword query (Subject)

```sql
SELECT path, idx->>'Title' AS title
FROM object_state
WHERE idx->'Subject' ?| ARRAY['Python', 'Plone'];
```

### Path query (children of a folder)

```sql
SELECT path, idx->>'Title' AS title
FROM object_state
WHERE idx->>'path' LIKE '/plone/folder/%'
  AND (idx->>'path_depth')::integer = 3;
```

### Aggregations

```sql
-- Count by portal_type
SELECT idx->>'portal_type' AS type, COUNT(*)
FROM object_state
WHERE path IS NOT NULL
GROUP BY idx->>'portal_type'
ORDER BY count DESC;

-- Distinct subjects
SELECT DISTINCT jsonb_array_elements_text(idx->'Subject') AS subject
FROM object_state
WHERE idx ? 'Subject'
ORDER BY subject;
```

## Performance tips

- Use `EXPLAIN ANALYZE` to check query plans.
- The `idx` column has a GIN index -- `@>` containment is fast.
- Expression B-tree indexes exist for common fields (`modified`, `created`, `portal_type`, `sortable_title`, `review_state`, `uid`).
- Prefer `idx @> '{"key:" "value"}'::jsonb` over `idx->>'key' = 'value'` (the former uses the GIN index).
- Path queries use expression B-tree indexes on `idx->>'path'` and `idx->>'path_parent'`.
- For text fields, `to_tsvector('simple', idx->>'Title') @@ plainto_tsquery('simple', 'term')` uses the GIN expression index.

## Important: read-only

Never modify catalog columns directly via SQL.
Catalog writes must go through the ZODB transaction lifecycle (Plone -> `catalog_object()` -> `CatalogStateProcessor` -> atomic commit).
Direct SQL updates will be overwritten on the next ZODB transaction that touches the same object.
