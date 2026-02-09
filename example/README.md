# plone.pgcatalog Example Setup

Try out **plone.pgcatalog** with a full Plone 6 site backed by
**zodb-pgjsonb** (PostgreSQL JSONB storage) and the PG catalog extension.

## Prerequisites

- **Docker** and **Docker Compose** (v2+)
- **Python 3.12+**
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Quick Start

### 1. Start PostgreSQL

```bash
cd example/
docker compose up -d
```

| Service    | Port | Purpose                     | Credentials                     |
|------------|------|-----------------------------|---------------------------------|
| PostgreSQL | 5433 | ZODB object storage (JSONB) | user=zodb password=zodb db=zodb |

### 2. Create a Python virtual environment

```bash
cd ..  # back to plone-pgcatalog root
uv venv -p 3.13
source .venv/bin/activate
uv pip install -r example/requirements.txt
```

This installs Plone, zodb-pgjsonb, and plone.pgcatalog as editable packages.

### 3. Generate a Zope instance

```bash
uvx cookiecutter -f --no-input --config-file /dev/null \
    gh:plone/cookiecutter-zope-instance \
    target=instance \
    wsgi_listen=0.0.0.0:8081 \
    initial_user_name=admin \
    initial_user_password=admin
```

### 4. Copy the example configuration

```bash
cp example/zope.conf instance/etc/zope.conf
cp example/zope.ini instance/etc/zope.ini
cp example/site.zcml instance/etc/site.zcml
mkdir -p instance/var/blobtemp
```

### 5. Start Zope

```bash
.venv/bin/runwsgi instance/etc/zope.ini
```

### 6. Create a Plone site

Open http://localhost:8081 in your browser and create a new Plone site
using the admin/admin credentials.

### 7. Install the pgcatalog profile

Go to **Site Setup > Add-ons** and install **plone.pgcatalog**.
This adds the catalog columns (`path`, `idx`, `searchable_text`) and
indexes to the `object_state` table.

## Exploring the Catalog Data

The power of plone.pgcatalog: your catalog indexes are stored as
**queryable JSONB** in PostgreSQL, directly on the `object_state` table.

### Connect with psql

```bash
psql -h localhost -p 5433 -U zodb -d zodb
```

Or start pgAdmin:

```bash
docker compose --profile tools up -d
```

Open http://localhost:5050 (login: admin@example.com / admin).

### Example SQL queries

**List portal types and counts (from catalog idx):**

```sql
SELECT idx->>'portal_type' AS type,
       count(*) AS count
FROM object_state
WHERE idx IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
```

**Full-text search using the tsvector column:**

```sql
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE searchable_text @@ plainto_tsquery('simple', 'your search term')
ORDER BY ts_rank(searchable_text, plainto_tsquery('simple', 'your search term')) DESC
LIMIT 20;
```

**Find all published documents:**

```sql
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE idx->>'portal_type' = 'Document'
  AND idx->>'review_state' = 'published'
ORDER BY path;
```

**Path queries (subtree):**

```sql
SELECT zoid, path, idx->>'portal_type' AS type
FROM object_state
WHERE path LIKE '/plone/news/%'
ORDER BY path;
```

**Security filter (what Anonymous can see):**

```sql
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE idx IS NOT NULL
  AND idx->'allowedRolesAndUsers' ?| ARRAY['Anonymous']
ORDER BY path;
```

**Find folderish content:**

```sql
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE (idx->>'is_folderish')::boolean = true
ORDER BY path;
```

**Check the catalog schema:**

```sql
\d object_state
```

You should see columns: `path`, `parent_path`, `path_depth`, `idx` (jsonb),
`searchable_text` (tsvector) alongside the base zodb-pgjsonb columns.

## Cleanup

Keep data for next time:

```bash
docker compose down
```

Remove all data (fresh start):

```bash
docker compose down -v
```
