# plone.pgcatalog Example Setup

Try out **plone.pgcatalog** with a full Plone 6 site backed by
**zodb-pgjsonb** (PostgreSQL JSONB storage) and ~800 Wikipedia articles
to search through.

## Prerequisites

- **Docker** and **Docker Compose** (v2+)
- **Python 3.12+**
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Quick Start

```bash
cd example/
```

### 1. Start PostgreSQL

**Standard (tsvector ranking):**

```bash
docker compose up -d
```

**With BM25 ranking (recommended):**

```bash
PG_IMAGE=tensorchord/vchord-suite:pg17-latest docker compose up -d
```

| Variant    | Image                                    | Search ranking               |
|------------|------------------------------------------|------------------------------|
| Standard   | `postgres:17`                            | `ts_rank_cd` (tsvector)      |
| BM25       | `tensorchord/vchord-suite:pg17-latest`   | BM25 (IDF + term saturation) |

Both expose PostgreSQL on **port 5433**. plone.pgcatalog auto-detects
the BM25 extensions at startup -- no configuration changes needed.

> **Switching later:** `docker compose down -v`, then restart with
> the other image. A full catalog reindex is needed after switching.

### 2. Install dependencies

```bash
uv venv -p 3.13
source .venv/bin/activate
uv pip install -r requirements.txt
```

### 3. Generate a Zope instance

```bash
uvx cookiecutter -f --no-input --config-file /dev/null \
    gh:plone/cookiecutter-zope-instance \
    target=instance \
    wsgi_listen=0.0.0.0:8081 \
    initial_user_name=admin \
    initial_user_password=admin
cp zope.conf instance/etc/zope.conf
cp zope.ini instance/etc/zope.ini
cp site.zcml instance/etc/site.zcml
mkdir -p instance/var/blobtemp
```

### 4. Create the site and import content

```bash
zconsole run instance/etc/zope.conf create_site.py
```

This single command creates a Plone Classic UI site, installs
plone.pgcatalog (catalog columns + indexes), and imports ~800
Wikipedia geography articles as published Documents (CC BY-SA 4.0).

### 5. Start Zope

```bash
runwsgi instance/etc/zope.ini
```

Open http://localhost:8081/Plone/ and log in with **admin / admin**.

Try searching for "volcano", "Mount Everest", or "Amazon River" in the
Plone search bar, or via the REST API:

```bash
curl -s -H "Accept: application/json" \
  "http://localhost:8081/Plone/@search?SearchableText=volcano&sort_limit=5" \
  | python -m json.tool
```

## Exploring the Data in PostgreSQL

The catalog indexes are stored as **queryable JSONB** directly on the
`object_state` table.

### Connect with psql

```bash
psql -h localhost -p 5433 -U zodb -d zodb
```

Or start pgAdmin:

```bash
docker compose --profile tools up -d
# Open http://localhost:5050 (login: admin@example.com / admin)
```

### Example SQL queries

```sql
-- Portal types and counts
SELECT idx->>'portal_type' AS type, count(*)
FROM object_state WHERE idx IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

-- Full-text search (tsvector, works with both variants)
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE searchable_text @@ plainto_tsquery('simple', 'volcano')
ORDER BY ts_rank(searchable_text, plainto_tsquery('simple', 'volcano')) DESC
LIMIT 10;

-- BM25 ranking (BM25 variant only)
SELECT zoid, path, idx->>'Title' AS title,
       search_bm25 <&> to_bm25query('idx_os_search_bm25',
           tokenize('volcano', 'pgcatalog_default')) AS score
FROM object_state
WHERE searchable_text @@ plainto_tsquery('simple', 'volcano')
ORDER BY score ASC
LIMIT 10;

-- Published documents
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE idx->>'portal_type' = 'Document'
  AND idx->>'review_state' = 'published'
ORDER BY path;

-- Subtree query
SELECT zoid, path, idx->>'portal_type' AS type
FROM object_state
WHERE path LIKE '/plone/library/%'
ORDER BY path;

-- Security filter (what Anonymous can see)
SELECT zoid, path, idx->>'Title' AS title
FROM object_state
WHERE idx->'allowedRolesAndUsers' ?| ARRAY['Anonymous']
ORDER BY path;
```

## Cleanup

```bash
docker compose down      # keep data
docker compose down -v   # remove all data (fresh start)
```
