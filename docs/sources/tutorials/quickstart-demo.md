<!-- diataxis: tutorial -->

# Quickstart: Run plone.pgcatalog in 5 Minutes

## What You Will Build

In this tutorial you will run a multilingual Plone 6 site backed by PostgreSQL,
import ~800 Wikipedia articles in English, German, and Chinese, and explore
full-text search with language-aware stemming.

By the end you will have a live Plone instance whose entire catalog lives in
PostgreSQL JSONB -- queryable from `psql`, the REST API, or the Plone search box.

## Prerequisites

- Docker and Docker Compose v2+
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Step 1: Clone the Repository

```bash
git clone https://github.com/bluedynamics/plone-pgcatalog.git
cd plone-pgcatalog/example
```

All remaining commands assume you are inside the `example/` directory.

## Step 2: Start PostgreSQL

Choose one of two variants.  Both expose PostgreSQL on **port 5433**.

### Standard variant (tsvector ranking)

```bash
docker compose up -d
```

### BM25 variant (recommended for better search quality)

```bash
PG_IMAGE=tensorchord/vchord-suite:pg17-latest docker compose up -d
```

The BM25 image ships the `pg_tokenizer` and `vchord_bm25` extensions.
plone.pgcatalog auto-detects them at startup -- no configuration changes needed.

:::{tip}
You can switch variants later by running `docker compose down -v` and starting
again with the other image.  A full catalog reindex is required after switching.
:::

## Step 3: Install Python Dependencies

```bash
uv venv -p 3.13
source .venv/bin/activate
uv pip install -r requirements.txt
```

This installs Plone 6, plone.pgcatalog, zodb-pgjsonb, and the example
distribution in one step.  The `constraints.txt` file pins known-good versions.

## Step 4: Generate a Zope Instance

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

The `zope.conf` shipped in the example directory configures zodb-pgjsonb
(connecting to PostgreSQL on port 5433) and sets the environment variable
`PGCATALOG_BM25_LANGUAGES=en,de,zh` for the BM25 variant.

## Step 5: Create the Site and Import Content

```bash
.venv/bin/zconsole run instance/etc/zope.conf scripts/create_site.py
```

This single command:

1. Creates a Plone Classic UI site (`/Plone`)
2. Installs the **plone.pgcatalog** add-on (catalog columns and indexes)
3. Installs **plone.app.multilingual** with EN, DE, and ZH language folders
4. Imports ~800 Wikipedia geography articles as published Documents
5. Links translations via PAM's `ITranslationManager`

The import takes about one to two minutes depending on your hardware.

## Step 6: Start Zope

```bash
.venv/bin/runwsgi instance/etc/zope.ini
```

Visit <http://localhost:8081/Plone> in your browser.  Log in with
**admin / admin**.

## Step 7: Explore Search

### Search in the Browser

Type "volcano" into the Plone search box.  You should see English articles
about volcanoes, ranked by relevance.

Try different languages:

- **English:** "volcano", "Mount Everest", "Amazon River"
- **German:** "Vulkan", "Amazonas", "Mount Everest"
- **Chinese:** "火山", "亚马逊河", "珠穆朗玛峰"

### Search via the REST API

```bash
# English search
curl -s "http://localhost:8081/Plone/@search?SearchableText=volcano&sort_limit=5" \
  -H "Accept: application/json" -u admin:admin | python -m json.tool

# German search -- within the German language root
curl -s "http://localhost:8081/Plone/de/@search?SearchableText=Vulkan&sort_limit=5" \
  -H "Accept: application/json" -u admin:admin | python -m json.tool
```

Notice that the German search for "Vulkan" also matches articles containing
"Vulkane" or "Vulkans" -- PostgreSQL's German stemmer reduces them to the same
root.

### Explore with psql

Connect directly to PostgreSQL:

```bash
psql -h localhost -p 5433 -U zodb -d zodb
```

```sql
-- Count articles by language
SELECT idx->>'Language' AS lang, COUNT(*)
FROM object_state
WHERE idx IS NOT NULL AND idx->>'portal_type' = 'Document'
GROUP BY idx->>'Language';

-- Full-text search with ranking (tsvector)
SELECT path, idx->>'Title' AS title,
       ts_rank_cd(searchable_text, q) AS rank
FROM object_state, plainto_tsquery('english', 'volcano') q
WHERE searchable_text @@ q
ORDER BY rank DESC
LIMIT 5;

-- Published documents only
SELECT path, idx->>'Title' AS title, idx->>'Language' AS lang
FROM object_state
WHERE idx->>'portal_type' = 'Document'
  AND idx->>'review_state' = 'published'
ORDER BY path
LIMIT 20;
```

## Step 8: Clean Up

```bash
docker compose down -v
```

This removes the PostgreSQL container and its data volume.  Omit `-v` if you
want to keep the data for next time.

## What You Learned

- plone.pgcatalog stores all catalog data in PostgreSQL JSONB columns
- Full-text search with language-aware stemming works out of the box
- Catalog data is queryable from any PostgreSQL client (psql, pgAdmin, your
  application code)
- The standard Plone search API and REST API work unchanged

## Next Steps

- {doc}`migrate-from-zcatalog` to migrate an existing site from ZCatalog
- {doc}`multilingual-search` to understand language-aware search in depth
