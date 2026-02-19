# plone.pgcatalog Multilingual Example

Try out **plone.pgcatalog** with a multilingual Plone 6 site backed by
**zodb-pgjsonb** (PostgreSQL JSONB storage) and ~800+ Wikipedia articles
in **English, German, and Chinese** to search through.

Uses **plone.app.multilingual** (PAM) for language folders and translation
linking.  With the BM25 variant, each language gets its own BM25 tokenizer
with language-specific stemming (English Porter2, German Snowball, Chinese
jieba segmentation).

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
| BM25       | `tensorchord/vchord-suite:pg17-latest`   | BM25 per-language (IDF + stemming) |

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
zconsole run instance/etc/zope.conf scripts/create_site.py
```

This single command:

1. Creates a Plone Classic UI site (`/Plone`)
2. Installs **plone.app.multilingual** with EN, DE, ZH language folders
3. Installs **plone.pgcatalog** (catalog columns + indexes)
4. Imports ~800+ Wikipedia geography articles across all three languages
5. Links translations via PAM's `ITranslationManager`

### 5. Start Zope

```bash
runwsgi instance/etc/zope.ini
```

Open http://localhost:8081/Plone/ and log in with **admin / admin**.

Try searching in different languages:

- **English:** "volcano", "Mount Everest", "Amazon River"
- **German:** "Vulkan", "Amazonas", "Mount Everest"
- **Chinese:** "火山", "亚马逊河", "珠穆朗玛峰"

Or via the REST API:

```bash
# English search
curl -s -H "Accept: application/json" \
  "http://localhost:8081/Plone/@search?SearchableText=volcano&sort_limit=5" \
  | python -m json.tool

# German search (within German language root)
curl -s -H "Accept: application/json" \
  "http://localhost:8081/Plone/de/@search?SearchableText=Vulkan&sort_limit=5" \
  | python -m json.tool
```

## BM25 Language Configuration

The `zope.conf` includes:

```
<environment>
    PGCATALOG_BM25_LANGUAGES en,de,zh
</environment>
```

This tells plone.pgcatalog to create per-language BM25 columns with
language-specific tokenizers:

| Language | Column           | Tokenizer          | Stemmer/Segmenter |
|----------|------------------|--------------------|-------------------|
| English  | `search_bm25_en` | `pgcatalog_en`    | Porter2           |
| German   | `search_bm25_de` | `pgcatalog_de`    | German Snowball   |
| Chinese  | `search_bm25_zh` | `pgcatalog_zh`    | jieba             |
| Fallback | `search_bm25`    | `pgcatalog_default` | (none)          |

Other options:

- **`auto`** — auto-detect from `portal_languages` at startup
- **Omit** — defaults to `en` only (single-language mode)

## Exploring the Data in PostgreSQL

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
-- Articles by language
SELECT idx->>'Language' AS lang, count(*)
FROM object_state WHERE idx IS NOT NULL AND idx->>'portal_type' = 'Document'
GROUP BY 1 ORDER BY 2 DESC;

-- Full-text search (tsvector, works with both variants)
SELECT zoid, path, idx->>'Title' AS title, idx->>'Language' AS lang
FROM object_state
WHERE searchable_text @@ plainto_tsquery('simple', 'volcano')
ORDER BY ts_rank(searchable_text, plainto_tsquery('simple', 'volcano')) DESC
LIMIT 10;

-- BM25 ranking — English (BM25 variant only)
SELECT zoid, path, idx->>'Title' AS title,
       search_bm25_en <&> to_bm25query('idx_os_search_bm25_en',
           tokenize('volcano', 'pgcatalog_en')) AS score
FROM object_state
WHERE searchable_text @@ plainto_tsquery('english', 'volcano')
  AND search_bm25_en IS NOT NULL
ORDER BY score ASC
LIMIT 10;

-- BM25 ranking — German
SELECT zoid, path, idx->>'Title' AS title,
       search_bm25_de <&> to_bm25query('idx_os_search_bm25_de',
           tokenize('Vulkan', 'pgcatalog_de')) AS score
FROM object_state
WHERE searchable_text @@ plainto_tsquery('german', 'Vulkan')
  AND search_bm25_de IS NOT NULL
ORDER BY score ASC
LIMIT 10;

-- BM25 ranking — Chinese
SELECT zoid, path, idx->>'Title' AS title,
       search_bm25_zh <&> to_bm25query('idx_os_search_bm25_zh',
           tokenize('火山', 'pgcatalog_zh')) AS score
FROM object_state
WHERE searchable_text @@ plainto_tsquery('simple', '火山')
  AND search_bm25_zh IS NOT NULL
ORDER BY score ASC
LIMIT 10;

-- Translation groups (articles that are translations of each other)
SELECT e.path AS en_path, e.idx->>'Title' AS en_title,
       d.path AS de_path, d.idx->>'Title' AS de_title
FROM object_state e
JOIN object_state d ON d.idx->>'Language' = 'de'
  AND d.path LIKE '/plone/de/library/%'
WHERE e.idx->>'Language' = 'en'
  AND e.path LIKE '/plone/en/library/%'
LIMIT 10;

-- Published documents
SELECT zoid, path, idx->>'Title' AS title, idx->>'Language' AS lang
FROM object_state
WHERE idx->>'portal_type' = 'Document'
  AND idx->>'review_state' = 'published'
ORDER BY path
LIMIT 20;

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
