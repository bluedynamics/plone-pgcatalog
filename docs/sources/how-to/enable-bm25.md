<!-- diataxis: how-to -->

# Enable BM25 Ranking

## Overview

BM25 provides probabilistic relevance ranking, improving search quality over tsvector's `ts_rank_cd()`. It is auto-detected at startup -- no code changes are needed. When BM25 extensions are available, plone.pgcatalog switches from `TsvectorBackend` to `BM25Backend` automatically.

## Step 1: Install VectorChord-BM25

### Docker (recommended)

```bash
docker run -d --name plone-pg-bm25 \
  -e POSTGRES_USER=zodb \
  -e POSTGRES_PASSWORD=zodb \
  -e POSTGRES_DB=zodb \
  -p 5432:5432 \
  tensorchord/vchord-suite:pg17-latest
```

### Manual Installation

Install the `vchord_bm25` and `pg_tokenizer` PostgreSQL extensions.
Both must appear in `pg_available_extensions` for auto-detection to succeed.
See the [VectorChord-BM25 documentation](https://github.com/tensorchord/VectorChord-bm25) for build instructions.

## Step 2: Configure Languages

Set the `PGCATALOG_BM25_LANGUAGES` environment variable before starting Zope:

```bash
# Explicit language list
export PGCATALOG_BM25_LANGUAGES=en,de,fr

# Auto-detect from Plone's portal_languages
export PGCATALOG_BM25_LANGUAGES=auto

# Single language (default if not set)
export PGCATALOG_BM25_LANGUAGES=en
```

Each language gets a dedicated `search_bm25_{lang}` column with a language-specific tokenizer (Snowball stemmer for Western languages, jieba/lindera for CJK). A fallback `search_bm25` column is always created for unmapped languages and cross-language search.

See {doc}`../reference/search-backends` for the full `LANG_TOKENIZER_MAP`.

## Step 3: Restart Zope

On restart, plone.pgcatalog auto-detects the extensions and:

1. Creates per-language `search_bm25_{lang}` columns on `object_state`
2. Sets up tokenizers via `pg_tokenizer` (`create_tokenizer()`)
3. Creates BM25 indexes for each column
4. Switches the active backend from `TsvectorBackend` to `BM25Backend`

Check the log for:

```
BM25 search backend activated (languages=['en', 'de', 'fr'])
```

## Step 4: Rebuild the Catalog

A full reindex is required to populate the new BM25 columns:

1. Go to ZMI > portal_catalog > Advanced tab
2. Click "Clear and Rebuild"

Or via script:

```python
catalog = portal.portal_catalog
catalog.clearFindAndRebuild()
import transaction; transaction.commit()
```

## Switching Back to Tsvector

Remove the VectorChord-BM25 extensions (or switch to a standard `postgres:17` image) and restart Zope.
plone.pgcatalog automatically falls back to `TsvectorBackend` when the extensions are not detected in `pg_available_extensions`.

The existing tsvector `searchable_text` column is always maintained regardless of which backend is active, so no rebuild is needed when switching back.
