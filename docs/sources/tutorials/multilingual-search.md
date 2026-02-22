<!-- diataxis: tutorial -->

# Tutorial: Set Up Multilingual Search

## What You Will Build

A Plone site with English, German, and Chinese content, where search results
are ranked using language-specific stemming.  German "Vulkan" will match
"Vulkane", English "running" will match "run", and Chinese text is properly
segmented.

By the end of this tutorial you will understand how plone.pgcatalog uses
per-object language information to choose the right stemmer, and how to verify
that stemming is working correctly.

## Prerequisites

- A working plone.pgcatalog installation
- plone.app.multilingual installed and configured
- Content in multiple languages

:::{tip}
The {doc}`quickstart-demo` tutorial sets up a multilingual site with ~800
example articles in three languages.  Follow that first if you do not have a
multilingual site ready.
:::

## Step 1: Verify Language Configuration

In Plone's **Site Setup** > **Language**, ensure your desired languages are
enabled.  plone.pgcatalog reads the `Language` field from each content object
at index time.

You can verify the configured languages from a zconsole shell:

```python
from plone.i18n.interfaces import ILanguageSchema
from plone.registry.interfaces import IRegistry
from zope.component import getUtility

registry = getUtility(IRegistry)
settings = registry.forInterface(ILanguageSchema, prefix="plone")
print(f"Available: {settings.available_languages}")
print(f"Default:   {settings.default_language}")
```

Every content object carries a `Language` field.  When plone.pgcatalog indexes
an object, it uses this field to select the PostgreSQL text search
configuration for stemming.

## Step 2: Understand How Stemming Works

plone.pgcatalog maps each object's `Language` field to a PostgreSQL text search
configuration via the `pgcatalog_lang_to_regconfig()` SQL function.  Here are
some common mappings:

| Language | ISO Code | PG Configuration | What It Does |
|---|---|---|---|
| English | `en` | `english` | "running" -> "run", removes "the", "is" |
| German | `de` | `german` | "Vulkane" -> "vulkan", removes "der", "die" |
| French | `fr` | `french` | "coureurs" -> "coureur", removes "le", "la" |
| Spanish | `es` | `spanish` | "corriendo" -> "corr", removes "el", "la" |
| Chinese | `zh` | `simple` | Basic tokenization (BM25 adds jieba segmentation) |

PostgreSQL ships with built-in support for about 30 languages.  The `simple`
configuration performs basic whitespace tokenization without stemming -- used as
a fallback for languages without a dedicated stemmer.

This means a German search for "Vulkan" will find articles containing
"Vulkane", "Vulkans", or "vulkanisch" -- the stemmer reduces them all to the
same root form.

## Step 3: Create Test Content

If you followed the {doc}`quickstart-demo` tutorial, you already have
multilingual content.  Otherwise, create a few test documents to see stemming
in action.

In each language folder, create a Document with content that includes different
word forms:

- **English** (`/Plone/en/`): Create a Document titled "Volcanic Activity" with
  body text mentioning "volcano", "volcanoes", "volcanic", and "volcanism".
- **German** (`/Plone/de/`): Create a Document titled "Vulkanische Aktivitaet"
  with body text mentioning "Vulkan", "Vulkane", "Vulkans", and "vulkanisch".

Publish both documents so they appear in search results.

## Step 4: Test Language-Aware Search

### Via the REST API

```bash
# English: "volcano" matches "volcanoes", "volcanic", "volcanism"
curl -s "http://localhost:8081/Plone/@search?SearchableText=volcano&sort_limit=5" \
  -H "Accept: application/json" -u admin:admin | python -m json.tool

# German: "Vulkan" matches "Vulkane", "Vulkans", "vulkanisch"
curl -s "http://localhost:8081/Plone/de/@search?SearchableText=Vulkan&sort_limit=5" \
  -H "Accept: application/json" -u admin:admin | python -m json.tool
```

When searching within a language folder (e.g., `/Plone/de/`), Plone
automatically restricts results to that path.  The `Language` index is also
available as an explicit query parameter.

### Via Python

```python
from plone import api

catalog = api.portal.get_tool("portal_catalog")

# Search across all languages
results = catalog(SearchableText="volcano")
print(f"All languages: {len(results)} results")

# Search only German content
results = catalog(SearchableText="Vulkan", Language="de")
print(f"German only: {len(results)} results")

# Verify stemming -- singular and plural should return the same results
singular = catalog(SearchableText="volcano", Language="en")
plural = catalog(SearchableText="volcanoes", Language="en")
print(f"'volcano': {len(singular)}, 'volcanoes': {len(plural)}")
```

Both "volcano" and "volcanoes" should return the same result set because the
English stemmer reduces both to the same root.

## Step 5: Enable BM25 for Better CJK Search (Optional)

PostgreSQL's `simple` text search configuration provides basic whitespace
tokenization for Chinese, Japanese, and Korean.  This works for queries where
the user types exact character sequences, but it cannot segment continuous text
into words.

For proper word segmentation, enable BM25 by setting an environment variable
in your `zope.conf`:

```xml
<environment>
    PGCATALOG_BM25_LANGUAGES en,de,zh
</environment>
```

This tells plone.pgcatalog to create per-language BM25 columns with
specialized tokenizers:

| Language | BM25 Column | Tokenizer | Segmenter |
|---|---|---|---|
| English | `search_bm25_en` | `pgcatalog_en` | Porter2 stemmer |
| German | `search_bm25_de` | `pgcatalog_de` | German Snowball stemmer |
| Chinese | `search_bm25_zh` | `pgcatalog_zh` | jieba segmentation |

After changing the configuration, restart Zope and rebuild the catalog:

```python
import transaction
from plone import api

catalog = api.portal.get_tool("portal_catalog")
catalog.clearFindAndRebuild()
transaction.commit()
```

:::{note}
BM25 requires the `tensorchord/vchord-suite:pg17-latest` Docker image (or
equivalent PostgreSQL installation with `pg_tokenizer` and `vchord_bm25`
extensions).  plone.pgcatalog auto-detects these extensions at startup and
falls back to tsvector ranking when they are not available.
:::

## Step 6: Verify with SQL

Connect to PostgreSQL and inspect the indexed data directly.

```bash
psql -h localhost -p 5433 -U zodb -d zodb
```

```sql
-- Check language distribution
SELECT idx->>'Language' AS lang, COUNT(*)
FROM object_state
WHERE idx IS NOT NULL AND idx->>'portal_type' = 'Document'
GROUP BY idx->>'Language'
ORDER BY COUNT(*) DESC;

-- German stemming in action -- all forms of "Vulkan" match
SELECT path, idx->>'Title' AS title
FROM object_state, plainto_tsquery('german', 'Vulkan') q
WHERE searchable_text @@ q
LIMIT 5;

-- English stemming -- "volcano" and "volcanoes" produce the same query
SELECT plainto_tsquery('english', 'volcano');
SELECT plainto_tsquery('english', 'volcanoes');
-- Both return 'volcano':* (the stemmed form)

-- Compare with 'simple' (no stemming)
SELECT plainto_tsquery('simple', 'volcano');
SELECT plainto_tsquery('simple', 'volcanoes');
-- Returns 'volcano' and 'volcanoes' respectively (no stemming)
```

## What You Learned

- plone.pgcatalog uses each object's `Language` field to select a
  PostgreSQL text search configuration at index time
- About 30 languages are supported out of the box via PostgreSQL's built-in
  stemmers
- Stemming reduces inflected forms to a common root, so searches match
  regardless of grammatical form
- CJK languages benefit from BM25 with specialized word segmenters (jieba
  for Chinese)
- Language filtering works via the standard `Language` query parameter or
  by searching within a language folder path

## Next Steps

- {doc}`quickstart-demo` to try a full multilingual setup with example content
- {doc}`migrate-from-zcatalog` to migrate an existing site
