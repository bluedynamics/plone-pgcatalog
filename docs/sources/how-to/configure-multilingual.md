<!-- diataxis: how-to -->

# Configure Multilingual Full-Text Search

## How Language Detection Works

plone.pgcatalog reads the `Language` field from each object during indexing. This field is set by `plone.app.multilingual` for multilingual sites, or can be set manually on content types.

The language code is mapped via `pgcatalog_lang_to_regconfig()` (a PL/pgSQL function) to select the correct PostgreSQL text search configuration (stemmer) at both index time and query time.

## Supported Languages

30 languages are supported out of the box:

| Language | ISO Code | PG Config | BM25 Tokenizer |
|---|---|---|---|
| Arabic | `ar` | `arabic` | Snowball |
| Armenian | `hy` | `armenian` | Snowball |
| Basque | `eu` | `basque` | Snowball |
| Catalan | `ca` | `catalan` | Snowball |
| Danish | `da` | `danish` | Snowball |
| Dutch | `nl` | `dutch` | Snowball |
| English | `en` | `english` | Snowball (Porter2) |
| Estonian | `et` | `estonian` | Snowball |
| Finnish | `fi` | `finnish` | Snowball |
| French | `fr` | `french` | Snowball |
| German | `de` | `german` | Snowball |
| Greek | `el` | `greek` | Snowball |
| Hindi | `hi` | `hindi` | Snowball |
| Hungarian | `hu` | `hungarian` | Snowball |
| Indonesian | `id` | `indonesian` | Snowball |
| Irish | `ga` | `irish` | Snowball |
| Italian | `it` | `italian` | Snowball |
| Lithuanian | `lt` | `lithuanian` | Snowball |
| Nepali | `ne` | `nepali` | Snowball |
| Norwegian | `nb`/`nn`/`no` | `norwegian` | Snowball |
| Portuguese | `pt` | `portuguese` | Snowball |
| Romanian | `ro` | `romanian` | Snowball |
| Russian | `ru` | `russian` | Snowball |
| Serbian | `sr` | `serbian` | Snowball |
| Spanish | `es` | `spanish` | Snowball |
| Swedish | `sv` | `swedish` | Snowball |
| Tamil | `ta` | `tamil` | Snowball |
| Turkish | `tr` | `turkish` | Snowball |
| Yiddish | `yi` | `yiddish` | Snowball |
| Chinese | `zh` | `simple` | jieba |
| Japanese | `ja` | `simple` | lindera |
| Korean | `ko` | `simple` | lindera |

Objects with unmapped or empty `Language` fall back to `'simple'` config (no stemming, no stop words).

## Tsvector (Default, No Configuration)

Language-aware stemming works automatically for the `searchable_text` column. No configuration is needed beyond installing `plone.app.multilingual`. The tsvector is built with per-object language detection:

- **Title** and **Description** use `'simple'` config (no stemming, weight A/B)
- **SearchableText** body uses the object's `Language` field mapped to the appropriate regconfig (weight D)

## BM25 Per-Language Columns

To enable per-language BM25 ranking:

```bash
# Explicit list
export PGCATALOG_BM25_LANGUAGES=en,de,fr,zh

# Auto-detect from site languages
export PGCATALOG_BM25_LANGUAGES=auto
```

Requires VectorChord-BM25 extensions. See {doc}`enable-bm25`.

Each configured language gets a dedicated `search_bm25_{lang}` column. At query time, the search language determines which column is used for BM25 scoring. The fallback `search_bm25` column handles unconfigured languages and cross-language search.

## CJK Languages

Chinese, Japanese, and Korean use specialized segmenters (jieba/lindera) instead of Snowball stemmers. These are provided by `pg_tokenizer` and work automatically with `BM25Backend`.

For tsvector (without BM25), CJK languages fall back to `'simple'` config, which provides basic whitespace tokenization. For better CJK search quality, enable BM25.
