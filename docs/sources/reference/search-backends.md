<!-- diataxis: reference -->

# Search Backends Reference

This page documents the search backend abstraction, the two built-in
backends (TsvectorBackend and BM25Backend), language-to-tokenizer
mapping, and backend detection.

## SearchBackend ABC

Defined in `plone.pgcatalog.backends`. All search backends inherit
from this abstract base class.

| Method | Returns | Purpose |
|---|---|---|
| `get_extra_columns()` | `list[ExtraColumn]` | Additional columns for DDL |
| `get_schema_sql()` | `str` | DDL for backend-specific schema |
| `process_search_data(pending)` | `dict` | Column values for indexing |
| `build_search_clause(query_val, lang_val, pname_func)` | `tuple[str, dict, str \| None]` | WHERE clause, params, rank expression |
| `uncatalog_extra()` | `dict` | Column values to NULL on uncatalog |
| `install_schema(conn)` | `None` | Execute schema DDL statement-by-statement |
| `detect(dsn)` | `bool` (classmethod) | Check if backend is available |

**Properties:**

| Property | Type | Description |
|---|---|---|
| `rank_ascending` | `bool` | `False` for tsvector (higher score = more relevant), `True` for BM25 (lower score = more relevant) |

## TsvectorBackend

Always available. Uses PostgreSQL's built-in full-text search.

**Ranking:** `ts_rank_cd()` with weighted tsvector.

**Weighted tsvector construction:**

| Weight | Field | Priority |
|---|---|---|
| A (1.0) | Title | Highest |
| B (0.4) | Description | Medium |
| D (0.1) | SearchableText body | Lowest |

Ranking weights array: `{0.1, 0.2, 0.4, 1.0}` (D, C, B, A).

**Extra columns:** `searchable_text` (TSVECTOR).

**Schema DDL:** None (tsvector is part of core PostgreSQL).

**Detection:** Always returns `True`.

## BM25Backend

Optional. Requires VectorChord-BM25 (`vchord_bm25`) and pg_tokenizer
(`pg_tokenizer`) PostgreSQL extensions.

**Ranking:** BM25 scoring via the `<&>` operator with `to_bm25query()`.

**Pre-filter:** The tsvector GIN index eliminates non-matching rows
before BM25 scoring is applied. Both the tsvector WHERE clause and
the BM25 rank expression are generated together.

**Title boost:** Title text is repeated 3x in the tokenized input for
field boosting.

**Extra columns:**

- `searchable_text` (TSVECTOR) -- shared with TsvectorBackend for GIN
  pre-filtering.
- `search_bm25` (BM25VECTOR) -- fallback column for unmapped languages
  (no stemmer).
- `search_bm25_{lang}` (BM25VECTOR) -- per-language column with
  language-specific tokenizer. One column per configured language.

**Schema DDL:** Creates extensions, columns, tokenizers, and BM25
indexes. Tokenizer creation is wrapped in `DO $$ ... EXCEPTION ...$$`
for idempotent execution. The `install_schema()` method executes each
DDL statement individually (multi-statement strings fail silently in
transactional connections).

**Detection:** Checks `pg_available_extensions` for both `vchord_bm25`
and `pg_tokenizer`. Uses `pg_available_extensions` (not `pg_extension`)
so detection works before `CREATE EXTENSION` has been executed.

**Constructor:**

```python
BM25Backend(languages=None, tokenizer_prefix="pgcatalog")
```

- `languages`: list of ISO 639-1 codes. Defaults to `["en"]`. Each
  code is validated against `LANG_TOKENIZER_MAP`.
- `tokenizer_prefix`: prefix for pg_tokenizer names. Defaults to
  `"pgcatalog"`.

## LANG_TOKENIZER_MAP

Maps ISO 639-1 language codes to pg_tokenizer configuration. Defined in
`plone.pgcatalog.backends`.

### Snowball Stemmers

| ISO Code | Stemmer |
|---|---|
| `ar` | `arabic` |
| `hy` | `armenian` |
| `eu` | `basque` |
| `ca` | `catalan` |
| `da` | `danish` |
| `nl` | `dutch` |
| `en` | `english_porter2` |
| `et` | `estonian` |
| `fi` | `finnish` |
| `fr` | `french` |
| `de` | `german` |
| `el` | `greek` |
| `hi` | `hindi` |
| `hu` | `hungarian` |
| `id` | `indonesian` |
| `ga` | `irish` |
| `it` | `italian` |
| `lt` | `lithuanian` |
| `ne` | `nepali` |
| `nb` | `norwegian` |
| `nn` | `norwegian` |
| `no` | `norwegian` |
| `pt` | `portuguese` |
| `ro` | `romanian` |
| `ru` | `russian` |
| `sr` | `serbian` |
| `es` | `spanish` |
| `sv` | `swedish` |
| `ta` | `tamil` |
| `tr` | `turkish` |
| `yi` | `yiddish` |

### CJK Segmenters

| ISO Code | Pre-tokenizer |
|---|---|
| `zh` | `jieba` (Chinese segmenter) |
| `ja` | `lindera` (Japanese segmenter) |
| `ko` | `lindera` (Korean segmenter) |

Languages not in this map are served by the fallback column
(`search_bm25`) which uses `unicode_segmentation` without stemming.

## Backend Detection and Activation

### Automatic (at startup)

```python
from plone.pgcatalog.backends import detect_and_set_backend

# Called automatically by register_catalog_processor():
detect_and_set_backend(dsn, languages=["en", "de"])
```

Detection checks `pg_available_extensions` for `vchord_bm25` and
`pg_tokenizer`. If both are present, `BM25Backend` is activated with
the configured languages. Otherwise, `TsvectorBackend` is activated.

Language configuration is read from the `PGCATALOG_BM25_LANGUAGES`
environment variable (see {doc}`configuration`).

### Runtime access

```python
from plone.pgcatalog.backends import get_backend

backend = get_backend()  # Returns the active SearchBackend instance
```

Returns the active backend singleton. Defaults to `TsvectorBackend`
if `detect_and_set_backend()` has not been called.

### Manual override (testing)

```python
from plone.pgcatalog.backends import set_backend, reset_backend

set_backend(my_backend)   # Set a custom backend
reset_backend()           # Reset to default (TsvectorBackend)
```
