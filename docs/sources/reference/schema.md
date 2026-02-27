<!-- diataxis: reference -->

# Database Schema Reference

This page documents the PostgreSQL schema extensions, indexes, JSONB
structure, and SQL functions installed by plone.pgcatalog.

## object_state Table Extensions

plone.pgcatalog extends the `object_state` table (owned by zodb-pgjsonb)
with the following columns:

| Column | Type | Purpose |
|---|---|---|
| `path` | `TEXT` | Physical object path (e.g., `/plone/folder/doc`) |
| `parent_path` | `TEXT` | Parent path (for path queries) |
| `path_depth` | `INTEGER` | Path depth (for depth-limited queries) |
| `idx` | `JSONB` | All index and metadata values |
| `searchable_text` | `TSVECTOR` | Weighted full-text search vector |
| `search_bm25` | `BM25VECTOR` | BM25 fallback column (when BM25 active) |
| `search_bm25_{lang}` | `BM25VECTOR` | Per-language BM25 column (when BM25 active) |

The first five columns are always present. The `search_bm25` and
per-language `search_bm25_{lang}` columns are created only when
VectorChord-BM25 extensions are detected at startup.

## PostgreSQL Indexes

### Core Indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_path` | B-tree | `path` | Path column lookup (brain construction) |
| `idx_os_catalog` | GIN | `idx` | JSONB containment/existence queries |
| `idx_os_searchable_text` | GIN | `searchable_text` | Full-text search |

### Path Expression Indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_path` | B-tree | `idx->>'path'` | Path equality queries |
| `idx_os_cat_path_pattern` | B-tree (`text_pattern_ops`) | `idx->>'path'` | Path LIKE prefix queries |
| `idx_os_cat_path_parent` | B-tree | `idx->>'path_parent'` | Parent path queries (depth=1) |
| `idx_os_cat_path_depth` | B-tree | `(idx->>'path_depth')::integer` | Depth-limited queries |

### Date Expression Indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_modified` | B-tree | `pgcatalog_to_timestamptz(idx->>'modified')` | Date sorting/filtering |
| `idx_os_cat_created` | B-tree | `pgcatalog_to_timestamptz(idx->>'created')` | Date sorting/filtering |
| `idx_os_cat_effective` | B-tree | `pgcatalog_to_timestamptz(idx->>'effective')` | Date sorting/filtering |
| `idx_os_cat_expires` | B-tree | `pgcatalog_to_timestamptz(idx->>'expires')` | Date sorting/filtering |

### Field Expression Indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_sortable_title` | B-tree | `idx->>'sortable_title'` | Title sorting |
| `idx_os_cat_portal_type` | B-tree | `idx->>'portal_type'` | Type filtering |
| `idx_os_cat_review_state` | B-tree | `idx->>'review_state'` | Workflow state filtering |
| `idx_os_cat_uid` | B-tree | `idx->>'uid'` | UUID lookup |

### Text Expression Indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_title_tsv` | GIN | `to_tsvector('simple', COALESCE(idx->>'Title', ''))` | Title word-level matching |
| `idx_os_cat_description_tsv` | GIN | `to_tsvector('simple', COALESCE(idx->>'Description', ''))` | Description word-level matching |

Additional GIN expression indexes are auto-created at startup for any
addon `ZCTextIndex` fields discovered in `portal_catalog`. These follow
the naming pattern `idx_os_cat_{key}_tsv` and use the `'simple'`
regconfig for word-level matching.

### BM25 Indexes (Optional)

When VectorChord-BM25 extensions are installed:

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_search_bm25` | BM25 | `search_bm25 bm25_ops` | Fallback BM25 ranking |
| `idx_os_search_bm25_{lang}` | BM25 | `search_bm25_{lang} bm25_ops` | Per-language BM25 ranking |

All indexes use `WHERE idx IS NOT NULL` or `WHERE searchable_text IS NOT NULL`
partial index predicates to exclude uncataloged rows.

## idx JSONB Structure

All standard Plone catalog indexes and metadata columns are stored
together in a single JSONB document. Example for a typical Plone Page:

```json
{
  "UID": "abc123-def456-789",
  "Title": "My Document",
  "Description": "A sample document",
  "portal_type": "Document",
  "review_state": "published",
  "Creator": "admin",
  "Subject": ["Python", "Plone"],
  "created": "2025-01-15T10:00:00+00:00",
  "modified": "2025-02-20T14:30:00+00:00",
  "effective": "2025-01-15T10:00:00+00:00",
  "expires": null,
  "is_folderish": false,
  "is_default_page": false,
  "sortable_title": "my document",
  "allowedRolesAndUsers": ["Anonymous"],
  "Language": "en",
  "path": "/plone/my-document",
  "path_parent": "/plone",
  "path_depth": 2,
  "getObjPositionInParent": 5,
  "image_scales": null
}
```

Key conventions:

- **Index values** (used for PG queries) are converted to JSON-safe types:
  dates become ISO 8601 strings, etc.  These live at the top level of idx.
- **Metadata values** that are JSON-native (str, int, float, bool, None,
  and lists/dicts of these) also live at the top level of idx.
- **Non-JSON-native metadata** (Zope `DateTime`, stdlib `datetime`, `date`,
  etc.) is encoded via the Rust codec (`zodb-json-codec`) into a nested
  `"@meta"` key.  This preserves original Python types so that
  `brain.effective` returns a `DateTime` object, not a string.
  The `@meta` dict uses codec type markers (e.g. `@dt`, `@cls`+`@s`) and
  is decoded once per brain on first access (cached thereafter).
- Multi-value fields (e.g., `Subject`, `allowedRolesAndUsers`) are
  stored as JSON arrays.
- Boolean fields are stored as JSON `true`/`false`.
- `null` values are stored explicitly where the object has no value
  for that field.
- Path fields (`path`, `path_parent`, `path_depth`) are stored in
  both the dedicated table columns and the idx JSONB for unified
  path query support.
- Fields that are both indexes and metadata (e.g. `effective`) appear
  in both places: top-level idx holds the converted ISO string (for
  PG queries), while `@meta` holds the original `DateTime` (for brain
  attribute access).

## Text Extraction Queue (Optional)

Created when `PGCATALOG_TIKA_URL` is set. Provides a PostgreSQL-backed
job queue for asynchronous text extraction via Apache Tika.

### text_extraction_queue Table

| Column | Type | Default | Purpose |
|---|---|---|---|
| `id` | `BIGSERIAL` | auto | Primary key |
| `zoid` | `BIGINT` | — | Object zoid (references `object_state`) |
| `tid` | `BIGINT` | — | Transaction ID (identifies the blob version) |
| `content_type` | `TEXT` | — | MIME type (e.g., `application/pdf`) |
| `status` | `TEXT` | `'pending'` | Job status: `pending`, `processing`, `done`, or `failed` |
| `attempts` | `INTEGER` | `0` | Number of processing attempts |
| `max_attempts` | `INTEGER` | `3` | Maximum retry attempts before marking as `failed` |
| `error` | `TEXT` | — | Error message from the last failed attempt |
| `created_at` | `TIMESTAMPTZ` | `now()` | Job creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | `now()` | Last status change timestamp |

Constraints: `UNIQUE(zoid, tid)` prevents duplicate jobs for the same
object version.

### Queue Indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_teq_pending` | B-tree (partial) | `id WHERE status = 'pending'` | Fast dequeue of pending jobs |

### Queue Trigger

A `NOTIFY` trigger fires on every INSERT, sending a
`text_extraction_ready` notification with the job ID. This wakes the
extraction worker instantly without polling.

```sql
CREATE TRIGGER trg_notify_extraction
    AFTER INSERT ON text_extraction_queue
    FOR EACH ROW EXECUTE FUNCTION notify_extraction_ready();
```

See {doc}`../explanation/tika-extraction` for the full architecture and
{doc}`../how-to/enable-tika-extraction` for setup instructions.

## SQL Functions

See {doc}`sql-functions` for the full reference of
`pgcatalog_to_timestamptz()`, `pgcatalog_lang_to_regconfig()`,
`pgcatalog_merge_extracted_text()`, and rrule functions.

## rrule_plpgsql Schema

Installed automatically at startup. Provides a pure PL/pgSQL
implementation of RFC 5545 RRULE expansion for DateRecurringIndex
queries. No C extensions required.

The schema is created idempotently using `CREATE SCHEMA IF NOT EXISTS`
with exception handling for type definitions. Functions are installed
from the vendored `rrule_schema.sql` file in the package.

See {doc}`sql-functions` for `rrule."between"()` and `rrule."after"()`
function signatures.

## Schema Installation

Schema is applied automatically at startup via the
`CatalogStateProcessor.get_schema_sql()` method, called by zodb-pgjsonb
when the state processor is registered. The installation sequence is:

1. Catalog columns (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`)
2. SQL functions (`CREATE OR REPLACE FUNCTION`)
3. Catalog indexes (`CREATE INDEX IF NOT EXISTS`)
4. rrule schema (idempotent `CREATE SCHEMA IF NOT EXISTS`)
5. BM25 extensions and columns (if detected)
6. Text extraction queue and merge function (if `PGCATALOG_TIKA_URL` is set)

All DDL is idempotent and safe to re-execute on an existing database.
The `install_catalog_schema()` function in `schema.py` executes each
DDL block via `conn.execute()`.
