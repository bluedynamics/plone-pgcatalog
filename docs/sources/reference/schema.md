<!-- diataxis: reference -->

# Database schema reference

This page documents the PostgreSQL schema extensions, indexes, JSONB
structure, and SQL functions installed by plone.pgcatalog.

## object_state table extensions

plone.pgcatalog extends the `object_state` table (owned by zodb-pgjsonb)
with the following columns:

| Column | Type | Purpose |
|---|---|---|
| `path` | `TEXT` | Physical object path (for example, `/plone/folder/doc`) |
| `parent_path` | `TEXT` | Parent path (for path queries) |
| `path_depth` | `INTEGER` | Path depth (for depth-limited queries) |
| `idx` | `JSONB` | Index and metadata values (lightweight after extraction) |
| `searchable_text` | `TSVECTOR` | Weighted full-text search vector |
| `meta` | `JSONB` | Non-JSON-native metadata (`DateTime`, `image_scales`, etc.) |
| `object_provides` | `TEXT[]` | Interface-based lookups (extracted from idx) |
| `allowed_roles` | `TEXT[]` | Dedicated security filter column (allowedRolesAndUsers) |
| `search_bm25` | `BM25VECTOR` | BM25 fallback column (when BM25 active) |
| `search_bm25_{lang}` | `BM25VECTOR` | Per-language BM25 column (when BM25 active) |

The first eight columns are present after schema DDL has been applied
(automatically on first startup with a registered `CatalogStateProcessor`).

`parent_path` and `path_depth` are derived from `path` (the parent is
the path with its last segment removed; the depth is the number of
segments).  They are stored as separate columns because the most
frequent path queries—direct children (`depth=1`) and navigation
trees—become simple equality checks (`parent_path = X` or
`parent_path = ANY(...)`) instead of `LIKE` prefix scans combined
with depth filtering.

The `search_bm25` and
per-language `search_bm25_{lang}` columns are created only when
VectorChord-BM25 extensions are detected at startup.

## PostgreSQL indexes

### Core indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_path` | B-tree | `path` | Path column lookup (brain construction) |
| `idx_os_catalog` | GIN | `idx` | JSONB containment/existence queries |
| `idx_os_searchable_text` | GIN | `searchable_text` | Full-text search |

### Path expression indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_path` | B-tree | `idx->>'path'` | Path equality queries |
| `idx_os_cat_path_pattern` | B-tree (`text_pattern_ops`) | `idx->>'path'` | Path LIKE prefix queries |
| `idx_os_cat_path_parent` | B-tree | `idx->>'path_parent'` | Parent path queries (depth=1) |
| `idx_os_cat_path_depth` | B-tree | `(idx->>'path_depth')::integer` | Depth-limited queries |

### Date expression indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_modified` | B-tree | `pgcatalog_to_timestamptz(idx->>'modified')` | Date sorting/filtering |
| `idx_os_cat_created` | B-tree | `pgcatalog_to_timestamptz(idx->>'created')` | Date sorting/filtering |
| `idx_os_cat_effective` | B-tree | `pgcatalog_to_timestamptz(idx->>'effective')` | Date sorting/filtering |
| `idx_os_cat_expires` | B-tree | `pgcatalog_to_timestamptz(idx->>'expires')` | Date sorting/filtering |

### Field expression indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_sortable_title` | B-tree | `idx->>'sortable_title'` | Title sorting |
| `idx_os_cat_portal_type` | B-tree | `idx->>'portal_type'` | Type filtering |
| `idx_os_cat_review_state` | B-tree | `idx->>'review_state'` | Workflow state filtering |
| `idx_os_cat_uid` | B-tree | `idx->>'uid'` | UUID lookup |

### Composite indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_parent_type` | B-tree | `(path_parent, portal_type)` | Folder listings, navigation |
| `idx_os_cat_path_type` | B-tree | `(path pattern, portal_type)` | Collections, search |
| `idx_os_cat_path_depth_type` | B-tree | `(path pattern, path_depth, portal_type)` | Navigation tree |
| `idx_os_cat_type_state` | B-tree | `(portal_type, review_state)` | Workflow-filtered listings |

### Keyword GIN indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_allowed_roles` | GIN | `allowed_roles` | Security filter (every query) |
| `idx_os_object_provides` | GIN | `object_provides` | Interface-based lookups |
| `idx_os_cat_subject_gin` | GIN | `idx->'Subject'` | Subject keyword queries |

### Partial indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_nav_visible` | B-tree (partial) | Navigation fields where `exclude_from_nav=false` | Navigation listings (only ~1.6% of rows) |
| `idx_os_cat_events_upcoming` | B-tree (partial) | Event fields where `portal_type=Event` | Calendar/event queries |

### Text expression indexes

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_cat_title_tsv` | GIN | `to_tsvector('simple', COALESCE(idx->>'Title', ''))` | Title word-level matching |
| `idx_os_cat_description_tsv` | GIN | `to_tsvector('simple', COALESCE(idx->>'Description', ''))` | Description word-level matching |

Additional GIN expression indexes are autocreated at startup for any
addon `ZCTextIndex` fields discovered in `portal_catalog`.
These follow
the naming pattern `idx_os_cat_{key}_tsv` and use the `'simple'`
regconfig for word-level matching.

Custom catalog indexes (FieldIndex, DateIndex, BooleanIndex, UUIDIndex,
DateRecurringIndex) also get btree expression indexes automatically at
startup based on the IndexRegistry. Date fields use the
`pgcatalog_to_timestamptz()` wrapper.

### BM25 indexes (optional)

When VectorChord-BM25 extensions are installed:

| Index Name | Type | Expression | Purpose |
|---|---|---|---|
| `idx_os_search_bm25` | BM25 | `search_bm25 bm25_ops` | Fallback BM25 ranking |
| `idx_os_search_bm25_{lang}` | BM25 | `search_bm25_{lang} bm25_ops` | Per-language BM25 ranking |

All indexes use `WHERE idx IS NOT NULL` or `WHERE searchable_text IS NOT NULL`
partial index predicates to exclude uncataloged rows.

## idx JSONB structure

Standard Plone catalog indexes and JSON-native metadata are stored
together in a single JSONB document.
Heavy or high-cardinality keys are extracted into dedicated columns
(see {ref}`extracted-columns` below).
Example for a typical Plone Page:

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
  "Language": "en",
  "path": "/plone/my-document",
  "path_parent": "/plone",
  "path_depth": 2,
  "getObjPositionInParent": 5
}
```

Key conventions:

- **Index values** (used for PG queries) are converted to JSON-safe types:
  dates become ISO 8601 strings, etc.
  These live at the top level of idx.
- **Metadata values** that are JSON-native (str, int, float, bool, None,
  and lists/dicts of these) also live at the top level of idx.
- Multi-value fields (for example, `Subject`) are stored as JSON arrays.
- Boolean fields are stored as JSON `true`/`false`.
- `null` values are stored explicitly where the object has no value
  for that field.
- Path fields (`path`, `path_parent`, `path_depth`) are stored in
  both the dedicated table columns and the idx JSONB for unified
  path query support.
- Fields that are both indexes and metadata (for example `effective`) appear
  in both places: top-level idx holds the converted ISO string (for
  PG queries), while the `meta` column holds the original `DateTime`
  (for brain attribute access).

(extracted-columns)=
## Extracted columns (`ExtraIdxColumn`)

To keep `idx` compact (below the PostgreSQL TOAST threshold of ~2 KB),
heavy or high-cardinality keys are **extracted** from the idx dict at
write time and stored in dedicated columns.
The extraction is managed by a
generic `ExtraIdxColumn` registry in `columns.py`.

| idx key | Column | Type | Reason |
|---|---|---|---|
| `@meta` | `meta` | `JSONB` | Non-JSON-native metadata (`DateTime`, etc.); never queried via SQL, only used for brain attribute access |
| `object_provides` | `object_provides` | `TEXT[]` | Interface-based lookups; native GIN on `TEXT[]` is faster than JSONB containment |
| `allowedRolesAndUsers` | `allowed_roles` | `TEXT[]` | Security filter in every query; same optimization as `object_provides` |

Extracted keys are **popped** from the idx dict before it is written to
the `idx` column.
They do not appear in `idx` for newly written rows.
Pre-migration rows (written before this feature) may still contain these
keys in `idx`; the brain falls back to `idx["@meta"]` when the `meta`
column is `NULL`.

The `meta` column stores codec-encoded non-JSON-native metadata (Zope
`DateTime`, stdlib `datetime`, `date`, `image_scales`, etc.) via the Rust
codec (`zodb-json-codec`).
This preserves original Python types so that
`brain.effective` returns a `DateTime` object, not a string.
The `meta` dict uses codec type markers (for example `@dt`, `@cls`+`@s`) and
is decoded once per brain on first access (cached thereafter).

```{note}
Extracted keys are no longer accessible via `brain.object_provides` or
`brain.allowedRolesAndUsers`.
These are query-only index fields — not
catalog metadata — so brain attribute access is not needed.
If a custom addon registers one of these as metadata, the data is still in
the database (in its dedicated column) but would need a brain extension to
be surfaced.
```

## Text Extraction Queue (Optional)

Created when `PGCATALOG_TIKA_URL` is set.
Provides a PostgreSQL-backed
job queue for asynchronous text extraction via Apache Tika.

### text_extraction_queue Table

| Column | Type | Default | Purpose |
|---|---|---|---|
| `id` | `BIGSERIAL` | auto | Primary key |
| `zoid` | `BIGINT` |—| Object zoid (references `object_state`) |
| `tid` | `BIGINT` |—| Transaction ID (identifies the blob version) |
| `content_type` | `TEXT` |—| MIME type (for example, `application/pdf`) |
| `status` | `TEXT` | `'pending'` | Job status: `pending`, `processing`, `done`, or `failed` |
| `attempts` | `INTEGER` | `0` | Number of processing attempts |
| `max_attempts` | `INTEGER` | `3` | Maximum retry attempts before marking as `failed` |
| `error` | `TEXT` |—| Error message from the last failed attempt |
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
`text_extraction_ready` notification with the job ID.
This wakes the
extraction worker instantly without polling.

```sql
CREATE TRIGGER trg_notify_extraction
    AFTER INSERT ON text_extraction_queue
    FOR EACH ROW EXECUTE FUNCTION notify_extraction_ready();
```

See {doc}`../explanation/tika-extraction` for the full architecture and
{doc}`../how-to/enable-tika-extraction` for setup instructions.

## SQL functions

See {doc}`sql-functions` for the full reference of
`pgcatalog_to_timestamptz()`, `pgcatalog_lang_to_regconfig()`,
`pgcatalog_merge_extracted_text()`, and rrule functions.

## rrule_plpgsql schema

Installed automatically at startup.
Provides a pure PL/pgSQL
implementation of RFC 5545 RRULE expansion for DateRecurringIndex
queries.
No C extensions required.

The schema is created idempotently using `CREATE SCHEMA IF NOT EXISTS`
with exception handling for type definitions.
Functions are installed
from the vendored `rrule_schema.sql` file in the package.

See {doc}`sql-functions` for `rrule."between"()` and `rrule."after"()`
function signatures.

## Schema installation

Schema is applied automatically at startup via the
`CatalogStateProcessor.get_schema_sql()` method, called by zodb-pgjsonb
when the state processor is registered.
The installation sequence is:

1.
Catalog columns (`ALTER TABLE ...
ADD COLUMN IF NOT EXISTS`)
2.
SQL functions (`CREATE OR REPLACE FUNCTION`)
3.
Catalog indexes (`CREATE INDEX IF NOT EXISTS`)
4. rrule schema (idempotent `CREATE SCHEMA IF NOT EXISTS`)
5.
BM25 extensions and columns (if detected)
6.
Text extraction queue and merge function (if `PGCATALOG_TIKA_URL` is set)

All DDL is idempotent and safe to re-execute on an existing database.
The `install_catalog_schema()` function in `schema.py` executes each
DDL block via `conn.execute()`.
