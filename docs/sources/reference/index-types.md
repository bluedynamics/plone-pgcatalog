<!-- diataxis: reference -->

# Index Types Reference

This page documents the index types supported by plone.pgcatalog,
the mapping from ZCatalog meta types, special indexes stored outside
the `idx` JSONB column, and the index registry API.

## IndexType Enum

Defined in `plone.pgcatalog.columns`. Each value represents a category
of index behavior that determines how queries are translated to SQL.

| Enum Value | Description |
|---|---|
| `FIELD` | Single-value equality and range queries. |
| `KEYWORD` | Multi-value membership queries (OR/AND). |
| `DATE` | Timestamp queries with range support. |
| `BOOLEAN` | True/False containment check. |
| `DATE_RANGE` | Effective/expiry date range (used by `effectiveRange`). |
| `UUID` | UUID equality. |
| `TEXT` | Full-text search (tsvector or BM25). |
| `PATH` | Path hierarchy queries with depth support. |
| `GOPIP` | Integer position ordering (getObjPositionInParent). |

## META_TYPE_MAP

Maps ZCatalog `meta_type` strings to `IndexType` enum values.
Defined in `plone.pgcatalog.columns`.

| ZCatalog meta_type | IndexType |
|---|---|
| `FieldIndex` | `FIELD` |
| `KeywordIndex` | `KEYWORD` |
| `DateIndex` | `DATE` |
| `BooleanIndex` | `BOOLEAN` |
| `DateRangeIndex` | `DATE_RANGE` |
| `UUIDIndex` | `UUID` |
| `ZCTextIndex` | `TEXT` |
| `ExtendedPathIndex` | `PATH` |
| `GopipIndex` | `GOPIP` |

## Special Indexes

These indexes are stored in dedicated columns on the `object_state` table
rather than inside the `idx` JSONB column. They have `idx_key=None` in the
index registry.

| Index Name | Storage | Notes |
|---|---|---|
| `SearchableText` | `searchable_text` TSVECTOR column, plus optional `search_bm25_*` columns when VectorChord-BM25 is installed. | Language-aware via `pgcatalog_lang_to_regconfig()`. Weighted tsvector. |
| `effectiveRange` | `idx->>'effective'` and `idx->>'expires'` fields. | Compound date range query across two JSONB keys. |
| `path` | `path` TEXT column, `parent_path` TEXT column, and `path_depth` INTEGER column. | Dedicated columns for fast hierarchy queries. |

## Index Registry

The `IndexRegistry` is a singleton that maps index names to their type
information. It is populated at startup by `sync_from_catalog()`, which
reads the indexes defined on the Plone `ZCatalog` instance.

Each registry entry is a 3-tuple:

```
(IndexType, idx_key, source_attrs)
```

- **`IndexType`**: One of the enum values listed above.
- **`idx_key`**: The key used in the `idx` JSONB column, or `None` for
  special indexes.
- **`source_attrs`**: Tuple of source attribute names, as returned by
  `getIndexSourceNames()` on the ZCatalog index object.

Unknown ZCatalog `meta_type` values not present in `META_TYPE_MAP` are
skipped during sync. These are expected to be handled by
`IPGIndexTranslator` named utilities instead.

Access the registry:

```python
from plone.pgcatalog.columns import get_registry

registry = get_registry()
```

## Custom Index Types

Index types not listed in `META_TYPE_MAP` (such as `DateRecurringIndex`,
`DateRangeInRangeIndex`, or composite indexes) are supported via
`IPGIndexTranslator` named utilities. Each translator provides query
generation and index data extraction for its index type.

See {doc}`ipgindextranslator` for the interface specification and
implementation details.
