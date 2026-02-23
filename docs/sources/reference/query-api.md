<!-- diataxis: reference -->

# Query API Reference

This page documents the query interface for plone.pgcatalog, including
all supported index types, query parameters, sorting, pagination,
security filtering, DoS limits, and result objects.

## Standard Catalog Query Interface

plone.pgcatalog supports the same calling conventions as Plone's
`CatalogTool`:

```python
# Keyword arguments (most common)
results = catalog(portal_type="Document", sort_on="modified", sort_order="descending")

# Explicit searchResults (identical to __call__)
results = catalog.searchResults(portal_type="Document")

# Unrestricted (bypasses security filters)
results = catalog.unrestrictedSearchResults(portal_type="Document")
```

`searchResults()` and `__call__` auto-inject `allowedRolesAndUsers` and
`effectiveRange` filters based on the current user's roles.
`unrestrictedSearchResults()` bypasses all security filtering.

## Query Parameters by Index Type

### FieldIndex

Single-value indexes (e.g., `portal_type`, `review_state`, `Creator`).

```python
# Exact match
catalog(portal_type="Document")

# Multi-value (OR)
catalog(portal_type=["Document", "News Item"])

# Negation
catalog(portal_type={"query": "Document", "not": True})

# Negation with multiple values
catalog(portal_type={"not": ["Document", "News Item"]})

# Range: greater than or equal
catalog(modified={"query": DateTime("2025-01-01"), "range": "min"})

# Range: less than or equal
catalog(modified={"query": DateTime("2025-12-31"), "range": "max"})

# Range: between (inclusive)
catalog(modified={"query": [DateTime("2025-01-01"), DateTime("2025-12-31")], "range": "min:max"})
```

### KeywordIndex

Multi-value indexes where each object can have multiple values
(e.g., `Subject`, `allowedRolesAndUsers`).

```python
# Has keyword
catalog(Subject="Python")

# Has any of these keywords (OR, default operator)
catalog(Subject=["Python", "Plone"])

# Has all of these keywords (AND)
catalog(Subject={"query": ["Python", "Plone"], "operator": "and"})
```

The default `operator` is `"or"`.

### DateIndex

Timestamp indexes with range support (e.g., `created`, `modified`,
`effective`, `expires`).

```python
# Greater than or equal
catalog(created={"query": DateTime("2025-01-01"), "range": "min"})

# Less than or equal
catalog(effective={"query": DateTime(), "range": "max"})

# Between (inclusive)
catalog(created={"query": [DateTime("2025-01-01"), DateTime("2025-12-31")], "range": "min:max"})
```

Both Zope `DateTime` objects and Python `datetime` objects are accepted.
All dates are compared via `pgcatalog_to_timestamptz()` expression
indexes (see {doc}`sql-functions`).

### BooleanIndex

True/false indexes (e.g., `is_folderish`, `is_default_page`).

```python
catalog(is_folderish=True)
catalog(is_default_page=False)
```

### DateRangeIndex

Compound date range filter across `effective` and `expires` fields.
The only standard instance is `effectiveRange`.

```python
# Objects effective at the given time:
# effective <= now AND (expires >= now OR expires IS NULL)
catalog(effectiveRange=DateTime())
```

### UUIDIndex

UUID equality lookup (e.g., `UID`).

```python
catalog(UID="abc123-def456")
```

### ZCTextIndex (Full-Text Search)

Full-text search indexes: `SearchableText`, `Title`, `Description`,
and any addon `ZCTextIndex` fields.

```python
# SearchableText: full-text search across title, description, and body
catalog(SearchableText="my search term")

# Language-aware stemming (e.g., German stemming for "Katzen" → "Katz")
catalog(SearchableText="Katzen", Language="de")

# Title: word-level match (uses 'simple' regconfig)
catalog(Title="Quick Fox")

# Description: word-level match (uses 'simple' regconfig)
catalog(Description="introduction")
```

Relevance ranking is auto-applied when `SearchableText` is queried
without an explicit `sort_on`. The active search backend determines
the ranking strategy (see {doc}`search-backends`).

### ExtendedPathIndex

Hierarchical path queries with depth control.

```python
# All descendants (default, depth=-1)
catalog(path="/plone/folder")

# Exact path only (depth=0)
catalog(path={"query": "/plone/folder", "depth": 0})

# Immediate children only (depth=1)
catalog(path={"query": "/plone/folder", "depth": 1})

# Up to 2 levels deep
catalog(path={"query": "/plone/folder", "depth": 2})

# Navigation tree (siblings at each level along the path)
catalog(path={"query": "/plone/folder", "navtree": True})

# Multiple paths (OR)
catalog(path={"query": ["/plone/a", "/plone/b"]})
```

### Unregistered Indexes

Index names not in `META_TYPE_MAP` (e.g., `Language`, `TranslationGroup`
from `plone.app.multilingual`) are not silently skipped. The query
builder first checks for an `IPGIndexTranslator` named utility, then
falls back to a simple JSONB containment query:

```python
# Becomes: idx @> '{"Language": "en"}'::jsonb
catalog(Language="en")
```

This allows third-party add-on queries to work without explicit registry
entries, as long as the index value was stored in the `idx` JSONB during
indexing (via an `IPGIndexTranslator.extract()` or the standard
extraction path).

### GopipIndex

Integer ordering index (`getObjPositionInParent`). Used for sorting,
not typically queried directly.

```python
catalog(sort_on="getObjPositionInParent")
```

### DateRecurringIndex (via IPGIndexTranslator)

Recurring event date queries. Recurring events (with RRULE) are
expanded at query time via `rrule."between"()` and `rrule."after"()`
PL/pgSQL functions.

```python
# Range: events occurring between two dates
catalog(start={"query": [DateTime("2025-03-01"), DateTime("2025-03-31")], "range": "min:max"})

# Min: events occurring on or after a date
catalog(start={"query": DateTime("2025-03-01"), "range": "min"})

# Max: events starting on or before a date
catalog(start={"query": DateTime("2025-02-01"), "range": "max"})
```

See {doc}`ipgindextranslator` for implementation details.

### DateRangeInRangeIndex (via IPGIndexTranslator)

Overlap query for objects whose `[start, end]` date range overlaps
a query range. Supports recurring events.

```python
catalog(event_dates={"start": DateTime("2025-03-01"), "end": DateTime("2025-03-31")})
```

This finds objects whose `[obj_start, obj_end]` range overlaps the
query range `[2025-03-01, 2025-03-31]`.

See {doc}`ipgindextranslator` for implementation details.

## Sort Parameters

| Parameter | Type | Description |
|---|---|---|
| `sort_on` | `str` or `list[str]` | Index name(s) to sort by |
| `sort_order` | `str` or `list[str]` | `"ascending"` (default) or `"descending"` |
| `sort_limit` | `int` | Maximum results (capped at 10,000) |

Multi-column sorting:

```python
catalog(sort_on=["modified", "sortable_title"], sort_order=["descending", "ascending"])
```

When `sort_order` is shorter than `sort_on`, the last order value is
reused for remaining sort keys.

Sort expressions by index type:

| Index Type | ORDER BY Expression |
|---|---|
| `DATE` | `pgcatalog_to_timestamptz(idx->>'field')` |
| `GOPIP` | `(idx->>'field')::integer` |
| `BOOLEAN` | `(idx->>'field')::boolean` |
| `FIELD`, `KEYWORD`, `UUID` | `idx->>'field'` (text comparison) |
| `PATH` | `idx->>'field'` (text comparison) |

## Pagination

| Parameter | Type | Description | Limit |
|---|---|---|---|
| `b_start` | `int` | Result offset (0-based) | Max 1,000,000 |
| `b_size` | `int` | Page size | Max 10,000 |

When a `LIMIT` is present (via `sort_limit` or `b_size`), a single query
is executed using `COUNT(*) OVER()` as a window function. The total
matching count is available via `results.actual_result_count`.

## Security

- `searchResults()` auto-applies `allowedRolesAndUsers` and
  `effectiveRange` filters based on the current user's roles and
  permissions.
- `unrestrictedSearchResults()` bypasses all security filtering.
  Requires appropriate Zope permissions.
- `show_inactive=True` bypasses `effectiveRange` filtering (for users
  with `AccessInactivePortalContent` permission, this is automatic).

Security filters are injected by `apply_security_filters()` in
`query.py` before the query is passed to `build_query()`.

## DoS Limits

Hardcoded limits to prevent resource exhaustion:

| Limit | Value | Purpose |
|---|---|---|
| Max `sort_limit` / `b_size` | 10,000 | Prevent unbounded result sets |
| Max `b_start` | 1,000,000 | Prevent deep pagination |
| Max search text length | 1,000 chars | Prevent FTS query explosion |
| Max path list size | 100 | Prevent large IN clauses |

These limits are enforced in `query.py` and cannot be overridden.

## Result Objects

### CatalogSearchResults

Wraps a list of `PGCatalogBrain` objects.

- Implements `IFiniteSequence` and inherits from `ZTUtils.Lazy.Lazy`
  (required for `plone.restapi` serialization).
- `actual_result_count` attribute: total matching count. May differ
  from `len()` when `sort_limit` or `b_size` truncates results.
- Supports slicing: `results[10:20]` returns a new
  `CatalogSearchResults` preserving `actual_result_count`.
- Supports iteration, `len()`, and boolean evaluation.

### PGCatalogBrain

Lightweight result object backed by a PostgreSQL row. Implements
`ICatalogBrain`.

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `getPath()` | `str` | Physical path (e.g., `"/plone/folder/doc"`) |
| `getURL(relative=False)` | `str` | URL via request, or path in standalone mode |
| `getObject()` | object or `None` | Restricted traversal to the actual content object |
| `_unrestrictedGetObject()` | object or `None` | Unrestricted traversal |
| `getRID()` | `int` | ZOID (integer, used as record ID) |

**Properties:**

| Property | Returns | Description |
|---|---|---|
| `getId` | `str` | Last path segment (or `getId` from idx if available) |
| `data_record_id_` | `int` | ZCatalog compatibility alias for `getRID()` |

**Attribute access:**

All registered indexes and metadata columns are accessible as
attributes (e.g., `brain.portal_type`, `brain.Title`, `brain.Subject`).

- For registered indexes/metadata: returns `None` if the field is
  missing from the `idx` JSONB (Missing Value behavior, matching
  ZCatalog).
- For unknown attributes: raises `AttributeError`. This triggers
  `getObject()` fallback in `CatalogContentListingObject`.

**Lazy loading:**

When a request-scoped connection is available, brains are created in
lazy mode (without `idx` data). On first attribute access, all brains
in the result set have their `idx` loaded in a single batch query via
`CatalogSearchResults._load_idx_batch()`, using the same REPEATABLE
READ snapshot as the original search.
