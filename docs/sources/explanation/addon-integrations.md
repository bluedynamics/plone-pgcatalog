<!-- diataxis: explanation -->

# Addon Integrations

plone.pgcatalog replaces ZCatalog's BTree storage with PostgreSQL queries.
This page explains how third-party Plone add-ons interact with the
PostgreSQL-backed catalog and why compatibility works the way it does.

## How Addons Integrate Automatically

Most Plone addons register their indexes via GenericSetup `catalog.xml`.
These indexes are stored in `_catalog.indexes` (a `PersistentMapping`) and
are discovered by plone.pgcatalog at startup through `sync_from_catalog()`,
which reads each Plone site's `portal_catalog._catalog.indexes` and
populates the `IndexRegistry`.

Because the discovery is dynamic, addons that follow the standard
`catalog.xml` pattern require no special configuration.  Their index values
are extracted during `catalog_object()` and stored in the `idx` JSONB
column alongside all other indexes.

Addons that bypass the public catalog API and call ZCatalog internals
directly (e.g., `_apply_index()`) need compatibility adapters.  These
live in the `addons_compat/` package.

## eea.facetednavigation

`PGFacetedCatalog` in `addons_compat/eeafacetednavigation.py` overrides
`eea.facetednavigation.search.catalog.FacetedCatalog.apply_index()` to
query PostgreSQL directly via the `idx` JSONB column instead of
ZCatalog's BTree-based `_apply_index()`.

It dispatches by `IndexType` from the `IndexRegistry`:

| IndexType | SQL |
|---|---|
| FIELD, GOPIP, UUID | `idx @> '{"key": "value"}'::jsonb` |
| KEYWORD (single) | `idx @> '{"key": ["value"]}'::jsonb` |
| KEYWORD (multi) | `idx->'{key}' ?| array` |
| BOOLEAN | `idx @> '{"key": true}'::jsonb` |
| DATE | `idx @> '{"key": "iso-date"}'::jsonb` |
| Special/unknown | `frozenset()` |

Falls back to `IPGIndexTranslator` utilities, then to the original
BTree-based implementation when the catalog is not `IPGCatalogTool`.

### Activation

The adapter is registered via `overrides.zcml` (loaded by
`five:loadProductsOverrides`), conditionally included only when
`eea.facetednavigation` is installed:

```xml
<includeOverrides
    zcml:condition="installed eea.facetednavigation"
    package=".addons_compat"
    file="eeafacetednavigation_overrides.zcml"
    />
```

### Limitations

Special indexes (`SearchableText`, `effectiveRange`, `path`) are not
handled by `apply_index()` because they use dedicated PostgreSQL columns
and query patterns.  For these, the adapter returns `frozenset()` --
eea.facetednavigation then falls back to its own handling.

### Error Recovery

If a PostgreSQL query fails, the adapter catches the exception, logs it
via `log.exception()`, and falls back to the original BTree-based
`FacetedCatalog.apply_index()`.  This ensures the site remains functional
even if the PG connection is temporarily unavailable.

## plone.app.multilingual

`plone.app.multilingual` adds `Language` and `TranslationGroup` indexes
to the catalog.  These are not standard ZCatalog index types, so they are
not present in `META_TYPE_MAP`.

plone.pgcatalog handles them via the unregistered-index fallback path in
`query.py`: the query builder first checks for an `IPGIndexTranslator`
named utility, then falls back to a JSONB containment query:

```sql
-- catalog(Language="en") becomes:
SELECT zoid FROM object_state WHERE idx @> '{"Language": "en"}'::jsonb
```

No special configuration is needed.  The index values are extracted
during the standard `_extract_idx()` path because
`plone.app.multilingual` registers a plone.indexer adapter.

## collective.taxonomy

`collective.taxonomy` registers custom `FieldIndex` instances via
`catalog.xml` (e.g., `taxonomy_topic`, `taxonomy_region`).  These are
auto-discovered at startup by `sync_from_catalog()` and stored as
standard `FIELD` type entries in the `IndexRegistry`.

Queries work out of the box:

```python
catalog(taxonomy_topic="science")
```

## Products.DateRangeInRangeIndex

The `DateRangeInRangeIndex` (from `collective.DateRangeInRangeIndex` or
`plone.app.event`) tests whether an object's `[start, end]` date range
overlaps a query range.

plone.pgcatalog provides native support via the
`DateRangeInRangeIndexTranslator` (`IPGIndexTranslator` utility),
auto-discovered at startup.  The translator reads the `startindex` and
`endindex` configuration from each `DateRangeInRangeIndex` object and
generates an overlap SQL query.

## Writing Your Own Integration

If your addon needs custom query translation or index extraction beyond
what the default JSONB containment provides, implement an
`IPGIndexTranslator` named utility.

See {doc}`../how-to/write-custom-translator` for a step-by-step guide.
