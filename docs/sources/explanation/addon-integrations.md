<!-- diataxis: explanation -->

# Addon Integrations

plone.pgcatalog replaces ZCatalog's BTree storage with PostgreSQL queries.
Third-party Plone add-ons that bypass the public catalog API and call ZCatalog
internals directly (e.g., `_apply_index()`) need compatibility adapters.

These live in the `addons_compat/` directory.

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

Registered via `overrides.zcml` (loaded by `five:loadProductsOverrides`),
conditionally included only when `eea.facetednavigation` is installed.
