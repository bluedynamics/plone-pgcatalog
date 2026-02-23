<!-- diataxis: how-to -->

# Use with plone.distribution

## Adding to Your Distribution

plone.pgcatalog works with `plone.distribution` for automated site creation.

In your distribution's `profiles.json`, include the plone.pgcatalog profile in the `base` list:

```json
{
  "base": [
    "plone.app.contenttypes:default",
    "plonetheme.barceloneta:default",
    "plone.pgcatalog:default"
  ],
  "content": []
}
```

## Example Distribution

The `example/pgcatalog-example-distribution/` directory in the plone.pgcatalog repository provides a complete working example:

```
pgcatalog-example-distribution/
  pyproject.toml
  src/
    pgcatalog_example/
      __init__.py
      configure.zcml
      dependencies.zcml
      distributions.zcml
      distributions/
        pgcatalog_demo/
          profiles.json
          schema.json
```

The `schema.json` defines the site creation form fields (site_id, title, language, timezone). The `profiles.json` lists the GenericSetup profiles to apply.

## Site Creation via REST API

```bash
curl -X POST http://localhost:8081/@@plone.distribution/create \
  -H "Content-Type: application/json" \
  -d '{
    "distribution": "pgcatalog_demo",
    "site_id": "Plone",
    "title": "My Site",
    "default_language": "en",
    "portal_timezone": "UTC"
  }'
```

## GenericSetup Integration

The `setuphandlers.py` install step replaces `portal_catalog` with `PlonePGCatalogTool` (preserving existing addon indexes via snapshot/restore) and ensures:

- Essential Plone catalog indexes are present (UID, portal_type, Title, etc.)
- Orphaned ZCTextIndex lexicons are removed
- DDL schema is applied to PostgreSQL (catalog columns, expression indexes, functions)

All schema DDL is applied via the `CatalogStateProcessor` registration at startup, using the storage's own connection to avoid REPEATABLE READ lock conflicts.
