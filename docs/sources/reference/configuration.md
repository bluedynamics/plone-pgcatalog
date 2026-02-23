<!-- diataxis: reference -->

# Configuration Reference

This page documents all configuration options for plone.pgcatalog,
including Zope configuration, environment variables, GenericSetup profiles,
and dependency information.

## zope.conf Settings

plone.pgcatalog requires `zodb-pgjsonb` as its ZODB storage backend.
The storage is configured in `zope.conf`:

```ini
%import zodb_pgjsonb

<zodb_db main>
  <pgjsonb>
    dsn dbname=zodb host=localhost port=5432 user=zodb password=zodb
  </pgjsonb>
</zodb_db>
```

plone.pgcatalog itself is auto-discovered via `z3c.autoinclude` and does not
need a separate `%import` directive.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PGCATALOG_BM25_LANGUAGES` | (none) | Comma-separated ISO 639-1 codes, or `"auto"` to detect from `portal_languages`. Controls which per-language BM25 columns are created. Only relevant when VectorChord-BM25 extensions are installed. |
| `ZODB_TEST_DSN` | `dbname=zodb_test host=localhost port=5433 user=zodb password=zodb` | DSN for test database (tests only). |
| `BM25_TEST_DSN` | `dbname=zodb_test host=localhost port=5434 user=zodb password=zodb` | DSN for BM25 integration tests (tests only). |

## GenericSetup Profile

plone.pgcatalog ships a GenericSetup profile (`default`) that configures
Plone to use the PostgreSQL-backed catalog.

- **`setuphandlers.py`**: Replaces `portal_catalog` with `PlonePGCatalogTool`,
  preserving addon index definitions via automatic snapshot and restore.
- **`metadata.xml`**: Profile version 1.

The profile is applied automatically when installing the add-on through
the Plone control panel or via a dependency declaration in another profile.

## ZCML Registration

All ZCML registrations are loaded automatically via `z3c.autoinclude`.
The following registrations are made:

- `PlonePGCatalogTool` registered as `IPGCatalogTool` utility.
- `IDatabaseOpenedWithRoot` subscriber for startup initialization
  (schema creation, index registry sync, connection pool setup).
- `IPubEnd` subscriber that releases request-scoped PostgreSQL connections
  at the end of each HTTP request.

## Python Dependencies

| Package | Purpose |
|---|---|
| `psycopg[binary,pool]>=3.1` | PostgreSQL adapter with connection pooling. |
| `orjson>=3.9` | Fast JSONB deserialization. |
| `Products.CMFPlone` | Plone framework. |
| `zodb-pgjsonb>=1.1` | ZODB storage backend (provides the `object_state` table). |

## Optional Dependencies

| Package | Purpose |
|---|---|
| VectorChord-BM25 (`vchord_bm25`) | BM25 ranking extension for PostgreSQL. Enables relevance-ranked full-text search as an alternative to tsvector ranking. |
| pg_tokenizer | Text tokenization for BM25 (language-specific stemmers and vocabulary mapping). |

## Docker Images

| Image | Use Case |
|---|---|
| `postgres:17` | Standard PostgreSQL with tsvector-based full-text ranking. |
| `tensorchord/vchord-suite:pg17-latest` | PostgreSQL with VectorChord-BM25 and pg_tokenizer pre-installed. |
