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

### Using environment variables in zope.conf

ZConfig supports variable substitution via the `%define` directive and the
`${}` syntax. Combined with environment variables, this keeps secrets out
of the configuration file:

```ini
%import zodb_pgjsonb

<zodb_db main>
  <pgjsonb>
    dsn dbname=${ZODB_DB:zodb} host=${ZODB_HOST:localhost} port=${ZODB_PORT:5432} user=${ZODB_USER:zodb} password=${ZODB_PASSWORD:zodb}
    blob-dir ${ZODB_BLOB_DIR:/var/plone/blobs}
  </pgjsonb>
</zodb_db>
```

The `${VAR:default}` syntax falls back to the value after the colon when
the environment variable is not set. This works for any ZConfig directive,
not just `dsn`. See the
[ZConfig documentation](https://zconfig.readthedocs.io/en/latest/using-zconfig.html#variable-substitution)
for details.

plone.pgcatalog itself is auto-discovered via `z3c.autoinclude` and does not
need a separate `%import` directive.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PGCATALOG_BM25_LANGUAGES` | (none) | Comma-separated ISO 639-1 codes, or `"auto"` to detect from `portal_languages`. Controls which per-language BM25 columns are created. Only relevant when VectorChord-BM25 extensions are installed. |
| `PGCATALOG_TIKA_URL` | (none) | Tika server URL, e.g. `http://localhost:9998`. Enables async text extraction from binary content (PDFs, Office docs, images). When set, the queue table and merge function are created at startup. See {doc}`../how-to/enable-tika-extraction`. |
| `PGCATALOG_TIKA_CONTENT_TYPES` | common office/PDF/image types | Comma-separated MIME types to send to Tika. Default includes PDF, MS Office, OpenDocument, RTF, and common image formats. |
| `PGCATALOG_TIKA_INPROCESS` | (none) | Set to `true`, `1`, or `yes` to start the extraction worker as a daemon thread inside the Zope process. Requires `PGCATALOG_TIKA_URL`. |
| `ZODB_TEST_DSN` | `dbname=zodb_test host=localhost port=5433 user=zodb password=zodb` | DSN for test database (tests only). |
| `BM25_TEST_DSN` | `dbname=zodb_test host=localhost port=5434 user=zodb password=zodb` | DSN for BM25 integration tests (tests only). |

### Standalone Worker Environment Variables

These variables configure the `pgcatalog-tika-worker` CLI when running
as a standalone process (outside Zope):

| Variable | Default | Description |
|---|---|---|
| `TIKA_WORKER_DSN` | (required) | PostgreSQL connection string. |
| `TIKA_WORKER_URL` | (required) | Tika server URL. |
| `TIKA_WORKER_POLL_INTERVAL` | `5` | Seconds between polls when idle (LISTEN/NOTIFY provides instant wakeup). |
| `TIKA_WORKER_S3_BUCKET` | (none) | S3 bucket name for S3-tiered blobs. |
| `TIKA_WORKER_S3_ENDPOINT_URL` | (none) | S3 endpoint URL (for MinIO or compatible). |
| `TIKA_WORKER_S3_REGION` | (none) | S3 region name. |

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

| Package | Extra | Purpose |
|---|---|---|
| VectorChord-BM25 (`vchord_bm25`) | — | BM25 ranking extension for PostgreSQL. Enables relevance-ranked full-text search as an alternative to tsvector ranking. |
| pg_tokenizer | — | Text tokenization for BM25 (language-specific stemmers and vocabulary mapping). |
| `httpx>=0.24` | `tika` | HTTP client for Tika communication. Required for text extraction. |
| `boto3>=1.26` | `tika-s3` | AWS SDK for S3-tiered blob access. Only needed when blobs are stored in S3. |

Install extras with: `pip install plone.pgcatalog[tika]` or
`pip install plone.pgcatalog[tika-s3]`.

## Console Scripts

| Command | Description |
|---|---|
| `pgcatalog-tika-worker` | Standalone text extraction worker. Requires `TIKA_WORKER_DSN` and `TIKA_WORKER_URL` environment variables. See {doc}`../how-to/enable-tika-extraction`. |

## Docker Images

| Image | Use Case |
|---|---|
| `postgres:17` | Standard PostgreSQL with tsvector-based full-text ranking. |
| `tensorchord/vchord-suite:pg17-latest` | PostgreSQL with VectorChord-BM25 and pg_tokenizer pre-installed. |
| `apache/tika:latest` | Apache Tika server for text extraction from PDFs, Office docs, and images. Stateless, no persistent storage needed. |
