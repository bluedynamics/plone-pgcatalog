<!-- diataxis: how-to -->

# Enable Tika Text Extraction

## Overview

By default, Plone indexes text from rich-text fields (Title, Description,
body) into `searchable_text`. Binary content — PDFs, Word documents, Excel
spreadsheets, images — is not searchable because Plone cannot extract text
from them.

Apache Tika is a stateless HTTP service that extracts text from over 1400
file formats, including OCR for images via Tesseract. When enabled,
plone.pgcatalog enqueues binary content for asynchronous extraction via a
PostgreSQL job queue. A background worker sends each blob to Tika and
merges the extracted text into the object's `searchable_text` tsvector
(and BM25 columns, if active).

This feature is entirely opt-in. Without `PGCATALOG_TIKA_URL`, behavior
is unchanged.

## Step 1: Start Apache Tika

### Docker (recommended)

```bash
docker run -d --name tika \
  -p 9998:9998 \
  apache/tika:latest
```

Verify it is running:

```bash
curl -s http://localhost:9998/tika
# Should return an HTML page listing supported formats
```

### Docker Compose

If you use the zodb-pgjsonb example setup, Tika is available as a profile:

```bash
docker compose --profile tika up -d tika
```

### Production

In production, Tika should run as a separate service (or sidecar container)
accessible from the Zope/worker processes. Tika is stateless and needs no
persistent storage. A single Tika instance handles concurrent requests from
multiple workers.

Typical resource allocation: 512 MB–1 GB RAM. For OCR-heavy workloads
(images, scanned PDFs), allocate more.

## Step 2: Configure Environment Variables

Set `PGCATALOG_TIKA_URL` before starting Zope:

```bash
export PGCATALOG_TIKA_URL=http://localhost:9998
```

This single variable enables the entire extraction pipeline:

- The queue table (`text_extraction_queue`) and merge function are created
  at startup
- The `CatalogStateProcessor` starts enqueuing extraction jobs for objects
  with extractable binary content

### Optional: Customize Content Types

By default, the following MIME types are sent to Tika:

- `application/pdf`
- `application/msword`
- `application/vnd.openxmlformats-officedocument.wordprocessingml.document`
- `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
- `application/vnd.openxmlformats-officedocument.presentationml.presentation`
- `application/vnd.oasis.opendocument.text`
- `application/vnd.oasis.opendocument.spreadsheet`
- `application/rtf`
- `image/jpeg`, `image/png`, `image/tiff`, `image/webp`, `image/gif`

Override with a comma-separated list:

```bash
export PGCATALOG_TIKA_CONTENT_TYPES=application/pdf,application/msword,image/jpeg
```

## Step 3: Start the Extraction Worker

The worker dequeues jobs, fetches blobs, sends them to Tika, and writes
extracted text back to PostgreSQL. There are two modes:

### Option A: In-Process Worker (Development)

Add a second environment variable to run the worker as a daemon thread
inside the Zope process:

```bash
export PGCATALOG_TIKA_URL=http://localhost:9998
export PGCATALOG_TIKA_INPROCESS=true
```

The thread starts automatically on Zope startup. It shares nothing with
Zope's ZODB connections — it opens its own PostgreSQL connection and HTTP
client. The thread is marked `daemon=True`, so it stops when Zope shuts
down.

This mode is convenient for development but uses Zope's process resources.
For production, use the standalone worker.

### Option B: Standalone Worker (Production)

Run the worker as a separate process or container:

```bash
export TIKA_WORKER_DSN="dbname=zodb host=localhost port=5432 user=zodb password=zodb"
export TIKA_WORKER_URL=http://tika:9998
pgcatalog-tika-worker
```

The standalone worker:

- Connects directly to PostgreSQL (no Zope dependency)
- Uses `LISTEN`/`NOTIFY` for instant wakeup on new jobs
- Falls back to polling every `TIKA_WORKER_POLL_INTERVAL` seconds (default: 5)
- Uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent dequeuing
- Handles `SIGTERM`/`SIGINT` for graceful shutdown

For S3-tiered blobs:

```bash
export TIKA_WORKER_S3_BUCKET=zodb-blobs
export TIKA_WORKER_S3_ENDPOINT_URL=http://minio:9000
export TIKA_WORKER_S3_REGION=us-east-1
```

See {doc}`../reference/configuration` for the full list of worker
environment variables.

## Step 4: Rebuild the Catalog

A full reindex is needed to enqueue extraction jobs for existing objects:

1. Go to ZMI > portal_catalog > Advanced tab
2. Click "Clear and Rebuild"

Or via script:

```python
catalog = portal.portal_catalog
catalog.clearFindAndRebuild()
import transaction; transaction.commit()
```

After the rebuild, the worker processes enqueued jobs. You can monitor
progress:

```sql
-- Pending jobs
SELECT COUNT(*) FROM text_extraction_queue WHERE status = 'pending';

-- Completed jobs
SELECT COUNT(*) FROM text_extraction_queue WHERE status = 'done';

-- Failed jobs
SELECT * FROM text_extraction_queue WHERE status = 'failed';
```

## Step 5: Verify Extraction

Upload a PDF via Plone and wait a few seconds. Then query:

```sql
SELECT searchable_text::text
FROM object_state
WHERE path LIKE '%/my-uploaded-file';
```

The tsvector should contain terms extracted from the PDF content (at
weight `C`), alongside the synchronous Title/Description terms (at
weights `A`/`B`).

## How It Fits with BM25

When BM25 is active, the merge function also updates per-language BM25
columns. Title gets 3x boosting (weight `A`), Description gets weight
`B`, and extracted blob text gets weight `C`. This means a search for
"quantum computing" ranks a document with "quantum computing" in the
title higher than one that only mentions it in an attached PDF — exactly
the right behavior.

See {doc}`../explanation/tika-extraction` for a detailed architecture
explanation.

## Disabling Extraction

Remove `PGCATALOG_TIKA_URL` from the environment and restart Zope.
The queue table remains but no new jobs are enqueued. Existing
`searchable_text` values are preserved.

To clean up the queue table:

```sql
DROP TABLE IF EXISTS text_extraction_queue CASCADE;
```
