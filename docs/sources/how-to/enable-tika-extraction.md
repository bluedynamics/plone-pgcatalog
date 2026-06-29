<!-- diataxis: how-to -->

# Enable Tika text extraction

## Overview

By default, Plone indexes text from rich-text fields (Title, Description,
body) into `searchable_text`.
Binary content—PDFs, Word documents, Excel
spreadsheets, images—is not searchable because Plone cannot extract text
from them.

Apache Tika is a stateless HTTP service that extracts text from over 1400
file formats.
Optical character recognition (OCR) for images and scanned
PDFs is available only with the `-full` Tika image and additional
configuration—see [OCR for images and scanned PDFs](#ocr-for-images-and-scanned-pdfs).
When enabled,
plone.pgcatalog enqueues binary content for asynchronous extraction via a
PostgreSQL job queue.
A background worker sends each blob to Tika and
merges the extracted text into the object's `searchable_text` tsvector
(and BM25 columns, if active).

This feature is entirely opt-in.
Without `PGCATALOG_TIKA_URL`, behavior
is unchanged.

## Step 1: start Apache Tika

### Docker (recommended)

```bash
docker run -d --name tika \
  -p 9998:9998 \
  apache/tika:3.2.3.0
```

Pin an explicit version rather than `:latest` so extraction behavior is
reproducible across deploys.
The minimal image above does **not** include
an OCR engine; for OCR use the `-full` image (for example
`apache/tika:3.2.3.0-full`)—see
[OCR for images and scanned PDFs](#ocr-for-images-and-scanned-pdfs).

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
accessible from the Zope/worker processes.
Tika is stateless and needs no
persistent storage.
A single Tika instance handles concurrent requests from
multiple workers.

Typical resource allocation: 512 MB–1 GB RAM.
OCR (the `-full` image) is
CPU- and memory-heavy and much slower per document; size the Tika service
accordingly and raise `TIKA_WORKER_HTTP_TIMEOUT` (see below) so large scanned
PDFs do not time out.

## Step 2: configure environment variables

Set `PGCATALOG_TIKA_URL` before starting Zope:

```bash
export PGCATALOG_TIKA_URL=http://localhost:9998
```

This single variable enables the entire extraction pipeline:

- The queue table (`text_extraction_queue`) and merge function are created
  at startup
- The `CatalogStateProcessor` starts enqueuing extraction jobs for objects
  with extractable binary content

### Optional: customize content types

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

## Step 3: start the extraction worker

The worker dequeues jobs, fetches blobs, sends them to Tika, and writes
extracted text back to PostgreSQL.
Two modes are available:

### Option A: in-process worker (development)

Add a second environment variable to run the worker as a daemon thread
inside the Zope process:

```bash
export PGCATALOG_TIKA_URL=http://localhost:9998
export PGCATALOG_TIKA_INPROCESS=true
```

The thread starts automatically on Zope startup.
It shares nothing with
Zope's ZODB connections—it opens its own PostgreSQL connection and HTTP
client.
The thread is marked `daemon=True`, so it stops when Zope shuts
down.

This mode is convenient for development but uses Zope's process resources.
For production, use the standalone worker.

### Option B: standalone worker (production)

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
- Waits up to `TIKA_WORKER_HTTP_TIMEOUT` seconds for each Tika response
  (default: 120; raise it for OCR of large scanned PDFs)
- Uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent dequeuing
- Handles `SIGTERM`/`SIGINT` for graceful shutdown

For S3-tiered blobs:

```bash
export TIKA_WORKER_S3_BUCKET=zodb-blobs
export TIKA_WORKER_S3_ENDPOINT_URL=http://minio:9000
export TIKA_WORKER_S3_REGION=us-east-1
export TIKA_WORKER_S3_ACCESS_KEY=...
export TIKA_WORKER_S3_SECRET_KEY=...
```

If `TIKA_WORKER_S3_ACCESS_KEY` / `TIKA_WORKER_S3_SECRET_KEY` are not set, the
worker leaves credential resolution to boto3's default provider chain (the
standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables,
`~/.aws/credentials`, or an instance/IAM role).

See {doc}`../reference/configuration` for the full list of worker
environment variables.

## OCR for images and scanned PDFs

OCR is **not** part of the default Tika image. The minimal `apache/tika`
image bundles no OCR engine, so plain images, photos, and scanned
(image-only) PDFs are extracted as **empty text**: the queue row still
completes as `done`, but no body lexemes are merged into `searchable_text`.
The full-text index then silently lacks their content.

To enable OCR:

1. **Use the `-full` image**, which ships Tesseract and ImageMagick:

   ```bash
   docker run -d --name tika -p 9998:9998 apache/tika:3.2.3.0-full
   ```

2. **Configure OCR server-side.** The worker sends a plain `PUT /tika` with
   no OCR headers, so the OCR strategy and languages are set on the Tika
   service via a mounted `tika-config.xml`—for example OCR language
   `deu+eng`, and a PDF `ocrStrategy` of `auto` (or `ocr_and_text`) so
   image-only PDFs are run through OCR. See the
   [Apache Tika OCR documentation](https://cwiki.apache.org/confluence/display/TIKA/TikaOCR).

3. **Budget for it.** OCR is CPU- and memory-heavy and much slower per
   document. Raise `TIKA_WORKER_HTTP_TIMEOUT` (default 120 s) so large
   multi-page scans do not time out and get marked `failed`.

If you do not need OCR, the minimal image is the better choice: it is
smaller, faster, and avoids the resource cost.

## Step 4: rebuild the catalog

A full reindex is needed to enqueue extraction jobs for existing objects:

1.
Go to ZMI > portal_catalog > Advanced tab
2.
Click "Clear and Rebuild"

Or via script:

```python
catalog = portal.portal_catalog
catalog.clearFindAndRebuild()
import transaction; transaction.commit()
```

After the rebuild, the worker processes enqueued jobs.
You can monitor
progress:

```sql
-- Pending jobs
SELECT COUNT(*) FROM text_extraction_queue WHERE status = 'pending';

-- Completed jobs
SELECT COUNT(*) FROM text_extraction_queue WHERE status = 'done';

-- Failed jobs
SELECT * FROM text_extraction_queue WHERE status = 'failed';
```

## Step 5: verify extraction

Upload a PDF via Plone and wait a few seconds.
Then query:

```sql
SELECT searchable_text::text
FROM object_state
WHERE path LIKE '%/my-uploaded-file';
```

The tsvector should contain terms extracted from the PDF content (at
weight `C`), alongside the synchronous Title/Description terms (at
weights `A`/`B`).

## How it fits with BM25

When BM25 is active, the merge function also updates per-language BM25
columns.
Title gets 3x boosting (weight `A`), Description gets weight
`B`, and extracted blob text gets weight `C`.
This means a search for
"quantum computing" ranks a document with "quantum computing" in the
title higher than one that only mentions it in an attached PDF—exactly
the right behavior.

See {doc}`../explanation/tika-extraction` for a detailed architecture
explanation.

## Disabling extraction

Remove `PGCATALOG_TIKA_URL` from the environment and restart Zope.
The queue table remains but no new jobs are enqueued.
Existing
`searchable_text` values are preserved.

To clean up the queue table:

```sql
DROP TABLE IF EXISTS text_extraction_queue CASCADE;
```
