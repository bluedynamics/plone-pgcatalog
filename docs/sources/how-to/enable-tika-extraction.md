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

Cap the Java heap below the container's memory limit.
Then a document that exhausts memory fails with a Java `OutOfMemoryError` for that one request, instead of the kernel killing a process and Tika restarting:

```yaml
environment:
  JAVA_TOOL_OPTIONS: "-XX:MaxRAMPercentage=50"
```

Pin the image by digest as well as by tag.
A floating tag such as `latest` can change the major Tika version on any restart, and Apache republishes version tags too, so only the digest fixes the image you tested:

```yaml
image: apache/tika:4.1.0@sha256:<digest>
```

Look up the digest with `docker manifest inspect apache/tika:4.1.0`.

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

Matching ignores case and MIME parameters, so `application/pdf` also matches `APPLICATION/PDF` and `application/pdf; charset=binary`.

```{important}
If you run the standalone worker, set `PGCATALOG_TIKA_CONTENT_TYPES` on the worker too, to the same value.
The worker checks the allowlist again before it fetches each blob.
Without the variable it uses the default list, which includes image types, and logs a warning at startup.
```

### Optional: choose which metadata is indexed

Besides the body text, the worker merges a short list of Tika metadata fields into `searchable_text`.
By default these are the title, description, subject, creator, and keywords.
An image's EXIF caption arrives as `dc:description`, so photos become searchable by their captions even without OCR.

To change the list, set `PGCATALOG_TIKA_METADATA_FIELDS`:

```shell
export PGCATALOG_TIKA_METADATA_FIELDS=dc:title,dc:description
```

See {doc}`../reference/configuration` for the full list of settings.

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
export PGCATALOG_TIKA_CONTENT_TYPES=application/pdf,application/msword
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
export TIKA_WORKER_S3_ENDPOINT_URL=http://garage:3900
export TIKA_WORKER_S3_REGION=garage
export TIKA_WORKER_S3_ACCESS_KEY=...
export TIKA_WORKER_S3_SECRET_KEY=...
```

If `TIKA_WORKER_S3_ACCESS_KEY` / `TIKA_WORKER_S3_SECRET_KEY` are not set, the
worker leaves credential resolution to boto3's default provider chain (the
standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables,
`~/.aws/credentials`, or an instance/IAM role).

See {doc}`../reference/configuration` for the full list of worker
environment variables.

(ocr-for-images-and-scanned-pdfs)=

## OCR for images and scanned PDFs

OCR is **not** part of the default Tika image.
The minimal `apache/tika` image bundles no OCR engine, so the text *inside* images, photos, and scanned (image-only) PDFs is not extracted.
The queue row still completes as `done`, and an image still contributes its metadata, such as an EXIF caption, but the words visible in the picture do not reach `searchable_text`.

To enable OCR:

1. **Use the `-full` image**, which ships Tesseract and ImageMagick:

   ```bash
   docker run -d --name tika -p 9998:9998 apache/tika:3.2.3.0-full
   ```

2. **Configure OCR server-side.** The worker sends a plain `PUT /rmeta/text`
   with no OCR headers, so the OCR strategy and languages are set on the Tika
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
import transaction

transaction.commit()
```

A rebuild, like any full reindex of a file, also queues the extraction again for files that were extracted before, because the reindex replaces their searchable text.

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

(recover-failed-extractions)=

## Recover failed extractions

Find out what failed, and why, before you retry anything:

```sql
SELECT content_type, left(error, 70) AS error, count(*)
  FROM text_extraction_queue
 WHERE status = 'failed'
 GROUP BY 1, 2
 ORDER BY 3 DESC;
```

Errors such as `Connection refused` or `Operation not permitted` mean the job never reached Tika.
Current versions of the worker defer those jobs instead of failing them, so you see them only from older versions or from a long outage.

To retry, set the failed jobs back to `pending`:

```sql
UPDATE text_extraction_queue
   SET status = 'pending', attempts = 0, error = NULL
 WHERE status = 'failed';
```

The worker checks the allowlist when it claims each job.
Jobs whose content type is no longer allowed become `skipped` without their blob being fetched, so you do not need to exclude them in the statement.
This works only if the worker has the same `PGCATALOG_TIKA_CONTENT_TYPES` as Zope.

Retry while Tika is up and settled, not while a new version is being deployed.
Do not reset `skipped` jobs unless you have widened the allowlist, because the worker refuses them again.
See {ref}`queue-status-values` for what each status means.

(restore-lost-extracted-text)=

## Restore lost extracted text

Before plone.pgcatalog with the fix for [#244](https://github.com/bluedynamics/plone-pgcatalog/issues/244), a full reindex of a file dropped its extracted text and did not queue the extraction again.
A title edit, a workflow transition or "Clear and Rebuild" was enough.
Content that [zodb-pgjsonb#120](https://github.com/bluedynamics/zodb-pgjsonb/issues/120) had uncataloged and that was later edited normally is affected as well.
Such files are listed and found by title, but not by the words inside them.

`maintenance.requeue_lost_extractions()` finds them: their extraction job is `done`, but `searchable_text` has no text from Tika left.
It recatalogs them, which queues the extraction for the file versions they hold now.

Before you run it, make sure the worker has the same `PGCATALOG_TIKA_CONTENT_TYPES` as Zope and that Tika is pinned and settled.
Save this as `requeue_lost_extractions.py`:

```python
"""Run: zconsole run etc/zope.conf requeue_lost_extractions.py SITE_ID [--dry-run] [--include-failed]"""

from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SpecialUsers import system
from plone.pgcatalog.maintenance import requeue_lost_extractions
from zope.component.hooks import setSite

import sys
import transaction

# zconsole does not reset sys.argv: [zconsole, run, zope.conf, script, *args]
args = sys.argv[4:]
site_id = next(a for a in args if not a.startswith("--"))
dry_run = "--dry-run" in args

site = app[site_id]  # noqa: F821  (app is provided by zconsole)
setSite(site)
newSecurityManager(None, system)

result = requeue_lost_extractions(
    site.portal_catalog, dry_run=dry_run, include_failed="--include-failed" in args
)
for path in result.paths:
    print(path)
print(
    f"{result.checked} candidates, {len(result.paths)} "
    f"{'would be requeued' if dry_run else 'requeued'}, "
    f"{len(result.failed)} failed"
)
if dry_run:
    transaction.abort()
else:
    transaction.commit()
```

Count first with `--dry-run`, then run without it.
`--include-failed` also retries files whose job is `failed`.
It replaces the `UPDATE` statement in {ref}`the section above <recover-failed-extractions>` and only retries the file versions the content holds now, not replaced ones.

Files whose extraction legitimately produced no text, such as scanned PDFs or images without OCR, look the same as files whose text was lost.
They are queued again on every run, which costs a Tika call each but does no harm.
Telling them apart needs [#247](https://github.com/bluedynamics/plone-pgcatalog/issues/247).

## Disabling extraction

Remove `PGCATALOG_TIKA_URL` from the environment and restart Zope.
The queue table remains but no new jobs are enqueued.
Existing
`searchable_text` values are preserved.

To clean up the queue table:

```sql
DROP TABLE IF EXISTS text_extraction_queue CASCADE;
```
