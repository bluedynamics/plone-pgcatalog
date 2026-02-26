# Async Tika Text Extraction for plone-pgcatalog

Optional async text extraction from binary files (PDF, DOCX, images, etc.) via Apache Tika,
using PostgreSQL as the job queue. Default behavior (portal_transforms) is unchanged.

## Infrastructure: Tika Server

Apache Tika runs as a **separate Docker container** — a stateless HTTP service that accepts
binary files and returns extracted text. It has no storage, no config, no state.

**Official Docker image**: [`apache/tika`](https://hub.docker.com/r/apache/tika) (~400MB, includes Tesseract OCR)

```bash
# Development
docker run -d --name tika -p 9998:9998 apache/tika:latest

# Production (docker-compose alongside PG, Plone, worker)
services:
  tika:
    image: apache/tika:latest
    restart: unless-stopped
    ports:
      - "9998:9998"
    # No volumes, no env vars needed — fully stateless
    # Optional: limit memory for large PDFs
    deploy:
      resources:
        limits:
          memory: 1G
```

The Tika server exposes a single relevant endpoint:
- `PUT /tika` — send binary data, get plain text back
- Supports 1400+ formats: PDF, DOCX, PPTX, ODT, RTF, etc.
- **Images**: Tika includes Tesseract OCR — extracts text from scanned documents,
  photographs of text, and EXIF/IPTC metadata (captions, keywords, descriptions)
  from any image format (JPEG, PNG, TIFF, WebP, etc.)
- Content-Type header hints the format (optional, Tika auto-detects)

**Deployment topology**: Tika only needs to be reachable by the **worker** (not by Plone/Zope).
In a docker-compose setup the worker connects to `http://tika:9998` on the internal network.
No public exposure needed.

```
                    +-----------+
                    | Plone/Zope|
                    +-----+-----+
                          | ZODB commit (enqueues job)
                          v
                    +-----+-----+
                    | PostgreSQL |  <-- blob_state + text_extraction_queue
                    +-----+-----+
                          ^
              LISTEN/NOTIFY + SKIP LOCKED
                          |
                    +-----+-----+        +----------+
                    |   Worker  | -----> |  Tika    |
                    | (sidecar) |  PUT   | (sidecar)|
                    +-----------+        +----------+
```

## Worker: Two Deployment Modes

The worker is a plain Python loop that opens **its own PostgreSQL connection** (via `psycopg`)
and **its own HTTP connection** to Tika (via `httpx`). It does not open any ports, does not
serve HTTP, and has no dependency on Zope, Plone, or ZCML. It only needs a DSN and a Tika URL.

### Mode 1: In-process background thread (development / simple sites)

When `PGCATALOG_TIKA_INPROCESS=true` is set, the existing `IDatabaseOpenedWithRoot`
subscriber in `startup.py` spawns a **daemon thread** inside the Zope process at startup.
This thread:

1. Opens its own `psycopg.connect(dsn, autocommit=True)` — completely independent
   from Zope's ZODB connections
2. Calls `LISTEN text_extraction_ready` on that connection
3. Loops: dequeue job via `SELECT ... FOR UPDATE SKIP LOCKED` on the
   `text_extraction_queue` table, fetch blob bytes, PUT to Tika, UPDATE searchable_text
4. Sleeps on `LISTEN`/poll between jobs

It shares the Zope process but nothing else — no Zope imports, no ZODB, no transaction
machinery. It is marked `daemon=True`, so it dies automatically when Zope shuts down.
The DSN is read from the storage object that `startup.py` already has access to.

**No extra port, no extra process, no extra container.** Just a background thread with
a PG connection doing SQL + HTTP.

### Mode 2: Standalone process / sidecar container (production)

For production deployments with heavier extraction load, the worker runs as its own process:

```bash
# CLI (same Python environment as Plone, or a minimal venv with psycopg + httpx)
TIKA_WORKER_DSN="dbname=zodb host=pg" \
TIKA_WORKER_URL="http://tika:9998" \
pgcatalog-tika-worker

# Or as a Docker sidecar (same image as Plone, different entrypoint)
services:
  worker:
    image: my-plone-image:latest
    command: ["pgcatalog-tika-worker"]
    environment:
      TIKA_WORKER_DSN: "dbname=zodb host=pg user=zodb password=zodb"
      TIKA_WORKER_URL: "http://tika:9998"
      # Optional, only if blobs are in S3:
      TIKA_WORKER_S3_BUCKET: "my-blobs"
      TIKA_WORKER_S3_ENDPOINT_URL: "https://s3.amazonaws.com"
    depends_on:
      - pg
      - tika
```

The standalone worker behaves identically to the in-process thread — same code path,
same `TikaWorker` class. The only difference is the entry point and where it gets
its configuration (env vars instead of reading from the storage object).

Advantages of standalone mode:
- Scales independently (run N workers for parallel extraction)
- Doesn't share CPU/memory with Zope
- Can run on a different machine entirely (only needs PG + Tika network access)
- Restarts independently of Zope

## Plone Image Indexing

By default, Plone's `SearchableText` indexer for `Image` content only includes
Title and Description — the image blob itself is never passed through text extraction.
This made sense historically (no extraction mechanism available), but with Tika
enabled, images become searchable:

- **OCR**: scanned documents, photos of whiteboards, text in screenshots
- **EXIF metadata**: camera model, GPS location, date taken
- **IPTC/XMP**: captions, keywords, copyright — commonly used by photographers
  and stock photo libraries

When `PGCATALOG_TIKA_URL` is set, the enqueue logic treats **all blobs** as
extraction candidates by default, including `image/*` content types. The default
`PGCATALOG_TIKA_CONTENT_TYPES` includes:

```
application/pdf, application/msword,
application/vnd.openxmlformats-officedocument.*,
application/vnd.oasis.opendocument.*,
application/rtf,
image/jpeg, image/png, image/tiff, image/webp, image/gif
```

This means uploading an Image in Plone will:
1. Index Title + Description synchronously (as before)
2. Enqueue the image blob for async Tika extraction
3. Worker sends image to Tika → OCR + metadata extraction → text merged into
   `searchable_text` tsvector + BM25 columns

No custom `SearchableText` indexer adapter is needed — the blob extraction
happens entirely on the worker side, bypassing Plone's indexer chain.

## How It Works

```
Plone catalog_object()  [synchronous, at commit time]
  |
  |-- 1. Extract Title (weight A), Description (weight B), RichText body (weight D)
  |      -> searchable_text tsvector + BM25 columns
  |      (unchanged — object is immediately searchable by these fields)
  |
  |-- 2. if PGCATALOG_TIKA_URL set AND object has a blob with extractable content_type:
  |        INSERT INTO text_extraction_queue (same transaction — atomic with commit)
  v
text_extraction_queue (PG table)
  |-- NOTIFY trigger wakes worker instantly
  v
TikaWorker  [async, seconds later]
  |-- SELECT ... FOR UPDATE SKIP LOCKED (dequeue)
  |-- Fetch blob: PG bytea or S3 (reads blob_state directly)
  |-- HTTP PUT to Tika server -> get plain text
  |-- pgcatalog_merge_extracted_text(zoid, text):
  |     MERGE into existing tsvector with weight 'C' (between Description and body)
  |     Re-tokenize BM25 columns with Title(3x) + Description + extracted text
  |     (concatenates — never overwrites the synchronous Title/Description/body)
  v
Object now also searchable by blob content (PDF text, OCR, metadata)
```

**Weight hierarchy for BM25 relevance ranking:**

| Weight | Source | Indexed | Boost |
|--------|--------|---------|-------|
| A | Title | synchronous | highest (3x in BM25) |
| B | Description | synchronous | high |
| C | Tika-extracted blob text | async (worker) | medium |
| D | RichText body | synchronous | base |

A search match in the Title always ranks higher than a match on page 47 of a PDF.
The object is findable immediately by Title/Description; blob text appears seconds later.

## Configuration

| Variable | Where | Default | Purpose |
|----------|-------|---------|---------|
| `PGCATALOG_TIKA_URL` | plone-pgcatalog env | empty (disabled) | Tika server URL, e.g. `http://localhost:9998` |
| `PGCATALOG_TIKA_CONTENT_TYPES` | plone-pgcatalog env | PDF, Office, ODF, images | CSV of MIME types to extract (includes `image/*` for OCR) |
| `PGCATALOG_TIKA_INPROCESS` | plone-pgcatalog env | false | Start worker thread inside Zope |
| `TIKA_WORKER_DSN` | worker env | required | PG connection string |
| `TIKA_WORKER_URL` | worker env | required | Tika server URL |
| `TIKA_WORKER_S3_BUCKET` | worker env | empty | S3 bucket (for S3-tiered blobs) |
| `TIKA_WORKER_S3_ENDPOINT_URL` | worker env | empty | S3 endpoint |
| `TIKA_WORKER_S3_REGION` | worker env | empty | S3 region |
| `TIKA_WORKER_CONCURRENCY` | worker env | 2 | Worker threads |
| `TIKA_WORKER_POLL_INTERVAL` | worker env | 5 | Fallback poll seconds |

## Queue Table Schema (opt-in, created when PGCATALOG_TIKA_URL is set)

```sql
CREATE TABLE IF NOT EXISTS text_extraction_queue (
    id           BIGSERIAL PRIMARY KEY,
    zoid         BIGINT NOT NULL,
    tid          BIGINT NOT NULL,
    content_type TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(zoid, tid)
);

-- Partial index for fast dequeue
CREATE INDEX IF NOT EXISTS idx_teq_pending
    ON text_extraction_queue (id) WHERE status = 'pending';

-- Instant worker wakeup
CREATE OR REPLACE FUNCTION notify_extraction_ready() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('text_extraction_ready', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_extraction ON text_extraction_queue;
CREATE TRIGGER trg_notify_extraction
    AFTER INSERT ON text_extraction_queue
    FOR EACH ROW EXECUTE FUNCTION notify_extraction_ready();
```

## PL/pgSQL Merge Function

Installed by the active backend's `get_extraction_update_sql()`. The worker calls
`SELECT pgcatalog_merge_extracted_text(zoid, text)` -- doesn't need to know
which backend is active.

- **TsvectorBackend**: appends extracted text as tsvector weight 'C'
- **BM25Backend**: appends tsvector weight 'C' AND re-tokenizes BM25 columns
  with Title(3x boost) + Description + extracted text

## File Changes

### Modified files

1. **`src/plone/pgcatalog/schema.py`** -- Add `TEXT_EXTRACTION_QUEUE` DDL constant + NOTIFY trigger

2. **`src/plone/pgcatalog/extraction.py`** -- Add `extract_content_type(wrapper)`:
   extracts MIME type from primary field (IPrimaryFieldInfo) or `content_type` attr

3. **`src/plone/pgcatalog/catalog.py`** -- In `_set_pg_annotation()`, call
   `extract_content_type(wrapper)` and include `content_type` in pending dict

4. **`src/plone/pgcatalog/processor.py`**:
   - `process()`: accumulate tika candidates on `self._tika_candidates` when
     `PGCATALOG_TIKA_URL` is set and pending has `content_type`
   - `finalize(cursor)`: query `blob_state` for committed zoids, INSERT matching
     ones into `text_extraction_queue` (ON CONFLICT DO NOTHING for idempotency)
   - `get_schema_sql()`: conditionally include queue DDL + merge function DDL

5. **`src/plone/pgcatalog/backends.py`** -- Add `get_extraction_update_sql()` to
   both `TsvectorBackend` and `BM25Backend`, returning
   `CREATE OR REPLACE FUNCTION pgcatalog_merge_extracted_text(...)` PL/pgSQL

6. **`src/plone/pgcatalog/startup.py`** -- Add `_start_inprocess_worker()`,
   called from `register_catalog_processor()` when `PGCATALOG_TIKA_INPROCESS=true`

7. **`pyproject.toml`** -- Add `tika` extras: `httpx` (required), `boto3` (optional S3).
   Add console_scripts entry: `pgcatalog-tika-worker = plone.pgcatalog.tika_worker:main`

### New files

8. **`src/plone/pgcatalog/tika_worker.py`** -- Standalone worker module (Zope-free).
   Dependencies: `psycopg`, `httpx`, optionally `boto3`.
   - `TikaWorker` class: `run()` loop with LISTEN/NOTIFY + poll fallback
   - `_process_one()`: dequeue via `UPDATE ... SKIP LOCKED RETURNING`
   - `_fetch_blob()`: read from PG bytea or S3 based on `blob_state` row
   - `_extract()`: HTTP PUT to Tika, return text
   - `_update_searchable_text()`: call `pgcatalog_merge_extracted_text(zoid, text)`
   - `main()`: CLI entry point with signal handling
   - Retry logic: failed jobs go back to pending until max_attempts reached

9. **`tests/test_tika_worker.py`** -- Worker tests:
   - Mock Tika HTTP for extraction
   - Test PG bytea + S3 blob fetch paths
   - Test dequeue with SKIP LOCKED (concurrent safety)
   - Test retry/failure logic
   - Integration: enqueue -> worker -> verify searchable_text updated

10. **`tests/test_tika_enqueue.py`** -- Enqueue tests:
    - Verify finalize() enqueues when TIKA_URL set + blob exists
    - Verify non-blob objects are NOT enqueued
    - Verify content_type filtering
    - Verify UNIQUE constraint prevents duplicates

## Implementation Order

1. Schema + merge function DDL (schema.py + backends.py)
2. Content type extraction (extraction.py + catalog.py)
3. Enqueue logic (processor.py)
4. Worker module (tika_worker.py)
5. In-process thread (startup.py)
6. Packaging (pyproject.toml)
7. Tests

## Verification

1. **Unit tests**: `pytest tests/test_tika_worker.py tests/test_tika_enqueue.py`
2. **Integration test with real Tika**:
   ```bash
   docker run -d --name tika -p 9998:9998 apache/tika:latest
   PGCATALOG_TIKA_URL=http://localhost:9998 pytest tests/test_tika_worker.py -k integration
   ```
3. **Manual E2E**: Start Zope with `PGCATALOG_TIKA_URL` + `PGCATALOG_TIKA_INPROCESS=true`,
   upload a PDF, verify `SELECT searchable_text FROM object_state WHERE zoid = ...`
   gets populated within seconds
4. **Standalone worker**: `pgcatalog-tika-worker` with env vars, upload PDF via Plone,
   verify worker logs extraction and tsvector appears
5. **Existing tests still pass**: `pytest tests/` (688 tests) -- no regressions
