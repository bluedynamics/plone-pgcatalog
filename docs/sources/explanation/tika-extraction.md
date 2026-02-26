<!-- diataxis: explanation -->

# Tika Text Extraction Architecture

## The Problem

Plone's default search indexes text from rich-text fields: Title,
Description, and the HTML body of a Page or News Item. This works
because these fields contain plain text that Plone can read directly.

Binary files — PDFs, Word documents, spreadsheets, images — contain
text that is locked inside proprietary or compressed formats. Plone
cannot extract it natively. Without extraction, uploading a PDF titled
"Q4 Financial Report" makes it findable by title, but the 50 pages of
content inside the PDF are invisible to search.

Elasticsearch solves this with its Tika ingest pipeline. plone.pgcatalog
brings the same capability to PostgreSQL.

## Design Decisions

### Why Apache Tika?

Tika extracts text from over 1400 file formats via a single stateless
HTTP API. It handles PDFs (including scanned ones via Tesseract OCR),
Office documents, OpenDocument formats, images, and more. It is the
same technology Elasticsearch uses internally.

### Why PostgreSQL as the Job Queue?

Redis or RabbitMQ would add operational complexity. Since plone.pgcatalog
already depends on PostgreSQL, we use it as the queue too:

- **Transactional enqueue**: Jobs are inserted in the same transaction
  as the ZODB commit. If the transaction rolls back, the job disappears
  too. No orphaned jobs.
- **LISTEN/NOTIFY**: PostgreSQL's built-in pub/sub wakes the worker
  instantly when a new job arrives. No polling delay.
- **SKIP LOCKED**: Multiple workers can dequeue safely without
  contention. Each worker claims one job at a time; others skip locked
  rows.
- **Visibility**: Queue state is queryable via standard SQL. No
  separate monitoring infrastructure needed.

### Why Asynchronous?

Text extraction is slow — a large PDF can take seconds. Running it
synchronously during `catalog_object()` would block the Zope request
thread, making content saves unacceptably slow. The asynchronous
approach keeps the synchronous path fast (Title/Description/body are
indexed immediately) while extraction runs in the background.

### Why Not Store the Full Extracted Text?

The extracted text is not stored as a column. Instead, it is
transformed into a tsvector (and optionally BM25 vectors) and merged
into the existing `searchable_text` column. This is more space-efficient
and matches how PostgreSQL full-text search works: the search engine
operates on tsvectors, not raw text.

## Data Flow

```{mermaid}
sequenceDiagram
    participant Plone as Plone (catalog_object)
    participant Proc as CatalogStateProcessor
    participant PG as PostgreSQL
    participant Worker as TikaWorker
    participant Tika as Apache Tika

    Plone->>Proc: process(zoid, state)
    Note over Proc: Extract content_type<br/>from primary field
    Proc->>Proc: Accumulate candidate<br/>if extractable type
    Plone->>Proc: finalize(cursor)
    Proc->>PG: SELECT blob_state WHERE zoid IN (...)
    PG-->>Proc: rows with blob data
    Proc->>PG: INSERT INTO text_extraction_queue
    Note over PG: NOTIFY trigger fires

    PG-->>Worker: NOTIFY text_extraction_ready
    Worker->>PG: UPDATE ... FOR UPDATE SKIP LOCKED<br/>RETURNING job
    Worker->>PG: SELECT data FROM blob_state
    PG-->>Worker: blob bytes
    Worker->>Tika: PUT /tika (blob bytes)
    Tika-->>Worker: extracted text
    Worker->>PG: SELECT pgcatalog_merge_extracted_text(zoid, text)
    Worker->>PG: UPDATE status = 'done'
```

### Step-by-Step

1. **catalog_object()** extracts the object's MIME content type via
   `extract_content_type()` (tries `IPrimaryFieldInfo` first, then
   `content_type` attribute). The content type is included in the
   pending annotation.

2. **CatalogStateProcessor.process()** checks if `PGCATALOG_TIKA_URL`
   is set and the content type is in the extractable set. If so, the
   zoid is added to `self._tika_candidates`.

3. **CatalogStateProcessor.finalize()** runs in the same PostgreSQL
   transaction as the ZODB commit. It queries `blob_state` to find which
   candidates actually have blobs, then inserts jobs into
   `text_extraction_queue`. An `ON CONFLICT DO NOTHING` clause makes
   this idempotent.

4. The **NOTIFY trigger** on the queue table fires, sending a
   `text_extraction_ready` notification with the job ID.

5. The **TikaWorker** receives the notification (or wakes up on its
   poll interval). It dequeues one job using
   `UPDATE ... FOR UPDATE SKIP LOCKED RETURNING`, which atomically
   claims the job. Other workers skip this row.

6. The worker **fetches the blob** from `blob_state` (PG bytea) or S3
   (for S3-tiered blobs above the size threshold).

7. The worker sends the blob to **Tika** via `PUT /tika` with the
   content type header. Tika returns plain text.

8. The worker calls **`pgcatalog_merge_extracted_text(zoid, text)`**,
   a PL/pgSQL function that appends the extracted text to the
   existing `searchable_text` tsvector at weight `C`. When BM25 is
   active, the function also rebuilds BM25 vectors with the
   Title/Description/extracted text combined.

9. The job status is updated to `done`. On failure, the job returns
   to `pending` (up to `max_attempts` retries).

## Weight Hierarchy

The `searchable_text` tsvector uses PostgreSQL's four weight classes
to rank content by importance:

| Weight | Content | BM25 Boost | Source |
|--------|---------|-----------|--------|
| **A** | Title | 3x (repeated 3 times) | Synchronous (catalog_object) |
| **B** | Description | 1x | Synchronous (catalog_object) |
| **C** | Extracted blob text | 1x | Asynchronous (Tika worker) |
| **D** | Rich-text body | 1x | Synchronous (catalog_object) |

A search for "quantum computing" ranks a document with that phrase in
the title higher than one where it only appears in an attached PDF.
PostgreSQL's `ts_rank_cd()` (and BM25's scoring) respect these weights
automatically.

## Queue Table

The `text_extraction_queue` table is created when `PGCATALOG_TIKA_URL`
is set. See {doc}`../reference/schema` for the full schema.

Key design choices:

- **UNIQUE(zoid, tid)**: Prevents duplicate jobs for the same object
  version.
- **Partial index on `status = 'pending'`**: Makes dequeue queries
  fast regardless of how many completed jobs exist.
- **NOTIFY trigger**: Fires on every INSERT, waking the worker
  instantly.
- **attempts/max_attempts**: Built-in retry with configurable limit
  (default: 3). Failed jobs stay visible for debugging.

## Worker Modes

### In-Process (Development)

When `PGCATALOG_TIKA_INPROCESS=true`, the worker runs as a daemon
thread inside the Zope process. It opens its own PostgreSQL connection
and HTTP client — it shares nothing with Zope's ZODB connections or
transaction machinery.

The thread is marked `daemon=True`, meaning it dies automatically when
the Zope process exits. No separate shutdown handling is needed.

This mode is convenient for development and small deployments. The
trade-off is that extraction work competes with Zope for CPU and memory.

### Standalone (Production)

The `pgcatalog-tika-worker` CLI runs as a separate process (or
container). It depends only on `psycopg` and `httpx` — no Zope, no
Plone, no ZODB. This makes it lightweight and easy to deploy.

Multiple workers can run concurrently. The `SKIP LOCKED` dequeue
pattern ensures each job is processed exactly once, even under
concurrent load.

## Image Indexing

Tika includes Tesseract OCR, which can extract text from images
(JPEG, PNG, TIFF, WebP, GIF). By default, plone.pgcatalog configures
all common image types as extractable.

This means that after enabling Tika:

- A photo of a whiteboard becomes searchable by the text on the board
- A scanned invoice becomes searchable by its content
- An infographic becomes searchable by its labels and annotations

Plone does not make image blobs searchable by default (there was no
extraction mechanism). With Tika, this happens automatically for all
Image content types that have blobs.

## Interaction with Existing Search

Enabling Tika does not change how existing search works:

- **Title and Description** are still indexed synchronously during
  `catalog_object()`, with immediate availability.
- **Rich-text body** (SearchableText from `portal_transforms`) is
  still indexed synchronously.
- **Tika extraction** adds to the existing tsvector asynchronously.
  There is a brief window (seconds to minutes, depending on queue
  depth and Tika processing time) where the blob content is not yet
  searchable.

Sites that do not set `PGCATALOG_TIKA_URL` see no change in behavior,
schema, or performance. The queue table is not even created.
