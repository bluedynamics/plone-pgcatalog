<!-- diataxis: explanation -->

# Tika text extraction architecture

## The problem

Plone's default search indexes text from rich-text fields: Title,
Description, and the HTML body of a Page or News Item.
This works
because these fields contain plain text that Plone can read directly.

Binary files—PDFs, Word documents, spreadsheets, images—contain
text that is locked inside proprietary or compressed formats.
Plone
cannot extract it natively.
Without extraction, uploading a PDF titled
"Q4 Financial Report" makes it findable by title, but the 50 pages of
content inside the PDF are invisible to search.

Elasticsearch solves this with its Tika ingest pipeline. plone.pgcatalog
brings the same capability to PostgreSQL.

## Design decisions

### Why Apache Tika?

Tika extracts text from over 1400 file formats via a single stateless
HTTP API.
It handles PDFs (including scanned ones via Tesseract OCR),
Office documents, OpenDocument formats, images, and more.
It is the
same technology Elasticsearch uses internally.

### Why PostgreSQL as the job queue?

Redis or RabbitMQ would add operational complexity.
Since plone.pgcatalog
already depends on PostgreSQL, we use it as the queue too:

- **Transactional enqueue**: Jobs are inserted in the same transaction
  as the ZODB commit.
  If the transaction rolls back, the job disappears
  too.
  No orphaned jobs.
- **LISTEN/NOTIFY**: PostgreSQL's built-in pub/sub wakes the worker
  instantly when a new job arrives.
  No polling delay.
- **SKIP LOCKED**: Multiple workers can dequeue safely without
  contention.
  Each worker claims one job at a time; others skip locked
  rows.
- **Visibility**: Queue state is queryable via standard SQL. No
  separate monitoring infrastructure needed.

### Why asynchronous?

Text extraction is slow—a large PDF can take seconds.
Running it
synchronously during `catalog_object()` would block the Zope request
thread, making content saves unacceptably slow.
The asynchronous
approach keeps the synchronous path fast (Title/Description/body are
indexed immediately) while extraction runs in the background.

### Why not store the full extracted text?

The extracted text is not stored as a column.
Instead, it is
transformed into a tsvector (and optionally BM25 vectors) and merged
into the existing `searchable_text` column.
This is more space-efficient
and matches how PostgreSQL full-text search works: the search engine
operates on tsvectors, not raw text.

(tika-why-rmeta)=

### Why the worker uses `/rmeta/text`

Tika offers two ways to extract text, and the obvious one, `PUT /tika`, loses information in two ways.

It returns body text only.
An image without OCR has no body text, so it contributed nothing, even when its file carried a caption.

On Tika 4, it also returns Markdown.
Heading and list markers do no harm, because PostgreSQL's text search drops them as punctuation.
Link syntax does harm, because it puts the link target into the text.
The sentence `Siehe [die Akte](https://example.org/akte).` becomes five lexemes in the index, including `example.org` and `/akte).`, where the plain sentence gives two.
Every link in a document added junk to the full-text index.

`PUT /rmeta/text` returns JSON with one entry per document: the container first, then every embedded document.
Its body text is plain on both major Tika versions, and the metadata comes alongside.
The worker joins every entry's body text and adds a short whitelist of the container's metadata, set by `PGCATALOG_TIKA_METADATA_FIELDS`.
Embedded documents' metadata stays out, because the title of an attachment inside a PDF is rarely about the object being cataloged.

Two details are easy to get wrong.
The key holding the body text changed between major versions: `tk:content` on Tika 4, `X-TIKA:content` on Tika 3.
A worker reading only the old key gets empty text from every document, which looks like documents without text rather than like a bug, so the worker reads both.
And joining the entries does not count anything twice, because the container entry holds only the embedded files' names.
Word order does differ from `/tika`, which interleaved each name with its content, so phrase proximity in `searchable_text` differs while the set of terms does not.

(tika-why-deferral)=

### Why a missing Tika does not cost an attempt

A typical deployment runs a single Tika replica.
When that pod restarts, the service has no endpoint for about half a minute.
Requests fail with `Connection refused`, or with `Operation not permitted` on clusters whose network layer rejects connections to a service without backends.

Counting those failures as attempts made the queue fragile.
Three attempts went by within a second, every job claimed during the restart ended up `failed`, and someone had to reset them by hand.
On one production site, 1703 of 1708 failed jobs had never reached Tika at all.

A failure to connect says nothing about the document.
The worker therefore treats connection-level errors differently from everything else: it sets the job back to `pending`, pushes `not_before` forward by 5, then 30, then 120 seconds, and leaves `attempts` untouched.
Any other error still spends an attempt, so a document that Tika genuinely cannot parse still ends up `failed`.

The database fires a notification only when a job is inserted, not when a deferral runs out.
A deferred job is therefore picked up by the worker's polling fallback, which makes that polling essential rather than a nicety.

(tika-why-two-checks)=

### Why the allowlist is checked twice

`PGCATALOG_TIKA_CONTENT_TYPES` decides which blobs reach Tika.
It used to be checked once, when a job was queued.

That left two holes.
Narrowing the allowlist did not affect jobs already in the queue, so they were extracted anyway.
And any job set back to `pending` by hand bypassed the allowlist entirely.
On one site, more than a thousand failed jobs were images queued before images were excluded, and the natural recovery statement would have sent every one of them back to Tika.

The worker now checks the allowlist again when it claims a job, before it fetches the blob, which is where the cost lies.
A job whose type is not allowed becomes `skipped`, a terminal status separate from `failed`.
Both checks use one shared definition, so they cannot disagree about what is allowed or about what an unset variable means.

The standalone worker reads the same `PGCATALOG_TIKA_CONTENT_TYPES` as the Zope processes rather than a variable of its own.
Zope and the worker may reach Tika at different addresses, which is why the URL has two names.
The allowlist must never differ, and two names for one policy would invite exactly the drift this check removes.

## Data flow

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
    Worker->>Tika: PUT /rmeta/text (blob bytes)
    Tika-->>Worker: JSON, one entry per document
    Worker->>PG: SELECT pgcatalog_merge_extracted_text(zoid, text)
    Worker->>PG: UPDATE status = 'done'
```

### Step-by-Step

1. **catalog_object()** extracts index data including the `mime_type`
   catalog index (from the Plone `mime_type` FieldIndex).  The MIME
   type is stored in the `idx` JSONB as part of the pending annotation.

2. **CatalogStateProcessor.process()** reads `idx["mime_type"]` from
   the pending data and checks if `PGCATALOG_TIKA_URL` is set and the
   MIME type is in the extractable set. If so, the zoid is added to
   `self._tika_candidates`.

3. **CatalogStateProcessor.finalize()** runs in the same PostgreSQL
   transaction as the ZODB commit.
   It queries `blob_state` to find which
   candidates actually have blobs, then inserts jobs into
   `text_extraction_queue`.
   An `ON CONFLICT DO NOTHING` clause makes
   this idempotent.

4.
The **NOTIFY trigger** on the queue table fires, sending a
   `text_extraction_ready` notification with the job ID.

5.
The **TikaWorker** receives the notification (or wakes up on its
   poll interval).
   It dequeues one job using
   `UPDATE ...
   FOR UPDATE SKIP LOCKED RETURNING`, which atomically
   claims the job.
   Other workers skip this row.

6.
The worker **fetches the blob** from `blob_state` (PG bytea) or S3
   (for S3-tiered blobs above the size threshold).

7.
The worker sends the blob to **Tika** via `PUT /rmeta/text` with the
   content type header.
   Tika returns JSON with one entry per document: the container first, then each embedded document.
   The worker joins every entry's body text and adds a whitelist of the container's metadata, such as `dc:description`.
   See {ref}`tika-why-rmeta`.

8.
The worker calls **`pgcatalog_merge_extracted_text(zoid, text)`**,
   a PL/pgSQL function that appends the extracted text to the
   existing `searchable_text` tsvector at weight `C`.
   When BM25 is
   active, the function also rebuilds BM25 vectors with the
   Title/Description/extracted text combined.

9.
The job status is updated to `done`.
   If Tika cannot be reached, the job returns to `pending` with `not_before` pushed forward and without spending an attempt.
   Any other failure returns it to `pending` and spends one of its `max_attempts`.
   See {ref}`tika-why-deferral`.

## Weight hierarchy

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

## Queue table

The `text_extraction_queue` table is created when `PGCATALOG_TIKA_URL`
is set.
See {doc}`../reference/schema` for the full schema.

Key design choices:

- **UNIQUE(blob_zoid, tid)**: Prevents duplicate jobs for the same blob
  version.
  A full reindex replaces `searchable_text` with the indexer's text, which
  does not contain the extracted text.
  It therefore sets a finished job for the same blob version back to
  `pending`, so the text is extracted again.
  The worker only finishes a job that is still in `processing`, so a
  reindex during extraction cannot leave the job `done` without its text.
  Re-queued jobs are picked up by the worker's poll, since the NOTIFY
  trigger fires on INSERT only.
  [#247](https://github.com/bluedynamics/plone-pgcatalog/issues/247) plans
  to store the extracted text instead, so an edit no longer costs an
  extraction.
- **Partial index on `(not_before, id) WHERE status = 'pending'`**: Makes
  dequeue queries fast regardless of how many completed jobs exist, and
  lets the dequeue skip jobs that are deferred until later.
- **NOTIFY trigger**: Fires on every INSERT, waking the worker
  instantly.
- **attempts/max_attempts**: Built-in retry with configurable limit
  (default: 3) for failures that concern the document itself.
  Failed jobs stay visible for debugging.
- **not_before/deferrals**: Transport failures defer the job instead of
  spending an attempt.
- **skipped**: A terminal status for jobs refused on purpose, distinct
  from `failed`.
  See {ref}`queue-status-values`.

## Worker modes

### In-process (development)

When `PGCATALOG_TIKA_INPROCESS=true`, the worker runs as a daemon
thread inside the Zope process.
It opens its own PostgreSQL connection
and HTTP client—it shares nothing with Zope's ZODB connections or
transaction machinery.

The thread is marked `daemon=True`, meaning it dies automatically when
the Zope process exits.
No separate shutdown handling is needed.

This mode is convenient for development and small deployments.
The
trade-off is that extraction work competes with Zope for CPU and memory.

### Standalone (production)

The `pgcatalog-tika-worker` CLI runs as a separate process (or
container).
It depends only on `psycopg` and `httpx`—no Zope, no
Plone, no ZODB.
This makes it lightweight and easy to deploy.

Multiple workers can run concurrently.
The `SKIP LOCKED` dequeue
pattern ensures each job is processed exactly once, even under
concurrent load.

## Image indexing

What an image contributes depends on whether the Tika service can run optical character recognition (OCR).

The stock `apache/tika` image has no OCR engine.
It still parses images, but only for their metadata, and that is cheap: without OCR, Tika never decodes an image's pixels.
A 165 megapixel JPEG costs about 0.2 seconds and a few tens of MiB, measured at container memory limits from 512 MiB to 2 GiB.
Keeping image types in the default allowlist therefore costs little.
It also pays off, because the worker harvests the image's metadata.
An EXIF caption arrives as `dc:description` and becomes searchable without any OCR.

The `-full` image adds Tesseract, and then the text inside an image becomes searchable too:

- A photo of a whiteboard becomes searchable by the text on the board
- A scanned invoice becomes searchable by its content
- An infographic becomes searchable by its labels and annotations

OCR is a different cost class.
It renders the image to pixels, so memory grows with the pixel count rather than the file size.
A 165 megapixel photograph weighs only about 5 MB, yet on the `-full` image it filled a 1 GiB container and got Tesseract killed by the kernel.
A guard on file size cannot catch that, because the file is small.
Bounding OCR input by pixel count, using the smaller source images that plone.pgthumbor already produces, is tracked in [issue 241](https://github.com/bluedynamics/plone-pgcatalog/issues/241).

Plone does not make image blobs searchable by default, because it has no extraction mechanism.
With Tika, every Image content type with a blob contributes its metadata, and its text as well when OCR is available.

## Interaction with existing search

Enabling Tika does not change how existing search works:

- **Title and Description** are still indexed synchronously during
  `catalog_object()`, with immediate availability.
- **Rich-text body** (SearchableText from `portal_transforms`) is
  still indexed synchronously for non-File content types. For `IFile`
  objects, `portal_transforms` is skipped when Tika is active—the
  expensive `pdftotext`/`wv` calls and BFS graph traversal of the
  transform registry are avoided entirely. See
  {doc}`../how-to/custom-blob-searchabletext` for custom types.
- **Tika extraction** adds to the existing tsvector asynchronously.
  A brief window (seconds to minutes, depending on queue
  depth and Tika processing time) exists where the blob content is not yet
  searchable.

Sites that do not set `PGCATALOG_TIKA_URL` see no change in behavior,
schema, or performance.
The queue table is not even created.
