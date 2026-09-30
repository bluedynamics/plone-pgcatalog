<!-- diataxis: explanation -->

# Tika: bounded renditions, a pixel guard, and metadata harvesting

Issue: [#222](https://github.com/bluedynamics/plone-pgcatalog/issues/222)
Date: 2026-09-30
Status: design, awaiting review

## Intent

Full-text extraction must not be able to take Tika down, and it must not
spend blob I/O on work that can never yield anything. Beyond that, the
policy for *what* gets sent should follow from the Tika deployment rather
than from an environment variable an operator has to keep in sync by hand.

Success looks like this:

- No OOM caused by a single catalogued object, whatever its size.
- No `failed` rows for transient reasons, so no manual SQL resets.
- Images contribute the text they actually carry, which without OCR means
  their captions and keywords rather than nothing at all.
- An operator who switches the Tika image between the stock and the `-full`
  variant does not have to change any pgcatalog setting.

## What the measurements showed

Measured 2026-09-30 against `apache/tika:3.2.3.0` and
`apache/tika:3.2.3.0-full`, both in a container limited to 1 GiB, matching
the production limit reported in #222. Reproduction recipe in the appendix.

| Image | Payload | Time | Chars | Peak RSS | OOM |
|---|---|---|---|---|---|
| stock | `control.pdf`, 627 B | 0.0 s | 628 | 171 MiB | no |
| stock | JPEG, 12 MP | 0.1 s | 0 | 171 MiB | no |
| stock | JPEG, 165 MP / 5 MB | 0.2 s | 0 | 174 MiB | no |
| stock | PNG, 165 MP / 2 MB | 0.1 s | 0 | 225 MiB | no |
| stock | TIFF, 165 MP / 71 MB | 1.7 s | 0 | 238 MiB | no |
| stock | JPEG, 165 MP / 138 MB | 3.2 s | 0 | 314 MiB | no |
| `-full` | JPEG, 12 MP | 0.7 s | 2 | 413 MiB | no |
| `-full` | JPEG, 165 MP / 5 MB | 1.1 s | HTTP 422 | **1024 MiB** | **yes** |
| `-full` | JPEG, 4000 px / 16 MP | 0.7 s | 2 | 390 MiB | no |

Three conclusions, each of which changes what #222 proposed.

**Without OCR, images are close to free.** The issue assumed Tika "still
has to decode them". It does not. `JpegParser` reads EXIF only, and
`ImageParser` and `TiffParser` behave the same way. A 165 megapixel JPEG
costs 0.2 s and 174 MiB. Even a 138 MB upload stays at 314 MiB, because
Tika spools the request body to disk rather than into the heap. Dropping
image types from the default list therefore saves blob I/O, a queue row and
a round trip, but no Tika CPU or memory worth naming.

**Byte size is the wrong axis.** Tika's own `maxFileSizeToOcr` set to
10 MB let the 5.2 MB / 165 MP file through, and it OOMed again. Set to
1 MB it skipped cleanly, at 186 MiB. A 165 megapixel photograph weighs
5.2 MB, so any byte threshold an operator would plausibly choose misses
exactly the pathological case. `PGCATALOG_TIKA_MAX_BLOB_SIZE`, proposal 2
in the issue, would not have prevented the production incident.

**The OOM victim was the external OCR helper, not the JVM.** The cgroup
reported `oom_kill 1` with `oom_group_kill 0`, the request thread survived
to log `Text extraction failed (null)`, and the server answered HTTP 422
with `RestartCount=0`. A full outage with connection-refused, as reported
in #222, requires PID 1 to die and the orchestrator to restart the pod.
That is a different failure, and it means the production deployment's image
and memory configuration should be confirmed before concluding anything
from the incident. Our own `setups/production-like/compose.yml` in the
cloudbrine workspace runs `apache/tika:latest-full`, so OCR may well have
been active where the issue assumed it was not.

## Capability introspection, and its limit

Tika server exposes `GET /parsers`, `GET /parsers/details`,
`GET /mime-types` and `GET /detectors`, all answering JSON. So pgcatalog
*can* ask what the deployment supports. It reports 264 media types for the
stock image and 275 for `-full`.

It does not answer the question that matters, though. `image/jpeg` is
claimed by `JpegParser` in both images, `image/png` by `ImageParser` in
both, and the set of types where the two images disagree about the parser
is empty. A naive capability query would keep every image type in the list
and change nothing.

The usable signal is narrower and reliable: the eleven types only `-full`
reports include `image/ocr-jpeg`, `image/ocr-png`, `image/ocr-tiff` and
five more `image/ocr-*` pseudo-types. Those are registered by
`TesseractOCRParser` only when a working Tesseract binary was found. The
stock image reports zero types from a Tesseract or OCR parser, `-full`
reports eleven. **Presence of any `image/ocr-*` type is the OCR probe.**

## Design decisions

Three forks were settled before writing this document.

1. **Derivative discovery reads the object state directly.** pgcatalog
   recognises a pgthumbor derivative by the attribute name
   `_pgthumbor_source` in the JSON state it already parses for `@ref`
   collection. No import, no entry point, no release coupling. The price
   is that the attribute name becomes a contract, and it has to be
   documented as such in both repositories.
2. **The skip decision happens in the worker, at dequeue.** The queue row
   carries the pixel dimensions. The worker already holds the Tika
   connection, so it can run the OCR probe itself, and it decides before
   fetching the blob, which is where the real I/O cost sits. Zope stays
   free of a network dependency at boot.
3. **Metadata harvesting via `/rmeta/text` is in scope here**, not deferred.

## Architecture

### Component 1: bounded blob selection

`CatalogStateProcessor._enqueue_candidate` currently inserts a queue row
for *every* resolvable blob ref of a candidate
(`src/plone/pgcatalog/processor.py:423-448`). pgthumbor stores its
derivative as `_pgthumbor_source` on the field value, and that derivative
holds its own ZODB blob. Both blobs are therefore reachable from the same
object state, and the expectation is that a large image with pgthumbor
installed is enqueued **twice** today, once for the original and once for
the derivative, with the original being the row that can take Tika down.
Verifying this is step 0 below.

The rule is:

> Extract the **bounded rendition**: the derivative when one exists, and
> the original otherwise, because an image without a derivative is either
> already under pgthumbor's cap or pgthumbor never got to it.

pgthumbor creates a derivative exactly when the longest edge exceeds the
cap, or the colour space is not sRGB, or the image is palette-plus-alpha
(`plone/pgthumbor/derivative.py:111`). The default cap is 4000 px with a
ceiling of 8000 px (`plone/pgthumbor/config.py:13-21`), so a derivative is
bounded at 16 MP by default and 64 MP at the ceiling. The measured 16 MP
worst case OCRs in 0.7 s at 390 MiB, which leaves a factor of 2.6 of
headroom in a 1 GiB container.

This component also removes the double enqueue, which is a correctness fix
independent of everything else in this design.

#### Resolution happens at dequeue, not at enqueue

An earlier draft of this design resolved the derivative in the processor,
at enqueue. That is wrong, and the reason is worth spelling out because it
is the single largest trap in this change.

`generate_source_derivatives` is a synchronous subscriber on
`IObjectAddedEvent` and `IObjectModifiedEvent`, so in the uncontended case
the derivative *is* in the state before the state processor runs at
`tpc_vote`. But there are three independent ways for it to be absent, and
all three are normal operation rather than edge cases:

1. **The decode semaphore.** `_DECODE_SEMAPHORE` is a process-wide
   `BoundedSemaphore(1)` with `DECODE_TIMEOUT = 2.0`, and a thread that
   cannot get in "records a retry and gives up rather than queueing"
   (`plone/pgthumbor/subscribers.py:154`). Under any concurrency the
   derivative is simply not created in that transaction.
2. **The kill switch.** `max_edge <= 0` disables generation, and
   pgthumbor's own comment calls it "the documented kill switch, the thing
   an operator reaches for **during a bulk import** or an incident".
3. **Thumbor not configured.** `_configured_max_edge()` returns 0 when
   there is no Thumbor config, so no derivative is ever produced, and
   nothing is recorded either.

Case 1 and case 2 are exactly the mass-import scenario that #222 reports,
139k objects with many large photos. Resolving at enqueue would therefore
have skipped precisely the workload this design exists to make safe, and
case 3 would have made extraction quality depend silently on whether
Thumbor is configured. That is an unacceptable coupling between two
packages that are meant to be independently useful.

So the worker resolves the rendition at dequeue, against the *current*
state, by which time pgthumbor's retry or its backfill has usually run. The
processor's job shrinks to enqueuing one row per content object and image
field, pointing at the **original** blob, and recording what it knows in
`source_info`.

Two consequences to handle deliberately:

- **`blob_zoid` and `tid` stay the dedup key, not the work order.** The row
  identifies "make `searchable_text` current for this content version", and
  `UNIQUE(blob_zoid, tid)` keeps its meaning. Which blob is actually sent
  is decided at run time, and the blob that was used is written back into
  `source_info` for auditing.
- **A missing derivative is a deferral, not a terminal skip.** See
  component 4.

#### `_collect_ref_oids` has to learn structure

`_collect_ref_oids` flattens the state into an unordered list of zoids
(`src/plone/pgcatalog/processor.py:69-106`). It cannot say which ref came
from `_pgthumbor_source` and which is the field's own blob, and it cannot
pair an original with its derivative when an object has several image
fields, such as a lead image plus an attachment.

Pairing therefore needs a structure-aware walk that returns
`(field_path, blob_zoid)` rather than bare zoids. The existing flat
function stays for its current callers; the new one lives beside it and is
shared with the worker. This is real work, not a rename, and the plan
treats it as its own task.

#### The derivative's content type differs from the original's

pgthumbor's `_encode` picks the output format from the image, so a TIFF or
a CMYK JPEG original can have a PNG or sRGB JPEG derivative. Sending the
derivative under the *original's* MIME type would hand Tika a wrong hint.
The rendition's own content type has to travel with it, which means
`source_info` records it and the worker uses it for the `Content-Type`
header rather than the queue row's `content_type`.

### Component 2: source facts travel with the queue row

One nullable JSONB column is added to `text_extraction_queue`:

```sql
ALTER TABLE text_extraction_queue
    ADD COLUMN IF NOT EXISTS source_info JSONB;
```

Initial shape, written by the processor at enqueue:

```json
{"width": 15000, "height": 11000}
```

`plone.namedfile` computes `_width` and `_height` when the field data is
set, so the values are expected to be present in the object state that the
processor already parses. No blob read and no image decode is needed to
populate them. Confirming the exact state shape is step 0 below.

**Why JSONB and not two typed columns.** This follows the house pattern
rather than inventing one: `idx` is JSONB and `ExtraIdxColumn` /
`register_extra_idx_column` in `columns.py` exist to promote a key to a
typed column once something needs to query it. The pixel guard is
evaluated by the worker in Python after dequeue, so nothing queries these
values in SQL, and they stay in JSONB. Were we to add a page count for
scanned PDFs, or a colour mode, or a DPI, none of them would need DDL.

Note that the usual argument for this is wrong and is not the reason here:
`ALTER TABLE ... ADD COLUMN x INTEGER` with no default is metadata-only in
PostgreSQL, instant and without a table rewrite, so adding columns later is
cheap. The reason is that a job queue should not accumulate one column per
fact we happen to learn about blobs.

**What `source_info` is for, and what it is not.** It holds facts about the
*source blob* that are known *before* extraction and are used to route the
job. It is explicitly **not** a home for extraction results. The metadata
that component 5 harvests from Tika goes into `searchable_text` through
`pgcatalog_merge_extracted_text`, and outcomes live in `status` and
`error`. Naming the column `source_info` rather than `metadata` is
deliberate, because a generic name on a queue table invites exactly that
drift.

**The promotion path, if it is ever needed.** #132 moved `path` out of
`idx` into typed columns because seven indexes and the query builder needed
it, and that lesson applies here too the moment anything filters on these
values in SQL. A plausible future case is a separate worker pool for large
jobs, dequeuing with a predicate on pixel count. That wants either an
expression index,

```sql
CREATE INDEX idx_teq_pixels ON text_extraction_queue
    (((source_info->>'width')::bigint * (source_info->>'height')::bigint));
```

or a promoted typed column. Either is a small, local change at that point,
and naming it here means it gets decided deliberately rather than
discovered.

The column stays nullable. Rows written before this change, and content
types where dimensions are meaningless, carry NULL, and a missing or NULL
`width`/`height` means "no pixel guard applies".

**By contrast, `not_before` in component 6 is a typed column**, because the
dequeue predicate and the partial index both read it. That is the same rule
being applied, not an inconsistency.

### Component 3: the OCR probe

On startup the worker issues `GET /parsers/details` once with
`Accept: application/json`, walks the composite parser tree collecting
`supportedTypes`, and sets `ocr_available` to true when any type matches
`image/ocr-*`. The result is cached for the process lifetime, with a
re-probe at most every `PGCATALOG_TIKA_PROBE_INTERVAL` seconds, default
3600, so that swapping the Tika image does not require a worker restart.

If the probe fails, `ocr_available` is assumed **true**. That is the
conservative choice: assuming OCR means the pixel guard applies, and
applying the guard unnecessarily costs a little recall on oversized images,
whereas skipping the guard risks the OOM this design exists to prevent.

A `PGCATALOG_TIKA_OCR` environment variable overrides the probe in both
directions, for deployments that cannot reach the endpoint or that want to
pin the behaviour.

### Component 4: the decision matrix

Evaluated by the worker after dequeue and before any blob fetch.
`cap` is `PGCATALOG_TIKA_MAX_IMAGE_PIXELS`, default 16_000_000, chosen to
match a 4000 px square and therefore pgthumbor's default cap.

| OCR available | Rendition | Pixels vs cap | Action |
|---|---|---|---|
| no | either | either | extract, no guard |
| yes | any | unknown | extract, no guard |
| yes | any | `<=` cap | extract |
| yes | derivative | `>` cap | extract, and warn |
| yes | original, oversized | `>` cap | **defer**, then `skipped` |

The last two rows are where the care is.

**A derivative that is still over the cap is extracted anyway.** This
happens when pgthumbor's cap is set above ours, up to its 8000 px ceiling
which is 64 MP. Refusing it would mean two packages silently disagreeing
about a threshold and content vanishing in the gap. The worker extracts and
logs a warning naming both numbers, so the misconfiguration is visible and
fixable rather than merely absent.

**An oversized original with no derivative is deferred, not dropped.**
Because of the three absence paths in component 1, a missing derivative
usually means "not yet" rather than "never": pgthumbor recorded a
`REASON_RETRY`, or an operator has the kill switch on during an import.
So the row is re-queued with `not_before` in the future, and only once the
row's **age** exceeds `PGCATALOG_TIKA_RENDITION_GRACE`, default 3600
seconds, does it become `skipped`.

The budget is deliberately measured as age from `created_at` and **not**
as a count of deferrals. An earlier draft counted deferrals, which is
wrong because component 6 already uses that counter for transport backoff:
two connection errors plus two rendition waits would have exhausted the
budget and marked the row `skipped` for a reason that never occurred. Age
is also the more honest expression of the intent, which is "give
pgthumbor's backfill an hour", not "give it four tries".

The deferral reuses the `not_before` machinery from component 6, which is
why the two components ship in that order.

Two things follow from the measurements and deserve stating plainly.

With OCR off there is no guard at all, because there is nothing to guard
against: the metadata path is cheap at any pixel count, and component 5
means those images now contribute their captions. This is the opposite of
proposal 1 in the issue, which would have dropped image types entirely.

The guard only ever fires on a **known** oversized image. Unknown
dimensions fall through to today's behaviour rather than being dropped. A
guard that silently discards content whenever it lacks information is worse
than the problem it solves, and component 7 provides a safety net that does
not depend on pgcatalog knowing anything.

`skipped` is a new terminal status, distinct from `failed`. It is not an
error and must not be swept up by a reset of failed rows. The reason goes
into `error` as a stable machine-readable token, `skipped: pixels
165000000 > cap 16000000, no derivative after 3600s`, so an operator can
find and re-run these rows after raising the cap or configuring Thumbor.

#### The recall cost of this component is not measured

The design trades OCR recall for safety, and honesty requires saying that
the size of that trade is unknown. A derivative is downscaled and
re-encoded, so OCR on it is strictly worse than OCR on the original. For a
165 MP photograph that is irrelevant, since there is no text. For a large
document **stored as an image rather than as a PDF**, an A0 plan or a
newspaper page scanned at 300 dpi, 4000 px on the longest edge may well be
below the resolution Tesseract needs, and the text would quietly get worse
rather than disappear, which is harder to notice.

I measured memory and latency, not accuracy. The plan's first phase
measures it: OCR a text-bearing scan at full resolution and at 4000 px and
compare the character output. If the loss is severe, the honest response is
to raise the default cap and the Tika memory limit together rather than to
pretend the derivative is equivalent.

### Component 5: metadata harvesting

`TikaWorker._extract` currently issues `PUT /tika` with
`Accept: text/plain`, which returns body text only. For an image without
OCR that is zero characters, even when the file carries a caption. Measured
against a 12 MP JPEG on the stock image, `PUT /rmeta/text` returned
`dc:description` holding the EXIF ImageDescription, plus 35 further fields,
at metadata-only cost.

The worker switches to `PUT /rmeta/text` with `Accept: application/json`.
The response is a JSON array with one object per document, the container
first and embedded documents after it. Extraction becomes:

1. Concatenate `X-TIKA:content` across all entries, preserving today's
   behaviour for compound documents.
2. Append the values of a whitelist of metadata keys from the container
   entry, defaulting to `dc:title`, `dc:description`, `dc:subject`,
   `dc:creator` and `meta:keyword`, overridable via
   `PGCATALOG_TIKA_METADATA_FIELDS`.
3. Pass the joined string to `pgcatalog_merge_extracted_text`, unchanged.

The merge function takes a single text argument, so no schema or SQL
function change is needed.

**Concatenation does not double-count, which was worth checking.** Measured
on a ZIP holding two text files, the container entry's `X-TIKA:content` was
`'vertrag.txt\n\n\nanhang.txt'`, the file *names* only, and the two child
entries held the actual text. So the embedded text appears exactly once and
concatenating all entries reproduces what `PUT /tika` returns today.

What does change is **word order**. `/tika` interleaves each name with its
content, while concatenating rmeta entries yields all names first and then
all contents. For a `tsvector` that is a bag of words and irrelevant, but
it shifts adjacency, so any phrase search over `searchable_text` would see
different proximity. Acceptable, and worth a changelog note rather than
silence.

Two risks to handle in implementation. A document with many embedded
resources produces one JSON entry per resource, three for a two-file ZIP,
so a 500-page PDF full of images becomes a very large response. The worker
needs a response size ceiling and should cap embedded resources via Tika's
own header rather than parsing an unbounded body. And a metadata value that
repeats the body text inflates term frequency in the BM25 columns; the
whitelist is deliberately small for that reason, and de-duplication is a
follow-up if it proves to matter.

### Component 6: retry that can bridge a restart

Three attempts inside a second cannot bridge any server restart, which is
how 102 transient failures became `failed` rows in #222. Two changes:

```sql
ALTER TABLE text_extraction_queue
    ADD COLUMN IF NOT EXISTS not_before TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE text_extraction_queue
    ADD COLUMN IF NOT EXISTS deferrals  INTEGER NOT NULL DEFAULT 0;
```

The dequeue predicate gains `AND not_before <= now()`, and
`idx_teq_pending` is recreated to cover it.

Connection-level failures, meaning `httpx.ConnectError`,
`httpx.ConnectTimeout` and `httpx.RemoteProtocolError`, are re-queued with
`not_before = now() + backoff` and **without** incrementing `attempts`,
because the server being absent says nothing about the job. Backoff is
5 s, 30 s, 120 s, capped, tracked in a separate `deferrals` counter so a
permanently unreachable Tika cannot loop forever. Every other exception
keeps today's semantics and counts an attempt.

### Component 7: the Tika side

The deployment is half of the fix, and it is the half that holds when
pgcatalog is wrong. Documented in
`docs/sources/how-to/enable-tika-extraction.md`, not enforced in code:

- **A JVM heap ceiling below the container limit**, for example
  `JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=50`. Then the JVM raises
  `OutOfMemoryError` for one request instead of the kernel killing a
  process, and a single pathological file cannot cost a pod restart and a
  30 second outage.
- **`maxFileSizeToOcr` in a mounted `tika-config.xml`**, as a byte-axis
  backstop. It is on the wrong axis for the 165 MP case, as measured, but
  it is free and it catches the large-file case that the pixel guard does
  not see.
- **A parser allow-list** for deployments that do not want image parsing at
  all, which makes the server's capability set declared rather than
  accidental, and makes a stray image cost nothing.

A matching change to `setups/production-like/compose.yml` lives in the
cloudbrine workspace repository and is a cross-repo follow-up, not part of
this change.

## Step 0: verify before implementing

Three assumptions in this design were inferred from reading code, not
measured. They belong at the front of the implementation, and each one can
invalidate a component.

1. **The JSON state shape.** Does a `NamedBlobImage` field value carry
   `_width` and `_height` in the state, and does `_pgthumbor_source` appear
   as a nested object with its own `@ref` to a blob? Components 1 and 2
   depend on both. Confirm the same for the derivative itself, since
   selecting it is pointless if its own dimensions are not recorded and the
   guard then sees NULL. Build the fixtures from a real state dump, not
   from what the consumer wants to see.
2. **The double enqueue.** Confirm that a large image with pgthumbor
   installed produces two queue rows today. If it does not, component 1
   changes shape and the reason the original reaches Tika is elsewhere.
3. **The production Tika image.** Confirm whether the deployment in #222
   ran a `-full` image. If it did, the issue's premise was wrong and
   images are worth keeping in the pipeline, which this design already
   assumes. If it ran the stock image, the OOM has a cause not covered
   here and needs its own investigation.

## Non-goals

- **PDF rasterisation.** A scanned PDF with `ocrStrategy=auto` renders
  pages to images inside the JVM, which an image pixel guard does not see
  and which was not measured. Plausibly the real cause of the production
  incident. It needs its own issue.
- **Tesseract tuning**, languages and preprocessing. Server-side
  configuration, already documented.
- **Audio and video.** Worth its own issue, and the measurements are
  recorded below because they sharpen this design's central argument.
- **Reworking the default content type list** beyond what the OCR probe
  implies. The list stays, normalisation of the incoming MIME type is
  handled below as a separate small fix.

## The cost axis depends on the content class

A note for whoever picks up audio and video, because it reframes the
pixel-versus-byte argument above rather than merely extending it.

Tika does not ignore them. The **stock** image claims 30 `audio/*` and
`video/*` types across 11 parsers, including `Mp3Parser`, `MP4Parser`,
`OggParser`, `FlacParser` and `FLVParser`. What ignores them is
pgcatalog's `_DEFAULT_CONTENT_TYPES`, which lists none of them.

And unlike images, they yield body text through the endpoint the worker
*already* uses. Measured on the stock image, `PUT /tika` with
`Accept: text/plain` returned 151 characters for an MP3 and 95 for an MP4,
because `Mp3Parser` and `MP4Parser` write the tags into the content stream
where the image parsers emit nothing. Enabling them is a content-type list
change, not a code change.

The reason it still needs its own issue is that **the cost sits somewhere
else**. For images the transfer is cheap and the decode is expensive, and
only with OCR. For audio and video the parse is trivial and the *transfer*
is the cost: the worker streams the entire blob, so a 2 GB video means 2 GB
of S3 egress and 2 GB spooled to Tika's temp disk to read a few hundred
bytes of tags.

**So for this content class a byte ceiling is the right axis**, which
partially rehabilitates proposal 2 of #222. It was the wrong instrument for
images, as measured. It is the correct instrument here.

A prefix fetch is a viable optimisation for audio and not for video. A
64 KiB prefix of a 5 MB MP3 returned 150 of the 151 characters, the missing
one being a digit of the computed duration, because ID3v2 sits at the front.
An MP4 written by ffmpeg with default settings puts `moov` at offset 19234
of 22463, that is at the **end**, so a prefix misses the metadata entirely
and the atom's position cannot be known without parsing.

## Adjacent fix worth carrying

`_should_extract` matches the MIME type against a set by exact string
(`src/plone/pgcatalog/processor.py:64-68`). A value of
`text/plain; charset=utf-8` or `APPLICATION/PDF` therefore never matches,
although both are shapes Plone can hold.

Normalising is not quite the one-liner it looks like, though. Tika's own
supported-type set contains parameterised entries, `audio/ogg; codecs=opus`
and `audio/ogg; codecs=speex` among them, so blanket parameter stripping
changes what matches what. The fix is therefore: lowercase and collapse
whitespace, look up the full normalised string first, and fall back to the
bare type before the parameters. That keeps a parameterised configuration
entry meaningful while making `text/plain; charset=utf-8` match
`text/plain`.

## Testing

- **Blob selection**, unit, against state fixtures captured from a real
  Plone object with and without a pgthumbor derivative. Per the standing
  lesson on fakes, the fixtures come from what the other side really
  writes, not from what the processor expects to read.
- **Decision matrix**, unit, one case per row of the table plus the
  probe-failure default.
- **OCR probe parsing**, unit, against the real `/parsers/details` payloads
  from both images, committed as fixtures. The appendix says how to
  regenerate them.
- **`/rmeta/text` parsing**, unit, against a captured real response,
  including a compound document with embedded resources.
- **Backoff**, unit, asserting that a connection error does not increment
  `attempts` and does set `not_before`.
- **Schema migration**, integration, in the single module permitted to use
  the PG fixture. Runs strictly serially, never overlapping another pytest
  run against the shared test database.

## PR decomposition

Sequenced so each PR is independently reviewable and shippable, with the
correctness fixes first. The order changed once component 4 grew a
deferral: the `not_before` machinery now has to land **before** the
decision matrix that uses it, so retry moved from fifth to third.

1. Step 0 verification, plus the MIME normalisation fix and the double
   enqueue fix. No new configuration, no new columns.
2. The `source_info` column, the structure-aware ref walk, dimensions and
   rendition pairing written at enqueue.
3. Retry backoff, `not_before` and `deferrals`. Fixes proposal 3 of #222
   on its own and provides the machinery component 4 needs.
4. OCR probe, the decision matrix, dequeue-time rendition resolution, and
   the `skipped` status.
5. `/rmeta/text` and metadata harvesting.
6. Documentation for the Tika side, and the reference page for the new
   environment variables.

PRs 1 and 3 each fix something real on their own and do not depend on the
rest, so they can ship while the accuracy question in component 4 is still
being measured.

Each PR carries its own `CHANGES.md` entry.

Out of scope and tracked separately: audio and video, in #223.

## Appendix: reproducing the measurements

```bash
# Two servers, production's memory limit.
docker run -d --name tika-min  --memory=1g -p 9998:9998 apache/tika:3.2.3.0
docker run -d --name tika-full --memory=1g -p 9997:9998 apache/tika:3.2.3.0-full

# Capability payloads, the OCR probe's input.
curl -s -H 'Accept: application/json' http://localhost:9998/parsers/details
curl -s -H 'Accept: application/json' http://localhost:9997/parsers/details

# One extraction, with peak RSS from the cgroup rather than docker stats,
# which samples too slowly to see a one second request.
cid=$(docker inspect --format '{{.Id}}' tika-full)
cg=/sys/fs/cgroup/system.slice/docker-$cid.scope
curl -s -o /dev/null -w '%{http_code}\n' -X PUT --data-binary @big.jpg \
     -H 'Content-Type: image/jpeg' http://localhost:9997/tika
echo "peak $(( $(cat $cg/memory.peak) / 1048576 )) MiB"
cat $cg/memory.events          # oom / oom_kill counters
docker inspect --format '{{.State.OOMKilled}} {{.State.Running}}' tika-full
```

`memory.peak` is not writable without root, so restart the container
between runs to get a clean watermark. Fixtures were a 15000x11000 JPEG at
quality 85, which lands at 5.2 MB, plus PNG and TIFF at the same pixel
count and a noise-filled JPEG at 138 MB to separate the byte axis from the
pixel axis.
