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

> **Two Tika majors are in play, and the difference is load-bearing.**
> The first round was measured against `apache/tika:3.2.3.0`. Production
> (cluster kup6s, namespace `aaf-prod`) turned out to run the **stock**
> `apache/tika:latest`, which is **Apache Tika 4.1.0**, at a 2 GiB limit
> and 0.5 CPU. Everything below was re-measured against that exact digest
> on 2026-10-02. The cost findings held; two design assumptions did not,
> and they are called out where they bite: the OCR probe in component 3
> and the content key in component 5.

Measured 2026-09-30 against `apache/tika:3.2.3.0` and
`apache/tika:3.2.3.0-full` at a 1 GiB limit, which is what #222 reported,
and re-measured 2026-10-02 against the production image, stock
`apache/tika` 4.1.0 at 2 GiB and 0.5 CPU. Reproduction recipe in the
appendix.

Re-measurement on the production image and version, after a JVM warm-up
and reported as the delta over a warmed 392 MiB baseline:

| Payload | Time | Chars | Peak delta |
|---|---|---|---|
| ZIP, two text files | 0.02 s | 117 | +0 MiB |
| JPEG, 12 MP | 0.19 s | 0 | +2 MiB |
| **JPEG, 165 MP** | **0.18 s** | 0 | **+23 MiB** |

`oom_kill 0`. Re-measured at the limit the incident actually ran under,
and below it: at **1 GiB** the same 165 MP JPEG costs +27 MiB in 0.2 s,
and at stage's **512 MiB** it costs +12 MiB in 0.5 s. No OOM in either.

**And the incident's cause is now known, from the production queue rather
than from inference.** Of 1708 `failed` rows, 1689 carry
`[Errno 1] Operation not permitted` and 14 carry
`[Errno 111] Connection refused`: on a Cilium cluster the socket load
balancer returns `EPERM` for a `connect()` to a ClusterIP with no ready
backend. The single-replica Tika Service had no endpoint. **Those
documents were never parsed at all**, and every row sits at
`attempts = 3 / max_attempts = 3`, so three retries inside a second burned
the budget during a roughly 30 second restart. Only 5 rows are genuine
document failures, all `422`.

So the extraction cost of images is not what broke production; service
downtime plus a retry policy that cannot bridge it is. That is component 6,
and it is why the plan ships it second. The pixel guard and the rendition selection are insurance against
a future switch to `-full`, not a fix for the reported incident, and the
plan is sequenced accordingly.

Warm up before measuring: on half a CPU the first request after a restart
takes about 12 seconds whatever the payload, which is JIT compilation and
not work.

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

On **3.2.3** there was a usable signal: the eleven types only `-full`
reported included eight `image/ocr-*` pseudo-types, which
`TesseractOCRParser` registers only when it found a working Tesseract.

On **4.1.0 that signal is gone**, and with it the whole idea of deriving
the policy from introspection. Stock and `-full` return semantically
identical payloads: 89 parser classes, the same `supportedTypes`, 279
media types, zero `image/ocr-*` in either. The two files differ only in
key ordering. Meanwhile `-full` really does OCR.

So introspection answers nothing on the production major, and component 3
tests the behaviour instead. The endpoints remain useful for what they
honestly report, which types have a parser at all, and the captured
payloads are kept as fixtures for both majors.

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

`CatalogStateProcessor._enqueue_candidate` inserts a queue row for every
resolvable blob ref of a candidate
(`src/plone/pgcatalog/processor.py:423-448`). An earlier draft of this
section expected that to double-enqueue any image carrying a pgthumbor
derivative. **Measured 2026-10-01, it does not**, and the reason matters
for everything below: `_resolve_wrappers` goes exactly one level deep,
content state to wrapper state to blob, while a derivative's blob is two
levels down. The derivative is reached as an inner ref, found absent from
`blob_state`, and correctly dropped. Two image fields give two rows, one
per field. Evidence in `tests/fixtures/state/README.md`.

So the derivative is not mis-selected today, it is **unreachable**, and
this component is a pure addition rather than a correction.

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

#### Three persistent hops, so walk then fetch

Measured, not assumed. A `NamedBlobImage` is **not** stored inline in the
content object's state: it is its own persistent object, and a pgthumbor
derivative is a second one hanging off it.

```
Content            zoid 3   image -> @ref zoid 4
NamedBlobImage     zoid 4   _blob -> @ref zoid 7   (Blob, the original)
                            _width 15000, _height 11000, contentType
                            _pgthumbor_source -> @ref zoid 14
NamedBlobImage     zoid 14  _blob -> @ref zoid 15  (Blob, the derivative)
                            _width 4000, _height 2933, _pgthumbor_is_source
```

`_collect_ref_oids` flattens a state into bare zoids
(`src/plone/pgcatalog/processor.py:69-106`), so it cannot say which ref
came from `_pgthumbor_source`, and no function over a *single* state can
pair an original with its derivative at all, because the derivative's
dimensions and blob are one `object_state` row further away.

The resolution is **walk then fetch**: a pure walk that returns
`(path, zoid, class_name)` per ref, plus a pure reader for one wrapper
state, with the caller doing the fetching between them. The alternative, a
resolver callback inside the pairing function, was rejected because it
would put I/O into the one part of this design that can otherwise be unit
tested without a database.

One gift from the measurement: the `@ref` marker uses the two-element
form, `["<hex oid>", "<dotted class name>"]`, so a `NamedBlobImage` ref is
distinguishable from a `ZODB.blob.Blob` ref **without loading either
object**. The walk is therefore cheap, and the second fetch is one extra
batched query per transaction rather than per object.

`_collect_ref_oids` stays where it is for its other callers.

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

### Component 3: the OCR probe, by behaviour not by introspection

Whether OCR is available cannot be asked of Tika 4.x. Measured on 4.1.0,
the stock and `-full` images return **semantically identical**
`/parsers/details`: 89 parser classes with the same `supportedTypes`, no
class present in one and absent from the other, and zero `image/ocr-*`
types in either. The byte difference between the two payloads is key
ordering. Yet `-full` demonstrably OCRs, with Tesseract 5.5.0, and stock
returns nothing. The `image/ocr-*` signal that worked on 3.2.3 is gone.

So the worker **tests the capability instead of reading the
advertisement**. At startup it sends a small PNG carrying the known word
`TIKAOCR` to `PUT /tika` and treats a non-empty response as "OCR
available".

- The asset ships with the package at
  `src/plone/pgcatalog/assets/ocr_probe.png`, 3990 bytes, 320x90.
- Measured cost: **0.02 s** against stock, returning empty, and **0.17 s**
  against `-full`, returning `TIKAOCR`.
- The result is cached for `PGCATALOG_TIKA_PROBE_INTERVAL` seconds,
  default 3600, so swapping the Tika image does not need a worker restart.
- `PGCATALOG_TIKA_OCR` overrides it in either direction.

If the probe cannot reach Tika at all, `ocr_available` is assumed
**true**. That is the conservative choice: assuming OCR turns the pixel
guard on, which costs a little recall, while assuming no OCR turns it off
and hands Tika the file that started #222.

This is better than what introspection could have given us even on 3.2.3.
`image/ocr-*` was an internal implementation detail that a major version
duly renamed away; a round trip through the actual parser cannot go stale
like that. It also keeps the design's promise that an operator switching
between the stock and `-full` images changes no pgcatalog setting.

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

**Measured 2026-10-01, see `benchmarks/tika_ocr_downscale.md`.** On a
dense A0 page at 300 dpi, pgthumbor's default 4000 px cap costs 3
percentage points of phrase recall and 1.4 of word recall against the
original. Small, and worth not being OOM-killed, so the default stands.

Two things that measurement changed. Resolution is **not monotonic**:
pgthumbor's 8000 px ceiling scored 76.0% where its 4000 px default scored
97.0%, reproduced twice in the 24-25 px glyph band. So an earlier draft of
this section was wrong to suggest raising the cap as the remedy for poor
recall; raising it can make OCR worse while costing four times the pixels.
And the collapse is governed by **glyph height**, not image size, so the
cap bounds memory but cannot promise recall: the same 4000 px that costs
3% here would destroy a plan sheet whose annotations render half as large.

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

1. Concatenate the content of all entries, preserving today's behaviour
   for compound documents. **The key depends on the Tika major**:
   `tk:content` on 4.x, `X-TIKA:content` on 3.x. Tika 4 renamed the
   namespace, so code that reads only `X-TIKA:content` returns empty text
   against production and that looks like "the document has no text"
   rather than like a bug. Read `tk:content` first and fall back, per
   entry, so one worker build serves both majors.
   `resourceName` likewise became `tk:resource-name`; Dublin Core keys and
   `Content-Type` are unchanged.
2. Append the values of a whitelist of metadata keys from the container
   entry, defaulting to `dc:title`, `dc:description`, `dc:subject`,
   `dc:creator` and `meta:keyword`, overridable via
   `PGCATALOG_TIKA_METADATA_FIELDS`.
3. Pass the joined string to `pgcatalog_merge_extracted_text`, unchanged.

The merge function takes a single text argument, so no schema or SQL
function change is needed.

**It also removes a regression Tika 4 introduced.** `PUT /tika` with
`Accept: text/plain` returns **Markdown** on 4.x where 3.x returned
unmarked text. Heading and bullet markers are harmless, since
`to_tsvector` discards them as punctuation, but Markdown **link syntax
puts the URL into the text**. Measured: `Siehe [die Akte](https://example.org/akte).`
tokenises to five lexemes including `example.org`, `example.org/akte).`
and `/akte).`, where the same sentence from `/rmeta/text` gives two. A
link-heavy page therefore gains up to three junk lexemes per link, some
with punctuation glued on.

`tk:content` from `/rmeta/text` is plain text on 4.x, verified against the
production digest. So switching endpoints is not only how images
contribute their captions, it is also how `searchable_text` stops
absorbing Markdown syntax and link targets.

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

## Step 0: done, and two of three came back different

The three assumptions this design inferred from reading code were measured
on 2026-10-01. Evidence: `tests/fixtures/state/README.md` and
`benchmarks/tika_ocr_downscale.md`.

1. **`_width` and `_height` in the state: confirmed.** Present under
   exactly those names on both the original and the derivative, with
   `contentType` and `filename`. The pixel count needs no blob read and no
   decode, as component 2 assumed.
2. **`_pgthumbor_source` as a nested dict: wrong.** It is an `@ref` to a
   separate persistent object. Component 1 now says so, and the plan's
   pairing task was redesigned as walk-then-fetch.
3. **The double enqueue: does not exist.** One row per image field, which
   is correct. Component 1 now says so, and the plan lost that fix.

A fourth question the design had been answering by assertion rather than
by measurement: **what downscaling costs OCR.** At pgthumbor's default
4000 px cap, 3 percentage points of phrase recall and 1.4 of word recall
on a dense A0 page at 300 dpi. The default
`PGCATALOG_TIKA_MAX_IMAGE_PIXELS = 16000000` stands. Two surprises came
with it, both recorded in component 4: recall is **not monotonic** in
resolution, and the collapse tracks **glyph height** rather than image
size.

**The fourth question, the production Tika image, is answered too, and it
was the stock one.** `apache/tika:latest`, resolving to Apache Tika 4.1.0,
at limits 500m/2Gi. So there is no OCR in production, a 165 megapixel
image costs 23 MiB there, and **the OOM in #222 has a cause this design
does not address**. The 564 `failed` rows are where to look; the
diagnostic query is in the how-to. Most likely candidate remains PDF
rasterisation, named under non-goals.

That answer also exposed the two breakages above, because the first round
of measurements had been taken against 3.2.3 while production had already
drifted to 4.1.0 on a floating `:latest` tag with nobody deciding it.

## Non-goals

- **PDF rasterisation.** A scanned PDF with `ocrStrategy=auto` renders
  pages to images inside the JVM, which an image pixel guard does not see
  and which was not measured. An earlier draft called this the likely
  cause of the production incident. **That was wrong twice over**: the
  image has no Tesseract, so `AUTO` does not rasterise at all, and the
  incident's cause is now known from the queue data. It stays a non-goal,
  without the speculation.
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

Reordered once production turned out to run the stock image. Without OCR
the pixel guard never fires and the rendition is never selected, so
components 1 to 4 are insurance against a future `-full` switch while
components 5 and 6 are what aaf-prod actually gets value from today. The
work that helps a real site therefore goes first.

Within that, component 4's deferral needs component 6's `not_before`
machinery, so retry stays ahead of the matrix either way.

0. **Phase 0, done.** The step-0 verifications, the OCR recall benchmark
   and real fixtures for both Tika majors. No production code.
1. **MIME normalisation**, alone. Done. No new columns, no dependencies.
   Fixes blobs that were silently never queued.
2. **Retry backoff**, `not_before` and `deferrals`. Fixes proposal 3 of
   #222, which is the 564 `failed` rows in aaf-prod, and provides the
   machinery component 4 needs later.
3. **`/rmeta/text`** and metadata harvesting, with the `tk:content`
   key handled for both majors. Gives a no-OCR site the text its images
   actually carry, which today is thrown away.
4. **The rendition groundwork**, in commit order: the walk-then-fetch ref
   module, the `source_info` column, then the enqueue that fills it.
   Nothing apart, one migration.
5. **The behavioural OCR probe, the decision matrix**, dequeue-time
   rendition resolution and the `skipped` status.
6. **Documentation** for the Tika side, the new environment variables, and
   the queue-status reset recipe.

PRs 1, 2 and 3 each stand alone and each fixes something a real site is
losing today. PRs 4 and 5 only pay off if OCR is ever switched on.

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
