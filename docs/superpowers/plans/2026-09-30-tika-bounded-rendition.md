# Tika Bounded Rendition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Tika text extraction unable to be taken down by a single
large image, stop spending blob I/O on work that yields nothing, and let
the policy follow the Tika deployment instead of a hand-maintained
environment list.

**Architecture:** The processor enqueues one row per content object and
image field, pointing at the original blob and recording what it knows
about the source in a `source_info` JSONB column. The worker resolves the
*bounded rendition* at dequeue against current state, preferring a
pgthumbor source derivative over the original, guards on pixel count only
when OCR is actually available, and defers rather than drops when a
derivative has not appeared yet. Extraction moves from `PUT /tika` to
`PUT /rmeta/text` so images contribute their captions even with no OCR.

**Tech Stack:** Python 3.13, psycopg3, PostgreSQL 18, httpx, pytest,
ZODB/Plone catalog API, zodb-pgjsonb state-processor plugin model, Apache
Tika 3.2.3 server.

**Spec:** `docs/superpowers/specs/2026-09-30-tika-bounded-rendition-design.md`

**Tracking issue:** [bluedynamics/plone-pgcatalog#222](https://github.com/bluedynamics/plone-pgcatalog/issues/222)

## Global Constraints

- **Module-level imports only.** No inline imports outside test or layer
  setup, unless a cycle is verified and noted.
- **ruff `C901` complexity ceiling 13, zero `noqa`.** The worker's
  `_process_one` is already close; new branching goes into helpers.
- **Every PR carries its own `CHANGES.md` entry**, including the
  test-only and tooling ones.
- **Tests run strictly serially.** Never start a second pytest run against
  the shared `zodb_test` database while one is in flight; overlapping runs
  produce phantom failures.
- **Only `tests/test_pg_integration.py` may call
  `fixture.create(PGCATALOG_PG_FIXTURE)`.** A second module doing so
  produces 65 fixture-not-found errors in full runs.
- **PG-backed tests need the DSN.** Start the `zodb-pgjsonb-dev`
  container and run with `env -u ZODB_TEST_DSN`, against
  `dbname=zodb_test`.
- **New environment variables and their defaults**, copied from the spec:
  `PGCATALOG_TIKA_MAX_IMAGE_PIXELS` = `16000000`,
  `PGCATALOG_TIKA_OCR` = unset (probe decides),
  `PGCATALOG_TIKA_PROBE_INTERVAL` = `3600`,
  `PGCATALOG_TIKA_RENDITION_GRACE` = `3600`,
  `PGCATALOG_TIKA_METADATA_FIELDS` =
  `dc:title,dc:description,dc:subject,dc:creator,meta:keyword`.
- **Backoff ladder** for transport deferrals: 5 s, 30 s, 120 s, capped at
  120 s.
- **Two Tika majors must both work.** Production (kup6s, `aaf-prod`) runs
  the **stock** `apache/tika:latest`, which is **4.1.0**, at limits
  500m/2Gi, so **no OCR**. 3.x is still in the field elsewhere. Key
  differences: the content key is `tk:content` on 4.x and
  `X-TIKA:content` on 3.x; `resourceName` became `tk:resource-name`;
  `image/ocr-*` exists only on 3.x. Fixtures for both majors live in
  `tests/fixtures/tika/` and any change touching Tika responses is tested
  against both.

## Review Focus

Five conditions the spec implies that no task's happy path exercises,
most likely to bite first. Each one's test is named in the task that owns
the code.

1. **An image field with no `_width`/`_height` in state.** Legacy uploads
   and formats Pillow could not size leave them absent. The guard must
   treat missing dimensions as "no guard applies" and must not do
   arithmetic on `None`. Pinned in Task 6.
2. **Two image fields where only one has a derivative.** A lead image plus
   an attachment. The pairing must not hand one field's derivative to the
   other field. Pinned in Task 4.
3. **Tika answers `/rmeta/text` with a non-JSON body.** A 500 with an HTML
   error page, or a truncated response. Parsing must fail the one job as a
   normal error and must not kill the worker loop. Pinned in Task 10.
4. **A Tika major that renames what the code reads.** No longer
   hypothetical: 4.1.0 removed the `image/ocr-*` signal and renamed
   `X-TIKA:content` to `tk:content`. The probe is now behavioural and the
   content key is tried under both names, and both facts are pinned
   against real payloads from both majors. `PGCATALOG_TIKA_OCR` must still
   win over the probe in either direction. Pinned in Tasks 9 and 12.
5. **A `skipped` or attempt-exhausted row must never be resurrected.**
   Adding `not_before` to the dequeue predicate must not widen it. Pinned
   in Task 8.

---

## Correction to the spec, carried here

The spec's component 4 reuses the `deferrals` counter for both transport
backoff and waiting on a derivative. That conflates two unrelated things:
two connection errors plus two rendition waits would reach
`max_deferrals` and the row would be marked `skipped` for a reason that
did not happen.

This plan instead gives rendition waiting a **time budget** measured from
`created_at`, `PGCATALOG_TIKA_RENDITION_GRACE`, default 3600 seconds,
while `deferrals` stays purely a transport-backoff counter. A time budget
is also the more honest expression of the intent, which is "give
pgthumbor's backfill an hour", not "give it four tries". Task 8 folds
this back into the spec.

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `src/plone/pgcatalog/mimetypes.py` | MIME normalisation and the extractable-type lookup | **Create** |
| `src/plone/pgcatalog/blobrefs.py` | Structure-aware `@ref` walk, original/derivative pairing | **Create** |
| `src/plone/pgcatalog/tika_policy.py` | OCR probe, the decision matrix, pure and side-effect free | **Create** |
| `src/plone/pgcatalog/tika_rmeta.py` | `/rmeta/text` response parsing and metadata whitelist | **Create** |
| `src/plone/pgcatalog/processor.py` | Enqueue: one row per field, `source_info` payload | **Modify** `_should_extract` (64-68), candidate accumulation (213-228), `_enqueue_candidate` (423-448), `_insert_queue_row` (450-467) |
| `src/plone/pgcatalog/schema.py` | `source_info`, `not_before`, `deferrals`, index | **Modify** `TEXT_EXTRACTION_QUEUE` |
| `src/plone/pgcatalog/tika_worker.py` | Dequeue predicate, rendition resolution, outcome handling | **Modify** `_process_one` (121-191), `_extract` (195-227) |
| `tests/test_mimetypes.py` | MIME normalisation | **Create** |
| `tests/test_blobrefs.py` | The ref walk and pairing | **Create** |
| `tests/test_tika_policy.py` | Probe and matrix, with real Tika payload fixtures | **Create** |
| `tests/test_tika_rmeta.py` | Response parsing | **Create** |
| `tests/fixtures/tika/` | Captured real Tika payloads | **Create** |
| `tests/test_tika_enqueue.py` | Enqueue behaviour | **Modify** |
| `tests/test_tika_worker.py` | Worker behaviour | **Modify** |
| `docs/sources/how-to/enable-tika-extraction.md` | Tika-side configuration | **Modify** |
| `docs/sources/reference/configuration.md` | New environment variables | **Modify** |
| `CHANGES.md` | Changelog | **Modify** per PR |

Four new small modules rather than growth in `processor.py` and
`tika_worker.py`: the policy and the parsing are pure functions that both
the Zope process and the standalone worker need, and pure functions are
what makes the decision matrix testable without a database or an HTTP
server.

---

# Phase 0: measure before building

Three assumptions in the spec were read out of code, not measured, and one
design default rests on an unmeasured accuracy claim. Phase 0 produces
facts and fixtures. It writes no production code.

## Task 1: Capture the real object state shapes

**Files:**
- Create: `tests/fixtures/state/README.md`
- Create: `tests/fixtures/state/image_with_derivative.json`
- Create: `tests/fixtures/state/image_without_derivative.json`
- Create: `tests/fixtures/state/two_image_fields.json`

**Interfaces:**
- Consumes: nothing.
- Produces: JSON files holding real `object_state.state` payloads, used as
  fixtures by Tasks 4, 5 and 6. The keys they are checked for are
  `_blob`, `_width`, `_height`, `contentType` and `_pgthumbor_source`.

- [ ] **Step 1: Bring up a Plone with both add-ons**

Use the workspace `production-like` setup, which already runs Postgres,
Tika and Thumbor:

```bash
cd ../../../setups/production-like
docker compose up -d postgres tika thumbor
```

Install `plone.pgcatalog` and `plone.pgthumbor` in the instance, and set
`PGTHUMBOR_SOURCE_MAX_EDGE=4000` so derivative generation is on.

- [ ] **Step 2: Upload three images through the Plone UI**

- one 15000x11000 JPEG, which is over the cap and must get a derivative
- one 800x600 JPEG, which is under the cap and must not
- one content item with two image fields populated, a lead image and an
  image attachment, where only the first is oversized

Generate the JPEGs with the script in the spec's appendix.

- [ ] **Step 3: Dump the states**

```bash
docker compose exec postgres psql -U zodb -d zodb -At -c "
  SELECT jsonb_pretty(state::jsonb) FROM object_state
   WHERE state::text LIKE '%_blob%' ORDER BY tid DESC LIMIT 3"
```

Save each payload into the corresponding fixture file. Redact nothing;
these are synthetic objects.

- [ ] **Step 4: Answer the three step-0 questions in the README**

Write `tests/fixtures/state/README.md` recording, with the evidence
inline, whether:

1. the field value carries `_width` and `_height`, and under exactly those
   key names;
2. `_pgthumbor_source` appears as a nested object with its own `_blob`
   `@ref`, its own `_width`/`_height`, and its own `contentType`;
3. the oversized image produced **two** rows in
   `text_extraction_queue`, confirming the double enqueue.

```sql
SELECT id, zoid, blob_zoid, content_type, status
  FROM text_extraction_queue ORDER BY id;
```

- [ ] **Step 5: Stop if an answer is no**

If `_width`/`_height` are absent, Task 5 cannot read dimensions from state
and the plan needs a new source for them, most likely Pillow on the
derivative only. If `_pgthumbor_source` is not in the state, Task 4's
pairing has nothing to pair and the whole bounded-rendition approach needs
rethinking. Either outcome is a stop-and-report, not a workaround.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/state/
git commit -m "test: capture real object state fixtures for Tika rendition work

Assisted-by: Claude Opus 5"
```

## Task 2: Measure the OCR accuracy cost of downscaling

**Files:**
- Create: `benchmarks/tika_ocr_downscale.md`

**Interfaces:**
- Consumes: nothing.
- Produces: the measured default for `PGCATALOG_TIKA_MAX_IMAGE_PIXELS`,
  consumed as a constant by Task 6.

- [ ] **Step 1: Build a text-bearing oversized scan**

A photograph has no text, so the 165 MP JPEG from the spec cannot answer
this. Render a dense page of text at A0 size and 300 dpi, about 11000 px
on the long edge, then a 4000 px version of the same page.

```bash
python3 - <<'PY'
from PIL import Image, ImageDraw, ImageFont
import textwrap
W, H = 9933, 14043           # A0 at 300 dpi
img = Image.new("RGB", (W, H), "white")
d = ImageDraw.Draw(img)
font = ImageFont.truetype(
    "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf", 42)
body = ("Kaufvertrag ueber die Liegenschaft Einlagezahl 412 "
        "Grundbuch Attersee samt allen Rechten und Pflichten. ") * 200
y = 200
for line in textwrap.wrap(body, width=150)[:260]:
    d.text((200, y), line, fill="black", font=font)
    y += 52
img.save("scan-full.jpg", "JPEG", quality=85)
img.resize((4000, int(4000 * H / W)), Image.LANCZOS).save(
    "scan-4000.jpg", "JPEG", quality=85)
PY
```

- [ ] **Step 2: OCR both against the -full image**

```bash
docker run -d --name tika-acc --memory=4g -p 9993:9998 apache/tika:3.2.3.0-full
for f in scan-full.jpg scan-4000.jpg; do
  printf "%-16s " $f
  curl -s -X PUT --data-binary @$f -H 'Content-Type: image/jpeg' \
       -H 'Accept: text/plain' http://localhost:9993/tika | wc -c
done
```

Note the 4 GiB limit: this task measures accuracy, not memory, and the
full-resolution run must actually complete to give a baseline.

- [ ] **Step 3: Compare against the known ground truth**

The input text is known exactly, so compute recall per resolution rather
than eyeballing character counts. Record in
`benchmarks/tika_ocr_downscale.md`: characters returned, the fraction of
the 200 repetitions of the phrase `Einlagezahl 412` found, and wall time
for each resolution.

- [ ] **Step 4: Decide the default and write it down**

If 4000 px recall is within a few percent of full resolution, keep
`PGCATALOG_TIKA_MAX_IMAGE_PIXELS = 16000000`. If it drops materially,
record the lowest edge that holds recall, convert it to a pixel count, and
use that as the default instead, together with the Tika memory limit that
pixel count requires. Either way the document states the number and the
evidence for it, because Task 6 hardcodes it.

- [ ] **Step 5: Tear down and commit**

```bash
docker rm -f tika-acc
git add benchmarks/tika_ocr_downscale.md
git commit -m "bench: OCR recall at full resolution vs a 4000 px rendition

Assisted-by: Claude Opus 5"
```

## Task 3: Capture the Tika payload fixtures

**Files:**
- Create: `tests/fixtures/tika/parsers_details_stock.json`
- Create: `tests/fixtures/tika/parsers_details_full.json`
- Create: `tests/fixtures/tika/rmeta_compound_zip.json`
- Create: `tests/fixtures/tika/rmeta_image_exif.json`
- Create: `tests/fixtures/tika/README.md`

**Interfaces:**
- Consumes: nothing.
- Produces: fixture files read by Tasks 7 and 10.

- [ ] **Step 1: Capture the two capability payloads**

```bash
docker run -d --name tk-s --memory=1g -p 9998:9998 apache/tika:3.2.3.0
docker run -d --name tk-f --memory=1g -p 9997:9998 apache/tika:3.2.3.0-full
sleep 15
curl -s -H 'Accept: application/json' http://localhost:9998/parsers/details \
  > tests/fixtures/tika/parsers_details_stock.json
curl -s -H 'Accept: application/json' http://localhost:9997/parsers/details \
  > tests/fixtures/tika/parsers_details_full.json
```

- [ ] **Step 2: Capture two `/rmeta/text` responses**

A compound document and an image with EXIF, built as in the spec's
appendix:

```bash
curl -s -X PUT --data-binary @compound.zip -H 'Content-Type: application/zip' \
  -H 'Accept: application/json' http://localhost:9998/rmeta/text \
  > tests/fixtures/tika/rmeta_compound_zip.json
curl -s -X PUT --data-binary @normal.jpg -H 'Content-Type: image/jpeg' \
  -H 'Accept: application/json' http://localhost:9998/rmeta/text \
  > tests/fixtures/tika/rmeta_image_exif.json
```

- [ ] **Step 3: Record provenance**

`tests/fixtures/tika/README.md` states the exact image tags, the date, and
the commands above. A fixture whose origin is unknown is a fixture nobody
dares update.

- [ ] **Step 4: Assert the expected invariants by hand once**

```bash
python3 -c "
import json
s=json.load(open('tests/fixtures/tika/parsers_details_stock.json'))
f=json.load(open('tests/fixtures/tika/parsers_details_full.json'))
def types(n,a=None):
    a=a if a is not None else set()
    a.update(n.get('supportedTypes') or [])
    for c in (n.get('children') or n.get('parsers') or []): types(c,a)
    return a
ts,tf=types(s),types(f)
assert not [t for t in ts if t.startswith('image/ocr-')], 'stock must have none'
assert len([t for t in tf if t.startswith('image/ocr-')])==8, 'full must have 8'
print('ok: stock 0, full 8 image/ocr-* types')
"
docker rm -f tk-s tk-f
```

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/tika/
git commit -m "test: capture real Tika 3.2.3 capability and rmeta payloads

Assisted-by: Claude Opus 5"
```

---

# PR 1 (Task 6) and PR 4 (Tasks 4, 7, 5)

Phase 0 changed the shipping order here. The double-enqueue bug this plan
expected **does not exist** (see `tests/fixtures/state/README.md`), so
Task 5 is no longer a fix and has no independent value. The grouping is
now:

- **PR 1 = Task 6 alone.** MIME normalisation. No new columns, no
  dependency on anything else in this plan, shippable immediately.
  **Done.**
- **PR 4 = Tasks 4, 5 and 7 together.** The reference walk, the
  `source_info` column and the enqueue that fills it. Apart they do
  nothing, and they share one migration.

These two are not adjacent in shipping order any more. Production runs the
stock Tika image, so the rendition groundwork helps aaf-prod only if OCR is
ever switched on, while retry (PR 2, Task 8) and metadata harvesting
(PR 3, Tasks 12 and 13) each fix something a real site is losing today.
Read the sections in document order, but ship in PR-number order.

Commit order inside PR 4 is **4, 7, 5**: the walk, then the column, then
the enqueue that fills it.

Task numbers are unchanged so the ledger and the Interfaces blocks stay
valid. Only the grouping moved.

## Task 4: Reference walk and per-state image facts

Phase 0 measured the real shape and it is not what this task originally
assumed. A `NamedBlobImage` is its own persistent object, and a pgthumbor
derivative is a second one hanging off it, so an image's blob sits three
persistent hops from the content object and the derivative's blob one hop
further. A single function over a single state cannot pair them.

Chosen resolution: **walk then fetch.** Every function here reads exactly
one `object_state` and the caller fetches the next, so the module stays
pure, the processor and the standalone worker can share it, and the unit
tests need no database. The alternative, a resolver callback, would have
put I/O inside the pairing function.

The `@ref` marker's two-element form carries the dotted class name, so a
wrapper ref is distinguishable from a blob ref **without fetching
either**, which is what makes the walk cheap.

**Files:**
- Create: `src/plone/pgcatalog/blobrefs.py`
- Create: `tests/test_blobrefs.py`

**Interfaces:**
- Consumes: the fixtures from Task 1.
- Produces:
  - `Ref` frozen dataclass: `path: tuple[str, ...]`, `zoid: int`,
    `class_name: str | None`
  - `walk_refs(state) -> list[Ref]`
  - `blob_wrapper_refs(state) -> list[Ref]` — the content object's refs to
    `NamedBlob*` objects, excluding anything under a derivative
  - `ImageFacts` frozen dataclass: `blob_zoid: int | None`,
    `width: int | None`, `height: int | None`,
    `content_type: str | None`, `derivative_zoid: int | None`
  - `image_facts(wrapper_state) -> ImageFacts` — reads **one**
    `NamedBlob*` state
  - `DERIVATIVE_KEY = "_pgthumbor_source"`, `BLOB_CLASS = "ZODB.blob.Blob"`

The caller shape, which Tasks 5 and 11 both follow:

```python
for ref in blob_wrapper_refs(content_state):
    facts = image_facts(fetch_state(ref.zoid))
    if facts.derivative_zoid is not None:
        derivative = image_facts(fetch_state(facts.derivative_zoid))
```

- [ ] **Step 1: Write the failing test**

```python
"""Tests for reference walking and per-state image facts.

The fixtures are real ZODB output, not hand-written shapes: see
tests/fixtures/state/README.md for how they were captured and why the
structure is three levels deep.
"""

from plone.pgcatalog.blobrefs import blob_wrapper_refs
from plone.pgcatalog.blobrefs import image_facts

import json
import pytest


def load(name):
    with open(f"tests/fixtures/state/{name}.json") as fh:
        return json.load(fh)


def graph(doc):
    """A fetch_state callable over a fixture's own object graph."""
    objects = {int(z): s for z, s in doc["objects"].items()}
    return objects.get


def test_walk_finds_the_image_wrapper_with_its_class_name():
    doc = load("image_with_derivative")
    content = doc["objects"][str(doc["content_zoid"])]
    refs = blob_wrapper_refs(content)
    assert len(refs) == 1
    assert refs[0].path == ("image",)
    assert refs[0].class_name == "plone.namedfile.file.NamedBlobImage"


def test_image_facts_reads_one_wrapper_state():
    doc = load("image_with_derivative")
    fetch = graph(doc)
    content = doc["objects"][str(doc["content_zoid"])]
    (ref,) = blob_wrapper_refs(content)

    facts = image_facts(fetch(ref.zoid))
    assert (facts.width, facts.height) == (15000, 11000)
    assert facts.content_type == "image/jpeg"
    assert facts.blob_zoid in doc["blob_zoids"]
    assert facts.derivative_zoid is not None


def test_the_derivative_is_one_more_fetch_away():
    """The finding that reshaped this task: the derivative's dimensions are
    in its own object_state, not in the original's."""
    doc = load("image_with_derivative")
    fetch = graph(doc)
    content = doc["objects"][str(doc["content_zoid"])]
    (ref,) = blob_wrapper_refs(content)
    original = image_facts(fetch(ref.zoid))

    derivative = image_facts(fetch(original.derivative_zoid))
    assert max(derivative.width, derivative.height) == 4000
    assert derivative.blob_zoid != original.blob_zoid
    assert derivative.blob_zoid in doc["blob_zoids"]
    assert derivative.derivative_zoid is None, "a derivative has no derivative"


def test_small_image_has_no_derivative():
    doc = load("image_without_derivative")
    fetch = graph(doc)
    content = doc["objects"][str(doc["content_zoid"])]
    (ref,) = blob_wrapper_refs(content)
    facts = image_facts(fetch(ref.zoid))
    assert (facts.width, facts.height) == (800, 600)
    assert facts.derivative_zoid is None


def test_two_fields_do_not_cross_assign_the_derivative():
    """Review Focus 2: a lead image's derivative must not land on the
    attachment, and the attachment must report none at all."""
    doc = load("two_image_fields")
    fetch = graph(doc)
    content = doc["objects"][str(doc["content_zoid"])]
    by_field = {r.path: image_facts(fetch(r.zoid)) for r in blob_wrapper_refs(content)}
    assert set(by_field) == {("image",), ("attachment",)}
    assert by_field[("image",)].derivative_zoid is not None
    assert by_field[("attachment",)].derivative_zoid is None
    assert (by_field[("image",)].width, by_field[("image",)].height) == (
        15000,
        11000,
    )
    assert (
        by_field[("attachment",)].width,
        by_field[("attachment",)].height,
    ) == (800, 600)


def test_blob_wrapper_refs_skips_refs_under_a_derivative():
    """Walking a wrapper state must not report the derivative as a field."""
    doc = load("image_with_derivative")
    fetch = graph(doc)
    content = doc["objects"][str(doc["content_zoid"])]
    (ref,) = blob_wrapper_refs(content)
    assert blob_wrapper_refs(fetch(ref.zoid)) == []


@pytest.mark.parametrize("state", [None, {}, "", "not json", [], 7])
def test_degenerate_states_are_empty_not_raising(state):
    assert blob_wrapper_refs(state) == []
    facts = image_facts(state)
    assert facts.blob_zoid is None
    assert facts.width is None


def test_missing_dimensions_are_none_not_zero():
    """Review Focus 1: a legacy wrapper has no _width/_height."""
    facts = image_facts({"_blob": {"@ref": ["00000000000003e8", "ZODB.blob.Blob"]}})
    assert facts.blob_zoid == 1000
    assert facts.width is None and facts.height is None
    assert facts.content_type is None


def test_single_element_ref_form_is_accepted():
    """Not every @ref carries a class name; the short form must still walk."""
    facts = image_facts({"_blob": {"@ref": "00000000000003e8"}})
    assert facts.blob_zoid == 1000
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_blobrefs.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'plone.pgcatalog.blobrefs'`

- [ ] **Step 3: Write the implementation**

```python
"""Reference walking and per-state image facts.

Phase 0 measured the real shape, recorded in
``tests/fixtures/state/README.md``: a ``NamedBlobImage`` is its own
persistent object and a pgthumbor source derivative is a second one
hanging off it, so an image's blob sits three persistent hops from the
content object and the derivative's blob one hop further.

Nothing here does I/O.  Each function reads exactly one ``object_state``
and the caller fetches the next, which is what lets the state processor
and the standalone worker share this module and lets the unit tests run
without a database.
"""

from dataclasses import dataclass

import json


__all__ = [
    "BLOB_CLASS",
    "DERIVATIVE_KEY",
    "ImageFacts",
    "Ref",
    "blob_wrapper_refs",
    "image_facts",
    "walk_refs",
]


# pgthumbor stores the derivative under this attribute of the wrapper.
# A cross-package contract; see the design doc and pgthumbor's own docs.
DERIVATIVE_KEY = "_pgthumbor_source"
BLOB_CLASS = "ZODB.blob.Blob"

# plone.namedfile's blob-backed wrappers: NamedBlobImage and NamedBlobFile.
_WRAPPER_MARKER = "NamedBlob"


@dataclass(frozen=True)
class Ref:
    """One ``@ref`` marker, with where it sat and what it points at."""

    path: tuple[str, ...]
    zoid: int
    class_name: str | None = None


@dataclass(frozen=True)
class ImageFacts:
    """What one ``NamedBlob*`` state says about itself.

    ``derivative_zoid`` is another wrapper's zoid, not a blob's: reading
    its dimensions takes a second fetch.
    """

    blob_zoid: int | None = None
    width: int | None = None
    height: int | None = None
    content_type: str | None = None
    derivative_zoid: int | None = None


def _as_dict(state):
    """*state* as a dict, or an empty dict for anything unusable."""
    if isinstance(state, str):
        try:
            state = json.loads(state)
        except (json.JSONDecodeError, TypeError, ValueError):
            return {}
    return state if isinstance(state, dict) else {}


def _parse_ref(value):
    """``(zoid, class_name)`` for an ``@ref`` marker, else None.

    Both forms occur: ``{"@ref": "hex"}`` and
    ``{"@ref": ["hex", "dotted.Class"]}``.  The two-element form is what
    zodb-json-codec writes for a persistent reference, and its class name
    is why a wrapper can be told from a blob without fetching it.
    """
    if not isinstance(value, dict):
        return None
    ref = value.get("@ref")
    if ref is None:
        return None
    if isinstance(ref, list):
        hex_oid = ref[0] if ref else None
        class_name = ref[1] if len(ref) > 1 else None
    else:
        hex_oid, class_name = ref, None
    if not isinstance(hex_oid, str) or len(hex_oid) != 16:
        return None
    try:
        return int(hex_oid, 16), class_name
    except ValueError:
        return None


def walk_refs(state):
    """Every ``@ref`` in *state* as a :class:`Ref`, keeping its path.

    List indices are not part of the path: a ref inside a list gets the
    path of the key that holds the list.
    """
    found = []

    def walk(obj, path):
        parsed = _parse_ref(obj)
        if parsed is not None:
            found.append(Ref(path, parsed[0], parsed[1]))
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                walk(value, (*path, key))
        elif isinstance(obj, list):
            for item in obj:
                walk(item, path)

    walk(_as_dict(state), ())
    return found


def blob_wrapper_refs(state):
    """Refs from a *content* state to its ``NamedBlob*`` field values.

    Refs reached through :data:`DERIVATIVE_KEY` are excluded, so walking a
    wrapper's own state does not report its derivative as a field.
    """
    return [
        ref
        for ref in walk_refs(state)
        if ref.class_name
        and _WRAPPER_MARKER in ref.class_name
        and DERIVATIVE_KEY not in ref.path
    ]


def image_facts(wrapper_state):
    """Read one ``NamedBlob*`` state.

    Dimensions come back as None rather than 0 when absent, because a
    legacy upload can lack them and the pixel guard must be able to tell
    "no opinion" from "no pixels".
    """
    state = _as_dict(wrapper_state)
    blob = _parse_ref(state.get("_blob"))
    derivative = _parse_ref(state.get(DERIVATIVE_KEY))
    return ImageFacts(
        blob_zoid=blob[0] if blob else None,
        width=state.get("_width"),
        height=state.get("_height"),
        content_type=state.get("contentType"),
        derivative_zoid=derivative[0] if derivative else None,
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest tests/test_blobrefs.py -v`
Expected: 15 passed (the parametrised degenerate case counts six).

- [ ] **Step 5: Check the lint ceiling**

Run: `uvx ruff@0.16.7 check src/plone/pgcatalog/blobrefs.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/blobrefs.py tests/test_blobrefs.py
git commit -m "feat: reference walk and per-state image facts

Walk then fetch, rather than one function over one state: phase 0
measured that a NamedBlobImage is its own persistent object and the
pgthumbor derivative a second one hanging off it, so pairing needs two
fetches. Keeping these pure lets the processor and the standalone worker
share them and lets the tests run with no database. Refs #222.

Assisted-by: Claude Opus 5"
```

## Task 5: Record source facts at enqueue

Not a bug fix. Phase 0 verified that the enqueue path is already correct:
two image fields produce two rows, one per field, and the derivative is
reached as an inner ref, found absent from `blob_state`, and properly
dropped. This task **adds** the facts the worker needs to route the job,
and changes no existing row's identity.

Run after Task 7, since it writes a column Task 7 creates.

**Files:**
- Modify: `src/plone/pgcatalog/processor.py`, candidate accumulation
  (213-228) and `_resolve_wrappers` / `_enqueue_candidate` (395-448)
- Modify: `tests/test_tika_enqueue.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: `blob_wrapper_refs`, `image_facts`, `ImageFacts` from Task 4;
  the `source_info` column from Task 7.
- Produces: queue rows whose `source_info` holds
  `{"width", "height", "derivative_zoid", "derivative_tid",
  "derivative_width", "derivative_height", "derivative_content_type"}`,
  every key optional. Row identity, `blob_zoid` and `tid`, is unchanged.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_tika_enqueue.py`. The fixture graph is written into
`object_state` so the processor resolves it the way production does,
rather than being handed a shape the test invented.

```python
class TestSourceInfoAtEnqueue:
    """The queue row carries what the worker needs to route the job."""

    def test_oversized_image_records_its_derivative(self, tika_db):
        doc = load_state_fixture("image_with_derivative")
        rows = enqueue_fixture(tika_db, doc, mime_type="image/jpeg")

        assert len(rows) == 1, "one row per image field, unchanged"
        info = rows[0]["source_info"]
        assert (info["width"], info["height"]) == (15000, 11000)
        assert info["derivative_zoid"] is not None
        assert max(info["derivative_width"], info["derivative_height"]) == 4000
        assert info["derivative_content_type"] == "image/jpeg"
        assert rows[0]["blob_zoid"] != info["derivative_zoid"], (
            "the row still points at the original"
        )

    def test_small_image_records_dimensions_and_no_derivative(self, tika_db):
        doc = load_state_fixture("image_without_derivative")
        rows = enqueue_fixture(tika_db, doc, mime_type="image/jpeg")
        info = rows[0]["source_info"]
        assert (info["width"], info["height"]) == (800, 600)
        assert "derivative_zoid" not in info

    def test_two_fields_get_a_row_each_with_their_own_facts(self, tika_db):
        """Review Focus 2, at the enqueue level."""
        doc = load_state_fixture("two_image_fields")
        rows = enqueue_fixture(tika_db, doc, mime_type="image/jpeg")
        assert len(rows) == 2
        by_size = {r["source_info"]["width"]: r["source_info"] for r in rows}
        assert set(by_size) == {15000, 800}
        assert by_size[15000]["derivative_zoid"] is not None
        assert "derivative_zoid" not in by_size[800]

    def test_no_dimensions_means_no_dimension_keys(self, tika_db):
        """Review Focus 1: a legacy wrapper with no _width/_height must
        still enqueue, with source_info simply lacking them."""
        doc = {
            "content_zoid": 900,
            "blob_zoids": [902],
            "objects": {
                "900": {
                    "file": {
                        "@ref": [
                            "0000000000000385",
                            "plone.namedfile.file.NamedBlobFile",
                        ]
                    }
                },
                "901": {"_blob": {"@ref": ["0000000000000386", "ZODB.blob.Blob"]}},
            },
        }
        rows = enqueue_fixture(tika_db, doc, mime_type="application/pdf")
        assert len(rows) == 1
        assert rows[0]["source_info"] is None
```

Two helpers in the same commit:

```python
def load_state_fixture(name):
    with open(f"tests/fixtures/state/{name}.json") as fh:
        return json.load(fh)


def enqueue_fixture(conn, doc, mime_type):
    """Write a fixture's object graph into PG, enqueue, return the rows.

    The states go into object_state and the blobs into blob_state so the
    processor's own resolution runs, rather than the test standing in for
    it.
    """
    with conn.cursor() as cur:
        for zoid, state in doc["objects"].items():
            cur.execute(
                "INSERT INTO object_state (zoid, tid, state) "
                "VALUES (%s, 1, %s) ON CONFLICT DO NOTHING",
                (int(zoid), Json(state)),
            )
        for blob_zoid in doc["blob_zoids"]:
            cur.execute(
                "INSERT INTO blob_state (zoid, tid, chunk) "
                "VALUES (%s, 1, %s) ON CONFLICT DO NOTHING",
                (blob_zoid, b"x"),
            )
        conn.commit()

    content_zoid = doc["content_zoid"]
    set_pending(
        content_zoid,
        {
            "path": "/plone/item",
            "idx": {"portal_type": "Image", "mime_type": mime_type},
            "searchable_text": "",
        },
    )
    proc = CatalogStateProcessor()
    proc._tika_candidates = []
    with mock.patch("plone.pgcatalog.processor.TIKA_URL", "http://tika:9998"):
        state = json.dumps(doc["objects"][str(content_zoid)])
        proc.process(content_zoid, "plone.dexterity.content", "Image", state)
        with conn.cursor() as cur:
            proc.finalize(cur)
        conn.commit()

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT blob_zoid, content_type, source_info "
            "  FROM text_extraction_queue WHERE zoid = %s ORDER BY id",
            (content_zoid,),
        )
        return cur.fetchall()
```

- [ ] **Step 2: Run to verify it fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_enqueue.py -k SourceInfo -v`
Expected: FAIL. `source_info` comes back `None` for every row, because
nothing writes it yet.

- [ ] **Step 3: Fetch the second hop**

`_resolve_wrappers` currently does one batched `object_state` query:
content refs that are not blobs are fetched, and their inner refs are
looked up in `blob_state`. A derivative is one hop further, so a second
batched query is needed. One extra query per transaction batch, not per
object.

```python
def _resolve_derivatives(self, cursor, wrapper_states):
    """Facts for every derivative referenced by a fetched wrapper.

    A second batched round trip: the derivative is itself a
    NamedBlob* object, so its blob, dimensions and content type live
    in its own object_state row.  See tests/fixtures/state/README.md.
    """
    wanted = {
        facts.derivative_zoid
        for facts in wrapper_states.values()
        if facts.derivative_zoid is not None
    }
    if not wanted:
        return {}
    cursor.execute(
        "SELECT DISTINCT ON (zoid) zoid, state FROM object_state "
        "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
        {"zoids": list(wanted)},
    )
    return {row["zoid"]: image_facts(row["state"]) for row in cursor.fetchall()}
```

- [ ] **Step 4: Build the payload**

```python
def _source_info(facts, derivative, blob_rows):
    """The ``source_info`` payload for one image field.

    Only facts known before extraction, and only ones the worker routes
    on: extraction results belong in ``searchable_text``.  Returns None
    rather than an empty dict so a row with nothing to say stores SQL
    NULL instead of ``{}``.
    """
    info = {}
    if facts.width and facts.height:
        info["width"] = facts.width
        info["height"] = facts.height
    if derivative is not None and derivative.blob_zoid in blob_rows:
        info["derivative_zoid"] = derivative.blob_zoid
        info["derivative_tid"] = blob_rows[derivative.blob_zoid]
        if derivative.width and derivative.height:
            info["derivative_width"] = derivative.width
            info["derivative_height"] = derivative.height
        if derivative.content_type:
            info["derivative_content_type"] = derivative.content_type
    return info or None
```

Note that `derivative_zoid` in `source_info` is the derivative's **blob**
zoid, not its wrapper's: the worker fetches a blob, so storing the wrapper
zoid would make it repeat the hop. The wrapper zoid is not kept.

Then pass it through `_enqueue_candidate` into the sixth argument of
`_insert_queue_row` from Task 7, and change the candidate accumulation at
`processor.py:213-228` to store `blob_wrapper_refs(state)` under
`"wrapper_refs"` instead of `_collect_ref_oids(state)` under
`"blob_refs"`.

`_collect_ref_oids` stays where it is for its other caller; it is not
removed by this task.

- [ ] **Step 5: Run the whole enqueue suite**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_enqueue.py -v`
Expected: all pass, including the eight pre-existing `len(rows) == 1`
assertions, whose meaning must not have changed.

- [ ] **Step 6: Check the lint ceiling**

Run: `uvx ruff@0.16.7 check src/plone/pgcatalog/processor.py`
Expected: clean. If `finalize` crossed C901's 13, the derivative round
trip moves into its own method rather than taking a `noqa`.

- [ ] **Step 7: Add the changelog entry and commit**

```bash
git add src/plone/pgcatalog/processor.py tests/test_tika_enqueue.py CHANGES.md
git commit -m "feat: record source facts on the Tika queue row

A queued extraction now carries the image's pixel dimensions and, when
pgthumbor has produced one, its source derivative's blob, dimensions and
content type. The worker routes on these without fetching a blob.

Row identity is unchanged: the row still points at the original, and two
image fields still produce two rows. Reaching the derivative needs a
second batched object_state query, because a derivative is its own
persistent NamedBlob* object rather than an inline attribute. Refs #222.

Assisted-by: Claude Opus 5"
```

## Task 6: MIME normalisation with parameter fallback

**Files:**
- Create: `src/plone/pgcatalog/mimetypes.py`
- Create: `tests/test_mimetypes.py`
- Modify: `src/plone/pgcatalog/processor.py:64-68`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `normalise(content_type: str | None) -> str | None`
  - `matches(content_type: str | None, allowed: set[str]) -> bool`

- [ ] **Step 1: Write the failing test**

```python
"""Tests for MIME type normalisation used by the Tika enqueue gate."""

from plone.pgcatalog.mimetypes import matches
from plone.pgcatalog.mimetypes import normalise


def test_lowercases_and_strips_whitespace():
    assert normalise("  APPLICATION/PDF ") == "application/pdf"


def test_collapses_parameter_whitespace_but_keeps_the_parameter():
    assert normalise("audio/ogg;codecs=opus") == "audio/ogg; codecs=opus"
    assert normalise("audio/ogg ;  codecs=opus") == "audio/ogg; codecs=opus"


def test_none_and_empty_are_none():
    assert normalise(None) is None
    assert normalise("   ") is None


def test_parameterised_value_matches_a_bare_configured_type():
    """The production shape that never matched before."""
    assert matches("text/plain; charset=utf-8", {"text/plain"})


def test_full_string_wins_over_the_bare_type():
    """Tika's own type set contains parameterised entries, so an explicit
    parameterised configuration entry must stay meaningful."""
    allowed = {"audio/ogg; codecs=opus"}
    assert matches("audio/ogg; codecs=opus", allowed)
    assert not matches("audio/ogg", allowed)


def test_bare_type_still_matches_exactly():
    assert matches("application/pdf", {"application/pdf"})
    assert not matches("application/zip", {"application/pdf"})


def test_no_content_type_never_matches():
    assert not matches(None, {"application/pdf"})
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_mimetypes.py -v`
Expected: FAIL, `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
"""MIME type normalisation for the Tika extraction gate.

``_should_extract`` used to compare the catalogued ``mime_type`` against a
set by exact string, so ``text/plain; charset=utf-8`` and
``APPLICATION/PDF`` never matched although Plone can hold both.  Blanket
parameter stripping is not the fix either: Tika's own supported-type set
contains parameterised entries such as ``audio/ogg; codecs=opus``, so the
full value is tried first and the bare type second.
"""

__all__ = ["matches", "normalise"]


def normalise(content_type):
    """*content_type* lowercased, with parameter spacing canonicalised.

    Returns None for None, empty and whitespace-only input, which is what
    the caller treats as "no opinion".
    """
    if not content_type or not content_type.strip():
        return None
    parts = [p.strip() for p in content_type.strip().lower().split(";")]
    essence, params = parts[0], [p for p in parts[1:] if p]
    return "; ".join([essence, *params])


def matches(content_type, allowed):
    """Whether *content_type* is in *allowed*, full value before bare type."""
    normalised = normalise(content_type)
    if normalised is None:
        return False
    if normalised in allowed:
        return True
    return normalised.split(";")[0] in allowed
```

- [ ] **Step 4: Point `_should_extract` at it**

```python
def _should_extract(content_type):
    """Check if a content type should be sent to Tika for extraction."""
    return matches(content_type, TIKA_CONTENT_TYPES)
```

`TIKA_CONTENT_TYPES` is built from the environment, so normalise its
entries at construction too, otherwise a configured `Application/PDF`
still fails to match.

- [ ] **Step 5: Run both suites**

Run: `.venv/bin/pytest tests/test_mimetypes.py tests/test_tika_enqueue.py -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/mimetypes.py tests/test_mimetypes.py \
        src/plone/pgcatalog/processor.py CHANGES.md
git commit -m "fix: normalise MIME types before the Tika extraction gate

text/plain; charset=utf-8 and APPLICATION/PDF never matched the
configured set. The full normalised value is tried before the bare type,
so a parameterised configuration entry such as audio/ogg; codecs=opus
stays meaningful. Refs #222.

Assisted-by: Claude Opus 5"
```

## Task 7: Schema migration for `source_info`

Run before Task 5, which writes the column this task creates.

**Files:**
- Modify: `src/plone/pgcatalog/schema.py`, the `TEXT_EXTRACTION_QUEUE` constant
- Modify: `src/plone/pgcatalog/processor.py`, `_insert_queue_row`
- Modify: `tests/test_tika_worker.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: nothing. Task 5 is its consumer.
- Produces: a `source_info JSONB` column, nullable, and
  `_insert_queue_row(cursor, zoid, blob_zoid, tid, content_type, source_info)`.

- [ ] **Step 1: Write the failing test**

```python
def test_source_info_column_is_added_idempotently(worker_db):
    """Re-running the DDL on an existing table must not fail."""
    worker_db.execute(TEXT_EXTRACTION_QUEUE)
    worker_db.commit()
    row = worker_db.execute(
        "SELECT data_type, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'text_extraction_queue' "
        "  AND column_name = 'source_info'"
    ).fetchone()
    assert row["data_type"] == "jsonb"
    assert row["is_nullable"] == "YES"
```

- [ ] **Step 2: Run to verify it fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_worker.py -k source_info -v`
Expected: FAIL, the query returns None.

- [ ] **Step 3: Add the DDL**

In the `TEXT_EXTRACTION_QUEUE` constant, beside the existing
`blob_zoid` migration and in the same idempotent style:

```sql
ALTER TABLE text_extraction_queue
    ADD COLUMN IF NOT EXISTS source_info JSONB;
```

Add `source_info` to the `CREATE TABLE` body as well, so a fresh install
and a migrated one end up identical.

- [ ] **Step 4: Write it from `_insert_queue_row`**

```python
    def _insert_queue_row(
        self, cursor, zoid, blob_zoid, tid, content_type, source_info=None
    ):
        cursor.execute(
            "INSERT INTO text_extraction_queue "
            "  (zoid, blob_zoid, tid, content_type, source_info) "
            "VALUES (%(zoid)s, %(blob_zoid)s, %(tid)s, %(ct)s, %(si)s) "
            "ON CONFLICT (blob_zoid, tid) DO NOTHING",
            {
                "zoid": zoid,
                "blob_zoid": blob_zoid,
                "tid": tid,
                "ct": content_type,
                "si": Json(source_info) if source_info else None,
            },
        )
```

- [ ] **Step 5: Run the suites**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_worker.py tests/test_tika_enqueue.py -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/schema.py src/plone/pgcatalog/processor.py \
        tests/test_tika_worker.py CHANGES.md
git commit -m "feat: record source facts for a queued extraction in source_info

Refs #222.

Assisted-by: Claude Opus 5"
```

---

# PR 2: retry that can bridge a Tika restart

**Ships second, not third.** Production runs the stock image, so the
rendition work buys aaf-prod nothing today while this fixes its 564
`failed` rows. See the spec's PR decomposition.

Fixes proposal 3 of #222 on its own, and provides the deferral machinery
that PR 4 needs. Shippable without any of the rendition work.

## Task 8: `not_before`, `deferrals`, and a dequeue predicate that stays narrow

**Files:**
- Modify: `src/plone/pgcatalog/schema.py`
- Modify: `src/plone/pgcatalog/tika_worker.py:121-191`
- Modify: `tests/test_tika_worker.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - columns `not_before TIMESTAMPTZ NOT NULL DEFAULT now()` and
    `deferrals INTEGER NOT NULL DEFAULT 0`
  - `TikaWorker._defer(conn, job_id, delay, reason)`
  - `BACKOFF_LADDER = (5, 30, 120)`

- [ ] **Step 1: Write the failing tests**

```python
TRANSPORT_ERRORS = (
    httpx.ConnectError("refused"),
    httpx.ConnectTimeout("timed out"),
    httpx.RemoteProtocolError("server disconnected"),
)


@pytest.mark.parametrize("exc", TRANSPORT_ERRORS, ids=lambda e: type(e).__name__)
def test_transport_error_defers_without_spending_an_attempt(worker_db, exc):
    """Three attempts in a second cannot bridge a 30 s restart (#222)."""
    queue_one_job(worker_db, job_id=1)
    worker = TikaWorker(DSN, "http://tika:9998")
    with patch.object(worker, "_extract", side_effect=exc):
        worker._process_one()
    row = fetch_job(worker_db, 1)
    assert row["status"] == "pending"
    assert row["attempts"] == 0, "a missing server says nothing about the job"
    assert row["deferrals"] == 1
    assert row["not_before"] > row["created_at"]


def test_backoff_ladder_grows_then_caps(worker_db):
    queue_one_job(worker_db, job_id=2)
    worker = TikaWorker(DSN, "http://tika:9998")
    delays = []
    for _ in range(4):
        worker_db.execute(
            "UPDATE text_extraction_queue SET not_before = now() WHERE id = 2"
        )
        worker_db.commit()
        with patch.object(
            worker, "_extract", side_effect=httpx.ConnectError("refused")
        ):
            worker._process_one()
        row = fetch_job(worker_db, 2)
        delays.append(round((row["not_before"] - row["updated_at"]).total_seconds()))
    assert delays == [5, 30, 120, 120]


def test_other_errors_still_spend_an_attempt(worker_db):
    queue_one_job(worker_db, job_id=3)
    worker = TikaWorker(DSN, "http://tika:9998")
    with patch.object(worker, "_extract", side_effect=ValueError("bad body")):
        worker._process_one()
    row = fetch_job(worker_db, 3)
    assert row["attempts"] == 1
    assert row["deferrals"] == 0


def test_deferred_job_is_invisible_until_not_before(worker_db):
    queue_one_job(worker_db, job_id=4)
    worker_db.execute(
        "UPDATE text_extraction_queue "
        "   SET not_before = now() + interval '1 hour' WHERE id = 4"
    )
    worker_db.commit()
    worker = TikaWorker(DSN, "http://tika:9998")
    assert worker._process_one() is False, "nothing claimable yet"


def test_skipped_and_exhausted_rows_are_never_resurrected(worker_db):
    """Review Focus 5: adding not_before must not widen the predicate."""
    queue_one_job(worker_db, job_id=5)
    queue_one_job(worker_db, job_id=6)
    worker_db.execute(
        "UPDATE text_extraction_queue SET status = 'skipped' WHERE id = 5"
    )
    worker_db.execute(
        "UPDATE text_extraction_queue "
        "   SET attempts = max_attempts, not_before = now() WHERE id = 6"
    )
    worker_db.commit()
    worker = TikaWorker(DSN, "http://tika:9998")
    assert worker._process_one() is False
```

- [ ] **Step 2: Run to verify they fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_worker.py -k "defer or backoff or resurrect" -v`
Expected: FAIL, `column "deferrals" does not exist`.

- [ ] **Step 3: Add the DDL and the index**

```sql
ALTER TABLE text_extraction_queue
    ADD COLUMN IF NOT EXISTS not_before TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE text_extraction_queue
    ADD COLUMN IF NOT EXISTS deferrals INTEGER NOT NULL DEFAULT 0;

DROP INDEX IF EXISTS idx_teq_pending;
CREATE INDEX IF NOT EXISTS idx_teq_pending
    ON text_extraction_queue (not_before, id) WHERE status = 'pending';
```

The index leads on `not_before` because the dequeue now filters on it
before ordering by `id`.

- [ ] **Step 4: Narrow the dequeue and add the deferral path**

Add `AND not_before <= now()` to the claim query's `WHERE`, keeping
`status = 'pending'` and `attempts < max_attempts` exactly as they are so
the predicate only ever narrows.

```python
BACKOFF_LADDER = (5, 30, 120)

_TRANSPORT_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.RemoteProtocolError,
)


    def _defer(self, conn, job_id, delay, reason):
        """Re-queue a job later without spending one of its attempts.

        A Tika that is absent, restarting or mid-deploy says nothing about
        the job, so counting it as an attempt is what turned 102 transient
        failures into ``failed`` rows in #222.
        """
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE text_extraction_queue SET "
                "  status = 'pending', "
                "  attempts = GREATEST(attempts - 1, 0), "
                "  deferrals = deferrals + 1, "
                "  not_before = now() + make_interval(secs => %(delay)s), "
                "  error = %(reason)s, updated_at = now() "
                "WHERE id = %(id)s",
                {"delay": delay, "reason": reason[:1000], "id": job_id},
            )
            conn.commit()
```

`attempts` is decremented because the claim query already incremented it;
`GREATEST(..., 0)` keeps it from going negative if a row is deferred
before ever being claimed.

In the `except` block of `_process_one`, branch on the exception type:

```python
            except Exception as exc:
                if isinstance(exc, _TRANSPORT_ERRORS):
                    delay = BACKOFF_LADDER[
                        min(row["deferrals"], len(BACKOFF_LADDER) - 1)
                    ]
                    log.info(
                        "Tika unreachable, deferring job %d by %ds: %s",
                        job_id, delay, exc,
                    )
                    self._defer(conn, job_id, delay, f"deferred: {exc}")
                    return True
                ...existing failure handling unchanged...
```

The claim query must return `deferrals` for this to work; add it to the
`RETURNING` list.

- [ ] **Step 5: Run to verify they pass**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_worker.py -v`
Expected: all pass, including the pre-existing failure-path tests.

- [ ] **Step 6: Note the NOTIFY gap in the docstring**

`trg_notify_extraction` fires `AFTER INSERT` only, so a deferred job gets
no wakeup when its `not_before` passes; the `poll_interval` fallback is
what picks it up. Say so where the poll loop is defined, so nobody later
"optimises" the polling away.

- [ ] **Step 7: Commit**

```bash
git add src/plone/pgcatalog/schema.py src/plone/pgcatalog/tika_worker.py \
        tests/test_tika_worker.py CHANGES.md
git commit -m "fix: defer Tika jobs with backoff instead of failing them

Three attempts inside a second cannot bridge a Tika restart, which is how
102 transient connection errors became failed rows needing a manual SQL
reset. Connection-level errors now re-queue on a 5/30/120 s ladder
without spending an attempt. Fixes proposal 3 of #222.

Assisted-by: Claude Opus 5"
```

---

# PR 5: OCR probe, decision matrix, dequeue-time resolution

**Ships last of the code PRs.** Without OCR the matrix always answers
"extract" and the rendition is never selected, so this is insurance
against a future `-full` switch rather than a fix for anything observed.

## Task 9: The OCR probe, by behaviour

Rewritten after measuring the production Tika. Introspection cannot
answer this on 4.x: stock and `-full` return semantically identical
`/parsers/details` (89 parser classes, same `supportedTypes`, 279 media
types, zero `image/ocr-*` in either, differing only in key ordering) while
`-full` demonstrably OCRs. The `image/ocr-*` signal existed on 3.2.3 and
was renamed away.

So the probe sends a known image through the real parser and reads the
answer. That also cannot go stale across a major version.

**Files:**
- Create: `src/plone/pgcatalog/tika_policy.py`
- Create: `tests/test_tika_policy.py`
- Asset: `src/plone/pgcatalog/assets/ocr_probe.png` (already committed,
  3990 bytes, 320x90, the word `TIKAOCR`)
- Modify: `pyproject.toml`, to ship the asset

**Interfaces:**
- Consumes: the probe asset; the fixtures from Task 3.
- Produces:
  - `OCR_PROBE_PATH` — the packaged asset's path
  - `probe_ocr(put_image) -> bool` where *put_image* is a callable taking
    the probe bytes and returning Tika's response text
  - `OcrProbe(put_image, interval=3600, override=None)` with
    `.available() -> bool`
  - `ocr_types(payload) -> set[str]` kept for diagnostics only, **not**
    used for the decision; it is what proves the 4.x payloads carry no
    signal

- [ ] **Step 1: Write the failing test**

```python
"""Tests for the Tika OCR capability probe and the extraction policy.

The probe is behavioural because introspection cannot answer the question
on Tika 4.x; `test_4_1_0_payloads_carry_no_ocr_signal` is what pins that.
"""

from plone.pgcatalog.tika_policy import ocr_types
from plone.pgcatalog.tika_policy import OCR_PROBE_PATH
from plone.pgcatalog.tika_policy import OcrProbe
from plone.pgcatalog.tika_policy import probe_ocr

import json
import pytest


def load(name):
    with open(f"tests/fixtures/tika/{name}") as fh:
        return json.load(fh)


def test_the_probe_asset_ships_and_is_the_size_we_measured():
    assert OCR_PROBE_PATH.exists()
    assert OCR_PROBE_PATH.stat().st_size == 3990


def test_text_back_means_ocr_is_available():
    """Measured: -full returns 'TIKAOCR' in 0.17 s."""
    assert probe_ocr(lambda data: "\n\nTIKAOCR\n") is True


def test_empty_response_means_no_ocr():
    """Measured: stock returns '' in 0.02 s."""
    assert probe_ocr(lambda data: "") is False
    assert probe_ocr(lambda data: "   \n ") is False


def test_the_probe_sends_the_packaged_asset():
    seen = {}

    def put_image(data):
        seen["len"] = len(data)
        return "TIKAOCR"

    probe_ocr(put_image)
    assert seen["len"] == 3990


def test_4_1_0_payloads_carry_no_ocr_signal():
    """Why this probe is behavioural. On 4.1.0 the stock and -full
    capability payloads are semantically identical, so no introspection
    could tell them apart."""
    stock = load("parsers_details_4_1_0_stock.json")
    full = load("parsers_details_4_1_0_full.json")
    assert ocr_types(stock) == set()
    assert ocr_types(full) == set(), (
        "4.1.0 -full advertises no OCR types although it does OCR"
    )


def test_3_2_3_payloads_did_carry_the_signal():
    """Kept so a future Tika restoring the signal is noticed rather than
    assumed."""
    assert ocr_types(load("parsers_details_3_2_3_stock.json")) == set()
    assert len(ocr_types(load("parsers_details_3_2_3_full.json"))) == 8


def test_probe_failure_assumes_ocr_is_present():
    """The safe direction: assuming OCR turns the pixel guard on."""

    def explode(data):
        raise OSError("connection refused")

    assert OcrProbe(explode).available() is True


@pytest.mark.parametrize(
    "override,response,expected",
    [(True, "", True), (False, "TIKAOCR", False)],
)
def test_override_wins_in_both_directions(override, response, expected):
    probe = OcrProbe(lambda data: response, override=override)
    assert probe.available() is expected


def test_result_is_cached_for_the_interval():
    calls = []

    def put_image(data):
        calls.append(1)
        return "TIKAOCR"

    probe = OcrProbe(put_image, interval=3600)
    assert probe.available() is True
    assert probe.available() is True
    assert len(calls) == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_tika_policy.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'plone.pgcatalog.tika_policy'`

- [ ] **Step 3: Write the implementation**

```python
"""Tika capability probing and the extraction decision, as pure functions.

Whether this Tika can OCR cannot be read off ``/parsers/details``.
Measured on 4.1.0, the stock and ``-full`` images return semantically
identical payloads -- 89 parser classes, the same supported types, 279
media types, no ``image/ocr-*`` in either -- while ``-full`` really does
OCR, with Tesseract 5.5.0.  The ``image/ocr-*`` family that carried the
signal on 3.2.3 was renamed away by the major bump.

So the probe runs a known image through the real parser.  A round trip
cannot go stale the way an implementation detail can.
"""

from pathlib import Path
from time import monotonic

import logging


__all__ = [
    "OCR_PROBE_PATH",
    "OCR_TYPE_PREFIX",
    "OcrProbe",
    "ocr_types",
    "probe_ocr",
]

log = logging.getLogger(__name__)

# A 320x90 PNG carrying the word TIKAOCR.  Measured round trip: 0.02 s
# against the stock image, returning empty, and 0.17 s against -full,
# returning the word.
OCR_PROBE_PATH = Path(__file__).parent / "assets" / "ocr_probe.png"

# Kept for diagnostics only.  The decision does not use it: see the
# module docstring.
OCR_TYPE_PREFIX = "image/ocr-"


def _supported_types(node, acc):
    acc.update(node.get("supportedTypes") or ())
    for child in node.get("children") or node.get("parsers") or ():
        _supported_types(child, acc)
    return acc


def ocr_types(payload):
    """The ``image/ocr-*`` types a ``/parsers/details`` payload lists.

    Diagnostic only.  Empty on 4.x for both image variants, which is the
    reason :func:`probe_ocr` exists.
    """
    return {
        t for t in _supported_types(payload, set()) if t.startswith(OCR_TYPE_PREFIX)
    }


def probe_ocr(put_image):
    """Whether this Tika OCRs, by sending it the probe image.

    *put_image* takes the image bytes and returns Tika's response text.
    Injected rather than built here so this module stays free of httpx
    and therefore unit-testable.
    """
    return bool(put_image(OCR_PROBE_PATH.read_bytes()).strip())


class OcrProbe:
    """Cached OCR availability for one Tika endpoint."""

    def __init__(self, put_image, interval=3600, override=None):
        self._put_image = put_image
        self._interval = interval
        self._override = override
        self._value = None
        self._checked_at = None

    def available(self):
        if self._override is not None:
            return self._override
        now = monotonic()
        if self._value is not None and now - self._checked_at < self._interval:
            return self._value
        try:
            self._value = probe_ocr(self._put_image)
        except Exception as exc:
            # Assume OCR: that turns the pixel guard on, which costs a
            # little recall.  Assuming no OCR would turn it off and hand
            # Tika the 165 MP file that started #222.
            log.warning("OCR probe failed, assuming OCR is present: %s", exc)
            self._value = True
        self._checked_at = now
        return self._value
```

- [ ] **Step 4: Ship the asset**

The worker reads `OCR_PROBE_PATH` at runtime, so the PNG has to be in the
wheel. With hatchling, add to `pyproject.toml`:

```toml
[tool.hatch.build.targets.wheel]
artifacts = ["src/plone/pgcatalog/assets/*.png"]
```

Verify rather than trust it:

```bash
.venv/bin/python -m build --wheel --outdir /tmp/pgc-wheel 2>&1 | tail -2
unzip -l /tmp/pgc-wheel/*.whl | grep ocr_probe.png
```
Expected: the PNG is listed. A probe whose asset is missing from the wheel
fails closed to "OCR present", which silently enables the guard in
production.

- [ ] **Step 5: Run to verify it passes**

Run: `.venv/bin/pytest tests/test_tika_policy.py -v`
Expected: 10 passed.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/tika_policy.py tests/test_tika_policy.py \
        pyproject.toml
git commit -m "feat: detect Tika OCR availability by behaviour, not introspection

Tika 4.1.0 reports semantically identical /parsers/details for the stock
and -full images, 89 parser classes with the same supported types and no
image/ocr-* in either, while -full demonstrably OCRs. The signal that
worked on 3.2.3 was renamed away, so the worker now sends a packaged
3990-byte PNG through the real parser instead: 0.02 s and empty on stock,
0.17 s and 'TIKAOCR' on -full. Refs #222.

Assisted-by: Claude Opus 5"
```

## Task 10: The decision matrix

**Files:**
- Modify: `src/plone/pgcatalog/tika_policy.py`
- Modify: `tests/test_tika_policy.py`

**Interfaces:**
- Consumes: `OcrProbe` from Task 9.
- Produces:
  - `MAX_IMAGE_PIXELS_DEFAULT` = the value Task 2 measured
  - `Decision` frozen dataclass: `action` in `{"extract", "defer", "skip"}`,
    `blob_zoid`, `tid`, `content_type`, `reason`, `warn`
  - `decide(source_info, content_type, ocr_available, cap, age_seconds, grace) -> Decision`

- [ ] **Step 1: Write the failing tests**

```python
from plone.pgcatalog.tika_policy import decide

CAP = 16_000_000
ORIGINAL = {"width": 15000, "height": 11000}
WITH_DERIV = {
    **ORIGINAL,
    "derivative_zoid": 1001,
    "derivative_tid": 77,
    "derivative_width": 4000,
    "derivative_height": 2933,
    "derivative_content_type": "image/jpeg",
}


def test_no_ocr_means_no_guard_at_any_size():
    """Measured: the stock image parses a 165 MP JPEG in 0.2 s at 174 MiB."""
    d = decide(
        ORIGINAL, "image/jpeg", ocr_available=False, cap=CAP, age_seconds=0, grace=3600
    )
    assert d.action == "extract"
    assert d.blob_zoid is None, "None means the row's own blob_zoid"


def test_oversized_with_ocr_prefers_the_derivative():
    d = decide(
        WITH_DERIV, "image/jpeg", ocr_available=True, cap=CAP, age_seconds=0, grace=3600
    )
    assert d.action == "extract"
    assert (d.blob_zoid, d.tid) == (1001, 77)
    assert d.content_type == "image/jpeg"


def test_derivative_over_our_cap_is_extracted_with_a_warning():
    """pgthumbor's ceiling is 8000 px, ours is 4000. Two packages
    disagreeing must not make content vanish in the gap."""
    big_deriv = {**WITH_DERIV, "derivative_width": 8000, "derivative_height": 8000}
    d = decide(
        big_deriv, "image/jpeg", ocr_available=True, cap=CAP, age_seconds=0, grace=3600
    )
    assert d.action == "extract"
    assert d.warn is not None
    assert "8000" in d.warn and "16000000" in d.warn


def test_oversized_without_derivative_defers_inside_the_grace():
    d = decide(
        ORIGINAL, "image/jpeg", ocr_available=True, cap=CAP, age_seconds=60, grace=3600
    )
    assert d.action == "defer"
    assert "derivative" in d.reason


def test_oversized_without_derivative_skips_after_the_grace():
    d = decide(
        ORIGINAL,
        "image/jpeg",
        ocr_available=True,
        cap=CAP,
        age_seconds=3601,
        grace=3600,
    )
    assert d.action == "skip"
    assert "165000000" in d.reason and "16000000" in d.reason


def test_missing_dimensions_never_guard():
    """Review Focus 1: no arithmetic on None, and no silent drop."""
    for info in (None, {}, {"width": 15000}, {"height": 11000}):
        d = decide(
            info,
            "image/jpeg",
            ocr_available=True,
            cap=CAP,
            age_seconds=99999,
            grace=3600,
        )
        assert d.action == "extract", f"{info!r} must fall through"


def test_non_image_types_are_untouched():
    d = decide(
        None, "application/pdf", ocr_available=True, cap=CAP, age_seconds=0, grace=3600
    )
    assert d.action == "extract"
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_tika_policy.py -k decide -v`
Expected: FAIL, `ImportError: cannot import name 'decide'`.

- [ ] **Step 3: Implement**

```python
@dataclass(frozen=True)
class Decision:
    """What to do with one dequeued job."""

    action: str  # "extract" | "defer" | "skip"
    blob_zoid: int | None = None  # None: use the queue row's own blob
    tid: int | None = None
    content_type: str | None = None
    reason: str = ""
    warn: str | None = None


def _pixels(info, prefix=""):
    """Pixel count from *info*, or None when either dimension is missing."""
    if not info:
        return None
    w = info.get(f"{prefix}width")
    h = info.get(f"{prefix}height")
    if not w or not h:
        return None
    return w * h


def decide(source_info, content_type, ocr_available, cap, age_seconds, grace):
    """Route one job. Pure: no I/O, no clock, no database.

    ``age_seconds`` is the row's age, which is how long pgthumbor has had
    to produce a derivative.  A count of deferrals would not do: transport
    backoff uses the same counter, so four connection errors would skip a
    row for a rendition reason that never happened.
    """
    if not ocr_available:
        return Decision("extract", reason="no ocr, metadata path is cheap")

    pixels = _pixels(source_info)
    if pixels is None or pixels <= cap:
        return Decision("extract")

    deriv_zoid = (source_info or {}).get("derivative_zoid")
    if deriv_zoid is not None:
        deriv_pixels = _pixels(source_info, "derivative_")
        warn = None
        if deriv_pixels and deriv_pixels > cap:
            warn = (
                f"derivative {source_info.get('derivative_width')}x"
                f"{source_info.get('derivative_height')} is still over "
                f"cap {cap}; pgthumbor's cap is above ours"
            )
        return Decision(
            "extract",
            blob_zoid=deriv_zoid,
            tid=source_info.get("derivative_tid"),
            content_type=source_info.get("derivative_content_type") or content_type,
            warn=warn,
        )

    if age_seconds <= grace:
        return Decision(
            "defer",
            reason=f"waiting for a derivative, {pixels} > cap {cap}",
        )
    return Decision(
        "skip",
        reason=f"skipped: pixels {pixels} > cap {cap}, no derivative after {grace}s",
    )
```

Set `MAX_IMAGE_PIXELS_DEFAULT` to the number Task 2 recorded.

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest tests/test_tika_policy.py -v`
Expected: 14 passed.

- [ ] **Step 5: Check the complexity ceiling**

Run: `uvx ruff@0.16.7 check src/plone/pgcatalog/tika_policy.py`
Expected: clean. `decide` has five branches; if it grows past C901's 13,
split the derivative branch into a helper rather than adding a `noqa`.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/tika_policy.py tests/test_tika_policy.py
git commit -m "feat: decision matrix for bounded Tika renditions

Refs #222.

Assisted-by: Claude Opus 5"
```

## Task 11: Wire the policy into the worker

**Files:**
- Modify: `src/plone/pgcatalog/tika_worker.py`
- Modify: `src/plone/pgcatalog/schema.py`, the `skipped` status comment
- Modify: `tests/test_tika_worker.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: `decide`, `OcrProbe` from Tasks 9 and 10.
- Produces: `TikaWorker(..., ocr=None, max_image_pixels=..., grace=...)`
  and a `skipped` terminal status.

- [ ] **Step 1: Write the failing tests**

```python
def test_oversized_image_with_a_derivative_sends_the_derivative(worker_db):
    queue_one_job(
        worker_db, job_id=10, content_type="image/jpeg", source_info=WITH_DERIV
    )
    insert_blob(worker_db, zoid=1001, tid=77, data=b"derivative-bytes")
    worker = TikaWorker(DSN, "http://tika:9998", ocr=True)
    sent = {}
    with patch.object(
        worker, "_put_to_tika", side_effect=lambda **kw: sent.update(kw) or "text"
    ):
        worker._process_one()
    assert sent["blob_zoid"] == 1001
    assert fetch_job(worker_db, 10)["status"] == "done"


def test_oversized_image_without_a_derivative_defers_then_skips(worker_db):
    queue_one_job(
        worker_db,
        job_id=11,
        content_type="image/jpeg",
        source_info={"width": 15000, "height": 11000},
    )
    worker = TikaWorker(DSN, "http://tika:9998", ocr=True, grace=3600)
    worker._process_one()
    row = fetch_job(worker_db, 11)
    assert row["status"] == "pending"
    assert row["not_before"] > row["created_at"]

    worker_db.execute(
        "UPDATE text_extraction_queue "
        "   SET created_at = now() - interval '2 hours', not_before = now() "
        " WHERE id = 11"
    )
    worker_db.commit()
    worker._process_one()
    row = fetch_job(worker_db, 11)
    assert row["status"] == "skipped"
    assert "no derivative" in row["error"]


def test_a_skipped_job_never_fetches_the_blob(worker_db):
    """The point of deciding at dequeue is to save the blob I/O."""
    queue_one_job(
        worker_db,
        job_id=12,
        content_type="image/jpeg",
        source_info={"width": 15000, "height": 11000},
    )
    worker_db.execute(
        "UPDATE text_extraction_queue "
        "   SET created_at = now() - interval '2 hours' WHERE id = 12"
    )
    worker_db.commit()
    worker = TikaWorker(DSN, "http://tika:9998", ocr=True)
    with patch.object(worker, "_blob_source") as source:
        worker._process_one()
    source.assert_not_called()


def test_no_ocr_extracts_a_165_megapixel_image(worker_db):
    queue_one_job(
        worker_db,
        job_id=13,
        content_type="image/jpeg",
        source_info={"width": 15000, "height": 11000},
    )
    worker = TikaWorker(DSN, "http://tika:9998", ocr=False)
    with patch.object(worker, "_extract", return_value="caption text"):
        worker._process_one()
    assert fetch_job(worker_db, 13)["status"] == "done"
```

- [ ] **Step 2: Run to verify they fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_worker.py -k "derivative or skipped or megapixel" -v`
Expected: FAIL, `TikaWorker() got an unexpected keyword argument 'ocr'`.

- [ ] **Step 3: Split `_process_one` and insert the decision**

`_process_one` is already near the complexity ceiling, so the new
branching goes into `_run_job`, called from the existing `try`:

```python
    def _run_job(self, conn, row):
        """Decide and execute one claimed job. Returns nothing; raises on
        extraction failure so the caller's handler stays in charge."""
        decision = decide(
            row["source_info"],
            row["content_type"],
            ocr_available=self._ocr.available(),
            cap=self.max_image_pixels,
            age_seconds=(datetime.now(UTC) - row["created_at"]).total_seconds(),
            grace=self.grace,
        )
        if decision.warn:
            log.warning("job %d: %s", row["id"], decision.warn)

        if decision.action == "defer":
            self._defer(conn, row["id"], self.grace_step, decision.reason)
            return
        if decision.action == "skip":
            self._terminate(conn, row["id"], "skipped", decision.reason)
            log.info("Skipped job %d: %s", row["id"], decision.reason)
            return

        blob_zoid = decision.blob_zoid or row["blob_zoid"] or row["zoid"]
        tid = decision.tid if decision.blob_zoid else row["tid"]
        text = self._extract(
            conn, blob_zoid, tid, decision.content_type or row["content_type"]
        )
        self._update_searchable_text(conn, row["zoid"], text)
        self._terminate(conn, row["id"], "done", None)
```

Add `source_info` and `created_at` to the claim query's `RETURNING` list,
and record the blob actually used back into `source_info` with a
`jsonb_set` in `_terminate` when `decision.blob_zoid` was set.

`_terminate(conn, job_id, status, error)` replaces the two inline status
updates so `done`, `skipped` and `failed` share one code path.

- [ ] **Step 4: Document `skipped` as non-recoverable-by-reset**

In the `TEXT_EXTRACTION_QUEUE` DDL comment, state that `status` takes
`pending`, `processing`, `done`, `failed` and `skipped`, and that a reset
of stuck work must target `failed` only. The #222 workaround was a
hand-written `UPDATE ... SET status='pending'`, and somebody will write it
again.

- [ ] **Step 5: Run the full worker suite**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_worker.py tests/test_tika_worker_extras.py -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/tika_worker.py src/plone/pgcatalog/schema.py \
        tests/test_tika_worker.py CHANGES.md
git commit -m "feat: send the bounded rendition and guard on pixels, not bytes

An oversized image now extracts from its pgthumbor source derivative when
one exists. Without OCR there is no guard at all, because a 165 MP JPEG
costs 0.2 s and 174 MiB on the stock image; only the OCR path rasterises.
A missing derivative defers for a grace period rather than being dropped,
because pgthumbor's decode semaphore and its bulk-import kill switch both
mean 'not yet' rather than 'never'. Refs #222.

Assisted-by: Claude Opus 5"
```

---

# PR 3: metadata harvesting

**Ships third.** On a no-OCR site this is the only part of the design that
adds text that is being thrown away today: an image's EXIF caption.

## Task 12: Parse `/rmeta/text`

**Files:**
- Create: `src/plone/pgcatalog/tika_rmeta.py`
- Create: `tests/test_tika_rmeta.py`

**Interfaces:**
- Consumes: the fixtures from Task 3.
- Produces:
  - `METADATA_FIELDS_DEFAULT` tuple
  - `extract_text(payload: list[dict], fields: tuple[str, ...]) -> str`
  - `MAX_RESPONSE_BYTES` = `33_554_432`

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for /rmeta/text response parsing."""

from plone.pgcatalog.tika_rmeta import extract_text
from plone.pgcatalog.tika_rmeta import METADATA_FIELDS_DEFAULT

import json
import pytest


def load(name):
    with open(f"tests/fixtures/tika/{name}") as fh:
        return json.load(fh)


def test_compound_document_text_appears_exactly_once():
    """Measured: the container entry holds only the file names, the child
    entries hold the text, so concatenation does not double-count."""
    text = extract_text(load("rmeta_4_1_0_compound_zip.json"), METADATA_FIELDS_DEFAULT)
    assert text.count("ALPHA") == 1
    assert text.count("BRAVO") == 1
    assert "vertrag.txt" in text


def test_image_caption_is_harvested_without_ocr():
    text = extract_text(load("rmeta_4_1_0_image_exif.json"), METADATA_FIELDS_DEFAULT)
    assert "Sonnenuntergang am Attersee" in text


def test_only_whitelisted_metadata_is_included():
    payload = [
        {
            "X-TIKA:content": "body",
            "dc:title": "Kept",
            "X-TIKA:parse_time_millis": "42",
            "tiff:Model": "ILCE-7RM5",
        }
    ]
    text = extract_text(payload, ("dc:title",))
    assert "Kept" in text
    assert "42" not in text
    assert "ILCE-7RM5" not in text


def test_metadata_comes_from_the_container_entry_only():
    payload = [
        {"X-TIKA:content": "outer", "dc:title": "Container"},
        {"X-TIKA:content": "inner", "dc:title": "Embedded"},
    ]
    text = extract_text(payload, ("dc:title",))
    assert "Container" in text
    assert "Embedded" not in text
    assert "inner" in text, "embedded *content* is still kept"


def test_tika_4_content_key_is_read():
    """Production runs 4.1.0, where the key is tk:content. Reading only
    the 3.x name returned empty text for every document."""
    text = extract_text(
        [{"tk:content": "Kaufvertrag Attersee", "dc:title": "T"}],
        ("dc:title",),
    )
    assert "Kaufvertrag Attersee" in text


def test_tika_3_content_key_still_works():
    text = extract_text([{"X-TIKA:content": "Altbestand"}], ())
    assert "Altbestand" in text


def test_both_majors_from_their_real_fixtures_agree():
    """The same ZIP through 3.2.3 and 4.1.0 must yield the same words."""
    old = extract_text(load("rmeta_3_2_3_compound_zip.json"), ())
    new = extract_text(load("rmeta_4_1_0_compound_zip.json"), ())
    for token in ("ALPHA", "BRAVO", "vertrag.txt", "anhang.txt"):
        assert token in old, f"{token} missing from the 3.2.3 fixture"
        assert token in new, f"{token} missing from the 4.1.0 fixture"


def test_the_4_1_0_key_wins_when_both_are_present():
    """Defensive: a proxy or a mixed payload must not double-count."""
    text = extract_text([{"tk:content": "NEW", "X-TIKA:content": "OLD"}], ())
    assert "NEW" in text
    assert "OLD" not in text


@pytest.mark.parametrize("payload", [[], {}, None, "not a list"])
def test_degenerate_payloads_yield_empty_text(payload):
    """Review Focus 3: a malformed body must not raise out of the parser."""
    assert extract_text(payload, METADATA_FIELDS_DEFAULT) == ""


def test_list_valued_metadata_is_joined():
    payload = [{"X-TIKA:content": "", "dc:subject": ["Vertrag", "Attersee"]}]
    text = extract_text(payload, ("dc:subject",))
    assert "Vertrag" in text and "Attersee" in text
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_tika_rmeta.py -v`
Expected: FAIL, `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
"""Parsing for Tika's ``PUT /rmeta/text`` responses.

``PUT /tika`` returns body text only, which for an image without OCR is
zero characters even when the file carries a caption.  ``/rmeta/text``
returns one JSON object per document, the container first and embedded
documents after it, each with its text under ``X-TIKA:content``.

Concatenating every entry's content does not duplicate anything: measured
on a ZIP of two text files, the container's content was the file *names*
only and the children held the text.  Word order does differ from
``/tika``, which interleaves names with contents, so phrase proximity in
``searchable_text`` shifts.
"""

__all__ = ["extract_text", "METADATA_FIELDS_DEFAULT", "MAX_RESPONSE_BYTES"]

# Tika 4 renamed the metadata namespace: X-TIKA:content became
# tk:content, resourceName became tk:resource-name.  Dublin Core keys and
# Content-Type are unchanged.  Reading only the 3.x name against a 4.x
# server returns empty text for every document, which looks like "no text
# in this file" rather than like a bug, so both are tried per entry.
CONTENT_KEYS = ("tk:content", "X-TIKA:content")

METADATA_FIELDS_DEFAULT = (
    "dc:title",
    "dc:description",
    "dc:subject",
    "dc:creator",
    "meta:keyword",
)

# A 500-page PDF yields one entry per embedded resource, so the body is
# not bounded by the source size.  32 MiB is far above any real document
# and far below anything that would trouble the worker.
MAX_RESPONSE_BYTES = 33_554_432


def _as_text(value):
    if isinstance(value, (list, tuple)):
        return " ".join(str(v) for v in value if v)
    return str(value) if value else ""


def _content_of(entry):
    """One entry's body text, under whichever major's key is present."""
    for key in CONTENT_KEYS:
        if key in entry:
            return _as_text(entry[key])
    return ""


def extract_text(payload, fields):
    """Body text of every entry, plus whitelisted container metadata.

    Returns "" for anything that is not a non-empty list of dicts, so a
    truncated or non-JSON-shaped body fails the one job rather than the
    worker loop.
    """
    if not isinstance(payload, list) or not payload:
        return ""
    entries = [e for e in payload if isinstance(e, dict)]
    if not entries:
        return ""

    parts = []
    for key in fields:
        # Metadata from the container only: an embedded document's title
        # is rarely about the object being catalogued.
        value = _as_text(entries[0].get(key))
        if value:
            parts.append(value)
    for entry in entries:
        content = _content_of(entry).strip()
        if content:
            parts.append(content)
    return "\n".join(parts)
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest tests/test_tika_rmeta.py -v`
Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/tika_rmeta.py tests/test_tika_rmeta.py
git commit -m "feat: parse Tika /rmeta/text responses with a metadata whitelist

Refs #222.

Assisted-by: Claude Opus 5"
```

## Task 13: Switch the worker to `/rmeta/text`

**Files:**
- Modify: `src/plone/pgcatalog/tika_worker.py:195-227`
- Modify: `tests/test_tika_worker.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: `extract_text`, `MAX_RESPONSE_BYTES` from Task 12.
- Produces: `_extract` unchanged in signature, returning text from
  `/rmeta/text`.

- [ ] **Step 1: Write the failing tests**

```python
def test_extract_calls_rmeta_and_caps_embedded_resources(worker_db):
    worker = TikaWorker(DSN, "http://tika:9998")
    with patch("plone.pgcatalog.tika_worker.httpx.Client") as client:
        put = client.return_value.__enter__.return_value.put
        put.return_value = MagicMock(
            status_code=200,
            headers={"content-length": "40"},
            json=lambda: [{"X-TIKA:content": "body", "dc:title": "T"}],
        )
        with patch.object(
            worker, "_blob_source", return_value={"kind": "bytes", "data": b"x"}
        ):
            text = worker._extract(worker_db, 1, 1, "application/pdf")
    url = put.call_args[0][0]
    assert url.endswith("/rmeta/text")
    headers = put.call_args[1]["headers"]
    assert headers["Accept"] == "application/json"
    assert "X-Tika-MaxEmbeddedResources" in headers
    assert "body" in text and "T" in text


def test_non_json_body_raises_a_job_error_not_a_loop_error(worker_db):
    """Review Focus 3: a 200 with an HTML error page."""
    worker = TikaWorker(DSN, "http://tika:9998")
    with patch("plone.pgcatalog.tika_worker.httpx.Client") as client:
        put = client.return_value.__enter__.return_value.put
        put.return_value = MagicMock(
            status_code=200,
            headers={"content-length": "20"},
            json=MagicMock(side_effect=ValueError("not json")),
        )
        with patch.object(
            worker, "_blob_source", return_value={"kind": "bytes", "data": b"x"}
        ):
            with pytest.raises(ValueError):
                worker._extract(worker_db, 1, 1, "application/pdf")


def test_oversized_response_is_refused_before_parsing(worker_db):
    worker = TikaWorker(DSN, "http://tika:9998")
    with patch("plone.pgcatalog.tika_worker.httpx.Client") as client:
        put = client.return_value.__enter__.return_value.put
        put.return_value = MagicMock(
            status_code=200,
            headers={"content-length": str(64 * 1024 * 1024)},
            json=MagicMock(side_effect=AssertionError("must not parse")),
        )
        with patch.object(
            worker, "_blob_source", return_value={"kind": "bytes", "data": b"x"}
        ):
            with pytest.raises(ValueError, match="response too large"):
                worker._extract(worker_db, 1, 1, "application/pdf")
```

- [ ] **Step 2: Run to verify they fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_tika_worker.py -k "rmeta or non_json or oversized" -v`
Expected: FAIL, the request still goes to `/tika`.

- [ ] **Step 3: Change `_extract`**

Keep the S3 streaming path exactly as it is; only the URL, the `Accept`
header and the response handling change.

```python
        headers = {
            "Accept": "application/json",
            "X-Tika-MaxEmbeddedResources": str(self.max_embedded_resources),
        }
```

```python
resp = client.put(
    f"{self.tika_url}/rmeta/text",
    content=content,
    headers=headers,
)
resp.raise_for_status()
declared = int(resp.headers.get("content-length") or 0)
if declared > MAX_RESPONSE_BYTES:
    raise ValueError(
        f"Tika response too large: {declared} bytes > {MAX_RESPONSE_BYTES}"
    )
return extract_text(resp.json(), self.metadata_fields)
```

`max_embedded_resources` defaults to 1000 and comes from
`TIKA_WORKER_MAX_EMBEDDED_RESOURCES`.

- [ ] **Step 4: Run the full suite**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/ -v`
Expected: all pass. The pre-existing worker tests that assert on `/tika`
need updating in this commit, not skipping.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/tika_worker.py tests/test_tika_worker.py CHANGES.md
git commit -m "feat: harvest metadata by extracting via /rmeta/text

PUT /tika returns body text only, so an image without OCR yielded zero
characters even when it carried an EXIF caption. /rmeta/text returns
dc:description and friends at metadata-only cost, measured at 0.1 s and
174 MiB for a 12 MP JPEG on the stock image. Word order changes relative
to /tika, which shifts phrase proximity in searchable_text. Refs #222.

Assisted-by: Claude Opus 5"
```

---

# PR 6: documentation

## Task 14: Document the Tika side and the new settings

**Files:**
- Modify: `docs/sources/how-to/enable-tika-extraction.md`
- Modify: `docs/sources/reference/configuration.md`
- Modify: `docs/sources/explanation/tika-extraction.md`
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: the measured numbers from Tasks 2 and 3.
- Produces: nothing code depends on.

- [ ] **Step 1: Replace the OCR section's cost claims with measurements**

The current page says images "are extracted as empty text" without saying
what that costs. Replace with the measured table from the spec, and state
plainly that without OCR a 165 MP JPEG costs 0.2 s and 174 MiB, so the
default list keeping image types is deliberate.

- [ ] **Step 2: Add a "Sizing and safety" section**

Three things, in this order, with the reason for each:

```yaml
# The JVM must run out of heap before the container runs out of memory,
# otherwise the kernel kills a process and a pod restart costs every
# in-flight extraction. Measured: without this, a 165 MP JPEG pins a
# 1 GiB container at the limit and an OCR helper is OOM-killed.
environment:
  JAVA_TOOL_OPTIONS: -XX:MaxRAMPercentage=50
```

```xml
<!-- A byte-axis backstop. It does not catch a 165 MP photo, which
     weighs 5 MB, but it does catch the large-file case that the pixel
     guard cannot see. -->
<parser class="org.apache.tika.parser.ocr.TesseractOCRParser">
  <params>
    <param name="maxFileSizeToOcr" type="long">50000000</param>
  </params>
</parser>
```

And the parser allow-list for deployments that want no image parsing at
all, noting that it makes the capability set declared rather than
accidental.

- [ ] **Step 3: Document the environment variables**

Add to `docs/sources/reference/configuration.md`, each with its default
and one sentence of *why*, not just what:
`PGCATALOG_TIKA_MAX_IMAGE_PIXELS`, `PGCATALOG_TIKA_OCR`,
`PGCATALOG_TIKA_PROBE_INTERVAL`, `PGCATALOG_TIKA_RENDITION_GRACE`,
`PGCATALOG_TIKA_METADATA_FIELDS`,
`TIKA_WORKER_MAX_EMBEDDED_RESOURCES`.

- [ ] **Step 4: Document the queue statuses and the reset recipe**

State the five statuses, that `skipped` is terminal and deliberate, and
give the correct reset:

```sql
-- Retry transient failures. Do NOT include 'skipped': those rows were
-- refused on purpose and will be refused again.
UPDATE text_extraction_queue
   SET status = 'pending', attempts = 0, deferrals = 0, not_before = now()
 WHERE status = 'failed';
```

- [ ] **Step 5: Explain the pgthumbor relationship**

In the explanation page, say that an oversized image is extracted from its
pgthumbor source derivative, that `_pgthumbor_source` is a cross-package
contract, and that with no Thumbor configured oversized images are skipped
after the grace period rather than risking the OOM. Cross-link
pgthumbor's own documentation, and note the reciprocal line belongs in the
pgthumbor docs.

- [ ] **Step 6: Build the docs and commit**

Run: `make -C docs html` (or the project's documented equivalent)
Expected: no warnings introduced.

```bash
git add docs/ CHANGES.md
git commit -m "docs: Tika sizing, the new settings, and the pgthumbor contract

Assisted-by: Claude Opus 5"
```

---

## Self-review notes

**Spec coverage.** Component 1 is Tasks 4, 5 and 11; component 2 is Tasks
5 and 7; component 3 is Task 9; component 4 is Tasks 10 and 11; component
5 is Tasks 12 and 13; component 6 is Task 8; component 7 is Task 14. Step
0's three verifications are Task 1, the unmeasured accuracy claim is Task
2, and the adjacent MIME fix is Task 6. The audio/video section is
deliberately uncovered: it is #223.

**One spec change this plan makes.** Rendition waiting is a time budget
from `created_at` rather than a count of deferrals, because `deferrals` is
also the transport-backoff counter and sharing it would skip rows for a
reason that did not occur. Task 8's commit updates the spec to match.

**Interface consistency.** `Rendition` fields in Task 4 are the keys
`_source_info` reads in Task 5, which are the keys `decide` reads in Task
10, which are the names asserted in Task 11. `Decision.blob_zoid` is
`None` for "use the row's own blob" throughout, never `0`.

**Phase 0 outcome, folded back in.** Task 2 confirmed the default cap, so
the numbers in Tasks 10 and 14 stand. Task 1 found two of three
assumptions wrong, which rewrote Task 4 as walk-then-fetch, turned Task 5
from a fix into an addition, and removed PR 1's double-enqueue item. The
rulings are in the ledger.

**Known risk this plan still does not remove.** Whether the production
Tika in #222 ran a `-full` image is unanswered, and if it ran stock then
that incident has a cause this design does not address, most likely PDF
rasterisation. Asked on the issue; not a blocker for any task here.
