# Real object state fixtures for Tika rendition work

Captured 2026-10-01 for Task 1 of
`docs/superpowers/plans/2026-09-30-tika-bounded-rendition.md`, which exists
to check three assumptions the design read out of code rather than
measured. **Two of the three came back different from what the spec
assumed.** Read this before touching `blobrefs.py`.

## How they were captured

A real ZODB round trip, not a hand-written shape:

- `plone.namedfile` 8.x `NamedBlobImage`, built from real JPEG bytes
- written through `zodb_pgjsonb.storage.PGJsonbStorage` against a
  throwaway `zodb_phase0` database, `blob_threshold=100`
- `plone.pgthumbor.derivative.set_source_derivative(image, max_edge=4000)`
  called directly, which is the same code path the
  `IObjectAddedEvent`/`IObjectModifiedEvent` subscriber takes
- `object_state.state` then read straight out of PostgreSQL

Three cases, one file each:

| File | Content | Expectation |
|---|---|---|
| `image_with_derivative.json` | one 15000x11000 field | over the cap, must have a derivative |
| `image_without_derivative.json` | one 800x600 field | under the cap, must not |
| `two_image_fields.json` | 15000x11000 plus 800x600 | only the first may have one |

Each file holds the whole reachable object graph, because the structure
below means one state is not enough to answer anything:

```json
{
  "content_zoid": 3,
  "blob_zoids": [6, 7, 15],
  "objects": {"3": {...}, "4": {...}, "5": {...}, "14": {...}}
}
```

## Answer 1: `_width` and `_height` are there, as assumed

Confirmed, under exactly those names, on **both** the original and the
derivative, together with `contentType` and `filename`:

```json
{
  "_blob": {"@ref": ["0000000000000007", "ZODB.blob.Blob"]},
  "_width": 15000,
  "_height": 11000,
  "contentType": "image/jpeg",
  "filename": "image-15000x11000.jpg",
  "_pgthumbor_source": {
    "@ref": ["000000000000000e", "plone.namedfile.file.NamedBlobImage"]
  },
  "_pgthumbor_source_info": {
    "max_edge": 4000, "reason": "generated", "source_ids": {...}
  }
}
```

No blob read and no image decode is needed to learn the pixel count, which
is what component 2 of the spec depends on.

## Answer 2: the derivative is a separate persistent object, not a nested dict

**This is the finding that changes the implementation.** The spec, and
Task 4 of the plan, assumed `_pgthumbor_source` holds an inline dict whose
`_width`, `_height` and `contentType` sit in the same state. They do not.
There are **three levels of persistent objects**:

```
Content                      zoid 3
  image      -> @ref         zoid 4   NamedBlobImage   (the original)
  attachment -> @ref         zoid 5   NamedBlobImage

NamedBlobImage               zoid 4
  _blob             -> @ref  zoid 7   ZODB.blob.Blob   <- the original bytes
  _width = 15000, _height = 11000, contentType = "image/jpeg"
  _pgthumbor_source -> @ref  zoid 14  NamedBlobImage   (the derivative)
  _pgthumbor_source_info = {"reason": "generated", "max_edge": 4000}

NamedBlobImage               zoid 14
  _blob  -> @ref             zoid 15  ZODB.blob.Blob   <- the derivative bytes
  _width = 4000, _height = 2933, contentType = "image/jpeg"
  _pgthumbor_is_source = true
```

So a derivative's dimensions, content type and blob live **one
`object_state` row further away** than the plan's `pair_renditions(state)`
can see. That function cannot work on a single state; it needs either a
resolver callback or a two-step shape, walk then fetch.

Two details worth having:

- The `@ref` marker uses the **two-element form**,
  `["<hex oid>", "<dotted class name>"]`. The class name is carried in the
  reference itself, so a `NamedBlobImage` ref can be told from a
  `ZODB.blob.Blob` ref **without loading either object**. That makes the
  walk cheaper than expected.
- The derivative carries `_pgthumbor_is_source: true` and, unlike the
  original, no `_pgthumbor_source_info`. Either marker identifies it.

## Answer 3: there is no double enqueue

The spec expected an oversized image with pgthumbor installed to be
enqueued twice today. **It is not.** Verified by running the processor's
real resolution path against the real states:

```
content object zoid=3: fields=['attachment', 'image']
  level-1 refs from the content state: [4, 5]
    zoid 4: in blob_state? False
    zoid 5: in blob_state? False
  wrapper zoid 4 -> inner refs [7, 14]
    inner 7: BLOB
    inner 14: not a blob (['_blob', '_height', '_modified']...)
  wrapper zoid 5 -> inner refs [6]
    inner 6: BLOB

would enqueue 2 row(s): [('via wrapper', 7), ('via wrapper', 6)]
```

Two rows for two image fields, which is one per field and therefore
correct. The reason the derivative is not also enqueued is the same
structure as answer 2: `_resolve_wrappers` goes exactly **one** level deep,
content state to wrapper state to blob, and the derivative's blob is two
levels down. The derivative's `NamedBlobImage` is reached as an inner ref,
found absent from `blob_state`, and correctly dropped.

Consequences:

- PR 1 of the plan has **no double-enqueue bug to fix**. That item is
  withdrawn; the MIME normalisation fix stands on its own.
- The bounded-rendition work is a pure **addition**, not a correction. The
  derivative is not merely mis-selected today, it is unreachable.
