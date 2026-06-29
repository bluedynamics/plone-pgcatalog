# Tika-aware NamedfileFieldConverter (#114)

## Problem

When `PGCATALOG_TIKA_URL` is set, pgcatalog short-circuits `portal_transforms`
so the async Tika worker handles blob text extraction. Today only the **IFile**
`SearchableText` indexer is overridden (`SearchableText_file_override`, #41).
Content types that carry a blob in a field marked **`searchable`** via the
`plone.textindexer` behavior still run the synchronous transform.

## Diagnosis

Blob → `portal_transforms` during `SearchableText` indexing comes from exactly
two places:

1. `plone.app.contenttypes.indexers.SearchableText_file` (bound to `IFile`) —
   already overridden by pgcatalog.
2. `plone.app.dexterity.textindexer`'s `dynamic_searchable_text_indexer` (bound
   to the `IDexterityTextIndexer` marker behavior). For each `searchable` field
   it calls a field converter; **`NamedfileFieldConverter.convert()`** runs
   `portal_transforms.convertTo("text/plain", ...)` on `NamedBlobFile` data.
   This is the uncovered path.

`NamedfileFieldConverter` / the `IDexterityTextIndexFieldConverter` adapter is
consumed in core **only** by `dynamic_searchable_text_indexer`
(`indexer.py:71`) — nowhere else (no display/export path). Overriding it is
therefore safe and affects only `SearchableText` indexing.

Empirical check on the reference deployment (aaf-prod, read-only): 0
`searchable` `NamedBlob*` fields, 0 per-object transform calls in 24h. This is a
**defensive / forward-looking** fix, not a live bug; the original b49 report
was migration-era. The startup `Cannot register transform pdf_to_text` lines are
Plone-core `Products.PortalTransforms` registration logging, out of scope.

## Design

Add a pgcatalog subclass of `NamedfileFieldConverter` in `indexers.py`:

```python
class TikaAwareNamedfileFieldConverter(NamedfileFieldConverter):
    def convert(self):
        if os.environ.get("PGCATALOG_TIKA_URL", "").strip():
            # Tika extracts the blob asynchronously; skip the sync
            # portal_transforms call. Return the filename so it stays
            # searchable immediately (graceful degradation if the async
            # enqueue does not cover this blob — see #184).
            storage = self.field.interface(self.context)
            data = self.field.get(storage)
            if not data or data.getSize() == 0:
                return ""
            return safe_text(data.filename or "")
        return super().convert()
```

Register it in `overrides.zcml`, conditional on `installed plone.app.dexterity`
(the package providing `textindexer`). The subclass inherits
`@adapter(IDexterityContent, INamedFileField, IWidget)` and
`@implementer(IDexterityTextIndexFieldConverter)` from the parent, so the
override replaces the stock converter for the same registration.

`indexers.py` must import `NamedfileFieldConverter` defensively (textindexer may
be absent in minimal installs); the ZCML condition keeps the adapter
unregistered there.

### Why the converter, not a broad indexer override

Overriding the converter targets the exact transform call and leaves the
`dynamic_searchable_text_indexer` field-iteration intact. A broad
`IDexterityContent` `SearchableText` override would not shadow the
more-specific stock/project indexers (adapter specificity) and would make the
Tika-off passthrough non-trivial. The converter override has neither problem.

## Scope boundary

Returning the filename (not the body text) relies on the async Tika worker to
extract the blob content. The worker's enqueue is gated on the object-level
`mime_type` (primary field), so a `searchable` blob in a **secondary** field of
a type with no extractable primary `mime_type` may not be enqueued. That
completeness gap is tracked separately in **#184**. With the filename return,
the worst case is deferred/missing body text (only when Tika is active), never
a hard failure — strictly better than the current broken-transform path.

## Test plan

`tests/test_indexers.py` (or a new `tests/test_tika_indexer_override.py`):

- `convert()` with `PGCATALOG_TIKA_URL` set returns the filename and does **not**
  call `portal_transforms` (assert via a fake field/context capturing transform
  access, or assert the return equals the filename for a PDF blob).
- `convert()` with `PGCATALOG_TIKA_URL` unset delegates to the parent
  (`super().convert()` path) — original behavior.
- Empty / zero-size blob returns `""` in both modes.
- The override adapter is registered (ZCML loads) and resolves for a
  `NamedBlobFile` field.

No live Tika server needed — the override is a pure env-var branch.
