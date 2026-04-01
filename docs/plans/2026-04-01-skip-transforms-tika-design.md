# Skip portal_transforms for IFile when Tika is active

**Date:** 2026-04-01
**Status:** Approved
**Issue:** N/A (performance improvement for Tika-enabled sites)

## Problem

When Plone indexes a `File` object, the `SearchableText_file` indexer
(from `plone.app.contenttypes`) calls `portal_transforms` to extract
text from the blob's binary data (PDF, DOCX, etc.).  This is:

1. **Expensive:** spawns external processes (pdftotext, wv, etc.)
   synchronously during the request.
2. **Redundant when Tika is configured:** the async Tika worker
   already extracts text from blobs and merges it into
   `searchable_text` via `pgcatalog_merge_extracted_text`.
3. **Wasteful even when transforms are missing:** `_findPath()` does a
   full BFS graph traversal of the transform registry before
   concluding no path exists — not a cheap dict lookup.

## Scope

Only `SearchableText_file` (registered for `IFile`) calls
`portal_transforms`.  All other Plone SearchableText indexers
(IDocument, INewsItem, ICollection, IFolder, ILink) only concatenate
text fields — no transforms involved.

`IImage` does NOT extend `IFile` and has no transform-based indexer.

## Design

### New file: `src/plone/pgcatalog/indexers.py`

A `SearchableText` indexer adapter registered for `IFile`:

- **When `PGCATALOG_TIKA_URL` is set:** return `SearchableText(obj)`
  (Title + Description only).  No `_findPath`, no blob I/O, no
  transform call.  The Tika worker fills in the blob text
  asynchronously as weight 'C' in the tsvector.
- **When `PGCATALOG_TIKA_URL` is NOT set:** delegate to the original
  `plone.app.contenttypes.indexers.SearchableText_file` so the full
  transform pipeline runs as before.

### ZCML registration

Register in `overrides.zcml` to override the `plone.app.contenttypes`
registration for `IFile`.

### What doesn't change

- `portal_transforms` is untouched — no unregister/re-register.
- The Tika enqueue pipeline in `processor.py` — already works.
- Custom SearchableText indexers for other interfaces — unaffected
  (adapter specificity ensures more specific registrations win).
- Tsvector weighting: Title 'A', Description 'B', body 'D',
  Tika-extracted text 'C'.

### Fallback behavior

When `PGCATALOG_TIKA_URL` is NOT set, the override delegates to the
original indexer.  Zero impact for sites not using Tika.

## Custom types with blob fields

The override only covers `IFile`.  If a custom content type has blob
fields and uses its own `SearchableText` indexer that calls
`portal_transforms`, it will NOT be automatically short-circuited.

Developers with such custom types should either:

1. Make their type provide `IFile` (then the override applies), or
2. Register a similar conditional indexer for their custom interface
   that checks `PGCATALOG_TIKA_URL` and skips transforms when set.

This should be documented in the package's how-to section.

## Implementation

1. Create `src/plone/pgcatalog/indexers.py` with the conditional
   indexer function.
2. Add the adapter registration to `overrides.zcml`.
3. Add tests: with Tika URL set (returns Title+Description only),
   without Tika URL (delegates to original).
4. Add documentation section about custom blob types.
