# Custom types with blob fields and Tika

When `PGCATALOG_TIKA_URL` is configured, `plone.pgcatalog` overrides
the `SearchableText` indexer for `IFile` to skip the synchronous
`portal_transforms` pipeline. The Tika async worker extracts text
from blobs instead.

## What's covered automatically

- **File** content type (`IFile`)---the override handles this.

## What's NOT covered

Custom Dexterity content types with `NamedBlobFile` primary fields
that do NOT provide `IFile`.  If such a type has a custom
`SearchableText` indexer that calls `portal_transforms`, the
transforms will still run synchronously.

## How to add Tika support for custom types

Register a conditional indexer similar to the built-in override:

```python
from plone.app.contenttypes.indexers import SearchableText
from plone.indexer import indexer
from my.package.interfaces import IMyCustomType

import os

# Import your original indexer
from my.package.indexers import SearchableText_mycustomtype as _original


@indexer(IMyCustomType)
def SearchableText_mycustomtype_tika(obj):
    tika_url = os.environ.get("PGCATALOG_TIKA_URL", "").strip()
    if tika_url:
        return SearchableText(obj)
    return _original(obj)
```

Register it in your package's `overrides.zcml`:

```xml
<adapter
    factory=".indexers.SearchableText_mycustomtype_tika"
    name="SearchableText"
    />
```

This ensures:

- With Tika: only Title + Description are indexed synchronously;
  Tika extracts blob text async.
- Without Tika: the original transform-based indexer runs as before.
