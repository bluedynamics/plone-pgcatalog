# CMFEditions Write-Path Overhead Analysis

## Summary

CMFEditions adds significant overhead to every content creation and modification
in Plone, even when versioning is rarely used. It is installed by default and
hooks into `IObjectAddedEvent` / `IObjectModifiedEvent` via
`plone.app.versioningbehavior`.

## The Chain of Events

When `invokeFactory()` runs, `IObjectAddedEvent` fires. Two subscribers react:

1. **CMFCore catalog subscriber** — queues `index(obj)` (normal, expected)
2. **plone.app.versioningbehavior** — calls `portal_repository.save(obj)`

### Event Registration

**`plone/app/versioningbehavior/configure.zcml` (lines 34-44):**

```xml
<subscriber
    for="plone.app.versioningbehavior.behaviors.IVersioningSupport
         zope.lifecycleevent.interfaces.IObjectAddedEvent"
    handler=".subscribers.create_initial_version_after_adding" />

<subscriber
    for="plone.app.versioningbehavior.behaviors.IVersioningSupport
         zope.lifecycleevent.interfaces.IObjectModifiedEvent"
    handler=".subscribers.create_version_on_save" />
```

### What `portal_repository.save()` Does

**`Products/CMFEditions/CopyModifyMergeRepositoryTool.py:294-308`:**

```
save()
  -> transaction.savepoint(optimistic=True)    <- savepoint overhead
  -> _recursiveSave()
      -> portal_archivist.prepare(obj)
          -> _cloneByPickle(obj)               <- FULL PICKLE CLONE of the object
          -> modifier.getReferencedAttributes()
          -> modifier.beforeSaveModifier()     <- 5+ modifiers run sequentially
      -> handle inside/outside references
      -> portal_archivist.save(prep)
          -> portal_historiesstorage.save()    <- ZVC repository write
```

### Heavy Operations

1. **`_cloneByPickle(obj)`** (`ArchivistTool.py:265`) — serializes the entire
   object into a BytesIO buffer, then unpickles it back to create a deep copy.
   Done on *every save*, even for trivial edits.

2. **5 modifier passes** (`StandardModifiers.py`) — each walks the object graph:
   - SafeGlobalsModifier (workflow state)
   - SkipRegistryBasesPointersModifier
   - PortalFileOrphansModifier (handles file data separately)
   - VersionAwareReferenceInsideReferences
   - VersionAwareReferenceOutsideReferences

3. **ZVC storage write** (`ZVCStorageTool.py:216-239`) — stores the cloned
   version object in the ZODB, creating additional persistent objects.

4. **The savepoint itself** — while `optimistic=True` avoids flushing to disk,
   it still snapshots the transaction state for potential rollback on
   `ModifierException`.

### Impact Measured by Profiling

For a single Plone Document creation with CMFEditions active, profiling showed
**~193 objects** going through `store()` per document. Without CMFEditions,
a simple Document + folder ordering is maybe 5-10 objects. The extra ~180
objects are:

- Version history objects (ZVC repository storage)
- Portal repository tool modifications
- Modifier metadata objects
- The pickle-cloned version copy
- BTree nodes for the version history indexes

### Catalog Interaction

- CMFEditions does **NOT** query the catalog during `save()` or content creation
- The only catalog touch is during `revert()` which calls
  `_fixupCatalogData()` → `portal_catalog.indexObject(obj)` to resync
- `plone.app.versioningbehavior` delegates ALL versioning to `portal_repository`
  and does NOT directly call any catalog methods

### The Problem

CMFEditions is installed by default in Plone but most sites either:
- Don't use versioning at all
- Only want it for a few content types
- Would be fine with a simpler "last N versions" approach

Yet it adds overhead to **every** content creation and modification, even when
the user never looks at version history. The
`create_initial_version_after_adding` subscriber fires unconditionally for any
`IVersioningSupport` content — which is all Dexterity content by default.

### Possible Mitigations

1. **Disable the behavior**: Remove `IVersioningSupport` from content types
   that don't need versioning (Plone-level configuration)
2. **Lazy versioning**: Only create versions when explicitly requested
3. **Benchmark without versioning**: Add `--no-versioning` flag to benchmarks
   to measure pure storage overhead without CMFEditions noise

## Key Files

| Component | File | Lines |
|-----------|------|-------|
| Versioning subscriber | `plone/app/versioningbehavior/subscribers.py` | 64-115 |
| Savepoint creation | `Products/CMFEditions/CopyModifyMergeRepositoryTool.py` | 297 |
| Object cloning | `Products/CMFEditions/ArchivistTool.py` | 235-289 |
| Recursive save | `Products/CMFEditions/CopyModifyMergeRepositoryTool.py` | 451-496 |
| Storage layer | `Products/CMFEditions/ZVCStorageTool.py` | 216-239 |
| Modifier registry | `Products/CMFEditions/ModifierRegistryTool.py` | 207-220 |
| Catalog fixup (revert only) | `Products/CMFEditions/CopyModifyMergeRepositoryTool.py` | 667-672 |
