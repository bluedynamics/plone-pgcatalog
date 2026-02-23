<!-- diataxis: how-to -->

# Rebuild or Reindex the Catalog

## When to Rebuild

- After enabling BM25 (new columns need populating)
- After upgrading plone.pgcatalog (if release notes mention schema changes)
- After manual database restoration
- When catalog counts do not match actual content

## Full Rebuild (clearFindAndRebuild)

Clears all catalog data and re-indexes every object by traversing the site.

Via ZMI:

1. Navigate to portal_catalog > Advanced tab
2. Click "Clear and Rebuild"

Via script:

```python
catalog = portal.portal_catalog
catalog.clearFindAndRebuild()
import transaction; transaction.commit()
```

Expected timing: approximately 15 ms per object.

## Selective Reindex (reindexIndex)

Re-extracts a single index for all objects:

```python
catalog.reindexIndex("review_state")
import transaction; transaction.commit()
```

Useful after changing an indexer or adding a new index.

## Partial Reindex (automatic)

When Plone calls `reindexObject(idxs=["review_state"])`, plone.pgcatalog uses a lightweight JSONB merge (`||` operator) instead of full re-extraction. This happens automatically and does not trigger ZODB serialization of the object.

## Choosing the Right Operation

| Operation | Clears data? | Traverses site? | Speed | Use when |
|---|---|---|---|---|
| `clearFindAndRebuild()` | Yes | Yes | ~15 ms/obj | Schema changes, corrupt data, major upgrades |
| `refreshCatalog(clear=0)` | No | Re-catalogs existing | ~15 ms/obj | Reindex all without losing uncataloged objects |
| `refreshCatalog(clear=1)` | Yes | Yes | Same | Equivalent to `clearFindAndRebuild()` |
| `reindexIndex("name")` | No (single key) | No (PG only) | Fast | Single index changed, new indexer deployed |

**`clearFindAndRebuild()`** NULLs all catalog columns (path, idx,
searchable_text, backend extras), then traverses the entire portal tree
and calls `catalog_object()` on every object found.  Use this when
catalog data might be inconsistent with actual content.

**`refreshCatalog(clear=0)`** reads all cataloged paths from PostgreSQL,
resolves each from ZODB, and re-extracts index values.  It does not
discover objects that were never cataloged.

**`reindexIndex("name")`** is a PostgreSQL-only operation: it reads the
existing `idx` JSONB for all objects that have the named key and
re-applies it.  It does not re-extract values from the live Zope object.
To re-extract from objects, use `refreshCatalog()`.

## Troubleshooting

- Verify indexed object count in the ZMI Catalog tab.
- Check PostgreSQL directly:

  ```sql
  SELECT COUNT(*) FROM object_state WHERE path IS NOT NULL AND idx IS NOT NULL;
  ```

- If counts do not match: run `clearFindAndRebuild()`.
- If individual objects are missing: re-save the object in Plone (triggers `reindexObject()`).
