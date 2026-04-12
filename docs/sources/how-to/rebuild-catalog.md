<!-- diataxis: how-to -->

# Rebuild or reindex the catalog

## When to rebuild

- **Fresh install on an existing ZODB**—plone.pgcatalog added to a site
  that already has content (the ``object_state.path`` column is empty and
  needs populating)
- After enabling BM25 (new columns need populating)
- After upgrading plone.pgcatalog (if release notes mention schema changes)
- After manual database restoration
- When catalog counts do not match actual content

## Full rebuild (clearFindAndRebuild)

Clears all catalog data and re-indexes every object by traversing the site.

Via ZMI:

1.
Navigate to portal_catalog > Advanced tab
2.
Click "Clear and Rebuild"

Via script:

```python
catalog = portal.portal_catalog
catalog.clearFindAndRebuild()
import transaction; transaction.commit()
```

Expected timing: approximately 15 ms per object.

## Selective reindex (reindexIndex)

Re-extracts a single index from all ZODB objects:

```python
catalog.reindexIndex("review_state")
import transaction; transaction.commit()
```

Useful after changing an indexer or adding a new index.

## Partial reindex (automatic)

When Plone calls `reindexObject(idxs=["review_state"])`, plone.pgcatalog uses a lightweight JSONB merge (`||` operator) instead of full re-extraction.
This happens automatically and does not trigger ZODB serialization of the object.

## Choosing the right operation

| Operation | Clears data? | Traverses site? | Speed | Use when |
|---|---|---|---|---|
| `clearFindAndRebuild()` | Yes | Yes | ~15 ms/obj | Schema changes, corrupt data, major upgrades |
| `refreshCatalog(clear=0)` | No | Re-catalogs existing | ~15 ms/obj | Reindex all without losing uncataloged objects |
| `refreshCatalog(clear=1)` | Yes | Yes | Same | Equivalent to `clearFindAndRebuild()` |
| `reindexIndex("name")` | No (single key) | Yes (ZODB load) | ~5 ms/obj | Single index changed, new indexer deployed |

**`clearFindAndRebuild()`** NULLs all catalog columns (path, idx,
searchable_text, backend extras), then traverses the entire portal tree
from the `ISiteRoot` breadth-first and calls `catalog_object()` on every
object found—including discussion items on content objects (when
`plone.app.discussion` is installed).
Use this when catalog data might be inconsistent with actual content, or
when bootstrapping plone.pgcatalog on an existing site where the
`object_state.path` column is not yet populated.

Memory stays flat even on large sites: the traversal queue holds only
path strings, not objects.
Objects are loaded on demand via
`unrestrictedTraverse` and ghosted by `cacheMinimize()` after every
500 indexed objects.

**`refreshCatalog(clear=0)`** reads all cataloged paths from PostgreSQL,
resolves each from ZODB, and re-extracts index values.
It does not
discover objects that were never cataloged.
Use `clearFindAndRebuild()` for the initial population.

**`reindexIndex("name")`** loads each cataloged object from ZODB via
`unrestrictedTraverse`, extracts the requested index value, and writes
a JSONB merge update. This is faster than `refreshCatalog()` because
it only re-extracts the single requested index, not all of them.
Available via ZMI: Indexes & Metadata tab > [reindex] button per index.

## Troubleshooting

- Verify indexed object count in the ZMI Catalog tab.
- Check PostgreSQL directly:

  ```sql
  SELECT COUNT(*) FROM object_state WHERE path IS NOT NULL AND idx IS NOT NULL;
  ```

- If counts do not match: run `clearFindAndRebuild()`.
- If individual objects are missing: re-save the object in Plone (triggers `reindexObject()`).
