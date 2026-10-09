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
import transaction

transaction.commit()
```

Expected timing: approximately 15 ms per object.

## Selective reindex (reindexIndex)

Re-extracts a single index from all ZODB objects:

```python
catalog.reindexIndex("review_state")
import transaction

transaction.commit()
```

Useful after changing an indexer or adding a new index.

## Repair uncataloged content (repair_uncataloged)

Recatalogs content that exists in the ZODB but has no catalog data, without clearing the catalog.
Content got into this state through zodb-pgjsonb before the fix for
[zodb-pgjsonb#120](https://github.com/bluedynamics/zodb-pgjsonb/issues/120):
opening the edit form, the sharing tab, or any write without a full reindex.
The upgrade step to profile version 4 runs it once.
`refreshCatalog(clear=0)` cannot repair these objects, because it only visits rows that still have catalog data.

On large sites, run it from the command line instead of `@@plone-upgrade`, which can hit proxy timeouts.
Save this as `repair_uncataloged.py`:

```python
"""Run: zconsole run etc/zope.conf repair_uncataloged.py SITE_ID [--dry-run] [--all]"""

from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SpecialUsers import system
from plone.pgcatalog.maintenance import repair_uncataloged
from zope.component.hooks import setSite

import sys
import transaction

# zconsole does not reset sys.argv: [zconsole, run, zope.conf, script, *args]
args = sys.argv[4:]
site_id = next(a for a in args if not a.startswith("--"))
dry_run = "--dry-run" in args

site = app[site_id]  # noqa: F821  (app is provided by zconsole)
setSite(site)
newSecurityManager(None, system)

result = repair_uncataloged(
    site.portal_catalog, site, dry_run=dry_run, all_objects="--all" in args
)
for path in result.paths:
    print(path)
print(
    f"{result.checked} checked, {len(result.paths)} "
    f"{'would be recataloged' if dry_run else 'recataloged'}, "
    f"{len(result.failed)} failed"
)
if dry_run:
    transaction.abort()
else:
    transaction.commit()
```

Count first with `--dry-run` (on a copy of production if possible), then run without it.
The run commits every 500 objects and can be repeated after an interruption.
`--all` recatalogs every content object, not only the damaged ones.

When Tika extraction is configured, recataloged files are enqueued for extraction again, which restores their extracted text.
Expect a burst of extraction jobs.

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
| `repair_uncataloged()` | No | Yes | ~15 ms per repaired object | Content missing from listings and search but present in the ZODB |

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
