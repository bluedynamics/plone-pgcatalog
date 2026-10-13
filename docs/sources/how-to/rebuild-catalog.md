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

At the end, the rebuild runs `ANALYZE object_state`, so the query planner sees the new catalog data.
If you write your own bulk (re)catalog tool, call `plone.pgcatalog.maintenance.analyze_object_state(conn)` (or run `ANALYZE object_state`) when it is done.
Until then, queries can ignore the catalog indexes and become very slow.

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
It first runs the undo follow-up for all queued rows, see {ref}`undo-follow-up`.
Content got into this state through zodb-pgjsonb before the fix for
[zodb-pgjsonb#120](https://github.com/bluedynamics/zodb-pgjsonb/issues/120):
opening the edit form, the sharing tab, or any write without a full reindex.
The upgrade step to profile version 4 runs it once.
`refreshCatalog(clear=0)` cannot repair these objects, because it only visits rows that still have catalog data.

The repair refuses to run with zodb-pgjsonb older than 1.17.0, because that version would uncatalog the repaired objects again on their next plain write.
The upgrade step first counts rows that could be damaged and skips the site walk when there are none.

```{important}
With Tika extraction configured, the repair enqueues extraction for every repaired file, and so does the upgrade step.
Before you upgrade, make sure the extraction worker has the same `PGCATALOG_TIKA_CONTENT_TYPES` as Zope and that the Tika image is pinned.
See {doc}`enable-tika-extraction`.
```

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
Run it with the same environment variables as Zope, in particular `PGCATALOG_TIKA_URL` and `PGCATALOG_TIKA_CONTENT_TYPES`.
Without them, repaired files are not queued for extraction, and their text is extracted in-process by `portal_transforms` instead.
The log names every recataloged path and every path that could not be repaired.
The run commits every 500 objects and can be repeated after an interruption.
When a batch hits a write conflict with an editor, it is retried once; paths that conflict a second time are reported as failed, and you run the repair again later.
At the end, it runs `ANALYZE object_state`, so that the query planner sees the new rows.
`--all` recatalogs every content object, not only the damaged ones.

When Tika extraction is configured, recataloged files are enqueued for extraction again, which restores their extracted text.
Expect a burst of extraction jobs.
Text that was lost on files that look intact is restored separately, see {ref}`restore-lost-extracted-text`.

## Repair stale security data

Before [#271](https://github.com/bluedynamics/plone-pgcatalog/issues/271) was fixed, a cross-container move left the descendants of the moved folder with the `allowedRolesAndUsers` of their old location.
Search then showed content to users who had lost access, or hid it from users who gained it.
The upgrade step to profile version 6 repairs this once.
It reindexes only the security indexes of all cataloged objects (a partial reindex), so other index data is untouched, no object is written to the ZODB, and no text is extracted again.

To run it outside the upgrade, for example from a script on a large site:

```python
from plone.pgcatalog.maintenance import reindex_security

result = reindex_security(portal.portal_catalog, portal)
print(result.checked, result.failed)
transaction.commit()
```

It commits every 500 objects and can be repeated after an interruption.

(undo-follow-up)=

## Catalog after undo (undo.process_queue)

On a history-preserving storage, undo restores the objects but not the catalog data plone.pgcatalog keeps next to them.
With zodb-pgjsonb 1.18.0 or later, the undo hides the affected catalog rows in its own transaction and queues them in `pgcatalog_undo_queue`:

- Restored content objects lose all catalog data, like uncataloged objects.
- Rows below some of these objects lose their `allowedRolesAndUsers` data, so no restricted search (anonymous included) finds them.
  The undone transaction may have changed the security of these rows without writing them, as a sharing change, a workflow transition or a move does.
  See {ref}`undo-subtree-rule` for which objects this applies to.

The follow-up recomputes the queued rows from the restored content: a full reindex for restored objects (at their restored path; after the undo of a move, the descendants' paths are moved back in one `UPDATE`), and a security reindex for the rows below them.
Objects that cannot be found any more are uncataloged.
It runs:

- at the end of the request that did the undo, up to `PGCATALOG_UNDO_FOLLOWUP_LIMIT` rows (default 2000; a warning in the log gives the count of rows still hidden);
- from the ZMI: `portal_catalog`, Catalog tab, "Undo follow-up" (shown when the queue table exists, with a button when rows are queued), with the same limit;
- as the first step of `repair_uncataloged()`, without a limit;
- from a script, see below.

Rows that stay queued stay hidden, also after a restart; Zope logs a warning at startup while the queue is not empty.
A follow-up conflicting with an editor is retried once; rows that fail stay queued for the next run.

The follow-up writes the recataloged objects, so after it has run, the undo itself can no longer be undone.

(undo-subtree-rule)=

### When the rows below an object are hidden

The undo compares, for every restored object, the revision the undone transaction wrote with the revision the undo writes back.
The rows below the object lose their security data when:

- its state differs in anything but `modification_date` and `cmf_uid`: local roles, the local role block, permission settings (including only the acquire flag), but also a title, a newly created `__annotations__` or a changed id;
- its class differs;
- it changes place: it is renamed, or it is moved into or out of a container (the containers' entries for it differ);
- the undo cannot tell, for example with data it cannot read.

The rows below a deleted object (undo of an add) always lose their security data.

Adding, deleting or moving content writes the container only for its modification date (and `cmf_uid` on the first child), so undoing such a change recatalogs the container but leaves the rest of its content alone.
An undone edit of a container, even of only its title, hides the security data below it until the follow-up is done.

Known gaps, the same as without this comparison:

- Add-ons that keep local roles outside the object's own attributes (an `ILocalRoleProvider` reading annotations or other persistent objects): an undone change there does not hide the rows below.
- Permission or local role changes on the Zope root or on other ancestors that are not cataloged (an OFS folder above the site).
- A workflow transition that changes no permission writes only the workflow history; after its undo, the object's own `review_state` stays as it was until the object is reindexed.
- Transactions that only ran partial reindexes cannot be undone at all.
- Undoing the undo conflicts once the follow-up has run, see above.

Save this as `undo_follow_up.py`:

```python
"""Run: zconsole run etc/zope.conf undo_follow_up.py SITE_ID"""

from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SpecialUsers import system
from plone.pgcatalog.undo import process_queue
from zope.component.hooks import setSite

import sys
import transaction

site = app[sys.argv[4]]  # noqa: F821  (app is provided by zconsole)
setSite(site)
newSecurityManager(None, system)

result = process_queue(site.portal_catalog)  # all queued rows
print(
    f"{result.processed} processed, {result.dropped} uncataloged, "
    f"{result.failed} failed, {result.remaining} still queued"
)
transaction.commit()
```

`process_queue(catalog, limit=None)` commits every 500 rows itself and can be repeated after an interruption.
Pass a `limit` to process only that many rows.

Sites that undid transactions on a history-preserving storage with an older plone.pgcatalog or zodb-pgjsonb have stale catalog rows that nothing can detect; run one Clear and Rebuild.

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
| `undo.process_queue()` | No | No (queued rows only) | 1–3 ms per security row, ~15 ms per recataloged object | Rows an undo hid are still queued (Catalog tab shows the count) |

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
