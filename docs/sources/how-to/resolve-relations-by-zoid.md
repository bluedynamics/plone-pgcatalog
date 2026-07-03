<!-- diataxis: how-to -->

# Resolve relations to brains without waking objects

## Problem

You hold a set of ZODB object ids — typically from `zc.relation` /
`zope.intid` (a `RelationList` field, or back-references from
`catalog.findRelations(...)`) — and you want the catalog **brains** for
those objects: their metadata, filtered by the current user's `View`
permission.

The naive approach wakes every object:

```python
from zope.component import getUtility
from zope.intid.interfaces import IIntIds

intids = getUtility(IIntIds)
objs = [intids.queryObject(intid) for intid in relation_intids]  # loads each!
```

On a listing/aside tile this is an object-load N+1: each `queryObject`
loads the object (and, for an anonymous request, walks the acquisition
chain to check `View`). A page with a few dozen related items can cost
hundreds of ZODB loads.

## Solution

pgcatalog stores every record in `object_state` keyed by its `zoid` (the
BIGINT primary key — the catalog `rid` *is* the zoid). The built-in
`zoid` / `oid` query operator lets you fetch the security-filtered brains
for a set of object ids directly, **without waking the objects**:

```python
brains = catalog(zoid=[123, 456, 789])
```

Getting the zoids from relations is wake-free — the intid utility's
`KeyReferenceToPersistent` already stores the object's oid, and
`oid → zoid` is just `int.from_bytes(...)`:

```python
from zope.component import getUtility
from zope.intid.interfaces import IIntIds

intids = getUtility(IIntIds)
refs = intids.refs  # IOBTree: intid -> KeyReferenceToPersistent

zoids = []
for intid in relation_intids:
    kr = refs.get(intid)
    if kr is not None:
        zoids.append(int.from_bytes(kr.oid, "big"))  # no getObject()

brains = catalog(zoid=zoids)  # security-filtered, no wakes
```

If you happen to hold raw 8-byte ZODB oids instead of ints, pass them to
`oid=` and pgcatalog converts them for you:

```python
brains = catalog(oid=[obj._p_oid for obj in ...])
```

## Full example: related-items resolution

```python
from Acquisition import aq_inner
from zc.relation.interfaces import ICatalog
from zope.component import getUtility
from zope.intid.interfaces import IIntIds

def related_brains(context, catalog):
    intids = getUtility(IIntIds)
    relcat = getUtility(ICatalog)
    refs = intids.refs

    # direct references (a RelationList field) + back-references
    tids = [rv.to_id for rv in getattr(context, "related_items", []) or []]
    tids += [
        rel.from_id
        for rel in relcat.findRelations(
            {"to_id": intids.getId(aq_inner(context)),
             "from_attribute": "related_items"}
        )
    ]

    zoids = []
    for tid in dict.fromkeys(tids):          # de-dup, keep order
        kr = refs.get(tid)
        if kr is not None:
            zoids.append(int.from_bytes(kr.oid, "big"))

    return catalog(zoid=zoids)               # brains, view-filtered, no wakes
```

To preserve the relation order, build a `{brain zoid: brain}` map and
iterate your `zoids` list — a plain SQL `IN`/`ANY` does not guarantee
row order.

## Notes

- **Security.** `zoid` / `oid` compose with the normal query and with the
  `allowedRolesAndUsers` filter that `searchResults` injects, so
  `catalog(zoid=...)` returns only brains the current user may view.
  Use `unrestrictedSearchResults(zoid=...)` to bypass the filter.
- **No storage, no reindex.** The operator filters the existing primary-key
  column; there is nothing to index and no migration for existing sites.
- **Empty set matches nothing.** `catalog(zoid=[])` returns no rows (it
  never degrades to "match the whole table").
- **Accepted values.** `zoid` takes ints or digit strings; `oid` takes raw
  8-byte oids; both accept a scalar or a list. Non-coercible values are
  dropped (ZCatalog-lenient).
- **Composes.** `catalog(zoid=[...], portal_type="Document")` ANDs the two
  filters like any other query.
