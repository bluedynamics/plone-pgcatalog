"""Keep getObjPositionInParent ordering correct after container reorderings.

plone.folder's GopipIndex is a *fake* index: ZCatalog resolves positions at
sort time from the container's ordering, so Plone never reindexes siblings
after a reordering.  pgcatalog sorts on the stored snapshot
``(idx->>'getObjPositionInParent')::integer``, which goes stale on every
reorder — and on every delete, which shifts all following siblings (#216).

Design (see docs/plans/gopip-resync-on-reorder.md):

Nothing outside the SQL ``ORDER BY`` can observe the stored *value* —
ZCatalog's GopipIndex is a StubIndex (``getEntryForObject`` returns ``[]``)
and getObjPositionInParent is not metadata.  So stored values need not be
dense ordinals 0..n-1, only correctly *ordered* integers.  This module
maintains them as sparse ranks (LexoRank-style, step 1024):

1. A subscriber on ``(IOrderableFolder, IContainerModifiedEvent)`` records
   the container.  DefaultOrdering.moveObjectsByDelta (all moveObjects*
   paths, including the folder_contents rearrange views), OFS
   ``_setObject``/``_delObject`` (adds/deletes), and CopySupport
   (cut/paste, rename) all fire this event on the container.
2. A ``beforeCommitHook`` snapshots each recorded container's *final*
   ``idsInOrder()`` once per transaction into the pending store.
3. ``processor.finalize()`` reads the folder's current ranks (one index
   scan, no writes), computes the minimal set of rows violating the
   desired order via :func:`assign_ranks`, and rewrites only those.
   Prepend or drag & drop touch one row; append and delete touch none.

Existing dense positions are valid ranks, so there is no migration; a
folder heals on its first ordering event (or via
``maintenance.resync_gopip`` right after upgrade).

Known gap: ``IExplicitOrdering.orderObjects`` (sort a folder by key) fires
no event in plone.folder; no Plone UI path calls it.
"""

from Acquisition import aq_base
from plone.folder.interfaces import IExplicitOrdering
from plone.pgcatalog.move import _is_pgcatalog_active
from plone.pgcatalog.pending import _local
from plone.pgcatalog.pending import add_pending_gopip
from psycopg.types.json import Json

import bisect
import logging
import transaction


__all__ = ["RANK_STEP", "assign_ranks", "container_modified", "sync_folder_ranks"]


log = logging.getLogger(__name__)

RANK_STEP = 1024
_INT4_MIN = -(2**31)
_INT4_MAX = 2**31 - 1


# ---------------------------------------------------------------------------
# Rank assignment (pure)
# ---------------------------------------------------------------------------


def _longest_ordered_chain(current, desired):
    """Indices into *desired* forming a longest strictly-increasing rank chain.

    Ids without an integer rank in *current* can never be part of the chain
    (they must be assigned a rank anyway).  Classic patience-sorting LIS;
    strictness matters so duplicate legacy ranks are treated as violators.
    """
    ranks = []
    idx_of = []
    for i, cid in enumerate(desired):
        rank = current.get(cid)
        if isinstance(rank, int) and not isinstance(rank, bool):
            ranks.append(rank)
            idx_of.append(i)

    tails = []  # tails[k]: smallest tail rank of any increasing chain len k+1
    tails_idx = []  # position in ranks[] achieving tails[k]
    prev = [None] * len(ranks)
    for j, rank in enumerate(ranks):
        k = bisect.bisect_left(tails, rank)
        if k == len(tails):
            tails.append(rank)
            tails_idx.append(j)
        else:
            tails[k] = rank
            tails_idx[k] = j
        prev[j] = tails_idx[k - 1] if k > 0 else None

    chain = set()
    if tails_idx:
        j = tails_idx[-1]
        while j is not None:
            chain.add(idx_of[j])
            j = prev[j]
    return chain


def _fill(lo, hi, count, step):
    """*count* strictly-increasing ints between anchors *lo* and *hi*.

    Either anchor may be None (open end).  Returns None if the gap cannot
    hold *count* values (caller falls back to a full renumber).
    """
    if lo is None and hi is None:
        return [i * step for i in range(count)]
    if lo is None:
        first = hi - count * step
        if first <= _INT4_MIN:
            return None
        return [first + i * step for i in range(count)]
    if hi is None:
        last = lo + count * step
        if last >= _INT4_MAX:
            return None
        return [lo + (i + 1) * step for i in range(count)]
    if hi - lo - 1 < count:
        return None
    delta = (hi - lo) // (count + 1)
    return [lo + (i + 1) * delta for i in range(count)]


def _full_renumber(current, desired, step):
    """Dense re-rank of the whole folder — fallback on gap exhaustion."""
    if len(desired) * step >= _INT4_MAX:
        step = 1
    return {
        cid: i * step for i, cid in enumerate(desired) if current.get(cid) != i * step
    }


def assign_ranks(current, desired, step=RANK_STEP):
    """Minimal rank rewrites so stored ranks match the desired order.

    Args:
        current: ``{id: rank}`` as stored (rank may be None for rows whose
            idx lacks the key — those are always rewritten).
        desired: ids in the wanted order; every id must be a key of
            *current* (callers filter to ids that have a row).
        step: gap size for newly assigned ranks.

    Returns:
        ``{id: new_rank}`` for exactly the rows that must change.  Keeps a
        longest already-ordered chain untouched, so an append or a delete
        costs zero writes and a prepend or single drag & drop costs one.
    """
    keep = _longest_ordered_chain(current, desired)
    updates = {}
    n = len(desired)
    i = 0
    while i < n:
        if i in keep:
            i += 1
            continue
        run_start = i
        while i < n and i not in keep:
            i += 1
        lo = current[desired[run_start - 1]] if run_start > 0 else None
        hi = current[desired[i]] if i < n else None
        values = _fill(lo, hi, i - run_start, step)
        if values is None:
            return _full_renumber(current, desired, step)
        for cid, value in zip(desired[run_start:i], values, strict=True):
            if current.get(cid) != value:
                updates[cid] = value
    return updates


# ---------------------------------------------------------------------------
# SQL sync (called from processor.finalize, same PG txn as the ZODB commit)
# ---------------------------------------------------------------------------


def sync_folder_ranks(cursor, parent_path, ordered_ids):
    """Rewrite the ranks in one folder that violate *ordered_ids*.

    Reads the folder's current ranks (one index scan, no writes), lets
    :func:`assign_ranks` pick the minimal rewrite set, and updates only
    those rows.  Returns the number of rows written.

    Runs after the bulk path moves in finalize(), so parent_path is final.
    Ids without a cataloged row are skipped — a row written earlier in
    this transaction's batch is already visible to this cursor.
    """
    cursor.execute(
        "SELECT substring(path FROM length(%(parent)s) + 2) AS child_id, "
        "       (idx->>'getObjPositionInParent')::integer AS rank "
        "FROM object_state "
        "WHERE parent_path = %(parent)s AND idx IS NOT NULL",
        {"parent": parent_path},
    )
    current = {row["child_id"]: row["rank"] for row in cursor.fetchall()}
    desired = [cid for cid in ordered_ids if cid in current]
    updates = assign_ranks(current, desired)
    if not updates:
        return 0
    cursor.execute(
        "UPDATE object_state SET "
        "idx = jsonb_set(idx, '{getObjPositionInParent}', p.value) "
        "FROM jsonb_each(%(updates)s::jsonb) AS p(key, value) "
        "WHERE parent_path = %(parent)s "
        "  AND path = %(parent)s || '/' || p.key "
        "  AND idx IS NOT NULL",
        {"parent": parent_path, "updates": Json(updates)},
    )
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Event subscriber + commit-time snapshot
# ---------------------------------------------------------------------------


def container_modified(container, event):
    """Subscriber for (IOrderableFolder, IContainerModifiedEvent).

    Fires on reorders, adds, deletes, cut/paste and rename.  Only records
    the container; the ordering is read once per transaction at commit
    time so bulk operations stay O(n) instead of O(n**2).
    """
    if not _is_pgcatalog_active():
        return
    if getattr(aq_base(container), "getOrdering", None) is None:
        return
    if not IExplicitOrdering.providedBy(container.getOrdering()):
        # unordered/partial-unordered folders: positions never change
        return
    _register_container(container)


def _register_container(container):
    """Record *container* for a commit-time ordering snapshot (dedup per txn)."""
    txn = transaction.get()
    registry = getattr(_local, "gopip_registry", None)
    if registry is None or registry[0] is not txn:
        containers = {}
        _local.gopip_registry = (txn, containers)
        txn.addBeforeCommitHook(_snapshot_orderings, args=(containers,))
    else:
        containers = registry[1]
    key = container._p_oid or id(container)
    containers[key] = container


def _snapshot_orderings(containers):
    """beforeCommitHook: register each container's final ordering as pending."""
    for container in containers.values():
        try:
            path = "/".join(container.getPhysicalPath())
            ordered_ids = container.getOrdering().idsInOrder()
        except Exception:
            # container was deleted later in this transaction, or its
            # ordering is broken — its rows are uncataloged anyway
            log.debug("gopip: skipping ordering snapshot", exc_info=True)
            continue
        if ordered_ids:
            add_pending_gopip(path, ordered_ids)
