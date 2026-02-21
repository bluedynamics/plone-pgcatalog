"""Thread-local pending catalog data store for plone.pgcatalog.

Manages pending catalog index data between catalog_object() calls and
the CatalogStateProcessor's tpc_vote phase.  Uses thread-local storage
to avoid CMFEditions version-copy duplication (the annotation is NOT
stored on the object's __dict__, which gets cloned).

Also includes the ISavepointDataManager implementation so pending data
participates in ZODB transaction savepoints.
"""

from transaction.interfaces import IDataManagerSavepoint
from transaction.interfaces import ISavepointDataManager
from zope.interface import implementer

import threading
import transaction


__all__ = [
    "pop_all_partial_pending",
    "pop_pending",
    "set_partial_pending",
    "set_pending",
]


# Shared thread-local for all plone.pgcatalog state.
# Pending store uses: .pending, .partial_pending, ._pending_dm
# Pool module uses: .pgcat_conn, .pgcat_pool
_local = threading.local()


_MISSING = object()  # Sentinel for "no pending data"


def _get_pending():
    """Return the thread-local pending catalog data dict."""
    try:
        return _local.pending
    except AttributeError:
        _local.pending = {}
        return _local.pending


def _get_partial_pending():
    """Return the thread-local partial pending idx patches dict."""
    try:
        return _local.partial_pending
    except AttributeError:
        _local.partial_pending = {}
        return _local.partial_pending


def set_pending(zoid, data):
    """Register pending catalog data for a zoid.

    Args:
        zoid: ZODB OID as int
        data: dict with catalog columns, or None for uncatalog sentinel
    """
    _get_pending()[zoid] = data
    # Full update supersedes any partial pending for same zoid
    _get_partial_pending().pop(zoid, None)
    _ensure_joined()


def pop_pending(zoid):
    """Pop pending catalog data for a zoid, or return sentinel if absent.

    Returns:
        dict (catalog data), None (uncatalog), or _MISSING (no data).
    """
    return _get_pending().pop(zoid, _MISSING)


def set_partial_pending(zoid, idx_updates):
    """Register partial idx updates for a zoid.

    If a full pending entry already exists for this zoid (from a prior
    catalog_object call in the same transaction), merges the updates
    into the full pending's idx dict.  Otherwise stores in the
    partial_pending dict.

    IMPORTANT: Uses non-mutating merges (``{**old, **new}``) to preserve
    savepoint snapshot integrity.  ``PendingSavepoint`` uses shallow copies,
    so mutating shared dicts would corrupt rollback state.

    Args:
        zoid: ZODB OID as int
        idx_updates: dict of idx JSONB keys to update
    """
    full = _get_pending()
    if zoid in full and full[zoid] is not None:
        # Full pending exists: create new entry with merged idx.
        old = full[zoid]
        full[zoid] = {**old, "idx": {**old.get("idx", {}), **idx_updates}}
        return
    # Standalone partial: create new merged dict (savepoint-safe).
    pp = _get_partial_pending()
    existing = pp.get(zoid, {})
    pp[zoid] = {**existing, **idx_updates}
    _ensure_joined()


def pop_all_partial_pending():
    """Pop all partial pending data, returning and clearing the dict.

    Returns:
        dict of {zoid: {idx_key: value, ...}}
    """
    pp = _get_partial_pending()
    result = dict(pp)
    pp.clear()
    return result


@implementer(IDataManagerSavepoint)
class PendingSavepoint:
    """Snapshot of pending catalog data for savepoint rollback."""

    def __init__(self, snapshot, partial_snapshot):
        self._snapshot = snapshot
        self._partial_snapshot = partial_snapshot

    def rollback(self):
        pending = _get_pending()
        pending.clear()
        pending.update(self._snapshot)
        partial = _get_partial_pending()
        partial.clear()
        partial.update(self._partial_snapshot)


@implementer(ISavepointDataManager)
class PendingDataManager:
    """Participates in ZODB transaction to make pending data savepoint-aware.

    Joins lazily on first ``set_pending()`` call.  Clears pending on
    abort / tpc_finish / tpc_abort.
    """

    transaction_manager = None

    def __init__(self, txn):
        self._txn = txn
        self._joined = True

    def savepoint(self):
        return PendingSavepoint(dict(_get_pending()), dict(_get_partial_pending()))

    def abort(self, transaction):
        _get_pending().clear()
        _get_partial_pending().clear()
        self._joined = False  # AbortSavepoint may have unjoined us

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_vote(self, transaction):
        pass

    def tpc_finish(self, transaction):
        _get_pending().clear()
        _get_partial_pending().clear()

    def tpc_abort(self, transaction):
        _get_pending().clear()
        _get_partial_pending().clear()

    def sortKey(self):
        return "~plone.pgcatalog.pending"


def _ensure_joined():
    """Ensure a PendingDataManager is joined to the current transaction."""
    txn = transaction.get()
    try:
        dm = _local._pending_dm
        if dm._txn is txn and dm._joined:
            return
    except AttributeError:
        pass
    dm = PendingDataManager(txn)
    _local._pending_dm = dm
    txn.join(dm)
