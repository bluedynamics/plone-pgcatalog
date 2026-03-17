"""Move/rename optimization for plone.pgcatalog.

Replaces per-child Python reindex with a single bulk SQL path UPDATE.

When Plone moves or renames a folder, OFS dispatches IObjectWillBeMovedEvent
and IObjectMovedEvent to every descendant.  CMFCatalogAware's handler calls
unindexObject() then indexObject() on each child — extracting ALL indexes
(including expensive SearchableText) and marking each child _p_changed.

This module installs wrapper handlers that:
1. Set a thread-local flag before OFS dispatches to children
2. The flag causes indexObject/unindexObject to return immediately (no-op)
3. After dispatch completes, register a pending bulk path UPDATE
4. In processor.finalize(), the single SQL UPDATE fixes all paths at once

For cross-container moves (oldParent != newParent), also performs a targeted
security-only reindex of descendants (allowedRolesAndUsers).
"""

from dataclasses import dataclass
from plone.pgcatalog.pending import _local
from plone.pgcatalog.pending import add_pending_move

import logging


__all__ = [
    "MoveContext",
    "install_move_handlers",
    "is_move_in_progress",
]


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Move context stack (thread-local)
# ---------------------------------------------------------------------------


@dataclass
class MoveContext:
    """Tracks an in-progress move operation.

    Attributes:
        old_prefix: the old path of the moved container
        event_object: the root object being moved (event.object)
    """

    old_prefix: str
    event_object: object


def _get_move_context_stack():
    """Return the thread-local move context stack."""
    try:
        return _local.move_context_stack
    except AttributeError:
        _local.move_context_stack = []
        return _local.move_context_stack


def _push_move_context(ctx):
    """Push a move context onto the stack."""
    _get_move_context_stack().append(ctx)


def _pop_move_context():
    """Pop the top move context from the stack."""
    stack = _get_move_context_stack()
    if stack:
        return stack.pop()
    return None


def is_move_in_progress():
    """Return True if a move operation is currently in progress.

    Used by PlonePGCatalogTool.indexObject/unindexObject to skip
    per-child processing during moves.
    """
    return bool(_get_move_context_stack())


# ---------------------------------------------------------------------------
# OFS event handler wrappers
# ---------------------------------------------------------------------------


def _is_pgcatalog_active():
    """Check if PlonePGCatalogTool is the active portal_catalog.

    Returns False if standard ZCatalog is active — wrappers pass through.
    """
    try:
        from plone.pgcatalog.interfaces import IPGCatalogTool
        from Products.CMFCore.interfaces import ICatalogTool
        from zope.component import queryUtility

        catalog = queryUtility(ICatalogTool)
        return IPGCatalogTool.providedBy(catalog)
    except ImportError:
        return False


def _wrapped_dispatchObjectWillBeMovedEvent(ob, event):
    """Wrapper for OFS.subscribers.dispatchObjectWillBeMovedEvent.

    For the root object of a move (ob is event.object), sets the
    move-in-progress flag before OFS dispatches to children.
    """
    from OFS.subscribers import dispatchObjectWillBeMovedEvent as _orig_will

    is_root = ob is event.object
    is_move = (
        event.oldParent is not None
        and event.newParent is not None
        and is_root
        and _is_pgcatalog_active()
    )

    if is_move:
        old_prefix = "/".join((*event.oldParent.getPhysicalPath(), event.oldName))
        _push_move_context(
            MoveContext(old_prefix=old_prefix, event_object=event.object)
        )

    try:
        _orig_will(ob, event)
    finally:
        if is_move:
            _pop_move_context()


def _wrapped_dispatchObjectMovedEvent(ob, event):
    """Wrapper for OFS.subscribers.dispatchObjectMovedEvent.

    For the root object of a move (ob is event.object), sets the
    move-in-progress flag, then after dispatch:
    - For cross-container moves, reindexes security on descendants
    - Registers a pending bulk path UPDATE for finalize()
    """
    from OFS.subscribers import dispatchObjectMovedEvent as _orig_moved

    is_root = ob is event.object
    is_move = (
        event.oldParent is not None
        and event.newParent is not None
        and is_root
        and _is_pgcatalog_active()
    )

    if is_move:
        old_prefix = "/".join((*event.oldParent.getPhysicalPath(), event.oldName))
        new_prefix = "/".join((*event.newParent.getPhysicalPath(), event.newName))
        _push_move_context(
            MoveContext(old_prefix=old_prefix, event_object=event.object)
        )

    try:
        _orig_moved(ob, event)
    finally:
        if is_move:
            _pop_move_context()

            # Security reindex for cross-container moves
            if event.oldParent is not event.newParent:
                _reindex_security_for_move(ob, old_prefix)

            # Register bulk path UPDATE for finalize()
            old_depth = len([p for p in old_prefix.split("/") if p])
            new_depth = len([p for p in new_prefix.split("/") if p])
            add_pending_move(old_prefix, new_prefix, new_depth - old_depth)


# ---------------------------------------------------------------------------
# Security reindex for cross-container moves
# ---------------------------------------------------------------------------


def _reindex_security_for_move(ob, old_prefix):
    """Targeted security-only reindex for descendants after cross-container move.

    When a container is moved to a different parent, its descendants may
    inherit different permissions. This function reindexes only the
    allowedRolesAndUsers index for each descendant, using the OLD path
    prefix (paths in PG haven't been bulk-updated yet at this point).

    Uses _partial_reindex (lightweight JSONB merge) — no _p_changed,
    no ZODB serialization.
    """
    try:
        from Products.CMFCore.utils import getToolByName
    except ImportError:
        return

    catalog = getToolByName(ob, "portal_catalog", None)
    if catalog is None:
        return

    security_idxs = list(
        getattr(ob, "_cmf_security_indexes", ("allowedRolesAndUsers",))
    )

    # Query using OLD paths (still in PG at this point)
    try:
        brains = catalog.unrestrictedSearchResults(path=old_prefix)
    except Exception:
        log.warning(
            "Failed to query descendants for security reindex: %s",
            old_prefix,
            exc_info=True,
        )
        return

    for brain in brains:
        brain_path = brain.getPath()
        if brain_path == old_prefix:
            continue  # Parent handled by normal pipeline

        try:
            child = brain._unrestrictedGetObject()
        except (AttributeError, KeyError):
            continue
        if child is None:
            continue

        was_changed = getattr(child, "_p_changed", 0)
        catalog.reindexObject(child, idxs=security_idxs, update_metadata=0)
        if was_changed is None:
            child._p_deactivate()  # Don't keep in cache if wasn't loaded


# ---------------------------------------------------------------------------
# Handler installation
# ---------------------------------------------------------------------------


def install_move_handlers():
    """Replace OFS dispatch handlers with our wrappers.

    Called once at Zope startup.  Unregisters the OFS handlers for
    IObjectWillBeMovedEvent and IObjectMovedEvent dispatch, and
    registers our wrappers that set the move-in-progress flag.
    """
    try:
        from OFS.interfaces import IObjectWillBeMovedEvent
        from OFS.subscribers import dispatchObjectMovedEvent
        from OFS.subscribers import dispatchObjectWillBeMovedEvent
        from zope.component import getGlobalSiteManager
        from zope.lifecycleevent.interfaces import IObjectMovedEvent

        import OFS.interfaces
    except ImportError:
        log.warning(
            "Cannot install move handlers — OFS or zope.component not available"
        )
        return

    gsm = getGlobalSiteManager()

    # Replace IObjectWillBeMovedEvent dispatcher
    gsm.unregisterHandler(
        dispatchObjectWillBeMovedEvent,
        (OFS.interfaces.IItem, IObjectWillBeMovedEvent),
    )
    gsm.registerHandler(
        _wrapped_dispatchObjectWillBeMovedEvent,
        (OFS.interfaces.IItem, IObjectWillBeMovedEvent),
    )

    # Replace IObjectMovedEvent dispatcher
    gsm.unregisterHandler(
        dispatchObjectMovedEvent,
        (OFS.interfaces.IItem, IObjectMovedEvent),
    )
    gsm.registerHandler(
        _wrapped_dispatchObjectMovedEvent,
        (OFS.interfaces.IItem, IObjectMovedEvent),
    )

    log.info("Installed move optimization handlers")
