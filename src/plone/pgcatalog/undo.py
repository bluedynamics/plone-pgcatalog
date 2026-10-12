"""Catalog consistency after ZODB undo (#265, zodb-pgjsonb#121).

Undo restores object state, but not the catalog columns this package keeps
on ``object_state``: the restored rows keep the data of the undone
transaction, and rows the undone transaction changed in ``finalize()``
without writing the object (security partial reindex of descendants, bulk
path moves) are not touched at all.

Two steps:

1. :func:`hide_undone` runs in zodb-pgjsonb's ``undone()`` hook, inside the
   undo's PG transaction, before anything of the undo is written.  It hides
   the affected rows, fail closed, and queues them in
   ``pgcatalog_undo_queue``:

   - restored catalogable objects are hidden completely (all catalog
     columns NULL, like ``uncatalog_object``) and queued for ``recatalog``;
   - every row below a deleted object, and below a restored object whose
     undone change could have reached its descendants (see
     :func:`needs_subtree_rule`), loses its ``allowed_roles`` (invisible to
     every restricted search) and is queued for ``security``.  This is the
     subtree rule: the undone transaction may have changed their security
     data without writing them (sharing, workflow, move).

2. :func:`process_queue` recomputes the queued rows from the restored
   content in ordinary ZODB transactions, with application context.  The
   ``IPubSuccess`` subscriber :func:`drain_after_undo` runs it at the end of
   the request that did the undo; the ZMI, :func:`process_queue` itself and
   ``maintenance.repair_uncataloged()`` are the other ways to run it.

The hook only runs SQL: writing ZODB objects there would deadlock
(zodb-pgjsonb#132).
"""

from Acquisition import aq_base
from importlib.metadata import version as _dist_version
from plone.pgcatalog import maintenance
from plone.pgcatalog.gopip import _has_explicit_ordering
from plone.pgcatalog.gopip import _register_container
from plone.pgcatalog.indexing import uncatalog_assignments
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.pending import add_pending_move
from plone.pgcatalog.pool import get_pool
from Products.CMFCore.interfaces import ISiteRoot
from typing import NamedTuple
from ZODB.utils import p64
from ZODB.utils import u64
from zope.component.hooks import getSite
from zope.component.hooks import site as site_context

import logging
import os
import psycopg
import re
import threading
import transaction


__all__ = [
    "UndoFollowupResult",
    "check_hook_support",
    "drain_after_undo",
    "followup_limit",
    "hide_undone",
    "needs_subtree_rule",
    "process_queue",
    "queue_length",
    "warn_if_queue_not_empty",
]


log = logging.getLogger(__name__)

DEFAULT_FOLLOWUP_LIMIT = 2000
_LIMIT_ENV = "PGCATALOG_UNDO_FOLLOWUP_LIMIT"

_PGJSONB_UNDO_HOOK = (1, 18, 0)  # zodb-pgjsonb#121: undone() hook

# Referrer walk: content -> bucket -> BTree node(s) -> folder, repeated for
# every hidden ancestor (the undo of a delete hides a whole subtree, and an
# undone add or delete hides the container itself), up to the first row with
# a path or an object resolved earlier in the run.
_WALK_MAX_DEPTH = 32
_WALK_MAX_CANDIDATES = 10

# Set by hide_undone() in the thread that commits the undo; read by the
# IPubSuccess subscriber of the same request.
_local = threading.local()


# ---------------------------------------------------------------------------
# Hook side: hide and queue (inside the undo's PG transaction, SQL only)
# ---------------------------------------------------------------------------

_QUEUE_CONFLICT = (
    "ON CONFLICT (kind, zoid) DO UPDATE SET "
    "undone_tid = EXCLUDED.undone_tid, "
    "path = COALESCE(EXCLUDED.path, pgcatalog_undo_queue.path), "
    "queued_at = now()"
)

_QUEUE_RECATALOG_SQL = (
    "INSERT INTO pgcatalog_undo_queue (kind, zoid, path, undone_tid) "
    "SELECT 'recatalog', t.zoid, t.path, %(tid)s "
    "FROM unnest(%(zoids)s::bigint[], %(paths)s::text[]) "
    "  WITH ORDINALITY AS t(zoid, path, n) "
    f"ORDER BY t.n {_QUEUE_CONFLICT}"
)

# One statement per subtree root, over the text_pattern_ops path index.
_HIDE_SUBTREE_SQL = (
    "WITH hidden AS ("
    "  UPDATE object_state SET allowed_roles = NULL "
    "  WHERE path LIKE %(like)s AND idx IS NOT NULL "
    "  RETURNING zoid, path"
    ") "
    "INSERT INTO pgcatalog_undo_queue (kind, zoid, path, undone_tid) "
    "SELECT 'security', zoid, path, %(tid)s FROM hidden "
    f"ORDER BY path {_QUEUE_CONFLICT}"
)


def _like_prefix(path):
    """LIKE pattern for the rows strictly below *path*."""
    escaped = path.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "/%"


def _pre_undo_rows(cursor, restored, deleted):
    """Current rows of the undone zoids: ``{zoid: {path, class_mod, class_name}}``.

    A restored zoid without a current row (undo of an undo of an add) is
    looked up in ``object_history`` for its class; it has no path.
    """
    cursor.execute(
        "SELECT zoid, path, class_mod, class_name FROM object_state "
        "WHERE zoid = ANY(%(zoids)s)",
        {"zoids": [*restored, *deleted]},
    )
    rows = {row["zoid"]: row for row in cursor.fetchall()}
    missing = [zoid for zoid in restored if zoid not in rows]
    if missing:
        cursor.execute(
            "SELECT DISTINCT ON (zoid) zoid, NULL::text AS path, class_mod, "
            "class_name FROM object_history "
            "WHERE zoid = ANY(%(zoids)s) AND class_mod <> '' "
            "ORDER BY zoid, tid DESC",
            {"zoids": missing},
        )
        rows.update((row["zoid"], row) for row in cursor.fetchall())
    return rows


def _path_order(row):
    """Sort key: rows with a path by path, then the ones without."""
    path = row["path"] if row is not None else None
    return (path is None, path or "")


def _subtree_roots(paths):
    """Sorted, de-duplicated paths without the ones inside another root."""
    roots = []
    for path in sorted(set(paths)):
        if roots and path.startswith(roots[-1] + "/"):
            continue
        roots.append(path)
    return roots


# ---------------------------------------------------------------------------
# Which restored objects get the subtree rule
#
# Adding, deleting or moving content writes the container too (its
# modification date), so applying the subtree rule to every restored
# container would hide the whole site after undoing a delete in its root.
# The rule is therefore applied only where the undone change could have
# changed the security of the rows below: a descendant's allowedRolesAndUsers
# depends on the permission settings and local roles of itself and its
# ancestors, and on which objects are its ancestors.  Deliberately coarse:
# every change of the container's own state except the two keys that adding
# and removing children write counts, so a title edit still applies the
# rule.  When in doubt, the rule applies.
# ---------------------------------------------------------------------------

# Top-level state keys that adding, deleting or moving a child writes into
# its container, and that no security computation reads.
_HARMLESS_KEYS = frozenset({"modification_date", "cmf_uid"})
_ABSENT = object()


def _state_changed(before, after):
    """True unless the two states differ in harmless keys only (or not at all)."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return True
    keys = (before.keys() | after.keys()) - _HARMLESS_KEYS
    return any(before.get(key, _ABSENT) != after.get(key, _ABSENT) for key in keys)


def _relocated(entries):
    """Zoids whose membership in the refs of some undone object changes.

    A move or rename shows here: the container buckets of the old and the
    new location are undone too, one of them loses the moved object's ref
    and the other gains it.  None when *entries* cannot be read.
    """
    changed = set()
    try:
        for entry in entries.values():
            before = set(entry["before"]["refs"])
            after = set(entry["after"]["refs"]) if entry["after"] else set()
            changed |= before ^ after
    except (AttributeError, KeyError, TypeError):
        return None
    return changed


def needs_subtree_rule(zoid, entries, relocated):
    """Whether undoing *zoid*'s change can affect the security of its subtree.

    *entries* is the ``undone()`` hook's argument, *relocated* the result of
    :func:`_relocated`.  Compares what the undone transaction wrote
    (``before``) with what the undo writes (``after``); True when

    - the object's state differs in any key but ``modification_date`` and
      ``cmf_uid`` (local roles and their block, permission settings, and
      also a title, a created ``__annotations__`` or a changed ``id``),
    - its class differs,
    - it changes location (see :func:`_relocated`), or
    - this cannot be decided (no entry, no data, a deleted object).
    """
    if entries is None or relocated is None or zoid in relocated:
        return True
    try:
        entry = entries[zoid]
        before, after = entry["before"], entry["after"]
        if after is None:
            return True
        if (before["class_mod"], before["class_name"]) != (
            after["class_mod"],
            after["class_name"],
        ):
            return True
        return _state_changed(before["state"], after["state"])
    except (KeyError, TypeError):
        return True


def hide_undone(cursor, tid, restored, deleted, entries=None):
    """Hide and queue the catalog rows affected by undoing *tid*.

    Called by ``CatalogStateProcessor.undone()``, once per undone tid, on
    the cursor of the undo's PG transaction, before the undo is written
    (``object_state`` still shows the pre-undo rows).  *entries* holds both
    revisions of every undone zoid; without it every restored object gets
    the subtree rule.  Exceptions abort the undo.  Returns True when catalog
    data was hidden or queued.
    """
    if not restored and not deleted:
        return False
    rows = _pre_undo_rows(cursor, restored, deleted)

    # Restored catalogable objects: everything in their rows belongs to the
    # undone transaction or is derived from it.  Queue first, then hide.
    # Queued in path order, containers first (rows without a path last), so
    # the follow-up resolves ancestors before their descendants.
    recatalog = {}
    for zoid in sorted(restored, key=lambda z: _path_order(rows.get(z))):
        row = rows.get(zoid)
        if row is not None and maintenance._catalogable_class(
            row["class_mod"], row["class_name"]
        ):
            recatalog[zoid] = row["path"]
    if recatalog:
        cursor.execute(
            _QUEUE_RECATALOG_SQL,
            {"tid": tid, "zoids": list(recatalog), "paths": list(recatalog.values())},
        )
        cursor.execute(
            f"UPDATE object_state SET {uncatalog_assignments()} "
            "WHERE zoid = ANY(%(zoids)s)",
            {"zoids": list(recatalog)},
        )

    # Subtree rule: below a deleted object, and below a restored one whose
    # undone change can reach its descendants, the undone transaction may
    # have changed security data without writing the rows.  Read by the
    # current (pre-undo) paths, which is where the descendants' rows are.
    relocated = _relocated(entries) if entries is not None else None
    candidates = [
        zoid for zoid in restored if needs_subtree_rule(zoid, entries, relocated)
    ]
    candidates.extend(deleted)
    roots = _subtree_roots(
        rows[zoid]["path"] for zoid in candidates if rows.get(zoid, {}).get("path")
    )
    hidden = 0
    for root in roots:
        cursor.execute(_HIDE_SUBTREE_SQL, {"like": _like_prefix(root), "tid": tid})
        hidden += cursor.rowcount

    if not recatalog and not hidden:
        return False
    log.info(
        "undo of tid %d: %d objects hidden for recatalog, %d rows below %d "
        "roots hidden for security reindex (%d of %d restored objects need "
        "the subtree rule)",
        tid,
        len(recatalog),
        hidden,
        len(roots),
        len(candidates) - len(deleted),
        len(restored),
    )
    _local.followup_pending = True
    return True


# ---------------------------------------------------------------------------
# Follow-up: recompute the queued rows (ordinary ZODB transactions)
# ---------------------------------------------------------------------------


class UndoFollowupResult(NamedTuple):
    """Outcome of :func:`process_queue`."""

    processed: int  # queue rows applied and removed
    dropped: int  # objects that could not be resolved; uncataloged and removed
    failed: int  # rows left in the queue (error or second commit conflict)
    remaining: int  # rows still queued after the run


def followup_limit():
    """Rows per request-end drain, from ``PGCATALOG_UNDO_FOLLOWUP_LIMIT``."""
    raw = os.environ.get(_LIMIT_ENV, "").strip()
    if not raw:
        return DEFAULT_FOLLOWUP_LIMIT
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value <= 0:
        log.warning(
            "%s=%r is not a positive integer; using %d",
            _LIMIT_ENV,
            raw,
            DEFAULT_FOLLOWUP_LIMIT,
        )
        return DEFAULT_FOLLOWUP_LIMIT
    return value


def _table_exists(cur):
    cur.execute("SELECT to_regclass('pgcatalog_undo_queue') IS NOT NULL AS present")
    return cur.fetchone()["present"]


def queue_length(conn):
    """Number of queued rows, or None when the queue table does not exist."""
    with conn.cursor() as cur:
        if not _table_exists(cur):
            return None
        cur.execute("SELECT count(*) AS cnt FROM pgcatalog_undo_queue")
        return cur.fetchone()["cnt"]


def _read_queue(pool, limit):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur):
                return []
            cur.execute(
                "SELECT id, kind, zoid, path, undone_tid FROM pgcatalog_undo_queue "
                "ORDER BY id LIMIT %(limit)s",
                {"limit": limit},
            )
            return cur.fetchall()
    finally:
        pool.putconn(conn)


def _remaining(pool):
    conn = pool.getconn()
    try:
        return queue_length(conn) or 0
    finally:
        pool.putconn(conn)


_WALK_SQL = """
WITH RECURSIVE up(zoid, path, chain, depth) AS (
    SELECT o.zoid, o.path, ARRAY[]::bigint[], 1
    FROM object_state o
    WHERE o.refs @> ARRAY[%(zoid)s::bigint] AND o.zoid <> %(zoid)s
  UNION ALL
    SELECT o.zoid, o.path, up.chain || up.zoid, up.depth + 1
    FROM up JOIN object_state o ON o.refs @> ARRAY[up.zoid]
    WHERE up.path IS NULL
      AND up.zoid <> ALL(%(known)s::bigint[])
      AND up.depth < %(max_depth)s
      AND o.zoid <> ALL(up.chain || up.zoid || %(zoid)s::bigint)
)
SELECT zoid, path, chain FROM up
WHERE path IS NOT NULL OR zoid = ANY(%(known)s::bigint[])
ORDER BY depth
LIMIT %(limit)s
"""


class _Run:
    """State of one :func:`process_queue` run."""

    def __init__(self, catalog, site, pool):
        self.catalog = catalog
        self.jar = catalog._p_jar
        self.pool = pool
        self.site_path = "/".join(site.getPhysicalPath())
        # zoid -> path resolved in this run; seeds the referrer walk.
        self.known = {u64(aq_base(site)._p_oid): self.site_path}
        # old prefix -> new prefix of the moves inverted in this run.
        self.rewrites = {}

    # -- path resolution ----------------------------------------------------

    def _rewrite(self, hint):
        """Apply the prefix rewrites of this run to a path hint."""
        if not hint:
            return hint
        for old, new in self.rewrites.items():
            if hint == old or hint.startswith(old + "/"):
                return new + hint[len(old) :]
        return hint

    def _traverse(self, zoid, path):
        """The object at *path* if it is *zoid* and really lives there.

        After the undo of a move, the old location can still reach the
        object through acquisition; its physical path tells.
        """
        try:
            obj = self.catalog.unrestrictedTraverse(path, None)
            if obj is None or getattr(aq_base(obj), "_p_oid", None) != p64(zoid):
                return None
            if "/".join(obj.getPhysicalPath()) != path:
                return None
        except Exception:
            return None
        return obj

    def _object_id(self, zoid):
        try:
            return self.jar.get(p64(zoid)).getId()
        except Exception:
            return None

    def _walk(self, zoid):
        """Candidate paths of *zoid* from the rows that reference it."""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    _WALK_SQL,
                    {
                        "zoid": zoid,
                        "known": list(self.known),
                        "max_depth": _WALK_MAX_DEPTH,
                        "limit": _WALK_MAX_CANDIDATES,
                    },
                )
                found = cur.fetchall()
                chain_zoids = {z for row in found for z in row["chain"]}
                classes = {}
                if chain_zoids:
                    cur.execute(
                        "SELECT zoid, class_mod, class_name FROM object_state "
                        "WHERE zoid = ANY(%(zoids)s)",
                        {"zoids": list(chain_zoids)},
                    )
                    classes = {
                        row["zoid"]: (row["class_mod"], row["class_name"])
                        for row in cur.fetchall()
                    }
        finally:
            self.pool.putconn(conn)

        own_id = self._object_id(zoid)
        if own_id is None:
            return
        for row in found:
            base = self.known.get(row["zoid"], row["path"])
            # Content between the ancestor and the object contributes its id;
            # buckets, BTrees and other containers' internals do not.
            ids = []
            for member in reversed(row["chain"]):
                cls = classes.get(member)
                if cls is not None and maintenance._catalogable_class(*cls):
                    member_id = self._object_id(member)
                    if member_id is None:
                        break
                    ids.append(member_id)
            else:
                yield "/".join([base.rstrip("/"), *ids, own_id])

    def resolve(self, zoid, hint):
        """Return ``(obj, path)`` for a queue row, or None."""
        if hint:
            obj = self._traverse(zoid, hint)
            if obj is not None:
                return obj, hint
        for path in self._walk(zoid):
            obj = self._traverse(zoid, path)
            if obj is not None:
                return obj, path
        return None

    # -- applying -------------------------------------------------------------

    def _apply_row(self, row):
        """Apply one queue row; returns 'done', 'drop' or 'skip'."""
        hint = self._rewrite(row["path"])
        found = self.resolve(row["zoid"], hint)
        if found is None:
            log.warning(
                "undo follow-up: cannot resolve zoid %d (path before undo: %s); "
                "leaving it uncataloged",
                row["zoid"],
                row["path"],
            )
            return "drop"
        obj, path = found
        if not maintenance._is_catalogable(obj, self.catalog):
            log.debug("undo follow-up: %s is not catalogable", path)
            return "drop"
        if path != self.site_path and not path.startswith(self.site_path + "/"):
            return "skip"  # another site's object; its own drain handles it
        self.known[row["zoid"]] = path
        if row["kind"] == "recatalog":
            self.catalog.catalog_object(obj, path)
            if hint and hint != path:
                # The undone transaction moved the object: move the
                # descendants' rows back in one UPDATE in finalize().
                depth_delta = path.count("/") - hint.count("/")
                add_pending_move(hint, path, depth_delta)
                self.rewrites[hint] = path
            if _has_explicit_ordering(obj):
                # The restored ordering is the authority for the ranks.
                _register_container(obj)
        else:
            idxs = getattr(obj, "_cmf_security_indexes", ("allowedRolesAndUsers",))
            self.catalog.reindexObject(obj, idxs=list(idxs), update_metadata=0)
        return "done"

    def apply(self, rows):
        """Apply *rows*; return ``(done, broken)`` for ``_commit_batch``."""
        done, broken = [], []
        for row in rows:
            try:
                action = self._apply_row(row)
            except Exception:
                log.warning(
                    "undo follow-up: failed on zoid %d (%s)",
                    row["zoid"],
                    row["path"],
                    exc_info=True,
                )
                broken.append(row)
                continue
            if action != "skip":
                done.append((row, action))
        return done, broken

    def finish(self, done):
        """After a successful commit: uncatalog the dropped rows, dequeue all."""
        if not done:
            return
        dropped = [row["zoid"] for row, action in done if action == "drop"]
        conn = self.pool.getconn()
        try:
            with conn.transaction(), conn.cursor() as cur:
                if dropped:
                    # Only rows still hidden by the undo: a concurrent full
                    # reindex has made a row correct again.
                    cur.execute(
                        f"UPDATE object_state SET {uncatalog_assignments()} "
                        "WHERE zoid = ANY(%(zoids)s) AND allowed_roles IS NULL "
                        "AND (path IS NOT NULL OR idx IS NOT NULL)",
                        {"zoids": dropped},
                    )
                    if cur.rowcount:
                        cur.execute("SELECT nextval('pgcatalog_change_seq')")
                # A later undo of the same object updated undone_tid; that
                # row stays queued.
                cur.execute(
                    "DELETE FROM pgcatalog_undo_queue q "
                    "USING unnest(%(ids)s::bigint[], %(tids)s::bigint[]) "
                    "  AS d(id, tid) "
                    "WHERE q.id = d.id AND q.undone_tid = d.tid",
                    {
                        "ids": [row["id"] for row, _ in done],
                        "tids": [row["undone_tid"] for row, _ in done],
                    },
                )
        finally:
            self.pool.putconn(conn)


def process_queue(catalog, limit=None):
    """Recatalog what an undo hid, from the restored content.

    Processes up to *limit* queue rows (all with None) in queue order:
    ``recatalog`` rows get a full ``catalog_object()``, and when the object
    is no longer where it was before the undo (undo of a move or rename),
    the descendants' paths are moved back in one UPDATE; ``security`` rows
    get the ``allowedRolesAndUsers`` partial reindex.  Objects that cannot
    be found any more (undo of an add) are uncataloged.  Rows of objects
    outside *catalog*'s site are left for that site.

    Commits every 500 rows (a conflicting batch is retried once) and removes
    the processed rows after each commit.  Idempotent; unprocessed rows stay
    hidden.  Needs a Plone site around *catalog*.

    Returns:
        UndoFollowupResult(processed, dropped, failed, remaining)
    """
    pool = get_pool(catalog)
    rows = _read_queue(pool, limit)
    if not rows:
        return UndoFollowupResult(0, 0, 0, _remaining(pool))
    site = catalog._find_site_root()
    if site is None:
        raise RuntimeError(f"process_queue: no Plone site above {catalog!r}")
    run = _Run(catalog, site, pool)
    jar = catalog._p_jar
    processed = dropped = failed = 0
    batch_size = maintenance._REBUILD_BATCH
    with site_context(site):
        for start in range(0, len(rows), batch_size):
            chunk = rows[start : start + batch_size]
            done, broken = run.apply(chunk)
            done, broken = maintenance._commit_batch(
                jar, done, broken, lambda chunk=chunk: run.apply(chunk)
            )
            run.finish(done)
            dropped += sum(1 for _, action in done if action == "drop")
            processed += sum(1 for _, action in done if action == "done")
            failed += len(broken)
    remaining = _remaining(pool)
    log.info(
        "undo follow-up: %d processed, %d dropped, %d failed, %d remaining",
        processed,
        dropped,
        failed,
        remaining,
    )
    return UndoFollowupResult(processed, dropped, failed, remaining)


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------


def _pg_catalog(site):
    catalog = getattr(site, "portal_catalog", None)
    if catalog is not None and IPGCatalogTool.providedBy(catalog):
        return catalog
    return None


def _catalogs_for_request(request):
    """The PG catalogs to drain for *request*.

    The site the request was published in; for an undo in the ZMI of the
    Zope root, every Plone site below it.
    """
    parents = request.get("PARENTS") or []
    for obj in parents:
        if ISiteRoot.providedBy(obj):
            catalog = _pg_catalog(obj)
            return [catalog] if catalog is not None else []
    site = getSite()
    if site is not None and ISiteRoot.providedBy(site):
        catalog = _pg_catalog(site)
        return [catalog] if catalog is not None else []
    if not parents:
        return []
    catalogs = []
    for obj in parents[-1].objectValues():
        if ISiteRoot.providedBy(obj):
            catalog = _pg_catalog(obj)
            if catalog is not None:
                catalogs.append(catalog)
    return catalogs


def _drain(request):
    limit = followup_limit()
    for catalog in _catalogs_for_request(request):
        transaction.begin()
        try:
            result = process_queue(catalog, limit)
        finally:
            transaction.abort()
        if result.remaining:
            log.warning(
                "undo follow-up: %d catalog rows are still hidden after this "
                "request (%s=%d); run the drain from the ZMI (portal_catalog, "
                "Catalog tab), plone.pgcatalog.undo.process_queue() or "
                "maintenance.repair_uncataloged()",
                result.remaining,
                _LIMIT_ENV,
                limit,
            )


def drain_after_undo(event):
    """IPubSuccess subscriber: run the follow-up after a request that undid.

    Runs after the publisher's commit, in transactions of its own.  Never
    raises: an exception here would turn the finished request into an
    error, and a ConflictError into a retry that runs the undo again.
    """
    if not getattr(_local, "followup_pending", False):
        return
    _local.followup_pending = False
    try:
        _drain(event.request)
    except Exception:
        log.exception("undo follow-up failed; the queued rows stay hidden")
        try:
            transaction.abort()
        except Exception:
            log.debug("abort after failed undo follow-up failed", exc_info=True)


# ---------------------------------------------------------------------------
# Startup checks
# ---------------------------------------------------------------------------


def check_hook_support(storage):
    """Warn when *storage* can undo but zodb-pgjsonb has no ``undone()`` hook.

    The version pin only binds at install time (see
    ``maintenance._check_storage_fixed``).  Returns True when undo is
    supported and covered by the hook.
    """
    supports_undo = getattr(storage, "supportsUndo", None)
    if supports_undo is None or supports_undo() is not True:
        return False
    installed = _dist_version("zodb-pgjsonb")
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", installed)
    if match is not None and tuple(map(int, match.groups())) >= _PGJSONB_UNDO_HOOK:
        return True
    log.warning(
        "zodb-pgjsonb %s has no undone() hook (needs >= 1.18.0). Undo on this "
        "history-preserving storage leaves the catalog data of the undone "
        "transaction in place, including security data, until a Clear and "
        "Rebuild (#265).",
        installed,
    )
    return False


def warn_if_queue_not_empty(dsn):
    """Log a warning when rows hidden by an undo still wait for their follow-up.

    Startup does not drain: several processes would do it at once, and it
    needs a site.  Returns the count, or None when it could not be read.
    """
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            (present,) = conn.execute(
                "SELECT to_regclass('pgcatalog_undo_queue') IS NOT NULL"
            ).fetchone()
            if not present:
                return 0
            (count,) = conn.execute(
                "SELECT count(*) FROM pgcatalog_undo_queue"
            ).fetchone()
    except Exception:
        log.debug("Could not read the undo follow-up queue", exc_info=True)
        return None
    if count:
        log.warning(
            "%d catalog rows hidden by an undo still wait for their follow-up. "
            "Run it from the ZMI (portal_catalog, Catalog tab), with "
            "plone.pgcatalog.undo.process_queue() or with "
            "maintenance.repair_uncataloged().",
            count,
        )
    return count
