"""Standalone PG maintenance operations and ZCatalog compatibility shims.

Functions that operate directly on the PostgreSQL database without
requiring a Plone context.  Also contains the ``_CatalogCompat`` shim
and the unsupported-method factory used by ``PlonePGCatalogTool``.
"""

from Acquisition import aq_base
from Acquisition import aq_inner
from Acquisition import aq_parent
from Acquisition import Implicit
from importlib.metadata import version as _dist_version
from Persistence import Persistent
from persistent.mapping import PersistentMapping
from plone.folder.interfaces import IExplicitOrdering
from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.gopip import sync_folder_ranks
from plone.pgcatalog.indexing import reindex_object as _sql_reindex
from plone.pgcatalog.pgindex import _maybe_wrap_index
from plone.pgcatalog.pool import get_pool
from psycopg import sql as pgsql
from typing import NamedTuple
from ZODB.POSException import ConflictError
from ZODB.utils import u64
from zope.component.hooks import getSite
from zope.component.hooks import site as site_context

import importlib
import logging
import re
import transaction


log = logging.getLogger(__name__)


_REINDEX_BATCH_SIZE = 500
_REBUILD_BATCH = 500  # commit + cache-minimize every N objects during rebuild


def _commit_and_minimize(jar):
    """Commit the current transaction and minimize the ZODB cache.

    Committing flushes dirty objects to storage and clears the
    thread-local pending catalog data, allowing ``cacheMinimize()``
    to actually ghost them and reclaim memory.
    """
    transaction.commit()
    if jar is not None:
        jar.cacheMinimize()


def reindex_index(conn, name, batch_size=_REINDEX_BATCH_SIZE):
    """Re-apply a specific idx key across all cataloged objects.

    Uses server-side cursor with batched updates for memory efficiency
    on large catalogs.

    Args:
        conn: psycopg connection
        name: index name (idx JSONB key) to refresh
        batch_size: number of rows per batch (default 500)
    """
    count = 0
    with conn.cursor(name="reindex_cursor") as cur:
        cur.itersize = batch_size
        cur.execute(
            "SELECT zoid, idx FROM object_state "
            "WHERE idx IS NOT NULL AND idx ? %(key)s",
            {"key": name},
        )
        batch = cur.fetchmany(batch_size)
        while batch:
            for row in batch:
                value = row["idx"].get(name)
                if value is not None:
                    _sql_reindex(conn, zoid=row["zoid"], idx_updates={name: value})
                    count += 1
            log.info("reindex_index(%r): processed %d objects so far", name, count)
            batch = cur.fetchmany(batch_size)

    log.info("reindex_index(%r): updated %d objects total", name, count)
    return count


def remove_acquired_uids(conn):
    """Strip acquisition-inherited ``UID`` values from idx (#205).

    Before the ``wrap_object`` fix, non-ICatalogAware objects (a
    subsite's ``robots.txt``, tools, FTIs, workflow definitions, …)
    stored their *container's* UID in ``idx->>'UID'``, making UID
    lookups ambiguous.  Such rows are identified by having a ``UID`` in
    idx while their pickled state carries no own ``_plone.uuid``
    attribute — the plone.uuid ownership marker.

    One-shot data repair for existing databases; new writes are safe
    since the extraction fix.  Returns the number of repaired rows.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE object_state SET idx = idx - 'UID' "
            "WHERE idx ? 'UID' AND state->>'_plone.uuid' IS NULL"
        )
        count = cur.rowcount

    log.info("remove_acquired_uids: stripped acquired UID from %d rows", count)
    return count


def clear_catalog_data(conn):
    """Clear all catalog data (path, idx, searchable_text, and backend extras).

    The base object_state rows are preserved.
    """
    extra_nulls = get_backend().uncatalog_extra()
    # Use psycopg.sql.Identifier for safe column name quoting
    extra_parts = [
        pgsql.SQL(", {} = NULL").format(pgsql.Identifier(col)) for col in extra_nulls
    ]
    extra_sql = pgsql.SQL("").join(extra_parts)

    base_sql = pgsql.SQL(
        "UPDATE object_state SET "
        "path = NULL, parent_path = NULL, path_depth = NULL, "
        "idx = NULL, searchable_text = NULL"
    )
    query = pgsql.SQL("{base}{extra} WHERE idx IS NOT NULL").format(
        base=base_sql, extra=extra_sql
    )

    with conn.cursor() as cur:
        cur.execute(query)
        count = cur.rowcount

    log.info("clear_catalog_data: cleared %d objects", count)
    return count


def _traverse(base, path):
    """Simplified fast unrestricted traverse (same as plone.folder.nogopip).

    base: object to start from (usually the app root)
    path: absolute path as string
    returns: content at the end or None
    """
    current = base
    for cid in path.split("/"):
        if not cid:
            continue
        try:
            current = current[cid]
        except (AttributeError, KeyError, TypeError):
            return None
    return current


def resync_gopip(root, conn):
    """Heal stale getObjPositionInParent ranks for every ordered folder.

    Snapshots written before the gopip resync subscriber existed (#216)
    may be stale, and folder_contents drag & drop aborts on the resulting
    order mismatch — so affected folders cannot even heal through the UI.

    Walks every parent_path that has cataloged children, reads the
    container's ordering from ZODB (annotations only, the children stay
    ghosts), and applies the minimal rank diff.  Folders whose stored
    order is already correct cost one read and zero writes.

    Args:
        root: the Zope application root
        conn: psycopg connection (autocommit, e.g. from the pool)

    Returns:
        (folders_updated, rows_updated)
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT parent_path FROM object_state "
            "WHERE idx IS NOT NULL AND parent_path IS NOT NULL"
        )
        parent_paths = [row["parent_path"] for row in cur.fetchall()]

    folders = rows = 0
    for parent_path in sorted(parent_paths):
        container = _traverse(root, parent_path)
        if container is None:
            continue
        if getattr(aq_base(container), "getOrdering", None) is None:
            continue
        ordering = container.getOrdering()
        if not IExplicitOrdering.providedBy(ordering):
            continue
        ordered_ids = ordering.idsInOrder()
        if not ordered_ids:
            continue
        with conn.cursor() as cur:
            updated = sync_folder_ranks(cur, parent_path, ordered_ids)
        if updated:
            folders += 1
            rows += updated
            log.info("resync_gopip: %s — %d rank rows updated", parent_path, updated)

    log.info("resync_gopip: %d folders updated, %d rank rows total", folders, rows)
    return folders, rows


_PGJSONB_FIXED = (1, 17, 0)  # zodb-pgjsonb#120: plain writes no longer wipe columns


def _check_storage_fixed():
    """Refuse to repair against a zodb-pgjsonb that wipes catalog columns.

    The version pin in pyproject.toml only binds at install time; a build
    with --no-deps or overridden constraints can still run the old storage,
    which would uncatalog repaired objects again on their next plain write.
    """
    installed = _dist_version("zodb-pgjsonb")
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", installed)
    if match is None or tuple(map(int, match.groups())) < _PGJSONB_FIXED:
        raise RuntimeError(
            f"zodb-pgjsonb {installed} still wipes catalog columns on plain "
            "writes (zodb-pgjsonb#120); upgrade to >= 1.17.0 before repairing"
        )


def _commit_batch(jar, done, broken, redo):
    """Commit a repaired batch; on a write conflict abort and redo it once.

    *done* and *broken* are the first attempt's results; ``redo()`` repeats
    the batch (including any health check, since a concurrent editor may
    have recataloged meanwhile) and returns new ones.  Returns the results
    of the attempt that committed, or ``([], all paths)`` after a second
    conflict.
    """
    try:
        _commit_and_minimize(jar)
        return done, broken
    except ConflictError:
        transaction.abort()
        log.info("conflict on batch commit; retrying the batch once")
    done, broken = redo()
    try:
        _commit_and_minimize(jar)
        return done, broken
    except ConflictError:
        transaction.abort()
        log.warning("batch conflicted twice; %d paths reported as failed", len(done))
        return [], broken + done


def _catalogable_class(class_mod, class_name):
    """True when instances of the class could be cataloged (or unknown)."""
    try:
        cls = getattr(importlib.import_module(class_mod), class_name)
    except Exception:
        return True  # unknown or broken class: count it, the walk decides
    return callable(getattr(cls, "reindexObject", None))


def count_uncataloged_candidates(conn):
    """Upper bound for what :func:`repair_uncataloged` can find.

    Counts rows without catalog data whose class has ``reindexObject``.
    Deleted but unpacked objects and catalog-aware objects Plone never
    catalogs are included, so a positive count does not prove damage.
    Zero proves there is none, and the site walk can be skipped.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT class_mod, class_name, count(*) AS n FROM object_state "
            "WHERE idx IS NULL OR path IS NULL GROUP BY class_mod, class_name"
        )
        rows = cur.fetchall()
    return sum(
        row["n"]
        for row in rows
        if _catalogable_class(row["class_mod"], row["class_name"])
    )


class RepairResult(NamedTuple):
    """Outcome of :func:`repair_uncataloged`."""

    checked: int  # catalogable objects visited
    paths: list[str]  # recataloged paths (in a dry run: would be recataloged)
    failed: list[str]  # paths whose catalog_object() raised


def _is_catalogable(obj, catalog):
    """Plone's own rebuild criterion: CMF-catalog-aware, never the catalog."""
    base = aq_base(obj)
    if base is aq_base(catalog):
        return False
    return callable(getattr(base, "reindexObject", None))


def _healthy_zoids(conn, zoids):
    """Return the subset of *zoids* whose rows carry catalog data."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT zoid FROM object_state WHERE zoid = ANY(%s) "
            "AND path IS NOT NULL AND idx IS NOT NULL",
            (zoids,),
        )
        return {row["zoid"] for row in cur.fetchall()}


def _repair_batch(catalog, conn, batch, dry_run, all_objects):
    """Recatalog the damaged entries of *batch*; return (paths, failed)."""
    if all_objects:
        todo = batch
    else:
        healthy = _healthy_zoids(conn, [zoid for zoid, _, _ in batch])
        todo = [entry for entry in batch if entry[0] not in healthy]
    paths, failed = [], []
    for _, obj, path in todo:
        if dry_run:
            paths.append(path)
            continue
        try:
            catalog.catalog_object(obj, path)
        except Exception:
            log.warning("repair_uncataloged: failed to catalog %s", path, exc_info=True)
            failed.append(path)
        else:
            paths.append(path)
    return paths, failed


def repair_uncataloged(
    catalog, site, *, dry_run=False, all_objects=False, batch_size=_REBUILD_BATCH
):
    """Recatalog content whose catalog columns are missing, without clearing.

    Repairs rows wiped by zodb-pgjsonb#120: the object exists in the ZODB,
    but ``path`` or ``idx`` is NULL, so it is invisible to catalog queries
    and partial reindexes skip it.  Walks *site* like
    ``clearFindAndRebuild()`` but only touches catalog-aware objects whose
    row lacks catalog data (or every catalog-aware object with
    ``all_objects=True``).  Commits every *batch_size* objects; idempotent,
    so an interrupted run is simply repeated.  With ``dry_run=True`` it
    only reports and writes nothing.

    Returns:
        RepairResult(checked, paths, failed)
    """
    _check_storage_fixed()
    jar = catalog._p_jar
    checked = 0
    paths, failed = [], []
    batch = []

    def flush():
        nonlocal checked, batch
        current, batch = batch, []
        checked += len(current)
        done, broken = _repair_batch(catalog, conn, current, dry_run, all_objects)
        if dry_run:
            jar.cacheMinimize()
        else:
            done, broken = _commit_batch(
                jar,
                done,
                broken,
                lambda: _repair_batch(catalog, conn, current, False, all_objects),
            )
        paths.extend(done)
        failed.extend(broken)

    pool = get_pool(catalog)
    conn = pool.getconn()
    try:
        with site_context(site):
            for obj, path in catalog._walk_site_paths(site):
                oid = getattr(aq_base(obj), "_p_oid", None)
                if oid is None or not _is_catalogable(obj, catalog):
                    continue
                batch.append((u64(oid), obj, path))
                if len(batch) >= batch_size:
                    flush()
            if batch:
                flush()
        if paths and not dry_run:
            # Bulk writes leave the planner statistics stale (#224).
            with conn.cursor() as cur:
                cur.execute("ANALYZE object_state")
            log.info("repair_uncataloged: ANALYZE object_state done")
    finally:
        pool.putconn(conn)

    log.info(
        "repair_uncataloged: %d checked, %d %s, %d failed",
        checked,
        len(paths),
        "would be recataloged" if dry_run else "recataloged",
        len(failed),
    )
    return RepairResult(checked, paths, failed)


# ---------------------------------------------------------------------------
# _CatalogCompat: minimal shim for ZCatalogIndexes and addons
# ---------------------------------------------------------------------------


def _resolve_catalog(compat):
    """Find the PlonePGCatalogTool that owns this _CatalogCompat.

    Tries three resolution paths in order:

    1. ``__parent__`` set on the bare instance (by the v1->v2 migration
       step or by the ``indexes`` property's self-heal).
    2. Acquisition chain — ``aq_parent(aq_inner(compat))`` — when the
       compat is reached via an Acquisition wrapper and some parent in
       the chain is the catalog tool.
    3. ``zope.component.hooks.getSite().portal_catalog`` — works during
       request handling and in any code path that sets up the local
       site hook.

    Raises ``RuntimeError`` if all three fail.  **Never returns
    ``None``** — a silent ``None`` triggered the raw-index fallback
    bug that masked #143 / #146 for weeks.
    """
    parent = compat.__dict__.get("__parent__")
    if parent is not None:
        return parent
    via_aq = aq_parent(aq_inner(compat))
    if via_aq is not None:
        return via_aq
    site = getSite()
    if site is not None:
        tool = getattr(site, "portal_catalog", None)
        if tool is not None:
            return tool
    raise RuntimeError(
        "plone.pgcatalog._CatalogCompat: cannot find portal_catalog "
        "(no __parent__, no acquisition context, no getSite). "
        "_CatalogCompat is not usable outside a Plone site."
    )


class _CatalogIndexesView:
    """Transient dict-like view over ``_CatalogCompat._raw_indexes``
    that wraps each index with ``PGIndex`` on read-through access.

    Built fresh from ``_CatalogCompat.indexes`` on every attribute access
    and NEVER persisted.  Mutations pass through to the raw mapping
    unchanged (they write raw ZCatalog index objects, as the upstream
    catalog.py / setuphandlers.py code expects).

    Finds the catalog via ``_resolve_catalog(self._compat)`` which
    tries ``__parent__`` → acquisition chain → ``getSite()``.  If
    none of those three paths yield the catalog tool, a
    ``RuntimeError`` propagates out of ``__getitem__`` — the old
    silent raw-index fallback is gone (see #146).
    """

    __slots__ = ("_compat", "_raw")

    def __init__(self, compat, raw):
        self._compat = compat
        self._raw = raw

    # read-through access → wrapped
    def __getitem__(self, key):
        raw_index = self._raw[key]  # raises KeyError
        catalog = _resolve_catalog(self._compat)
        return _maybe_wrap_index(catalog, key, raw_index)

    def get(self, key, default=None):
        """Dict-style get with *default* on missing key.

        Catches only ``KeyError`` (missing index name).  A
        ``RuntimeError`` from an unreachable catalog is a configuration
        bug and must propagate — suppressing it would recreate the
        silent-empty-results failure mode #146 set out to eliminate.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        return key in self._raw

    def __iter__(self):
        return iter(self._raw)

    def __len__(self):
        return len(self._raw)

    def keys(self):
        return self._raw.keys()

    def values(self):
        # Materialize so callers can iterate twice (matches keys()'s
        # repeatable-view semantics from the underlying PersistentMapping).
        return [self[key] for key in self._raw]

    def items(self):
        return [(key, self[key]) for key in self._raw]

    # mutations → bypass wrapping, go to raw
    def __setitem__(self, key, value):
        self._raw[key] = value

    def __delitem__(self, key):
        del self._raw[key]

    def update(self, *args, **kwargs):
        self._raw.update(*args, **kwargs)

    def clear(self):
        self._raw.clear()

    def pop(self, key, *args):
        return self._raw.pop(key, *args)


class _CatalogCompat(Implicit, Persistent):
    """Minimal _catalog providing index object storage.

    ZCatalogIndexes._getOb() reads aq_parent(self)._catalog.indexes.
    eea.facetednavigation and many Plone internals read
    `catalog._catalog.indexes[name]` (and `.get(name)`, `.items()` …) directly.
    This shim provides just enough API for both — and crucially, the
    ``indexes`` attribute is a *view* that wraps each raw ZCatalog
    index with ``PGIndex``, so that direct dictionary access returns
    PG-backed results.

    Persisted state:
      _raw_indexes: PersistentMapping[str, ZCatalogIndex]   -- the real storage
      schema:       PersistentMapping[str, int]             -- metadata columns

    For existing ZODB instances the old attribute was ``indexes`` (a plain
    PersistentMapping); an upgrade step in
    ``plone.pgcatalog.upgrades.profile_2`` renames it to ``_raw_indexes``.
    """

    def __init__(self, parent=None):
        self._raw_indexes = PersistentMapping()
        self.schema = PersistentMapping()
        # Explicit parent pointer so aq_parent() works even when the
        # catalog is accessed through a plain attribute read (not
        # through an Acquisition wrapper).  Inside the ``indexes``
        # property, ``self`` is the bare instance (descriptors on
        # Implicit classes strip the Acquisition wrapper), so we rely
        # on ``__parent__`` rather than the wrapper chain.
        if parent is not None:
            self.__parent__ = parent

    @property
    def indexes(self):
        """Return a view that auto-wraps raw ZCatalog indexes with ``PGIndex``.

        The view is transient — built fresh on every access so it never
        gets pickled and never caches a stale catalog reference.
        ``aq_parent(aq_inner(self._compat))`` inside the view honors
        ``self.__parent__`` (set by ``PlonePGCatalogTool`` or by the
        profile upgrade step), so the catalog tool is reachable even
        through bare attribute access like ``tool._catalog.indexes``.

        Self-heals two forms of stale persisted state:

        1. Legacy ``indexes`` attribute (pre-b55) is renamed to
           ``_raw_indexes`` on first access.
        2. Missing ``__parent__`` attribute (prod sites where the
           v1->v2 upgrade ran before the #139 fix) is re-populated via
           ``zope.component.hooks.getSite().portal_catalog`` when
           available.  This avoids the silent raw-index fallback that
           masked #143/#146 for weeks.
        """
        state = self.__dict__
        raw = state.get("_raw_indexes")
        if raw is None:
            raw = state.pop("indexes", None)
            if raw is None:
                raw = PersistentMapping()
            state["_raw_indexes"] = raw
            self._p_changed = True
        if state.get("__parent__") is None:
            site = getSite()
            tool = getattr(site, "portal_catalog", None) if site else None
            if tool is not None:
                state["__parent__"] = tool
                self._p_changed = True
        return _CatalogIndexesView(self, raw)

    def getIndex(self, name):
        """Return a PG-backed index wrapper for *name*.

        Mirrors ``self.indexes[name]`` but implemented directly on the
        method so legacy callers (``eea.facetednavigation``,
        ``plone.app.vocabularies.Keywords``) keep working through the
        Acquisition wrapper.

        Unlike the pre-#146 implementation, this does **not** fall back
        to the raw ZCatalog index when the catalog tool is
        unreachable — a raw index has empty BTrees in pgcatalog and
        silently returns empty result sets, which masked #143/#146 for
        weeks.  Instead, ``_resolve_catalog`` raises ``RuntimeError``
        if none of its three lookup strategies finds the catalog.
        """
        raw_index = self._raw_indexes[name]  # raises KeyError if missing
        catalog = _resolve_catalog(self)
        return _maybe_wrap_index(catalog, name, raw_index)


# ---------------------------------------------------------------------------
# Unsupported ZCatalog methods → NotImplementedError
# ---------------------------------------------------------------------------

_UNSUPPORTED = {
    "getAllBrains": "Use searchResults() or direct PG queries",
    "searchAll": "Use searchResults() or direct PG queries",
    "getobject": "Use brain.getObject() instead",
    "getMetadataForUID": "Metadata is in idx JSONB — use searchResults",
    "getMetadataForRID": "Metadata is in idx JSONB — use searchResults",
    "getIndexDataForUID": "Use getIndexDataForRID(zoid) instead",
    "index_objects": "Use getIndexObjects() instead",
}


def _make_unsupported(name, msg):
    """Create a method that raises NotImplementedError."""

    def method(self, *args, **kw):
        raise NotImplementedError(
            f"PlonePGCatalogTool.{name}() is not supported. {msg}"
        )

    method.__name__ = name
    method.__doc__ = f"Not supported. {msg}"
    return method
