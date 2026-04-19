"""Standalone PG maintenance operations and ZCatalog compatibility shims.

Functions that operate directly on the PostgreSQL database without
requiring a Plone context.  Also contains the ``_CatalogCompat`` shim
and the unsupported-method factory used by ``PlonePGCatalogTool``.
"""

from Acquisition import aq_inner
from Acquisition import aq_parent
from Acquisition import Implicit
from Persistence import Persistent
from persistent.mapping import PersistentMapping
from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.indexing import reindex_object as _sql_reindex
from plone.pgcatalog.pgindex import _maybe_wrap_index
from psycopg import sql as pgsql

import logging


log = logging.getLogger(__name__)


_REINDEX_BATCH_SIZE = 500


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


# ---------------------------------------------------------------------------
# _CatalogCompat: minimal shim for ZCatalogIndexes and addons
# ---------------------------------------------------------------------------


class _CatalogIndexesView:
    """Transient dict-like view over ``_CatalogCompat._raw_indexes``
    that wraps each index with ``PGIndex`` on read-through access.

    Built fresh from ``_CatalogCompat.indexes`` on every attribute access
    and NEVER persisted.  Mutations pass through to the raw mapping
    unchanged (they write raw ZCatalog index objects, as the upstream
    catalog.py / setuphandlers.py code expects).

    Finds the catalog via acquisition from the _CatalogCompat instance
    that built the view — no ``api.portal.get_tool`` needed.  When no
    catalog is reachable (e.g. during tests or bootstrap), falls back
    to returning the raw index.
    """

    __slots__ = ("_compat", "_raw")

    def __init__(self, compat, raw):
        self._compat = compat
        self._raw = raw

    # read-through access → wrapped
    def __getitem__(self, key):
        raw_index = self._raw[key]  # raises KeyError
        catalog = aq_parent(aq_inner(self._compat))
        if catalog is None:
            return raw_index
        return _maybe_wrap_index(catalog, key, raw_index)

    def get(self, key, default=None):
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
        for key in self._raw:
            yield self[key]

    def items(self):
        for key in self._raw:
            yield (key, self[key])

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
        """
        return _CatalogIndexesView(self, self._raw_indexes)

    def getIndex(self, name):
        """Return a PG-backed index wrapper for *name*.

        Mirrors ``self.indexes[name]`` but implemented directly on the
        method so that when invoked through the Acquisition wrapper
        (``tool._catalog.getIndex("x")``), ``self`` is the wrapper and
        ``aq_parent(aq_inner(self))`` can reach the catalog — useful
        for legacy objects that don't yet carry ``__parent__``.

        Raises ``KeyError`` if *name* is not a known index.
        """
        raw_index = self._raw_indexes[name]  # raises KeyError if missing
        catalog = aq_parent(aq_inner(self))
        if catalog is None:
            return raw_index
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
