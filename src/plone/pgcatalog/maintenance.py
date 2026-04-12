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


class _CatalogCompat(Implicit, Persistent):
    """Minimal _catalog providing index object storage.

    ZCatalogIndexes._getOb() reads aq_parent(self)._catalog.indexes.
    eea.facetednavigation reads _catalog.getIndex(name).
    This shim provides just enough API for both.

    For existing ZODB instances the old full Catalog object persists
    and already has .indexes and .schema — our code only reads those
    attrs, so it works without migration.
    """

    def __init__(self):
        self.indexes = PersistentMapping()
        self.schema = PersistentMapping()

    def getIndex(self, name):
        """Return a PG-backed index wrapper for *name*.

        Plone code that bypasses ``catalog.Indexes[name]`` (notably
        ``plone.app.vocabularies.KeywordsVocabulary``,
        ``Products.CMFPlone.browser.search.Search.types_list``, and
        ``plone.app.event.setuphandlers``) accesses indexes via
        ``catalog._catalog.getIndex(name)``.  Returning the raw
        ZCatalog index would give those callers empty BTrees, so we
        wrap with ``PGIndex`` — same as ``catalog.Indexes[name]``.

        Raises ``KeyError`` if *name* is not a known index.
        """
        raw_index = self.indexes[name]  # raises KeyError if missing

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
