"""Standalone PG maintenance operations and ZCatalog compatibility shims.

Functions that operate directly on the PostgreSQL database without
requiring a Plone context.  Also contains the ``_CatalogCompat`` shim
and the unsupported-method factory used by ``PlonePGCatalogTool``.
"""

from Persistence import Persistent
from persistent.mapping import PersistentMapping
from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.indexing import reindex_object as _sql_reindex
from psycopg import sql as pgsql

import logging


log = logging.getLogger(__name__)


def reindex_index(conn, name):
    """Re-apply a specific idx key across all cataloged objects.

    Args:
        conn: psycopg connection
        name: index name (idx JSONB key) to refresh
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT zoid, idx FROM object_state "
            "WHERE idx IS NOT NULL AND idx ? %(key)s",
            {"key": name},
        )
        rows = cur.fetchall()

    count = 0
    for row in rows:
        value = row["idx"].get(name)
        if value is not None:
            _sql_reindex(conn, zoid=row["zoid"], idx_updates={name: value})
            count += 1

    log.info("reindex_index(%r): updated %d objects", name, count)
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


class _CatalogCompat(Persistent):
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
        return self.indexes[name]


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
