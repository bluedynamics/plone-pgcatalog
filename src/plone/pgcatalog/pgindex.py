"""PG-backed index proxies for ZCatalog internal API compatibility.

Plone code (plone.app.uuid, plone.app.vocabularies, plone.app.dexterity)
accesses ZCatalog internal data structures directly:

- ``catalog.Indexes["UID"]._index.get(uuid)`` → record ID lookup
- ``catalog.Indexes["portal_type"].uniqueValues(withLengths=True)``
- ``catalog.getpath(rid)`` / ``catalog.getrid(path)``

Since PlonePGCatalogTool never populates ZCatalog's BTrees, these calls
return empty results.  This module provides PG-backed substitutes:

- ``PGIndex`` wraps a real ZCatalog index and overrides ``_index``
  and ``uniqueValues()`` with PostgreSQL queries on the ``idx`` JSONB.
- ``PGCatalogIndexes`` replaces the ``Indexes`` container so that
  ``catalog.Indexes[name]`` returns a ``PGIndex`` wrapper.

Uses ZOID (the PostgreSQL integer primary key) as the record ID,
matching ``getpath()``/``getrid()`` on the catalog.
"""

from Acquisition import aq_inner
from Acquisition import aq_parent
from Products.ZCatalog.ZCatalogIndexes import ZCatalogIndexes

import logging


log = logging.getLogger(__name__)

_marker = []


class _PGIndexMapping:
    """Dict-like object backing ``PGIndex._index``.

    Translates ``_index.get(value)`` into a PG query on the ``idx``
    JSONB column.  Returns ZOID as the record ID.
    """

    __slots__ = ("_get_conn", "_idx_key")

    def __init__(self, idx_key, get_conn):
        self._idx_key = idx_key
        self._get_conn = get_conn

    def get(self, value, default=None):
        try:
            conn = self._get_conn()
        except Exception:
            return default
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state WHERE idx->>%(key)s = %(val)s LIMIT 1",
                {"key": self._idx_key, "val": str(value)},
            )
            row = cur.fetchone()
        return row["zoid"] if row else default

    def __contains__(self, value):
        return self.get(value) is not None

    def keys(self):
        try:
            conn = self._get_conn()
        except Exception:
            return []
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT idx->>%(key)s AS val FROM object_state "
                "WHERE idx ? %(key)s AND idx->>%(key)s IS NOT NULL",
                {"key": self._idx_key},
            )
            return [row["val"] for row in cur.fetchall()]


class PGIndex:
    """Proxy wrapping a ZCatalog index with PG-backed data access.

    Delegates all standard index interface methods to the wrapped
    index.  Overrides ``_index`` (property) and ``uniqueValues()``
    with PostgreSQL queries on the ``idx`` JSONB column.
    """

    def __init__(self, wrapped, idx_key, get_conn):
        self._wrapped = wrapped
        self._idx_key = idx_key
        self._pg_index = _PGIndexMapping(idx_key, get_conn)

    @property
    def _index(self):
        return self._pg_index

    def uniqueValues(self, name=None, withLengths=False):
        index_id = getattr(self._wrapped, "id", self._idx_key)
        if name is None:
            name = index_id
        elif name != index_id:
            return

        try:
            conn = self._pg_index._get_conn()
        except Exception:
            return

        key = self._idx_key
        with conn.cursor() as cur:
            if not withLengths:
                cur.execute(
                    "SELECT DISTINCT idx->>%(key)s AS val "
                    "FROM object_state "
                    "WHERE idx ? %(key)s AND idx->>%(key)s IS NOT NULL",
                    {"key": key},
                )
                for row in cur.fetchall():
                    yield row["val"]
            else:
                cur.execute(
                    "SELECT idx->>%(key)s AS val, COUNT(*) AS cnt "
                    "FROM object_state "
                    "WHERE idx ? %(key)s AND idx->>%(key)s IS NOT NULL "
                    "GROUP BY 1",
                    {"key": key},
                )
                for row in cur.fetchall():
                    yield (row["val"], row["cnt"])

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class PGCatalogIndexes(ZCatalogIndexes):
    """ZCatalogIndexes replacement that wraps indexes with PGIndex.

    When code accesses ``catalog.Indexes[name]``, this returns a
    ``PGIndex`` proxy instead of the raw ZCatalog index object.
    Special indexes (SearchableText, path, effectiveRange) with
    ``idx_key=None`` are returned unwrapped.
    """

    def _getOb(self, id, default=_marker):  # noqa: A002
        index = super()._getOb(id, default)
        if index is None or (default is not _marker and index is default):
            return index

        catalog = aq_parent(aq_inner(self))
        if catalog is None:
            return index

        # Only wrap for PG catalogs
        from plone.pgcatalog.interfaces import IPGCatalogTool

        if not IPGCatalogTool.providedBy(catalog):
            return index

        # Look up the JSONB key from IndexRegistry
        from plone.pgcatalog.columns import get_registry

        registry = get_registry()
        entry = registry.get(id)
        if entry is not None:
            idx_key = entry[1]  # (IndexType, idx_key, source_attrs)
            if idx_key is None:
                return index  # Special index — no wrapping needed
        else:
            idx_key = id  # Fallback: use index name as JSONB key

        return PGIndex(index, idx_key, catalog._get_pg_read_connection)
