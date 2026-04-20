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
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.query import _bool_to_lower_str
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
                {"key": self._idx_key, "val": _bool_to_lower_str(value)},
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

    def __init__(self, wrapped, idx_key, get_conn, index_type=None):
        self._wrapped = wrapped
        self._idx_key = idx_key
        self._index_type = index_type
        self._pg_index = _PGIndexMapping(idx_key, get_conn)

    @property
    def _index(self):
        return self._pg_index

    def uniqueValues(self, name=None, withLengths=False):
        """Return the distinct values of this index.

        For ``IndexType.KEYWORD`` the JSONB value is a *list* of tags,
        so the implementation expands the array with
        ``jsonb_array_elements_text`` — otherwise ``idx->>key`` coerces
        the array to its JSON text representation and callers (the
        querybuilder vocabulary, tag-cloud widgets, ...) see entries
        like ``'["a","b"]'`` instead of ``'a'`` and ``'b'``.  See #143.

        A defensive ``CASE jsonb_typeof = 'array'`` branch keeps the
        query alive if a single row holds a scalar under the same
        keyword key (legacy/corrupted data).
        """
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
        params = {"key": key}

        if self._index_type == IndexType.KEYWORD:
            # ``jsonb_array_elements_text`` is a set-returning function
            # and can't appear inside a ``CASE`` expression (Postgres
            # raises 0A000), so the defensive array/scalar split is
            # expressed as a ``UNION ALL`` subquery.  Array rows expand
            # one row per element; a legacy scalar row yields itself.
            inner = (
                "SELECT jsonb_array_elements_text(idx->%(key)s) AS val "
                "  FROM object_state "
                "  WHERE idx ? %(key)s "
                "    AND jsonb_typeof(idx->%(key)s) = 'array' "
                "UNION ALL "
                "SELECT idx->>%(key)s AS val "
                "  FROM object_state "
                "  WHERE idx ? %(key)s "
                "    AND jsonb_typeof(idx->%(key)s) NOT IN ('array', 'null')"
            )
            distinct_sql = f"SELECT DISTINCT val FROM ({inner}) u WHERE val IS NOT NULL"
            grouped_sql = (
                f"SELECT val, COUNT(*) AS cnt FROM ({inner}) u "
                "WHERE val IS NOT NULL GROUP BY val"
            )
        else:
            distinct_sql = (
                "SELECT DISTINCT idx->>%(key)s AS val "
                "FROM object_state "
                "WHERE idx ? %(key)s AND idx->>%(key)s IS NOT NULL"
            )
            grouped_sql = (
                "SELECT idx->>%(key)s AS val, COUNT(*) AS cnt "
                "FROM object_state "
                "WHERE idx ? %(key)s AND idx->>%(key)s IS NOT NULL "
                "GROUP BY 1"
            )

        with conn.cursor() as cur:
            if not withLengths:
                cur.execute(distinct_sql, params)
                for row in cur.fetchall():
                    yield row["val"]
            else:
                cur.execute(grouped_sql, params)
                for row in cur.fetchall():
                    yield (row["val"], row["cnt"])

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def _maybe_wrap_index(catalog, name, raw_index):
    """Wrap *raw_index* with ``PGIndex`` if *catalog* is a PG catalog.

    Returns the raw index unchanged when:

    - The raw index is ``None``.
    - The catalog is not an ``IPGCatalogTool``.
    - The index is registered with ``idx_key=None`` (special indexes
      like SearchableText, path, effectiveRange — they have dedicated
      columns and don't need PG-backed JSONB wrapping).
    """
    if raw_index is None:
        return None

    # The non-PG-catalog path is primarily a defensive guard for tests
    # and loose dependencies.  In a normal Plone install where this
    # package is active, every tool goes through IPGCatalogTool.
    if not IPGCatalogTool.providedBy(catalog):
        return raw_index

    registry = get_registry()
    entry = registry.get(name)
    index_type = None
    if entry is not None:
        index_type = entry[0]
        idx_key = entry[1]  # (IndexType, idx_key, source_attrs)
        if idx_key is None:
            return raw_index  # Special index — no wrapping needed
    else:
        idx_key = name  # Fallback: use index name as JSONB key

    return PGIndex(
        raw_index,
        idx_key,
        catalog._get_pg_read_connection,
        index_type=index_type,
    )


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

        return _maybe_wrap_index(catalog, id, index)
