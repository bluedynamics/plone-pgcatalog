"""Interfaces for plone.pgcatalog."""

from Products.CMFCore.interfaces import ICatalogTool
from zope.interface import Interface


class IPGCatalogTool(ICatalogTool):
    """Interface for the PostgreSQL-backed catalog tool."""


class IPGIndexTranslator(Interface):
    """Named utility that translates a custom index's data for PG storage + querying.

    Add-ons register named utilities implementing this interface to support
    custom index types not covered by ``META_TYPE_MAP`` (e.g. DateRangeInRange,
    CompositeIndex).  The utility name must match the index name.

    Wired into:
    - ``query.py``: ``_process_index()`` and ``_process_sort()`` use it as
      a fallback when the index is not in the ``IndexRegistry``.
    - ``catalog.py``: ``_extract_from_translators()`` calls ``extract()``
      for all registered translators during indexing.
    """

    def extract(obj, index_name):
        """Extract value(s) from obj for this index.

        Returns a dict to merge into the idx JSONB, e.g.:
            {"event_start": "2025-01-15T10:00:00", "event_end": "2025-01-20T18:00:00"}
        """

    def query(index_name, query_value, query_options):
        """Translate a ZCatalog query dict into a SQL fragment + params.

        Returns (sql_fragment, params_dict), e.g.:
            ("pgcatalog_to_timestamptz(idx->>'event_start') <= %(drir_date)s",
             {"drir_date": query_value})
        """

    def sort(index_name):
        """Return SQL expression for ORDER BY, or None if not sortable.

        Returns e.g.: "pgcatalog_to_timestamptz(idx->>'event_start')"
        """
