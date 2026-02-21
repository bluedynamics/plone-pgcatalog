"""Interfaces for plone.pgcatalog."""

from Products.CMFCore.interfaces import ICatalogTool
from zope.interface import Interface


__all__ = ["IPGCatalogTool", "IPGIndexTranslator"]


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

    **Security contract:**

    Implementations MUST use psycopg parameterized queries for all
    user-supplied values.  The ``query()`` method returns a raw SQL
    fragment that is appended directly to the WHERE clause — never
    interpolate user input into this fragment.  Only use ``%(name)s``
    placeholders with corresponding entries in the returned params dict.
    Index/column identifiers in the fragment should be hardcoded constants
    or validated against ``columns.validate_identifier()``.
    """

    def extract(obj, index_name):
        """Extract value(s) from obj for this index.

        Returns a dict to merge into the idx JSONB, e.g.:
            {"event_start": "2025-01-15T10:00:00", "event_end": "2025-01-20T18:00:00"}
        """

    def query(index_name, query_value, query_options):
        """Translate a ZCatalog query dict into a SQL fragment + params.

        The returned SQL fragment is inserted directly into a WHERE clause.
        All user-supplied values MUST use ``%(name)s`` parameter placeholders
        — never string-format values into the SQL.

        Returns (sql_fragment, params_dict), e.g.:
            ("pgcatalog_to_timestamptz(idx->>'event_start') <= %(drir_date)s",
             {"drir_date": query_value})
        """

    def sort(index_name):
        """Return SQL expression for ORDER BY, or None if not sortable.

        The returned expression is inserted directly into an ORDER BY clause.
        Only use hardcoded column references — never interpolate user input.

        Returns e.g.: "pgcatalog_to_timestamptz(idx->>'event_start')"
        """
