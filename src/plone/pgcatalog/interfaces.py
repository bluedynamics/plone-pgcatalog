"""Interfaces for plone.pgcatalog."""

from zope.interface import Interface


class IPGCatalogTool(Interface):
    """Marker interface for the PostgreSQL-backed catalog tool."""


class IPGIndexTranslator(Interface):
    """Adapter that translates a custom index's data for PG storage + querying.

    Add-ons register named adapters implementing this interface to support
    custom index types (e.g. DateRangeInRange, CompositeIndex).
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
