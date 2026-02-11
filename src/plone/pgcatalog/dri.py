"""DateRecurringIndex translator for plone.pgcatalog.

Translates Products.DateRecurringIndex queries to SQL using
rrule_plpgsql (pure PL/pgSQL RRULE implementation) for query-time
recurrence expansion.

Storage in idx JSONB:
    {index_name}:               ISO 8601 base date (e.g. "start")
    {index_name}_recurrence:    RFC 5545 RRULE string (if recurring)

Query strategy:
    - Non-recurring events: simple timestamptz comparison (like DateIndex)
    - Recurring events: rrule."between"() / rrule."after"() for range queries
"""

from plone.pgcatalog.columns import convert_value
from plone.pgcatalog.interfaces import IPGIndexTranslator
from zope.interface import implementer

import logging


log = logging.getLogger(__name__)


def _safe_getattr(obj, name):
    """Get attribute value, calling it if callable."""
    if not name:
        return None
    val = getattr(obj, name, None)
    if val is not None and callable(val):
        try:
            val = val()
        except Exception:
            return None
    return val


def _ensure_date_param(value):
    """Convert a date-like value to something psycopg can bind as timestamptz."""
    from datetime import date
    from datetime import datetime

    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    # Zope DateTime
    if hasattr(value, "asdatetime"):
        return value.asdatetime()
    if hasattr(value, "ISO8601"):
        return value.ISO8601()
    return str(value)


@implementer(IPGIndexTranslator)
class DateRecurringIndexTranslator:
    """IPGIndexTranslator for Products.DateRecurringIndex.

    Stores base date + RRULE string in idx JSONB.  Queries use
    rrule_plpgsql functions for recurrence expansion at query time.

    Args:
        date_attr:     object attribute for the base date (= index name)
        recurdef_attr: object attribute for the RRULE string
        until_attr:    object attribute for the until date (rarely used)
    """

    def __init__(self, date_attr, recurdef_attr, until_attr=""):
        from plone.pgcatalog.columns import validate_identifier

        validate_identifier(date_attr)
        self.date_attr = date_attr
        self.recurdef_attr = recurdef_attr
        self.until_attr = until_attr

    def extract(self, obj, index_name):
        """Extract base date and recurrence rule from obj."""
        date_val = _safe_getattr(obj, self.date_attr)
        if date_val is None:
            return {}

        result = {index_name: convert_value(date_val)}

        recurdef = _safe_getattr(obj, self.recurdef_attr)
        if recurdef:
            result[f"{index_name}_recurrence"] = str(recurdef)

        return result

    def query(self, index_name, raw, spec):
        """Translate a ZCatalog range query to SQL using rrule functions."""
        query_val = spec.get("query")
        range_spec = spec.get("range")

        if query_val is None:
            return ("TRUE", {})

        recurrence_key = f"{index_name}_recurrence"
        date_expr = f"pgcatalog_to_timestamptz(idx->>'{index_name}')"
        has_recurrence = (
            f"idx->>'{recurrence_key}' IS NOT NULL AND idx->>'{recurrence_key}' != ''"
        )

        if range_spec in ("min:max", "minmax") and isinstance(query_val, (list, tuple)):
            min_val = _ensure_date_param(query_val[0])
            max_val = _ensure_date_param(query_val[1])
            p_min = f"dri_{index_name}_min"
            p_max = f"dri_{index_name}_max"

            sql = (
                f"(CASE WHEN {has_recurrence}"
                f" THEN EXISTS ("
                f'SELECT 1 FROM rrule."between"('
                f"idx->>'{recurrence_key}', {date_expr},"
                f" %({p_min})s::timestamptz, %({p_max})s::timestamptz))"
                f" ELSE {date_expr}"
                f" BETWEEN %({p_min})s::timestamptz AND %({p_max})s::timestamptz"
                f" END)"
            )
            return (sql, {p_min: min_val, p_max: max_val})

        elif range_spec == "min":
            val = _ensure_date_param(query_val)
            p = f"dri_{index_name}_min"

            sql = (
                f"(CASE WHEN {has_recurrence}"
                f" THEN EXISTS ("
                f'SELECT 1 FROM rrule."after"('
                f"idx->>'{recurrence_key}', {date_expr},"
                f" %({p})s::timestamptz, 1))"
                f" ELSE {date_expr} >= %({p})s::timestamptz"
                f" END)"
            )
            return (sql, {p: val})

        elif range_spec == "max":
            # For max: if the base date <= query, the event qualifies.
            # Recurrence only adds later occurrences, so base date check
            # is sufficient for both recurring and non-recurring.
            val = _ensure_date_param(query_val)
            p = f"dri_{index_name}_max"
            sql = f"{date_expr} <= %({p})s::timestamptz"
            return (sql, {p: val})

        else:
            # Exact date match
            val = _ensure_date_param(query_val)
            p = f"dri_{index_name}_exact"

            sql = (
                f"(CASE WHEN {has_recurrence}"
                f" THEN EXISTS ("
                f'SELECT 1 FROM rrule."between"('
                f"idx->>'{recurrence_key}', {date_expr},"
                f" %({p})s::timestamptz, %({p})s::timestamptz))"
                f" ELSE {date_expr} = %({p})s::timestamptz"
                f" END)"
            )
            return (sql, {p: val})

    def sort(self, index_name):
        """Sort by base date."""
        return f"pgcatalog_to_timestamptz(idx->>'{index_name}')"
