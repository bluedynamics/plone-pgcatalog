"""DateRangeInRangeIndex translator for plone.pgcatalog.

Translates Products.DateRangeInRangeIndex overlap queries to SQL.
The original addon is a pure proxy over two existing date indexes
(start + end) and stores no data itself — pgcatalog replaces it
with a single SQL WHERE clause.

Query format (from the addon):
    catalog({'my_idx': {'start': datetime1, 'end': datetime2}})

Semantics:
    Find objects whose date range [obj_start, obj_end] overlaps with
    the query range [q_start, q_end].

Non-recurring case:
    obj_start <= q_end AND obj_end >= q_start

Recurring case (underlying start index is DateRecurringIndex):
    Any recurrence occurrence d satisfies d <= q_end AND d + duration >= q_start,
    where duration = base_end - base_start.  Simplified: is there an occurrence
    in [q_start - duration, q_end]?  Uses rrule."between"() for expansion.
"""

from plone.pgcatalog.columns import ensure_date_param as _ensure_date_param
from plone.pgcatalog.columns import validate_identifier
from plone.pgcatalog.interfaces import IPGIndexTranslator
from zope.interface import implementer

import logging


log = logging.getLogger(__name__)


@implementer(IPGIndexTranslator)
class DateRangeInRangeIndexTranslator:
    """IPGIndexTranslator for Products.DateRangeInRangeIndex.

    Pure query translator — stores no data.  The underlying DateIndex
    or DateRecurringIndex translators handle extraction into idx JSONB.

    Args:
        startindex: name of the underlying start date index
        endindex:   name of the underlying end date index
    """

    def __init__(self, startindex, endindex):
        validate_identifier(startindex)
        validate_identifier(endindex)
        self.startindex = startindex
        self.endindex = endindex

    def extract(self, obj, index_name):
        """No-op: underlying indexes handle extraction."""
        return {}

    def query(self, index_name, raw, spec):
        """Translate overlap query to SQL.

        The query dict has 'start' and 'end' keys (not 'query'/'range').
        """
        validate_identifier(index_name)

        q_start = raw.get("start")
        q_end = raw.get("end")

        if q_start is None or q_end is None:
            return ("TRUE", {})

        q_start = _ensure_date_param(q_start)
        q_end = _ensure_date_param(q_end)

        start_expr = f"pgcatalog_to_timestamptz(idx->>'{self.startindex}')"
        end_expr = f"pgcatalog_to_timestamptz(idx->>'{self.endindex}')"
        recurrence_key = f"{self.startindex}_recurrence"
        has_recurrence = (
            f"idx->>'{recurrence_key}' IS NOT NULL AND idx->>'{recurrence_key}' != ''"
        )

        p_start = f"driri_{index_name}_start"
        p_end = f"driri_{index_name}_end"

        # Non-recurring: classic overlap test
        simple_sql = (
            f"({start_expr} <= %({p_end})s::timestamptz"
            f" AND {end_expr} >= %({p_start})s::timestamptz)"
        )

        # Recurring: check if any occurrence overlaps the query range.
        # An occurrence at time d has range [d, d + duration].
        # Overlap with [q_start, q_end] iff d <= q_end AND d + duration >= q_start
        # iff d in [q_start - duration, q_end].
        # duration = base_end - base_start (constant for all occurrences).
        recurring_sql = (
            f"EXISTS ("
            f'SELECT 1 FROM rrule."between"('
            f"idx->>'{recurrence_key}', {start_expr},"
            f" %({p_start})s::timestamptz - ({end_expr} - {start_expr}),"
            f" %({p_end})s::timestamptz))"
        )

        sql = f"(CASE WHEN {has_recurrence} THEN {recurring_sql} ELSE {simple_sql} END)"
        return (sql, {p_start: q_start, p_end: q_end})

    def sort(self, index_name):
        """Sort by start date of the underlying start index."""
        return f"pgcatalog_to_timestamptz(idx->>'{self.startindex}')"
