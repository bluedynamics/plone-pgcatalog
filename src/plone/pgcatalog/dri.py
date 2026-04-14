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
from plone.pgcatalog.columns import ensure_date_param as _ensure_date_param
from plone.pgcatalog.columns import validate_identifier
from plone.pgcatalog.interfaces import IPGIndexTranslator
from zope.interface import implementer

import logging
import re


# RFC 5545 RRULE validation pattern: must start with a valid FREQ value.
# Accepts both full "RRULE:FREQ=..." and bare "FREQ=..." formats (Plone
# commonly stores bare format via plone.formwidget.recurrence).
_RRULE_PATTERN = re.compile(
    r"^(RRULE:)?FREQ=(YEARLY|MONTHLY|WEEKLY|DAILY|HOURLY|MINUTELY|SECONDLY)",
    re.IGNORECASE,
)

# Maximum allowed RRULE string length (prevent excessive PL/pgSQL parsing)
_MAX_RRULE_LENGTH = 1000


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


def _resolve_via_indexer(obj, attr_name):
    """Look up *attr_name* through the ``plone.indexer`` ``IIndexer`` chain.

    Computed Plone indexes (``general_end``, ``general_start`` from
    ``plone.app.event`` / ``bda.aaf.site``, etc.) are virtual attributes
    registered as named ``IIndexer`` adapters — they never exist as
    Python attributes on the content object.  Relying on
    ``IIndexableObjectWrapper.__getattr__`` to trigger them is fragile
    (the wrapper needs to be adapted correctly *and* the adapter needs
    to be reachable via the current component registry), so the
    translator resolves the adapter explicitly and falls back to
    attribute access only if nothing is registered.

    Returns ``None`` when plone.indexer is unavailable, when no catalog
    can be determined, when no named ``IIndexer`` is registered, or
    when the adapter call fails.
    """
    if not attr_name:
        return None
    try:
        from plone.indexer.interfaces import IIndexableObjectWrapper
        from plone.indexer.interfaces import IIndexer
        from zope.component import queryMultiAdapter
    except ImportError:
        return None

    raw = obj
    catalog = None
    if IIndexableObjectWrapper.providedBy(obj):
        # Reach through the wrapper so the adapter sees the raw object,
        # exactly like the ZCatalog indexing path.  Name-mangled attrs
        # are a stable part of plone.indexer's ABI.
        raw = getattr(obj, "_IndexableObjectWrapper__object", obj)
        catalog = getattr(obj, "_IndexableObjectWrapper__catalog", None)
    if catalog is None:
        try:
            from Products.CMFCore.utils import getToolByName

            catalog = getToolByName(obj, "portal_catalog", None)
        except Exception:
            catalog = None
    if catalog is None:
        return None

    indexer = queryMultiAdapter((raw, catalog), IIndexer, name=attr_name)
    if indexer is None:
        return None
    try:
        return indexer()
    except Exception:
        log.warning(
            "IIndexer %r raised while extracting for %r",
            attr_name,
            raw,
            exc_info=True,
        )
        return None


def _extract_attr(obj, attr_name):
    """Resolve *attr_name* via ``IIndexer`` first, then attribute access."""
    val = _resolve_via_indexer(obj, attr_name)
    if val is not None:
        return val
    return _safe_getattr(obj, attr_name)


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
        validate_identifier(date_attr)
        self.date_attr = date_attr
        self.recurdef_attr = recurdef_attr
        self.until_attr = until_attr

    def extract(self, obj, index_name):
        """Extract base date and recurrence rule from obj."""
        date_val = _extract_attr(obj, self.date_attr)
        if date_val is None:
            return {}

        result = {index_name: convert_value(date_val)}

        recurdef = _extract_attr(obj, self.recurdef_attr)
        if recurdef:
            recurdef = str(recurdef)
            if len(recurdef) > _MAX_RRULE_LENGTH:
                log.warning(
                    "RRULE too long (%d chars), ignoring recurrence for %s",
                    len(recurdef),
                    index_name,
                )
            elif not _RRULE_PATTERN.match(recurdef):
                log.warning(
                    "Invalid RRULE format %r, ignoring recurrence for %s",
                    recurdef[:50],
                    index_name,
                )
            else:
                result[f"{index_name}_recurrence"] = recurdef

        return result

    def query(self, index_name, raw, spec):
        """Translate a ZCatalog range query to SQL using rrule functions."""
        # Defensive validation: index_name comes from the component
        # architecture (ZCatalog registered name) and goes through
        # IndexRegistry validation at startup.  Belt-and-suspenders
        # check to prevent SQL injection via JSONB path expressions.
        validate_identifier(index_name)

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
        """Sort by base date.

        Note: index_name is validated in query() which is always
        called before sort() in the query pipeline.
        """
        return f"pgcatalog_to_timestamptz(idx->>'{index_name}')"
