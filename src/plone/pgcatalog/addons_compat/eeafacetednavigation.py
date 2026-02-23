"""PG-backed IFacetedCatalog adapter for eea.facetednavigation.

Overrides the default FacetedCatalog to query PostgreSQL directly via the
``object_state.idx`` JSONB column instead of ZCatalog BTree ``_apply_index()``.

When the active catalog is NOT an IPGCatalogTool, falls back to the default
BTree-based implementation so non-PG sites continue to work.
"""

from eea.facetednavigation.search.catalog import FacetedCatalog
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.interfaces import IPGIndexTranslator
from plone.pgcatalog.pool import get_pool
from plone.pgcatalog.pool import get_request_connection
from Products.CMFCore.utils import getToolByName
from psycopg.types.json import Json
from zope.component import queryUtility

import logging


log = logging.getLogger(__name__)


def _normalize_value(value):
    """Extract the effective query value from a ZCatalog query spec.

    If *value* is a dict with a ``query`` key, return the inner value.
    Otherwise return *value* unchanged.
    """
    if isinstance(value, dict):
        return value.get("query", value)
    return value


def _pg_apply_index(conn, index_id, index, value):
    """Query PostgreSQL for ZOIDs matching *index_id* = *value*.

    Dispatches by ``IndexType`` from the ``IndexRegistry``.

    Returns a ``frozenset`` of integer ZOIDs (empty on unknown/unsupported).
    """
    value = _normalize_value(value)

    registry = get_registry()
    entry = registry.get(index_id)

    if entry is not None:
        idx_type, idx_key, _source_attrs = entry
        if idx_key is None:
            # Special indexes (SearchableText, effectiveRange, path) are not
            # handled here -- fall through to empty result.
            return frozenset()
        return _dispatch_by_type(conn, idx_type, idx_key, value)

    # Fallback: IPGIndexTranslator utility
    translator = queryUtility(IPGIndexTranslator, name=index_id)
    if translator is not None:
        return _query_via_translator(conn, translator, index_id, value)

    return frozenset()


def _dispatch_by_type(conn, idx_type, idx_key, value):
    """Run the appropriate SQL for the given IndexType."""

    if idx_type in (IndexType.FIELD, IndexType.GOPIP, IndexType.UUID):
        return _query_jsonb_contains(conn, idx_key, str(value))

    if idx_type == IndexType.KEYWORD:
        return _query_keyword(conn, idx_key, value)

    if idx_type == IndexType.BOOLEAN:
        return _query_jsonb_contains(conn, idx_key, bool(value))

    if idx_type == IndexType.DATE:
        return _query_jsonb_contains(conn, idx_key, str(value))

    # DATE_RANGE, TEXT, PATH -- not supported in faceted single-index apply
    return frozenset()


# -- SQL helpers -------------------------------------------------------------


def _query_jsonb_contains(conn, idx_key, typed_value):
    """``idx @> '{"key": value}'::jsonb``"""
    sql = "SELECT zoid FROM object_state WHERE idx @> %(match)s::jsonb"
    with conn.cursor() as cur:
        cur.execute(sql, {"match": Json({idx_key: typed_value})})
        return frozenset(row["zoid"] for row in cur.fetchall())


def _query_keyword(conn, idx_key, value):
    """KeywordIndex: containment (single) or overlap (multi)."""
    if isinstance(value, (list, tuple)):
        sql = "SELECT zoid FROM object_state WHERE idx->%(key)s ?| %(vals)s::text[]"
        with conn.cursor() as cur:
            cur.execute(sql, {"key": idx_key, "vals": [str(v) for v in value]})
            return frozenset(row["zoid"] for row in cur.fetchall())
    else:
        return _query_jsonb_contains(conn, idx_key, [value])


def _query_via_translator(conn, translator, index_id, value):
    """Use an IPGIndexTranslator to build the WHERE fragment."""
    sql_fragment, params = translator.query(index_id, value, {})
    sql = f"SELECT zoid FROM object_state WHERE {sql_fragment}"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return frozenset(row["zoid"] for row in cur.fetchall())


# -- Adapter -----------------------------------------------------------------


class PGFacetedCatalog(FacetedCatalog):
    """IFacetedCatalog that queries PG instead of BTree ``_apply_index()``.

    Inherits ``__call__()`` from ``FacetedCatalog`` so query merging and
    event firing remain unchanged.
    """

    def apply_index(self, context, index, value):
        # If the catalog is not PG-backed, delegate to the original impl.
        catalog = getToolByName(context, "portal_catalog", None)
        if catalog is None or not IPGCatalogTool.providedBy(catalog):
            return super().apply_index(context, index, value)

        index_id = index.getId()

        try:
            pool = get_pool(catalog)
            conn = get_request_connection(pool)
            result = _pg_apply_index(conn, index_id, index, value)
        except Exception:
            log.exception(
                "PG query failed for faceted index %r, falling back to BTree",
                index_id,
            )
            return super().apply_index(context, index, value)

        return result, (index_id,)
