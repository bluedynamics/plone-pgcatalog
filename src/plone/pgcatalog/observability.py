"""plone.observability IMetricProvider producing content counts via SQL.

This module is imported only when the conditional ZCML registration fires
(``zcml:condition="installed plone.observability"``), so importing
plone.observability at module top level is safe.
"""

from plone.observability.interfaces import IMetricProvider
from plone.observability.metric import Metric
from plone.pgcatalog.pool import get_pool
from zope.interface import implementer

import logging
import os
import threading
import time


logger = logging.getLogger(__name__)

# Process-global TTL cache. Key: tuple(sorted(site_ids)) -> (timestamp, [Metric]).
# Must be module-level: the @@metrics view builds a fresh provider instance per
# request via getAdapters(), so an instance cache would never hit across scrapes.
_cache = {}
_cache_lock = threading.Lock()


def _ttl():
    return int(os.environ.get("PLONE_OBSERVABILITY_METRICS_CACHE_TTL", "60"))


_TOTAL_SQL = (
    "SELECT split_part(path, '/', 2) AS site, idx->>'portal_type' AS pt, "
    "count(*) AS n FROM object_state "
    "WHERE idx->>'portal_type' IS NOT NULL "
    "AND split_part(path, '/', 2) = ANY(%(sites)s) "
    "GROUP BY site, pt"
)

_STATE_SQL = (
    "SELECT split_part(path, '/', 2) AS site, idx->>'review_state' AS rs, "
    "count(*) AS n FROM object_state "
    "WHERE idx->>'review_state' IS NOT NULL "
    "AND split_part(path, '/', 2) = ANY(%(sites)s) "
    "GROUP BY site, rs"
)


def _aggregate(conn, site_ids):
    """Run the two GROUP BY queries restricted to site_ids; return list[Metric]."""
    params = {"sites": list(site_ids)}
    metrics = []
    with conn.cursor() as cur:
        cur.execute(_TOTAL_SQL, params)
        for row in cur.fetchall():
            metrics.append(
                Metric(
                    name="plone_content_total",
                    value=row["n"],
                    type="gauge",
                    scope="global",
                    help="Number of content objects by portal type",
                    labels={"portal_type": row["pt"], "site": row["site"]},
                )
            )
        cur.execute(_STATE_SQL, params)
        for row in cur.fetchall():
            metrics.append(
                Metric(
                    name="plone_content_by_state",
                    value=row["n"],
                    type="gauge",
                    scope="global",
                    help="Number of content objects by workflow state",
                    labels={"state": row["rs"], "site": row["site"]},
                )
            )
    return metrics


def _pg_site_ids(context):
    """Return ids of Plone sites whose portal_catalog provides IPGCatalogTool.

    During a migration pgcatalog may be installed while a site still uses a
    ZCatalog; such sites are left to plone.observability's generic provider.
    """
    try:
        from plone.base.interfaces import IPloneSiteRoot
    except ImportError:
        return []
    from plone.pgcatalog.interfaces import IPGCatalogTool

    site_ids = []
    for obj in context.objectValues():
        if not IPloneSiteRoot.providedBy(obj):
            continue
        try:
            catalog = obj.unrestrictedTraverse("portal_catalog", None)
        except Exception:
            catalog = None
        if catalog is not None and IPGCatalogTool.providedBy(catalog):
            site_ids.append(obj.getId())
    return site_ids


@implementer(IMetricProvider)
class PGContentMetricProvider:
    """Content-count metrics via SQL for IPGCatalogTool-backed sites."""

    name = "pgcontent"
    scope = "global"

    def __init__(self, context):
        self.context = context

    def collect(self):
        site_ids = _pg_site_ids(self.context)
        if not site_ids:
            return
        key = tuple(sorted(site_ids))
        now = time.time()
        with _cache_lock:
            entry = _cache.get(key)
            if entry is not None and (now - entry[0]) < _ttl():
                yield from entry[1]
                return
        try:
            pool = get_pool(self.context)
            with pool.connection() as conn:
                metrics = _aggregate(conn, list(key))
        except Exception:
            logger.debug("pgcontent metrics unavailable", exc_info=True)
            return
        with _cache_lock:
            _cache[key] = (now, metrics)
        yield from metrics
