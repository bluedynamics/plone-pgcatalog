"""plone.observability IMetricProvider producing content counts via SQL.

Registered in ``overrides.zcml`` under plone.observability's own adapter
name ``content``, so it replaces the generic ``ContentMetricProvider``
instead of running next to it (#285).  The generic provider works on
pgcatalog too (``PGIndex.uniqueValues``), so both would emit the same
series, and it counts the whole ``object_state`` table for every site.

This module is imported only when the conditional ZCML registration fires
(``zcml:condition="installed plone.observability"``), so importing
plone.observability at module top level is safe.
"""

from plone.base.interfaces import IPloneSiteRoot
from plone.observability.interfaces import IMetricProvider
from plone.observability.metric import Metric
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.pool import get_pool
from zope.interface import implementer

import logging
import os
import threading
import time


logger = logging.getLogger(__name__)

# Process-global TTL cache. Key: (pg site ids, ZCatalog site ids), each a
# sorted tuple -> (timestamp, [Metric]).
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


_ZCATALOG_INDEXES = (
    (
        "portal_type",
        "plone_content_total",
        "portal_type",
        "Number of content objects by portal type",
    ),
    (
        "review_state",
        "plone_content_by_state",
        "state",
        "Number of content objects by workflow state",
    ),
)


def _zcatalog_metrics(site_id, catalog):
    """Content counts of a ZCatalog-backed site, via the public index API.

    The provider replaces plone.observability's generic one, so it has to
    cover these sites itself.
    """
    metrics = []
    for index, name, label, help_text in _ZCATALOG_INDEXES:
        try:
            values = list(catalog.Indexes[index].uniqueValues(withLengths=True))
        except Exception:
            logger.debug("%s index not available in %s", index, site_id)
            continue
        for value, count in values:
            metrics.append(
                Metric(
                    name=name,
                    value=count,
                    type="gauge",
                    scope="global",
                    help=help_text,
                    labels={label: value, "site": site_id},
                )
            )
    return metrics


def _classify_sites(context):
    """Split the Plone sites in *context* by catalog backend.

    Returns ``(pg_site_ids, zcatalog_sites)``: the ids of sites whose
    portal_catalog provides IPGCatalogTool, and ``(site_id, catalog)``
    pairs for the rest.  During a migration pgcatalog may be installed
    while a site still uses a ZCatalog.
    """
    pg_site_ids = []
    zcatalog_sites = []
    for obj in context.objectValues():
        if not IPloneSiteRoot.providedBy(obj):
            continue
        try:
            catalog = obj.unrestrictedTraverse("portal_catalog", None)
        except Exception:
            catalog = None
        if catalog is None:
            continue
        if IPGCatalogTool.providedBy(catalog):
            pg_site_ids.append(obj.getId())
        else:
            zcatalog_sites.append((obj.getId(), catalog))
    return pg_site_ids, zcatalog_sites


@implementer(IMetricProvider)
class PGContentMetricProvider:
    """Content-count metrics: SQL for IPGCatalogTool-backed sites, the
    index API for ZCatalog-backed ones."""

    name = "content"
    scope = "global"

    def __init__(self, context):
        self.context = context

    def collect(self):
        pg_site_ids, zcatalog_sites = _classify_sites(self.context)
        if not pg_site_ids and not zcatalog_sites:
            return
        key = (
            tuple(sorted(pg_site_ids)),
            tuple(sorted(site_id for site_id, _ in zcatalog_sites)),
        )
        now = time.time()
        with _cache_lock:
            entry = _cache.get(key)
            if entry is not None and (now - entry[0]) < _ttl():
                yield from entry[1]
                return
        metrics = []
        complete = True
        if pg_site_ids:
            try:
                pool = get_pool(self.context)
                with pool.connection() as conn:
                    metrics.extend(_aggregate(conn, list(key[0])))
            except Exception:
                logger.debug("pgcatalog content metrics unavailable", exc_info=True)
                complete = False
        for site_id, catalog in zcatalog_sites:
            metrics.extend(_zcatalog_metrics(site_id, catalog))
        if complete:
            # A failed SQL round is retried on the next scrape, not cached.
            with _cache_lock:
                _cache[key] = (now, metrics)
        yield from metrics
