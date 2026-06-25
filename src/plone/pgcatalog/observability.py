"""plone.observability IMetricProvider producing content counts via SQL.

This module is imported only when the conditional ZCML registration fires
(``zcml:condition="installed plone.observability"``), so importing
plone.observability at module top level is safe.
"""

from plone.observability.metric import Metric

import logging
import os
import threading


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
