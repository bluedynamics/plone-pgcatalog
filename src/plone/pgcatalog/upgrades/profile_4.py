"""Profile v3 -> v4 upgrade: recatalog content wiped by zodb-pgjsonb#120.

Before zodb-pgjsonb's fix, any write of a cataloged object without a full
reindex (edit lock, sharing tab, write plus partial reindex) NULLed its
catalog columns.  Such objects are invisible to catalog queries, and
partial reindexes skip them.  This step runs
``maintenance.repair_uncataloged`` once.  The package requires the fixed
zodb-pgjsonb release, so the repaired rows cannot be wiped again.
"""

from Acquisition import aq_parent
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.maintenance import count_uncataloged_candidates
from plone.pgcatalog.maintenance import repair_uncataloged
from plone.pgcatalog.pool import get_pool

import logging


log = logging.getLogger(__name__)


def repair_uncataloged_content(context):
    """Recatalog content whose catalog columns are missing.

    Accepts either shape GenericSetup hands to upgrade handlers (the
    portal_setup tool or an ImportContext).  No-ops when the active
    catalog is not the PG tool.
    """
    getSite = getattr(context, "getSite", None)
    site = getSite() if getSite is not None else aq_parent(context)
    if site is None:
        log.warning("repair_uncataloged_content: cannot resolve site; skipping")
        return

    catalog = getattr(site, "portal_catalog", None)
    if catalog is None or not IPGCatalogTool.providedBy(catalog):
        log.info("repair_uncataloged_content: PG catalog not active; skipping")
        return

    pool = get_pool(catalog)
    conn = pool.getconn()
    try:
        candidates = count_uncataloged_candidates(conn)
    finally:
        pool.putconn(conn)
    if not candidates:
        log.info("repair_uncataloged_content: no candidates; skipping site walk")
        return

    result = repair_uncataloged(catalog, site)
    log.info(
        "repair_uncataloged_content: %d recataloged, %d failed (of %d checked)",
        len(result.paths),
        len(result.failed),
        result.checked,
    )
