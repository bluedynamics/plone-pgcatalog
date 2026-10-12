"""Profile v5 -> v6 upgrade: repair stale allowedRolesAndUsers (#271).

Since the move optimization (1.0.0b21), a cross-container move left the
descendants of the moved folder with the ``allowedRolesAndUsers`` of their
old location.  This step runs ``maintenance.reindex_security`` once, a
partial reindex of the security indexes only.
"""

from Acquisition import aq_parent
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.maintenance import reindex_security

import logging


log = logging.getLogger(__name__)


def reindex_security_indexes(context):
    """Reindex allowedRolesAndUsers for all cataloged objects.

    Accepts either shape GenericSetup hands to upgrade handlers (the
    portal_setup tool or an ImportContext).  No-ops when the active
    catalog is not the PG tool.
    """
    getSite = getattr(context, "getSite", None)
    site = getSite() if getSite is not None else aq_parent(context)
    if site is None:
        log.warning("reindex_security_indexes: cannot resolve site; skipping")
        return

    catalog = getattr(site, "portal_catalog", None)
    if catalog is None or not IPGCatalogTool.providedBy(catalog):
        log.info("reindex_security_indexes: PG catalog not active; skipping")
        return

    result = reindex_security(catalog, site)
    log.info(
        "reindex_security_indexes: %d reindexed, %d failed",
        result.checked,
        len(result.failed),
    )
