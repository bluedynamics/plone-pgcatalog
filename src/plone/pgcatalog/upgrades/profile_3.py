"""Profile v2 -> v3 upgrade: heal stale getObjPositionInParent ranks.

Before the gopip resync subscriber existed (#216), any reordering left
stale position snapshots behind, and folder_contents drag & drop aborts
on the resulting order mismatch — affected folders cannot heal through
the UI.  This step runs ``maintenance.resync_gopip`` once so all ordered
folders match their ZODB ordering again.
"""

from Acquisition import aq_parent
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.maintenance import resync_gopip

import logging


log = logging.getLogger(__name__)


def resync_gopip_ranks(context):
    """Resync stored getObjPositionInParent ranks with the ZODB orderings.

    Accepts either shape GenericSetup hands to upgrade handlers (the
    portal_setup tool or an ImportContext).  No-ops when the active
    catalog is not the PG tool.
    """
    getSite = getattr(context, "getSite", None)
    site = getSite() if getSite is not None else aq_parent(context)
    if site is None:
        log.warning("resync_gopip_ranks: cannot resolve site; skipping")
        return

    catalog = getattr(site, "portal_catalog", None)
    if catalog is None or not IPGCatalogTool.providedBy(catalog):
        log.info("resync_gopip_ranks: PG catalog not active; skipping")
        return

    root = site.getPhysicalRoot()
    with catalog._pg_connection() as conn:
        folders, rows = resync_gopip(root, conn)
    log.info("resync_gopip_ranks: healed %d folders (%d rank rows)", folders, rows)
