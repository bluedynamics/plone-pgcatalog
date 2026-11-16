"""Profile v6 -> v7 upgrade: uncatalog rows left behind by deletes (#290).

A delete could reindex an already detached object and write a catalog
row with the bare id as ``path`` over the uncatalog of the same
transaction.  This step runs ``maintenance.remove_detached_rows`` once, a
single SQL statement.
"""

from Acquisition import aq_parent
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.maintenance import remove_detached_rows

import logging


log = logging.getLogger(__name__)


def uncatalog_detached_rows(context):
    """Uncatalog rows whose path is not rooted.

    Accepts either shape GenericSetup hands to upgrade handlers (the
    portal_setup tool or an ImportContext).  No-ops when the active
    catalog is not the PG tool.
    """
    getSite = getattr(context, "getSite", None)
    site = getSite() if getSite is not None else aq_parent(context)
    if site is None:
        log.warning("uncatalog_detached_rows: cannot resolve site; skipping")
        return

    catalog = getattr(site, "portal_catalog", None)
    if catalog is None or not IPGCatalogTool.providedBy(catalog):
        log.info("uncatalog_detached_rows: PG catalog not active; skipping")
        return

    with catalog._pg_connection() as conn:
        count = remove_detached_rows(conn)
        conn.commit()
    log.info("uncatalog_detached_rows: uncataloged %d rows", count)
