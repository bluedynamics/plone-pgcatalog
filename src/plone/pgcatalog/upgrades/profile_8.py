"""Profile v7 -> v8 upgrade: recompute Manager-only security rows (#291).

Children pasted or restored together with their container were indexed
with ``allowedRolesAndUsers`` of only ``Manager``.  This step recomputes
the security indexes of all rows whose ``allowed_roles`` is exactly ``Manager``, one
partial reindex per affected row.
"""

from Acquisition import aq_parent
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.maintenance import manager_only_paths
from plone.pgcatalog.maintenance import reindex_security

import logging


log = logging.getLogger(__name__)


def recompute_manager_only_security(context):
    """Recompute the security indexes of Manager-only rows.

    Accepts either shape GenericSetup hands to upgrade handlers (the
    portal_setup tool or an ImportContext).  No-ops when the active
    catalog is not the PG tool.  Idempotent.
    """
    getSite = getattr(context, "getSite", None)
    site = getSite() if getSite is not None else aq_parent(context)
    if site is None:
        log.warning("recompute_manager_only_security: cannot resolve site; skipping")
        return

    catalog = getattr(site, "portal_catalog", None)
    if catalog is None or not IPGCatalogTool.providedBy(catalog):
        log.info("recompute_manager_only_security: PG catalog not active; skipping")
        return

    with catalog._pg_connection() as conn:
        paths = manager_only_paths(conn)
        conn.commit()
    if not paths:
        log.info("recompute_manager_only_security: no Manager-only rows; skipping")
        return

    result = reindex_security(catalog, site, paths=paths)
    log.info(
        "recompute_manager_only_security: %d recomputed, %d failed",
        result.checked,
        len(result.failed),
    )
