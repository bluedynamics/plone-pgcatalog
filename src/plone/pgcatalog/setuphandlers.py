"""GenericSetup install handler for plone.pgcatalog.

Installs the catalog schema extension (columns + indexes) on the
object_state table when the GenericSetup profile is applied.
"""

from plone.pgcatalog.schema import install_catalog_schema

import logging


log = logging.getLogger(__name__)


def install(context):
    """GenericSetup import step: install PG catalog schema.

    Reads the PG connection from the ZODB storage (zodb-pgjsonb)
    and applies ALTER TABLE statements to add catalog columns + indexes.
    """
    if context.readDataFile("install_pgcatalog.txt") is None:
        return

    site = context.getSite()
    conn = _get_pg_connection(site)
    if conn is None:
        log.warning("Could not get PG connection — schema not installed")
        return

    install_catalog_schema(conn)
    conn.commit()
    log.info("plone.pgcatalog: catalog schema installed on object_state")


def _get_pg_connection(site):
    """Get a psycopg connection from the ZODB storage.

    Walks the ZODB DB → storage chain to find a PGJsonbStorage
    and borrows its connection pool.
    """
    try:
        db = site._p_jar.db()
        storage = db.storage
        # PGJsonbStorage exposes _dsn and _pool
        if hasattr(storage, "_dsn"):
            from psycopg.rows import dict_row

            import psycopg

            return psycopg.connect(storage._dsn, row_factory=dict_row)
    except Exception:
        log.debug("Failed to get PG connection from storage", exc_info=True)

    return None
