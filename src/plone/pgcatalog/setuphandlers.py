"""GenericSetup install handler for plone.pgcatalog.

Installs the catalog schema extension (columns + indexes) on the
object_state table when the GenericSetup profile is applied.

The DDL uses autocommit + lock_timeout to avoid blocking on the
storage's REPEATABLE READ transactions.
"""

from plone.pgcatalog.schema import CATALOG_COLUMNS
from plone.pgcatalog.schema import CATALOG_FUNCTIONS
from plone.pgcatalog.schema import CATALOG_INDEXES

import logging


log = logging.getLogger(__name__)


def install(context):
    """GenericSetup import step: install PG catalog schema.

    Reads the PG connection from the ZODB storage (zodb-pgjsonb)
    and applies ALTER TABLE statements to add catalog columns + indexes.
    Uses autocommit + lock_timeout to avoid deadlock with REPEATABLE READ.
    """
    if context.readDataFile("install_pgcatalog.txt") is None:
        return

    site = context.getSite()
    conn = _get_pg_connection(site)
    if conn is None:
        log.warning("Could not get PG connection — schema not installed")
        return

    try:
        _install_schema_safe(conn)
    finally:
        conn.close()


def _install_schema_safe(conn):
    """Apply catalog DDL with error handling per statement.

    Each DDL block is executed individually.  IF NOT EXISTS / CREATE OR
    REPLACE make every statement idempotent, so partial success is safe.
    """
    for label, sql in [
        ("columns", CATALOG_COLUMNS),
        ("functions", CATALOG_FUNCTIONS),
        ("indexes", CATALOG_INDEXES),
    ]:
        try:
            conn.execute(sql)
            log.info("plone.pgcatalog DDL: %s applied", label)
        except Exception:
            log.warning(
                "plone.pgcatalog DDL: %s failed (may need manual apply)",
                label,
                exc_info=True,
            )


def _get_pg_connection(site):
    """Get a psycopg connection from the ZODB storage.

    Uses autocommit mode + lock_timeout to avoid blocking on the
    storage's existing REPEATABLE READ transactions.
    """
    try:
        db = site._p_jar.db()
        storage = db.storage
        # PGJsonbStorage exposes _dsn
        if hasattr(storage, "_dsn"):
            import psycopg

            conn = psycopg.connect(storage._dsn, autocommit=True)
            conn.execute("SET lock_timeout = '3s'")
            return conn
    except Exception:
        log.debug("Failed to get PG connection from storage", exc_info=True)

    return None
