"""Configuration and DSN discovery for plone.pgcatalog.

The PG connection string (DSN) is discovered from:
1. Environment variable PGCATALOG_DSN (highest priority)
2. The ZODB storage's DSN (if using zodb-pgjsonb)
3. Explicit DSN passed to PGCatalogTool constructor
"""

import os


def get_dsn(site=None):
    """Discover the PostgreSQL DSN.

    Args:
        site: optional Plone site object (for storage-based discovery)

    Returns:
        DSN string or None
    """
    # 1. Environment variable
    dsn = os.environ.get("PGCATALOG_DSN")
    if dsn:
        return dsn

    # 2. From ZODB storage (zodb-pgjsonb)
    if site is not None:
        dsn = _dsn_from_storage(site)
        if dsn:
            return dsn

    return None


def _dsn_from_storage(site):
    """Extract DSN from the ZODB storage backend."""
    try:
        db = site._p_jar.db()
        storage = db.storage
        if hasattr(storage, "_dsn"):
            return storage._dsn
    except (AttributeError, TypeError):
        pass
    return None
