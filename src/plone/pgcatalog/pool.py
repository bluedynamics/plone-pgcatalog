"""Connection pool discovery and request-scoped connection reuse.

The PostgreSQL connection pool is discovered from:
1. The ZODB storage's pool (if using zodb-pgjsonb)
2. Environment variable PGCATALOG_DSN (creates a fallback pool)

Request-scoped connection reuse avoids pool lock overhead for pages
with multiple catalog queries within a single Zope request.
"""

from plone.pgcatalog.pending import _local

import contextlib
import logging
import os


__all__ = [
    "discard_request_connection",
    "get_dsn",
    "get_pool",
    "get_request_connection",
    "get_storage_connection",
    "release_request_connection",
]


log = logging.getLogger(__name__)


_fallback_pool = None


def _install_orjson_loader():
    """Register orjson as psycopg's JSONB deserializer if available."""
    try:
        from psycopg.types.json import set_json_loads

        import orjson

        set_json_loads(orjson.loads)
    except ImportError:
        pass


_install_orjson_loader()


def get_request_connection(pool):
    """Get or create a request-scoped connection from the pool.

    Reuses the same connection for the duration of a Zope request,
    avoiding pool lock overhead for pages with multiple catalog queries.
    The connection is returned to the pool by ``release_request_connection()``,
    which is called by the IPubEnd subscriber at request end.

    A request connection the server closed meanwhile (failover,
    ``pg_terminate_backend``, idle timeout) is returned to its pool before
    a new one is checked out, so its slot is freed (#246).

    Note:
        If IPubEnd does not fire (e.g. SIGTERM, middleware crash), the
        thread-local connection is never returned to the pool, and its
        slot stays counted until the process ends: psycopg-pool's
        ``max_idle`` only applies to connections idle *in* the pool.
    """
    conn = getattr(_local, "pgcat_conn", None)
    if conn is not None and not conn.closed:
        return conn
    if conn is not None:
        discard_request_connection()
    conn = pool.getconn()
    _local.pgcat_conn = conn
    _local.pgcat_pool = pool
    return conn


def release_request_connection(event=None):
    """Return the request-scoped connection to the pool.

    Called by the IPubEnd subscriber at the end of each Zope request.
    Safe to call when no request-scoped connection is active (no-op).

    Issues an explicit ``conn.rollback()`` before pooling so the
    connection comes back idle (not 'idle in transaction').  This
    prevents the pool fallback path from leaving virtualxid locks
    that block ``CREATE INDEX CONCURRENTLY`` (#118).  ``rollback()``
    on a conn with no open transaction is a cheap no-op.
    """
    discard_request_connection()


def discard_request_connection():
    """Return the request-scoped connection to its pool and forget it.

    Clears the thread-local state first, so no path can return the same
    connection twice.  A closed connection is returned too: psycopg-pool
    discards it and frees its slot, while skipping ``putconn()`` would
    leak the slot for good (#246).
    """
    conn = getattr(_local, "pgcat_conn", None)
    pool = getattr(_local, "pgcat_pool", None)
    _local.pgcat_conn = None
    _local.pgcat_pool = None
    if conn is None or pool is None:
        return
    if not conn.closed:
        # Close any implicit txn opened by prior SELECTs.
        # Suppress exceptions — a dead conn shouldn't break pool return.
        with contextlib.suppress(Exception):
            conn.rollback()
    try:
        pool.putconn(conn)
    except Exception:
        log.warning("Failed to return connection to pool", exc_info=True)


def get_storage_connection(context):
    """Get PG connection from the ZODB storage instance.

    Returns the same connection used for ZODB object loads, so catalog
    queries see the same REPEATABLE READ snapshot.

    Args:
        context: persistent object with _p_jar (e.g. the catalog tool)

    Returns:
        psycopg connection or None if not available
    """
    try:
        return context._p_jar._storage.pg_connection
    except (AttributeError, TypeError):
        return None


def get_pool(context=None):
    """Discover the PostgreSQL connection pool.

    Args:
        context: persistent object with _p_jar (e.g. Plone site or tool)

    Returns:
        psycopg_pool.ConnectionPool

    Raises:
        RuntimeError: if no pool can be found
    """
    # 1. From ZODB storage (zodb-pgjsonb)
    if context is not None:
        pool = _pool_from_storage(context)
        if pool is not None:
            return pool

    # 2. Fallback: create pool from env var
    pool = _pool_from_env()
    if pool is not None:
        return pool

    raise RuntimeError(
        "Cannot find PG connection pool. Use zodb-pgjsonb storage or set PGCATALOG_DSN."
    )


def _pool_from_storage(context):
    """Extract connection pool from the ZODB storage backend."""
    try:
        storage = context._p_jar.db().storage
        return getattr(storage, "_instance_pool", None)
    except (AttributeError, TypeError):
        return None


def _pool_from_env():
    """Lazy-create a fallback pool from PGCATALOG_DSN env var."""
    global _fallback_pool
    if _fallback_pool is not None:
        return _fallback_pool

    dsn = os.environ.get("PGCATALOG_DSN")
    if not dsn:
        return None

    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    import atexit

    _fallback_pool = ConnectionPool(
        dsn,
        min_size=1,
        max_size=4,
        kwargs={"row_factory": dict_row},
        open=True,
    )
    atexit.register(_fallback_pool.close)
    return _fallback_pool


# Keep get_dsn for setuphandlers.py (DDL needs its own connection, not pool)
def get_dsn(context=None):
    """Discover the PostgreSQL DSN string.

    Args:
        context: persistent object with _p_jar

    Returns:
        DSN string or None
    """
    dsn = os.environ.get("PGCATALOG_DSN")
    if dsn:
        return dsn

    if context is not None:
        try:
            storage = context._p_jar.db().storage
            if hasattr(storage, "_dsn"):
                return storage._dsn
        except (AttributeError, TypeError):
            pass

    return None
