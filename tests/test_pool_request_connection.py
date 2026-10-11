"""Request-scoped connections broken server-side go back to the pool (#246).

Uses a real psycopg-pool with a single slot: a connection that is dropped
without ``putconn()`` keeps that slot counted forever, so the next
``getconn()`` times out.  Mocks cannot show this, they accept any call.
"""

from plone.pgcatalog.catalog import PlonePGCatalogTool
from plone.pgcatalog.pending import _local
from plone.pgcatalog.pool import get_request_connection
from plone.pgcatalog.pool import release_request_connection
from psycopg_pool import ConnectionPool
from tests.conftest import DSN

import plone.pgcatalog.catalog as catalog_mod
import psycopg
import pytest


@pytest.fixture
def one_slot_pool():
    pool = ConnectionPool(
        DSN, min_size=1, max_size=1, timeout=2, kwargs={"autocommit": True}
    )
    pool.open(wait=True)
    _local.pgcat_conn = None
    _local.pgcat_pool = None
    yield pool
    _local.pgcat_conn = None
    _local.pgcat_pool = None
    pool.close()


def _kill(conn):
    """Terminate *conn*'s backend, as a failover would, and let it notice."""
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
    with pytest.raises(psycopg.OperationalError):
        conn.execute("SELECT 1")
    assert conn.closed


def _assert_slot_free(pool):
    conn = pool.getconn(timeout=1)
    conn.execute("SELECT 1")
    pool.putconn(conn)


def test_release_returns_a_killed_connection(one_slot_pool):
    _kill(get_request_connection(one_slot_pool))

    release_request_connection()

    assert _local.pgcat_conn is None
    _assert_slot_free(one_slot_pool)


def test_replacing_a_killed_request_connection_frees_its_slot(one_slot_pool):
    killed = get_request_connection(one_slot_pool)
    _kill(killed)

    fresh = get_request_connection(one_slot_pool)

    assert fresh is not killed
    fresh.execute("SELECT 1")
    release_request_connection()
    _assert_slot_free(one_slot_pool)


def test_pg_connection_skips_and_returns_a_killed_request_connection(
    one_slot_pool, monkeypatch
):
    monkeypatch.setattr(catalog_mod, "get_pool", lambda context=None: one_slot_pool)
    tool = PlonePGCatalogTool()
    _kill(get_request_connection(one_slot_pool))

    with tool._pg_connection() as conn:
        conn.execute("SELECT 1")

    release_request_connection()
    _assert_slot_free(one_slot_pool)
