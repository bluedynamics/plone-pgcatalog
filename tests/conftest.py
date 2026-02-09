"""Shared test configuration for plone.pgcatalog tests."""

from plone.pgcatalog.schema import install_catalog_schema
from psycopg.rows import dict_row
from psycopg.types.json import Json
from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

import os
import psycopg
import pytest


# Allow DSN override via environment variable for CI.
# Default: local Docker on port 5433 (development setup).
DSN = os.environ.get(
    "ZODB_TEST_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)

TABLES_TO_DROP = (
    "DROP TABLE IF EXISTS blob_state, object_state, transaction_log CASCADE"
)


@pytest.fixture
def pg_conn():
    """Fresh database connection with base zodb-pgjsonb schema."""
    c = psycopg.connect(DSN, row_factory=dict_row)
    with c.cursor() as cur:
        cur.execute(TABLES_TO_DROP)
    c.commit()
    # Install the base object_state table (from zodb-pgjsonb)
    c.execute(HISTORY_FREE_SCHEMA)
    c.commit()
    yield c
    c.close()


@pytest.fixture
def pg_conn_with_catalog(pg_conn):
    """Database connection with base schema + catalog extension."""
    install_catalog_schema(pg_conn)
    pg_conn.commit()
    return pg_conn


def insert_object(conn, zoid, tid=1, class_mod="myapp", class_name="Doc", state=None):
    """Insert a bare object_state row for testing.

    Creates a transaction_log entry if needed, then inserts the object.
    Returns the zoid.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transaction_log (tid) VALUES (%(tid)s) ON CONFLICT DO NOTHING",
            {"tid": tid},
        )
        cur.execute(
            """
            INSERT INTO object_state
                (zoid, tid, class_mod, class_name, state, state_size)
            VALUES (%(zoid)s, %(tid)s, %(mod)s, %(cls)s, %(state)s, %(size)s)
            ON CONFLICT (zoid) DO UPDATE SET
                tid = %(tid)s, state = %(state)s, state_size = %(size)s
            """,
            {
                "zoid": zoid,
                "tid": tid,
                "mod": class_mod,
                "cls": class_name,
                "state": Json(state or {}),
                "size": len(str(state or {})),
            },
        )
    conn.commit()
    return zoid
