"""Shared test configuration for plone.pgcatalog tests."""

import os

from psycopg.rows import dict_row
from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

import psycopg
import pytest

from plone.pgcatalog.schema import install_catalog_schema


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
