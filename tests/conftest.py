"""Shared test configuration for plone.pgcatalog tests."""

from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.schema import install_catalog_schema
from psycopg.rows import dict_row
from psycopg.types.json import Json
from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

import os
import psycopg
import pytest


# Standard Plone indexes — used to populate the registry for tests.
# In production, these come from ZCatalog via GenericSetup catalog.xml imports.
_PLONE_DEFAULT_INDEXES = [
    # (name, IndexType, idx_key)
    # FieldIndex
    ("Creator", IndexType.FIELD, "Creator"),
    ("Type", IndexType.FIELD, "Type"),
    ("getId", IndexType.FIELD, "getId"),
    ("id", IndexType.FIELD, "id"),
    ("in_reply_to", IndexType.FIELD, "in_reply_to"),
    ("portal_type", IndexType.FIELD, "portal_type"),
    ("review_state", IndexType.FIELD, "review_state"),
    ("sortable_title", IndexType.FIELD, "sortable_title"),
    # KeywordIndex
    ("Subject", IndexType.KEYWORD, "Subject"),
    ("allowedRolesAndUsers", IndexType.KEYWORD, "allowedRolesAndUsers"),
    ("getRawRelatedItems", IndexType.KEYWORD, "getRawRelatedItems"),
    ("object_provides", IndexType.KEYWORD, "object_provides"),
    # DateIndex
    ("Date", IndexType.DATE, "Date"),
    ("created", IndexType.DATE, "created"),
    ("effective", IndexType.DATE, "effective"),
    ("end", IndexType.DATE, "end"),
    ("expires", IndexType.DATE, "expires"),
    ("modified", IndexType.DATE, "modified"),
    ("start", IndexType.DATE, "start"),
    # BooleanIndex
    ("is_default_page", IndexType.BOOLEAN, "is_default_page"),
    ("is_folderish", IndexType.BOOLEAN, "is_folderish"),
    ("exclude_from_nav", IndexType.BOOLEAN, "exclude_from_nav"),
    # DateRangeIndex (composite — idx_key=None)
    ("effectiveRange", IndexType.DATE_RANGE, None),
    # UUIDIndex
    ("UID", IndexType.UUID, "UID"),
    # FieldIndex (language — from plone.app.multilingual)
    ("Language", IndexType.FIELD, "Language"),
    # ZCTextIndex
    ("SearchableText", IndexType.TEXT, None),
    ("Title", IndexType.TEXT, "Title"),
    ("Description", IndexType.TEXT, "Description"),
    # ExtendedPathIndex (idx_key=None for built-in "path")
    ("path", IndexType.PATH, None),
    # Additional ExtendedPathIndex (idx_key=name, stored in idx JSONB)
    ("tgpath", IndexType.PATH, "tgpath"),
    # GopipIndex
    ("getObjPositionInParent", IndexType.GOPIP, "getObjPositionInParent"),
]

_PLONE_DEFAULT_METADATA = [
    "CreationDate",
    "EffectiveDate",
    "ExpirationDate",
    "ModificationDate",
    "getIcon",
    "getObjSize",
    "getRemoteUrl",
    "image_scales",
    "listCreators",
    "location",
    "mime_type",
]


@pytest.fixture(autouse=True, scope="session")
def populated_registry():
    """Populate the global index registry with standard Plone indexes.

    In production, this is done by sync_from_catalog() reading from the
    ZCatalog's registered indexes.  In tests, we register them directly.
    """
    registry = get_registry()
    for name, idx_type, idx_key in _PLONE_DEFAULT_INDEXES:
        registry.register(name, idx_type, idx_key)
    for name in _PLONE_DEFAULT_METADATA:
        registry.add_metadata(name)
    return registry


# Allow DSN override via environment variable for CI.
# Default: local Docker on port 5433 (development setup).
DSN = os.environ.get(
    "ZODB_TEST_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)

# BM25 tests need vchord_bm25 + pg_tokenizer extensions.
# Default: vchord-suite container on port 5434.
BM25_DSN = os.environ.get(
    "BM25_TEST_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5434",
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
