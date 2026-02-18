"""Integration tests for BM25 search backend (requires vchord_bm25 + pg_tokenizer).

Skipped entirely when extensions are not available.
Uses a separate DSN (BM25_TEST_DSN / port 5434) pointing to a
vchord-suite PostgreSQL instance with the required extensions.
"""

from plone.pgcatalog.backends import BM25Backend
from plone.pgcatalog.backends import reset_backend
from plone.pgcatalog.backends import set_backend
from plone.pgcatalog.query import build_query
from plone.pgcatalog.schema import install_catalog_schema
from psycopg.rows import dict_row
from psycopg.types.json import Json
from tests.conftest import BM25_DSN
from tests.conftest import TABLES_TO_DROP
from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

import psycopg
import pytest


# Skip entire module if BM25 extensions are not installed
pytestmark = pytest.mark.skipif(
    not BM25Backend.detect(BM25_DSN),
    reason="vchord_bm25 and/or pg_tokenizer not available at BM25_TEST_DSN",
)


@pytest.fixture(autouse=True)
def _bm25_backend():
    """Activate BM25 backend for integration tests."""
    backend = BM25Backend()
    set_backend(backend)
    yield
    reset_backend()


@pytest.fixture
def pg_conn_with_bm25():
    """Database connection to vchord-suite PG with BM25 schema installed."""
    conn = psycopg.connect(BM25_DSN, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute(TABLES_TO_DROP)
    conn.commit()
    conn.execute(HISTORY_FREE_SCHEMA)
    conn.commit()
    install_catalog_schema(conn)
    conn.commit()
    # Install BM25 extensions + tokenizer + column + index
    # (must use install_schema for per-statement execution)
    backend = BM25Backend()
    backend.install_schema(conn)
    conn.commit()
    yield conn
    conn.close()


def _insert_and_catalog(
    conn, zoid, path, title, description, body, tid=1, language="simple"
):
    """Insert an object and write BM25 + tsvector data."""
    idx = {"Title": title, "Description": description, "Language": language}
    combined = " ".join(filter(None, [title, title, title, description, body])) or None

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transaction_log (tid) VALUES (%(tid)s) ON CONFLICT DO NOTHING",
            {"tid": tid},
        )
        cur.execute(
            """
            INSERT INTO object_state
                (zoid, tid, class_mod, class_name, state, state_size)
            VALUES (%(zoid)s, %(tid)s, 'test', 'Doc', %(state)s, 10)
            ON CONFLICT (zoid) DO UPDATE SET tid = %(tid)s
            """,
            {"zoid": zoid, "tid": tid, "state": Json({})},
        )
        cur.execute(
            """
            UPDATE object_state SET
                path = %(path)s,
                idx = %(idx)s,
                searchable_text = setweight(
                    to_tsvector('simple'::regconfig, COALESCE(%(title)s, '')), 'A') ||
                    setweight(to_tsvector('simple'::regconfig, COALESCE(%(desc)s, '')), 'B') ||
                    setweight(to_tsvector(%(lang)s::regconfig, COALESCE(%(body)s, '')), 'D'),
                search_bm25 = CASE WHEN %(combined)s::text IS NOT NULL
                    THEN tokenize(%(combined)s::text, 'pgcatalog_default')
                    ELSE NULL END
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "path": path,
                "idx": Json(idx),
                "title": title,
                "desc": description,
                "body": body,
                "lang": language,
                "combined": combined,
            },
        )
    conn.commit()


class TestBM25Write:
    def test_tokenize_and_store(self, pg_conn_with_bm25):
        conn = pg_conn_with_bm25
        _insert_and_catalog(conn, 1, "/plone/doc1", "Test", "A test doc", "Body text")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT search_bm25 IS NOT NULL AS has_bm25 "
                "FROM object_state WHERE zoid = 1"
            )
            row = cur.fetchone()
        assert row["has_bm25"] is True

    def test_null_when_no_text(self, pg_conn_with_bm25):
        conn = pg_conn_with_bm25
        _insert_and_catalog(conn, 2, "/plone/doc2", "", "", "")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT search_bm25 IS NULL AS is_null FROM object_state WHERE zoid = 2"
            )
            row = cur.fetchone()
        assert row["is_null"] is True


class TestBM25Ranking:
    def test_title_match_ranks_higher(self, pg_conn_with_bm25):
        """Object with query term in title should rank higher than body-only."""
        conn = pg_conn_with_bm25

        # Doc A: "security" in title
        _insert_and_catalog(
            conn,
            10,
            "/plone/a",
            "Security Policy",
            "Important document",
            "Details about organizational guidelines",
        )
        # Doc B: "security" only in body
        _insert_and_catalog(
            conn,
            11,
            "/plone/b",
            "Guidelines",
            "Organizational document",
            "This document covers security practices and procedures",
        )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid, "
                "search_bm25 <&> to_bm25query('idx_os_search_bm25', "
                "tokenize(%(q)s, 'pgcatalog_default')) AS score "
                "FROM object_state WHERE zoid IN (10, 11) "
                "AND search_bm25 IS NOT NULL "
                "ORDER BY score ASC",
                {"q": "security"},
            )
            rows = cur.fetchall()

        assert len(rows) == 2
        # Doc A (title match) should be first (lower BM25 score = more relevant)
        assert rows[0]["zoid"] == 10


class TestBM25QueryBuilder:
    def test_query_uses_bm25_rank(self, pg_conn_with_bm25):
        """build_query should produce BM25 ranking expr when backend is active."""
        qr = build_query({"SearchableText": "security"})

        assert "searchable_text @@" in qr["where"]
        assert qr["order_by"] is not None
        assert "<&>" in qr["order_by"]
        assert "ASC" in qr["order_by"]

    def test_query_with_sort_on_overrides_bm25(self, pg_conn_with_bm25):
        """Explicit sort_on should override BM25 auto-ranking."""
        qr = build_query(
            {
                "SearchableText": "security",
                "sort_on": "sortable_title",
            }
        )

        assert "searchable_text @@" in qr["where"]
        assert qr["order_by"] is not None
        assert "<&>" not in qr["order_by"]
        assert "sortable_title" in qr["order_by"]
