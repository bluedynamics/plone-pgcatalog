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


@pytest.fixture
def pg_conn_with_multilingual_bm25():
    """Database connection with multilingual BM25 schema (en, de, zh)."""
    conn = psycopg.connect(BM25_DSN, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute(TABLES_TO_DROP)
    conn.commit()
    conn.execute(HISTORY_FREE_SCHEMA)
    conn.commit()
    install_catalog_schema(conn)
    conn.commit()
    backend = BM25Backend(languages=["en", "de", "zh"])
    backend.install_schema(conn)
    conn.commit()
    yield conn
    conn.close()


def _insert_and_catalog(
    conn,
    zoid,
    path,
    title,
    description,
    body,
    tid=1,
    language="en",
    backend=None,
):
    """Insert an object and write BM25 + tsvector data."""
    if backend is None:
        backend = BM25Backend()

    idx = {"Title": title, "Description": description, "Language": language}
    pending = {"idx": idx, "searchable_text": body}
    bm25_data = backend.process_search_data(pending)

    # Build language regconfig for tsvector
    from plone.pgcatalog.columns import language_to_regconfig

    regconfig = language_to_regconfig(language)

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

        # Build dynamic SET clause for BM25 columns
        set_parts = [
            "path = %(path)s",
            "idx = %(idx)s",
            "searchable_text = setweight("
            "to_tsvector('simple'::regconfig, COALESCE(%(title)s, '')), 'A') || "
            "setweight(to_tsvector('simple'::regconfig, COALESCE(%(desc)s, '')), 'B') || "
            "setweight(to_tsvector(%(lang)s::regconfig, COALESCE(%(body)s, '')), 'D')",
        ]
        params = {
            "zoid": zoid,
            "path": path,
            "idx": Json(idx),
            "title": title,
            "desc": description,
            "body": body,
            "lang": regconfig,
        }

        # Add each BM25 column from process_search_data
        for col_name, value in bm25_data.items():
            tok_name = None
            if col_name == "search_bm25":
                tok_name = backend._tok_name()
            else:
                # search_bm25_XX → XX
                lang_suffix = col_name.replace("search_bm25_", "")
                tok_name = backend._tok_name(lang_suffix)

            param_key = col_name
            params[param_key] = value
            set_parts.append(
                f"{col_name} = CASE WHEN %({param_key})s::text IS NOT NULL "
                f"THEN tokenize(%({param_key})s::text, '{tok_name}') "
                f"ELSE NULL END"
            )

        set_clause = ", ".join(set_parts)
        cur.execute(
            f"UPDATE object_state SET {set_clause} WHERE zoid = %(zoid)s",
            params,
        )
    conn.commit()


class TestBM25Write:
    def test_tokenize_and_store(self, pg_conn_with_bm25):
        conn = pg_conn_with_bm25
        _insert_and_catalog(conn, 1, "/plone/doc1", "Test", "A test doc", "Body text")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT search_bm25 IS NOT NULL AS has_bm25, "
                "search_bm25_en IS NOT NULL AS has_en "
                "FROM object_state WHERE zoid = 1"
            )
            row = cur.fetchone()
        assert row["has_bm25"] is True
        assert row["has_en"] is True

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
                "search_bm25_en <&> to_bm25query('idx_os_search_bm25_en', "
                "tokenize(%(q)s, 'pgcatalog_en')) AS score "
                "FROM object_state WHERE zoid IN (10, 11) "
                "AND search_bm25_en IS NOT NULL "
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


# ── Multilingual BM25 tests ──────────────────────────────────────────


class TestMultilingualBM25Write:
    def test_english_populates_en_column(self, pg_conn_with_multilingual_bm25):
        conn = pg_conn_with_multilingual_bm25
        backend = BM25Backend(languages=["en", "de", "zh"])
        _insert_and_catalog(
            conn,
            1,
            "/plone/en/doc1",
            "Test",
            "A test",
            "Body",
            language="en",
            backend=backend,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT search_bm25_en IS NOT NULL AS has_en, "
                "search_bm25_de IS NULL AS de_null, "
                "search_bm25_zh IS NULL AS zh_null, "
                "search_bm25 IS NOT NULL AS has_fb "
                "FROM object_state WHERE zoid = 1"
            )
            row = cur.fetchone()
        assert row["has_en"] is True
        assert row["de_null"] is True
        assert row["zh_null"] is True
        assert row["has_fb"] is True

    def test_german_populates_de_column(self, pg_conn_with_multilingual_bm25):
        conn = pg_conn_with_multilingual_bm25
        backend = BM25Backend(languages=["en", "de", "zh"])
        _insert_and_catalog(
            conn,
            2,
            "/plone/de/doc1",
            "Vulkan",
            "Aktiver Vulkan",
            "Lava fließt",
            language="de",
            backend=backend,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT search_bm25_de IS NOT NULL AS has_de, "
                "search_bm25_en IS NULL AS en_null, "
                "search_bm25 IS NOT NULL AS has_fb "
                "FROM object_state WHERE zoid = 2"
            )
            row = cur.fetchone()
        assert row["has_de"] is True
        assert row["en_null"] is True
        assert row["has_fb"] is True

    def test_unconfigured_lang_fallback_only(self, pg_conn_with_multilingual_bm25):
        conn = pg_conn_with_multilingual_bm25
        backend = BM25Backend(languages=["en", "de", "zh"])
        _insert_and_catalog(
            conn,
            3,
            "/plone/fr/doc1",
            "Volcan",
            "Un volcan",
            "Texte",
            language="fr",
            backend=backend,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT search_bm25_en IS NULL AS en_null, "
                "search_bm25_de IS NULL AS de_null, "
                "search_bm25_zh IS NULL AS zh_null, "
                "search_bm25 IS NOT NULL AS has_fb "
                "FROM object_state WHERE zoid = 3"
            )
            row = cur.fetchone()
        assert row["en_null"] is True
        assert row["de_null"] is True
        assert row["zh_null"] is True
        assert row["has_fb"] is True


class TestMultilingualBM25Query:
    def test_german_search_uses_de_column(self, pg_conn_with_multilingual_bm25):
        backend = BM25Backend(languages=["en", "de", "zh"])
        set_backend(backend)
        qr = build_query({"SearchableText": "Vulkan", "Language": "de"})
        assert "search_bm25_de" in qr["order_by"]
        assert "pgcatalog_de" in qr["order_by"]

    def test_chinese_search_uses_zh_column(self, pg_conn_with_multilingual_bm25):
        backend = BM25Backend(languages=["en", "de", "zh"])
        set_backend(backend)
        qr = build_query({"SearchableText": "火山", "Language": "zh"})
        assert "search_bm25_zh" in qr["order_by"]

    def test_no_language_uses_fallback(self, pg_conn_with_multilingual_bm25):
        backend = BM25Backend(languages=["en", "de", "zh"])
        set_backend(backend)
        qr = build_query({"SearchableText": "test"})
        assert "search_bm25 <&>" in qr["order_by"]
        assert "pgcatalog_default" in qr["order_by"]
