"""Tests for Tika extraction enqueue logic in CatalogStateProcessor."""

from plone.pgcatalog.processor import _should_extract
from plone.pgcatalog.processor import CatalogStateProcessor
from plone.pgcatalog.processor import TIKA_URL
from plone.pgcatalog.schema import TEXT_EXTRACTION_QUEUE
from psycopg.rows import dict_row
from psycopg.rows import tuple_row
from tests.conftest import DSN
from tests.conftest import insert_object

import psycopg.errors
import pytest


# Skip if no PG available
pytestmark = pytest.mark.skipif(not DSN, reason="No PostgreSQL DSN configured")


@pytest.fixture
def pg_conn_with_queue(pg_conn_with_catalog):
    """Database with catalog schema + extraction queue table."""
    conn = pg_conn_with_catalog
    # Drop and recreate to ensure clean state between tests
    conn.execute("DROP TABLE IF EXISTS text_extraction_queue, blob_state CASCADE")
    conn.commit()
    conn.execute(TEXT_EXTRACTION_QUEUE)
    conn.commit()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS blob_state ("
        "  zoid BIGINT NOT NULL,"
        "  tid BIGINT NOT NULL,"
        "  blob_size BIGINT NOT NULL DEFAULT 0,"
        "  data BYTEA,"
        "  s3_key TEXT,"
        "  PRIMARY KEY (zoid, tid)"
        ")"
    )
    conn.commit()
    return conn


class TestShouldExtract:
    """Test content type filtering."""

    def test_pdf(self):
        assert _should_extract("application/pdf") is True

    def test_docx(self):
        assert (
            _should_extract(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )
            is True
        )

    def test_jpeg(self):
        assert _should_extract("image/jpeg") is True

    def test_png(self):
        assert _should_extract("image/png") is True

    def test_html_not_extracted(self):
        assert _should_extract("text/html") is False

    def test_none_not_extracted(self):
        assert _should_extract(None) is False

    def test_empty_not_extracted(self):
        assert _should_extract("") is False


class TestEnqueueLogic:
    """Test that finalize() enqueues jobs for blobs."""

    def _insert_blob(self, conn, zoid, tid, data=b"fake-pdf-data"):
        """Insert a blob_state row."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, data) "
                "VALUES (%(zoid)s, %(tid)s, %(size)s, %(data)s) "
                "ON CONFLICT DO NOTHING",
                {"zoid": zoid, "tid": tid, "size": len(data), "data": data},
            )
        conn.commit()

    def _get_queue(self, conn):
        """Return all rows from text_extraction_queue."""
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM text_extraction_queue ORDER BY id")
            return cur.fetchall()

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_enqueue_blob_with_tika_url(self, pg_conn_with_queue):
        """When TIKA_URL is set and object has a blob, job is enqueued."""
        conn = pg_conn_with_queue
        zoid, tid = 100, 1

        insert_object(conn, zoid, tid)
        self._insert_blob(conn, zoid, tid)

        proc = CatalogStateProcessor()
        # Simulate process() accumulating a candidate
        proc._tika_candidates = [{"zoid": zoid, "content_type": "application/pdf"}]

        # Use tuple_row to match production cursor (zodb-pgjsonb uses default)
        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        rows = self._get_queue(conn)
        assert len(rows) == 1
        assert rows[0]["zoid"] == zoid
        assert rows[0]["tid"] == tid
        assert rows[0]["content_type"] == "application/pdf"
        assert rows[0]["status"] == "pending"

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_no_enqueue_without_blob(self, pg_conn_with_queue):
        """When object has no blob, no job is enqueued."""
        conn = pg_conn_with_queue
        zoid, tid = 200, 1

        insert_object(conn, zoid, tid)
        # No blob inserted

        proc = CatalogStateProcessor()
        proc._tika_candidates = [{"zoid": zoid, "content_type": "application/pdf"}]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        rows = self._get_queue(conn)
        assert len(rows) == 0

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_idempotent_enqueue(self, pg_conn_with_queue):
        """Duplicate enqueue for same zoid+tid is ignored (UNIQUE constraint)."""
        conn = pg_conn_with_queue
        zoid, tid = 300, 1

        insert_object(conn, zoid, tid)
        self._insert_blob(conn, zoid, tid)

        proc = CatalogStateProcessor()

        # Enqueue twice
        for _ in range(2):
            proc._tika_candidates = [{"zoid": zoid, "content_type": "application/pdf"}]
            with conn.cursor(row_factory=tuple_row) as cur:
                proc._enqueue_tika_jobs(cur)
            conn.commit()

        rows = self._get_queue(conn)
        assert len(rows) == 1

    def test_process_accumulates_candidates(self, pg_conn_with_queue):
        """process() accumulates tika candidates when TIKA_URL is set."""
        from plone.pgcatalog.pending import set_pending

        proc = CatalogStateProcessor()
        proc._tika_candidates = []

        # Only works when TIKA_URL is set -- test the accumulation logic
        if not TIKA_URL:
            pytest.skip("PGCATALOG_TIKA_URL not set")

        zoid = 400
        set_pending(
            zoid,
            {
                "path": "/plone/test",
                "idx": {"portal_type": "File"},
                "searchable_text": "test document",
                "content_type": "application/pdf",
            },
        )

        proc.process(zoid, "plone.app.contenttypes.content", "File", {})
        assert len(proc._tika_candidates) == 1
        assert proc._tika_candidates[0]["zoid"] == zoid
        assert proc._tika_candidates[0]["content_type"] == "application/pdf"


class TestQueueSchema:
    """Test the queue table schema."""

    def test_queue_table_exists(self, pg_conn_with_queue):
        conn = pg_conn_with_queue
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS ("
                "  SELECT FROM information_schema.tables "
                "  WHERE table_name = 'text_extraction_queue'"
                ")"
            )
            assert cur.fetchone()["exists"] is True

    def test_unique_constraint(self, pg_conn_with_queue):
        conn = pg_conn_with_queue
        with conn.cursor() as cur:
            cur.execute("INSERT INTO text_extraction_queue (zoid, tid) VALUES (1, 1)")
            conn.commit()
            # Second insert should conflict
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    "INSERT INTO text_extraction_queue (zoid, tid) VALUES (1, 1)"
                )
            conn.rollback()

    def test_notify_trigger_exists(self, pg_conn_with_queue):
        conn = pg_conn_with_queue
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tgname FROM pg_trigger WHERE tgname = 'trg_notify_extraction'"
            )
            row = cur.fetchone()
            assert row is not None
