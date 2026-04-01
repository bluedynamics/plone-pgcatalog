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
        """When TIKA_URL is set and object has a blob, job is enqueued.

        The content object (zoid=100) references a separate blob object
        (zoid=999) via @ref in its state.  The queue must store both.
        """
        conn = pg_conn_with_queue
        content_zoid, blob_zoid, tid = 100, 999, 1

        insert_object(conn, content_zoid, tid)
        self._insert_blob(conn, blob_zoid, tid)

        proc = CatalogStateProcessor()
        proc._tika_candidates = [
            {
                "zoid": content_zoid,
                "content_type": "application/pdf",
                "blob_refs": [blob_zoid],
            }
        ]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        rows = self._get_queue(conn)
        assert len(rows) == 1
        assert rows[0]["zoid"] == content_zoid
        assert rows[0]["blob_zoid"] == blob_zoid
        assert rows[0]["tid"] == tid
        assert rows[0]["content_type"] == "application/pdf"
        assert rows[0]["status"] == "pending"

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_no_enqueue_without_blob(self, pg_conn_with_queue):
        """When referenced oids have no blob_state entry, no job is enqueued."""
        conn = pg_conn_with_queue
        content_zoid, tid = 200, 1

        insert_object(conn, content_zoid, tid)

        proc = CatalogStateProcessor()
        proc._tika_candidates = [
            {
                "zoid": content_zoid,
                "content_type": "application/pdf",
                "blob_refs": [9999],  # no blob_state entry for this ref
            }
        ]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        rows = self._get_queue(conn)
        assert len(rows) == 0

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_no_enqueue_without_refs(self, pg_conn_with_queue):
        """Candidates with empty blob_refs don't query blob_state."""
        conn = pg_conn_with_queue

        proc = CatalogStateProcessor()
        proc._tika_candidates = [
            {"zoid": 200, "content_type": "application/pdf", "blob_refs": []}
        ]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        assert self._get_queue(conn) == []

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_idempotent_enqueue(self, pg_conn_with_queue):
        """Duplicate enqueue for same blob_zoid+tid is ignored."""
        conn = pg_conn_with_queue
        content_zoid, blob_zoid, tid = 300, 888, 1

        insert_object(conn, content_zoid, tid)
        self._insert_blob(conn, blob_zoid, tid)

        proc = CatalogStateProcessor()

        for _ in range(2):
            proc._tika_candidates = [
                {
                    "zoid": content_zoid,
                    "content_type": "application/pdf",
                    "blob_refs": [blob_zoid],
                }
            ]
            with conn.cursor(row_factory=tuple_row) as cur:
                proc._enqueue_tika_jobs(cur)
            conn.commit()

        rows = self._get_queue(conn)
        assert len(rows) == 1

    def test_process_accumulates_candidates_with_blob_refs(self, pg_conn_with_queue):
        """process() accumulates tika candidates with blob_refs from state."""
        from plone.pgcatalog.pending import set_pending

        proc = CatalogStateProcessor()
        proc._tika_candidates = []

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

        # State with a blob @ref
        state = {"file": {"_blob": {"@ref": "00000000000003e8"}}}
        proc.process(zoid, "plone.app.contenttypes.content", "File", state)
        assert len(proc._tika_candidates) == 1
        assert proc._tika_candidates[0]["zoid"] == zoid
        assert proc._tika_candidates[0]["content_type"] == "application/pdf"
        assert proc._tika_candidates[0]["blob_refs"] == [0x3E8]

    def test_process_skips_candidate_without_refs(self, pg_conn_with_queue):
        """process() does not accumulate candidate if state has no @ref."""
        from plone.pgcatalog.pending import set_pending

        proc = CatalogStateProcessor()
        proc._tika_candidates = []

        if not TIKA_URL:
            pytest.skip("PGCATALOG_TIKA_URL not set")

        zoid = 500
        set_pending(
            zoid,
            {
                "path": "/plone/test2",
                "idx": {"portal_type": "File"},
                "searchable_text": "",
                "content_type": "application/pdf",
            },
        )

        # State with no blob references
        state = {"title": "No blob here"}
        proc.process(zoid, "plone.app.contenttypes.content", "File", state)
        assert len(proc._tika_candidates) == 0


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

    def test_unique_constraint_on_blob_zoid_tid(self, pg_conn_with_queue):
        conn = pg_conn_with_queue
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO text_extraction_queue (zoid, blob_zoid, tid) "
                "VALUES (1, 10, 1)"
            )
            conn.commit()
            # Same blob_zoid+tid should conflict
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    "INSERT INTO text_extraction_queue (zoid, blob_zoid, tid) "
                    "VALUES (2, 10, 1)"
                )
            conn.rollback()

    def test_different_content_same_blob_allowed(self, pg_conn_with_queue):
        """Different content zoids with same blob_zoid+tid still conflict."""
        conn = pg_conn_with_queue
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO text_extraction_queue (zoid, blob_zoid, tid) "
                "VALUES (1, 10, 1)"
            )
            conn.commit()
            # Different zoid, same blob_zoid+tid — still unique violation
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    "INSERT INTO text_extraction_queue (zoid, blob_zoid, tid) "
                    "VALUES (99, 10, 1)"
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
