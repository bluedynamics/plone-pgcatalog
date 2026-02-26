"""Tests for TikaWorker (text extraction worker)."""

from plone.pgcatalog.schema import CATALOG_COLUMNS
from plone.pgcatalog.schema import CATALOG_FUNCTIONS
from plone.pgcatalog.schema import CATALOG_LANG_FUNCTION
from plone.pgcatalog.schema import TEXT_EXTRACTION_QUEUE
from plone.pgcatalog.schema import TSVECTOR_MERGE_FUNCTION
from plone.pgcatalog.tika_worker import TikaWorker
from psycopg.rows import dict_row
from psycopg.types.json import Json
from tests.conftest import DSN
from unittest.mock import MagicMock
from unittest.mock import patch
from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

import psycopg
import pytest
import threading


pytestmark = pytest.mark.skipif(not DSN, reason="No PostgreSQL DSN configured")


TABLES_TO_DROP = (
    "DROP TABLE IF EXISTS text_extraction_queue, "
    "blob_state, object_state, transaction_log CASCADE"
)


@pytest.fixture
def worker_db():
    """Fresh DB with all required tables for worker tests."""
    conn = psycopg.connect(DSN, row_factory=dict_row)
    conn.execute(TABLES_TO_DROP)
    conn.commit()
    conn.execute(HISTORY_FREE_SCHEMA)
    conn.commit()
    conn.execute(CATALOG_COLUMNS)
    conn.execute(CATALOG_FUNCTIONS)
    conn.execute(CATALOG_LANG_FUNCTION)
    conn.commit()
    conn.execute(TEXT_EXTRACTION_QUEUE)
    conn.commit()
    conn.execute(TSVECTOR_MERGE_FUNCTION)
    conn.commit()
    # Create blob_state table
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
    yield conn
    conn.close()


def _insert_object_with_blob(conn, zoid, tid=1, blob_data=b"fake pdf", idx=None):
    """Insert an object_state row + blob_state row."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transaction_log (tid) VALUES (%(tid)s) ON CONFLICT DO NOTHING",
            {"tid": tid},
        )
        cur.execute(
            "INSERT INTO object_state "
            "(zoid, tid, class_mod, class_name, state, state_size, idx) "
            "VALUES (%(zoid)s, %(tid)s, 'test', 'Doc', %(state)s, 10, %(idx)s) "
            "ON CONFLICT (zoid) DO UPDATE SET "
            "tid = %(tid)s, idx = %(idx)s",
            {"zoid": zoid, "tid": tid, "state": Json({}), "idx": Json(idx or {})},
        )
        cur.execute(
            "INSERT INTO blob_state (zoid, tid, blob_size, data) "
            "VALUES (%(zoid)s, %(tid)s, %(size)s, %(data)s) "
            "ON CONFLICT DO NOTHING",
            {"zoid": zoid, "tid": tid, "size": len(blob_data), "data": blob_data},
        )
    conn.commit()


def _enqueue_job(conn, zoid, tid=1, content_type="application/pdf"):
    """Insert a job into text_extraction_queue."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO text_extraction_queue (zoid, tid, content_type) "
            "VALUES (%(zoid)s, %(tid)s, %(ct)s) ON CONFLICT DO NOTHING",
            {"zoid": zoid, "tid": tid, "ct": content_type},
        )
    conn.commit()


def _get_queue_status(conn, zoid):
    """Return the status of the queue entry for a zoid."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT * FROM text_extraction_queue WHERE zoid = %(zoid)s",
            {"zoid": zoid},
        )
        return cur.fetchone()


class TestWorkerFetchBlob:
    """Test blob fetching from PG bytea."""

    def test_fetch_bytea_blob(self, worker_db):
        conn = worker_db
        blob_data = b"Hello from PDF"
        _insert_object_with_blob(conn, zoid=1, tid=1, blob_data=blob_data)

        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        with psycopg.connect(DSN) as fetch_conn:
            result = worker._fetch_blob(fetch_conn, 1, 1)
        assert result == blob_data

    def test_fetch_missing_blob_raises(self, worker_db):
        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        with (
            psycopg.connect(DSN) as fetch_conn,
            pytest.raises(ValueError, match="No blob"),
        ):
            worker._fetch_blob(fetch_conn, 999, 999)


class TestWorkerProcessOne:
    """Test dequeue + processing logic (mocked Tika)."""

    @patch("plone.pgcatalog.tika_worker.httpx.Client")
    def test_process_one_success(self, mock_client_cls, worker_db):
        conn = worker_db
        zoid, tid = 10, 1
        _insert_object_with_blob(conn, zoid, tid, blob_data=b"%PDF-fake")
        _enqueue_job(conn, zoid, tid)

        # Mock Tika response
        mock_response = MagicMock()
        mock_response.text = "Extracted text from PDF"
        mock_response.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.put.return_value = mock_response
        mock_client_cls.return_value = mock_client

        worker = TikaWorker(dsn=DSN, tika_url="http://tika:9998")
        result = worker._process_one()
        assert result is True

        # Verify queue status
        status = _get_queue_status(conn, zoid)
        assert status["status"] == "done"
        assert status["error"] is None

    @patch("plone.pgcatalog.tika_worker.httpx.Client")
    def test_process_one_failure_retries(self, mock_client_cls, worker_db):
        conn = worker_db
        zoid, tid = 20, 1
        _insert_object_with_blob(conn, zoid, tid)
        _enqueue_job(conn, zoid, tid)

        # Mock Tika failure
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.put.side_effect = Exception("Tika unavailable")
        mock_client_cls.return_value = mock_client

        worker = TikaWorker(dsn=DSN, tika_url="http://tika:9998")
        result = worker._process_one()
        assert result is True

        # Should be back to pending (attempts < max_attempts)
        status = _get_queue_status(conn, zoid)
        assert status["status"] == "pending"
        assert status["attempts"] == 1
        assert "Tika unavailable" in status["error"]

    def test_process_one_empty_queue(self, worker_db):
        worker = TikaWorker(dsn=DSN, tika_url="http://tika:9998")
        result = worker._process_one()
        assert result is False


class TestWorkerSearchableText:
    """Test that extracted text actually merges into searchable_text."""

    @patch("plone.pgcatalog.tika_worker.httpx.Client")
    def test_searchable_text_updated(self, mock_client_cls, worker_db):
        conn = worker_db
        zoid, tid = 30, 1
        _insert_object_with_blob(
            conn,
            zoid,
            tid,
            blob_data=b"%PDF-fake",
            idx={"Language": "en", "Title": "Test Doc"},
        )
        _enqueue_job(conn, zoid, tid)

        # Set initial searchable_text (simulating synchronous indexing)
        conn.execute(
            "UPDATE object_state SET searchable_text = "
            "to_tsvector('english', 'Test Doc') "
            "WHERE zoid = %(zoid)s",
            {"zoid": zoid},
        )
        conn.commit()

        # Mock Tika response
        mock_response = MagicMock()
        mock_response.text = "important findings about quantum computing"
        mock_response.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.put.return_value = mock_response
        mock_client_cls.return_value = mock_client

        worker = TikaWorker(dsn=DSN, tika_url="http://tika:9998")
        worker._process_one()

        # Verify searchable_text now contains the extracted terms
        with conn.cursor() as cur:
            cur.execute(
                "SELECT searchable_text::text FROM object_state WHERE zoid = %(zoid)s",
                {"zoid": zoid},
            )
            row = cur.fetchone()
        tsv_text = row["searchable_text"]
        # The extracted text should be in the tsvector
        assert "comput" in tsv_text or "quantum" in tsv_text  # stemmed


class TestWorkerConcurrency:
    """Test SKIP LOCKED concurrent dequeue safety."""

    @patch("plone.pgcatalog.tika_worker.httpx.Client")
    def test_skip_locked_no_double_processing(self, mock_client_cls, worker_db):
        """Two workers should not process the same job."""
        conn = worker_db

        # Create multiple jobs
        for i in range(1, 4):
            _insert_object_with_blob(conn, zoid=100 + i, tid=1)
            _enqueue_job(conn, zoid=100 + i, tid=1)

        # Mock Tika
        mock_response = MagicMock()
        mock_response.text = "extracted"
        mock_response.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.put.return_value = mock_response
        mock_client_cls.return_value = mock_client

        # Run two workers, each processing one job
        worker1 = TikaWorker(dsn=DSN, tika_url="http://tika:9998")
        worker2 = TikaWorker(dsn=DSN, tika_url="http://tika:9998")

        processed = []

        def run_worker(w):
            if w._process_one():
                processed.append(True)

        t1 = threading.Thread(target=run_worker, args=(worker1,))
        t2 = threading.Thread(target=run_worker, args=(worker2,))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        # Both should have processed a job (different ones via SKIP LOCKED)
        assert len(processed) == 2

        # All 3 jobs should be processed after one more round
        worker1._process_one()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM text_extraction_queue WHERE status = 'done'"
            )
            assert cur.fetchone()["count"] == 3


class TestWorkerShutdown:
    """Test graceful shutdown."""

    def test_shutdown_flag(self):
        worker = TikaWorker(dsn=DSN, tika_url="http://tika:9998")
        assert not worker._shutdown.is_set()
        worker.shutdown()
        assert worker._shutdown.is_set()
