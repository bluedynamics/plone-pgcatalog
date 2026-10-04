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

import httpx
import os
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


def _enqueue_job(conn, zoid, tid=1, content_type="application/pdf", blob_zoid=None):
    """Insert a job into text_extraction_queue."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO text_extraction_queue "
            "(zoid, blob_zoid, tid, content_type) "
            "VALUES (%(zoid)s, %(blob_zoid)s, %(tid)s, %(ct)s) "
            "ON CONFLICT DO NOTHING",
            {
                "zoid": zoid,
                "blob_zoid": blob_zoid if blob_zoid is not None else zoid,
                "tid": tid,
                "ct": content_type,
            },
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
        # /rmeta/text shape: one entry per document, 4.x key.

        mock_response.json.return_value = [{"tk:content": "Extracted text from PDF"}]

        mock_response.headers = {"content-length": "64"}
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
        # /rmeta/text shape: one entry per document, 4.x key.

        mock_response.json.return_value = [
            {"tk:content": "important findings about quantum computing"}
        ]

        mock_response.headers = {"content-length": "64"}
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
        # /rmeta/text shape: one entry per document, 4.x key.

        mock_response.json.return_value = [{"tk:content": "extracted"}]

        mock_response.headers = {"content-length": "64"}
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


# ── Integration tests (require real Tika server) ─────────────────────

TIKA_URL = os.environ.get("PGCATALOG_TIKA_URL", "").strip()


class TestWorkerIntegration:
    """End-to-end tests with real Tika server."""

    pytestmark = pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")

    def test_extract_plain_text(self, worker_db):
        """Worker extracts text from a plain text blob via real Tika."""
        conn = worker_db
        zoid, tid = 50, 1
        text_content = b"Important findings about quantum computing research"
        _insert_object_with_blob(
            conn,
            zoid,
            tid,
            blob_data=text_content,
            idx={"Language": "en", "Title": "Research"},
        )
        _enqueue_job(conn, zoid, tid, content_type="text/plain")

        # Set initial searchable_text
        conn.execute(
            "UPDATE object_state SET searchable_text = "
            "to_tsvector('english', 'Research') "
            "WHERE zoid = %(zoid)s",
            {"zoid": zoid},
        )
        conn.commit()

        # These test extraction, not policy: text/plain is outside the
        # default allowlist, which the worker now enforces (#235).
        worker = TikaWorker(dsn=DSN, tika_url=TIKA_URL, content_types={"text/plain"})
        result = worker._process_one()
        assert result is True

        # Verify extraction completed
        status = _get_queue_status(conn, zoid)
        assert status["status"] == "done"
        assert status["error"] is None

        # Verify searchable_text was updated with extracted terms
        with conn.cursor() as cur:
            cur.execute(
                "SELECT searchable_text::text FROM object_state WHERE zoid = %(zoid)s",
                {"zoid": zoid},
            )
            row = cur.fetchone()
        tsv_text = row["searchable_text"]
        assert "quantum" in tsv_text or "comput" in tsv_text

    def test_extract_html_content(self, worker_db):
        """Worker extracts text from HTML via real Tika."""
        conn = worker_db
        zoid, tid = 60, 1
        html_blob = b"<html><body><h1>PostgreSQL Performance</h1><p>Indexes matter.</p></body></html>"
        _insert_object_with_blob(conn, zoid, tid, blob_data=html_blob)
        _enqueue_job(conn, zoid, tid, content_type="text/html")

        conn.execute(
            "UPDATE object_state SET searchable_text = ''::tsvector, "
            'idx = \'{"Language": "en"}\'::jsonb '
            "WHERE zoid = %(zoid)s",
            {"zoid": zoid},
        )
        conn.commit()

        # These test extraction, not policy: text/html is outside the
        # default allowlist, which the worker now enforces (#235).
        worker = TikaWorker(dsn=DSN, tika_url=TIKA_URL, content_types={"text/html"})
        worker._process_one()

        status = _get_queue_status(conn, zoid)
        assert status["status"] == "done"

        with conn.cursor() as cur:
            cur.execute(
                "SELECT searchable_text::text FROM object_state WHERE zoid = %(zoid)s",
                {"zoid": zoid},
            )
            row = cur.fetchone()
        tsv_text = row["searchable_text"]
        # Tika should have extracted "PostgreSQL Performance" and "Indexes matter"
        assert "postgresql" in tsv_text or "perform" in tsv_text or "index" in tsv_text


# ── Transport deferral and backoff (#222) ────────────────────────────

TRANSPORT_ERRORS = (
    httpx.ConnectError("refused"),
    httpx.ConnectTimeout("timed out"),
    httpx.RemoteProtocolError("server disconnected"),
)


class TestTransportDeferral:
    """A Tika that is absent says nothing about the job.

    Three attempts inside one second cannot bridge a pod restart, which
    is how 102 transient connection errors became `failed` rows needing a
    manual SQL reset in #222.
    """

    @pytest.mark.parametrize("exc", TRANSPORT_ERRORS, ids=lambda e: type(e).__name__)
    def test_transport_error_defers_without_spending_an_attempt(self, worker_db, exc):
        _insert_object_with_blob(worker_db, zoid=800)
        _enqueue_job(worker_db, zoid=800)

        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        with patch.object(worker, "_extract", side_effect=exc):
            worker._process_one()

        row = _get_queue_status(worker_db, 800)
        assert row["status"] == "pending"
        assert row["attempts"] == 0, "a missing server is not the job's fault"
        assert row["deferrals"] == 1
        assert row["not_before"] > row["created_at"]

    def test_backoff_ladder_grows_then_caps(self, worker_db):
        _insert_object_with_blob(worker_db, zoid=801)
        _enqueue_job(worker_db, zoid=801)
        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")

        delays = []
        for _ in range(4):
            worker_db.execute(
                "UPDATE text_extraction_queue SET not_before = now() WHERE zoid = 801"
            )
            worker_db.commit()
            with patch.object(
                worker, "_extract", side_effect=httpx.ConnectError("refused")
            ):
                worker._process_one()
            row = _get_queue_status(worker_db, 801)
            delays.append(
                round((row["not_before"] - row["updated_at"]).total_seconds())
            )

        assert delays == [5, 30, 120, 120]

    def test_other_errors_still_spend_an_attempt(self, worker_db):
        """Only transport failures are free; a bad document is the job's
        own problem and must still exhaust its attempts."""
        _insert_object_with_blob(worker_db, zoid=802)
        _enqueue_job(worker_db, zoid=802)

        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        with patch.object(worker, "_extract", side_effect=ValueError("bad")):
            worker._process_one()

        row = _get_queue_status(worker_db, 802)
        assert row["attempts"] == 1
        assert row["deferrals"] == 0

    def test_deferred_job_is_invisible_until_not_before(self, worker_db):
        _insert_object_with_blob(worker_db, zoid=803)
        _enqueue_job(worker_db, zoid=803)
        worker_db.execute(
            "UPDATE text_extraction_queue "
            "   SET not_before = now() + interval '1 hour' WHERE zoid = 803"
        )
        worker_db.commit()

        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        assert worker._process_one() is False, "nothing claimable yet"

    def test_skipped_and_exhausted_rows_are_never_resurrected(self, worker_db):
        """Review Focus 5: adding not_before must only narrow the dequeue
        predicate, never widen it."""
        for zoid in (804, 805):
            _insert_object_with_blob(worker_db, zoid=zoid)
            _enqueue_job(worker_db, zoid=zoid)
        worker_db.execute(
            "UPDATE text_extraction_queue SET status = 'skipped' WHERE zoid = 804"
        )
        worker_db.execute(
            "UPDATE text_extraction_queue "
            "   SET attempts = max_attempts, not_before = now() WHERE zoid = 805"
        )
        worker_db.commit()

        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        assert worker._process_one() is False


class TestRmetaEndpoint:
    """The worker extracts via /rmeta/text, not /tika (#222).

    /tika returns body text only, so an image without OCR yields nothing
    even when it carries an EXIF caption, and on Tika 4 /tika returns
    Markdown, which puts link targets into searchable_text.
    """

    def _resp(self, **kw):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.headers = kw.pop("headers", {"content-length": "64"})
        for key, value in kw.items():
            setattr(resp, key, value)
        return resp

    def test_extract_calls_rmeta_and_caps_embedded_resources(self, worker_db):
        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        resp = self._resp()
        resp.json.return_value = [{"tk:content": "body", "dc:title": "T"}]

        with patch("plone.pgcatalog.tika_worker.httpx.Client") as client_cls:
            put = client_cls.return_value.__enter__.return_value.put
            put.return_value = resp
            with patch.object(
                worker,
                "_blob_source",
                return_value={"kind": "bytes", "data": b"x"},
            ):
                text = worker._extract(worker_db, 1, 1, "application/pdf")

        assert put.call_args[0][0].endswith("/rmeta/text")
        headers = put.call_args[1]["headers"]
        assert headers["Accept"] == "application/json"
        assert "X-Tika-MaxEmbeddedResources" in headers
        assert "body" in text
        assert "T" in text

    def test_non_json_body_raises_a_job_error_not_a_loop_error(self, worker_db):
        """Review Focus 3: a 200 carrying an HTML error page must fail the
        one job, not the worker loop."""
        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        resp = self._resp()
        resp.json = MagicMock(side_effect=ValueError("not json"))

        with patch("plone.pgcatalog.tika_worker.httpx.Client") as client_cls:
            client_cls.return_value.__enter__.return_value.put.return_value = resp
            with (
                patch.object(
                    worker,
                    "_blob_source",
                    return_value={"kind": "bytes", "data": b"x"},
                ),
                pytest.raises(ValueError),
            ):
                worker._extract(worker_db, 1, 1, "application/pdf")

    def test_oversized_response_is_refused_before_parsing(self, worker_db):
        """One entry per embedded resource means the body is not bounded by
        the source size, so the ceiling is checked before json()."""
        worker = TikaWorker(dsn=DSN, tika_url="http://localhost:9998")
        resp = self._resp(headers={"content-length": str(64 * 1024 * 1024)})
        resp.json = MagicMock(side_effect=AssertionError("must not parse"))

        with patch("plone.pgcatalog.tika_worker.httpx.Client") as client_cls:
            client_cls.return_value.__enter__.return_value.put.return_value = resp
            with (
                patch.object(
                    worker,
                    "_blob_source",
                    return_value={"kind": "bytes", "data": b"x"},
                ),
                pytest.raises(ValueError, match="response too large"),
            ):
                worker._extract(worker_db, 1, 1, "application/pdf")

    def test_a_real_tika_round_trip_yields_the_document_text(self, worker_db):
        """Against the live server configured for the suite, not a mock."""
        if not os.environ.get("PGCATALOG_TIKA_URL"):
            pytest.skip("PGCATALOG_TIKA_URL not set")
        worker = TikaWorker(dsn=DSN, tika_url=os.environ["PGCATALOG_TIKA_URL"])
        with patch.object(
            worker,
            "_blob_source",
            return_value={
                "kind": "bytes",
                "data": b"Kaufvertrag Seegrundstueck Attersee",
            },
        ):
            text = worker._extract(worker_db, 1, 1, "text/plain")
        assert "Kaufvertrag" in text
        assert "Attersee" in text


class TestDequeueAllowlist:
    """The content-type allowlist is re-checked at dequeue (#235).

    It used to be enforced at enqueue only, so a row queued under an older,
    wider allowlist was processed anyway, and any requeue bypassed the
    allowlist entirely. On one production site 1144 of 1708 failed rows were
    images queued before images were excluded.
    """

    def _worker(self, types):
        return TikaWorker(
            dsn=DSN, tika_url="http://localhost:9998", content_types=types
        )

    def test_disallowed_type_is_skipped_without_fetching_the_blob(self, worker_db):
        _insert_object_with_blob(worker_db, zoid=900)
        _enqueue_job(worker_db, zoid=900, content_type="image/jpeg")
        worker = self._worker({"application/pdf"})

        with (
            patch.object(worker, "_blob_source") as source,
            patch.object(worker, "_extract") as extract,
        ):
            assert worker._process_one() is True

        source.assert_not_called()
        extract.assert_not_called()
        row = _get_queue_status(worker_db, 900)
        assert row["status"] == "skipped"
        assert row["error"] == "skipped: content-type-not-allowed: image/jpeg"

    def test_allowed_type_is_extracted(self, worker_db):
        _insert_object_with_blob(worker_db, zoid=901)
        _enqueue_job(worker_db, zoid=901, content_type="application/pdf")
        worker = self._worker({"application/pdf"})

        with patch.object(worker, "_extract", return_value="body text"):
            worker._process_one()

        assert _get_queue_status(worker_db, 901)["status"] == "done"

    def test_parameterised_type_matches_a_bare_allowlist_entry(self, worker_db):
        """Same normalisation as the enqueue side, or the two disagree."""
        _insert_object_with_blob(worker_db, zoid=902)
        _enqueue_job(
            worker_db, zoid=902, content_type="application/pdf; charset=binary"
        )
        worker = self._worker({"application/pdf"})

        with patch.object(worker, "_extract", return_value="body text"):
            worker._process_one()

        assert _get_queue_status(worker_db, 902)["status"] == "done"

    def test_missing_content_type_is_skipped(self, worker_db):
        """A legacy row with no content type cannot pass the allowlist."""
        _insert_object_with_blob(worker_db, zoid=903)
        _enqueue_job(worker_db, zoid=903, content_type=None)
        worker = self._worker({"application/pdf"})

        with patch.object(worker, "_blob_source") as source:
            worker._process_one()

        source.assert_not_called()
        row = _get_queue_status(worker_db, 903)
        assert row["status"] == "skipped"
        assert row["error"] == "skipped: content-type-not-allowed: none"

    def test_a_skipped_row_is_never_claimed_again(self, worker_db):
        _insert_object_with_blob(worker_db, zoid=904)
        _enqueue_job(worker_db, zoid=904, content_type="image/gif")
        worker = self._worker({"application/pdf"})

        assert worker._process_one() is True
        assert worker._process_one() is False, "skipped is terminal"

    def test_failed_rows_are_not_touched(self, worker_db):
        """No sweep, no migration: rows that already exist are left alone
        unless the worker claims them, and it never claims a failed row."""
        _insert_object_with_blob(worker_db, zoid=905)
        _enqueue_job(worker_db, zoid=905, content_type="image/jpeg")
        worker_db.execute(
            "UPDATE text_extraction_queue "
            "   SET status = 'failed', attempts = max_attempts WHERE zoid = 905"
        )
        worker_db.commit()

        assert self._worker({"application/pdf"})._process_one() is False
        assert _get_queue_status(worker_db, 905)["status"] == "failed"

    def test_default_allowlist_comes_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("PGCATALOG_TIKA_CONTENT_TYPES", "Application/PDF")
        worker = TikaWorker(dsn="x", tika_url="y")
        assert worker.content_types == {"application/pdf"}
