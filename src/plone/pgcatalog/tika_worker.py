"""Standalone Tika text extraction worker.

Dequeues jobs from ``text_extraction_queue``, fetches blob data from
PG bytea or S3, sends to Apache Tika for extraction, and updates
``searchable_text`` (+ BM25 columns) on ``object_state``.

No Zope/Plone dependency -- only ``psycopg`` and ``httpx``.

Usage (standalone)::

    pgcatalog-tika-worker

    # or
    python -m plone.pgcatalog.tika_worker

Environment variables:

    TIKA_WORKER_DSN           PostgreSQL DSN (required)
    TIKA_WORKER_URL           Tika server URL (required)
    TIKA_WORKER_S3_BUCKET     S3 bucket name (optional, for S3-tiered blobs)
    TIKA_WORKER_S3_ENDPOINT_URL  S3 endpoint (optional)
    TIKA_WORKER_S3_REGION     S3 region (optional)
    TIKA_WORKER_S3_ACCESS_KEY S3 access key id (optional; falls back to boto3's
                              default credential chain when unset)
    TIKA_WORKER_S3_SECRET_KEY S3 secret access key (optional; see above)
    TIKA_WORKER_POLL_INTERVAL Seconds between polls when idle (default: 5)
    TIKA_WORKER_HTTP_TIMEOUT  Seconds to wait for a Tika HTTP response
                              (default: 120; raise it for OCR of large scanned
                              PDFs, which can exceed the default)
"""

from psycopg.rows import dict_row

import logging
import os
import psycopg
import signal
import sys
import threading
import time


# ``httpx`` (and ``boto3``, imported lazily in ``_get_s3_client``) ship only
# via the optional ``tika`` / ``tika-s3`` extras, but the
# ``pgcatalog-tika-worker`` console script is registered unconditionally.
# Import httpx defensively so the module stays importable without the extra;
# the worker fails fast with a clear hint instead of a bare
# ModuleNotFoundError.  See https://github.com/bluedynamics/plone-pgcatalog/issues/171
try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - exercised in a subprocess
    httpx = None


__all__ = ["TikaWorker", "main"]


log = logging.getLogger(__name__)


MISSING_EXTRA_HINT = (
    "pgcatalog-tika-worker requires extra dependencies that are not "
    "installed. Install the worker extra:\n"
    "    pip install plone-pgcatalog[tika]      # PG bytea blobs\n"
    "    pip install plone-pgcatalog[tika-s3]   # + S3-tiered blobs"
)


class TikaWorker:
    """PostgreSQL-backed text extraction worker using Apache Tika."""

    def __init__(
        self, dsn, tika_url, s3_config=None, poll_interval=5, http_timeout=120.0
    ):
        self.dsn = dsn
        self.tika_url = tika_url.rstrip("/")
        self.s3_config = s3_config
        self.poll_interval = poll_interval
        self.http_timeout = http_timeout
        self._shutdown = threading.Event()
        self._s3_client = None

    def run(self):
        """Main loop: LISTEN for notifications, process jobs."""
        with psycopg.connect(self.dsn, autocommit=True) as listen_conn:
            listen_conn.execute("LISTEN text_extraction_ready")
            log.info(
                "Tika worker started (tika=%s, poll=%ds)",
                self.tika_url,
                self.poll_interval,
            )

            while not self._shutdown.is_set():
                # Drain all available jobs
                while not self._shutdown.is_set() and self._process_one():
                    pass

                if self._shutdown.is_set():
                    break

                # Wait for notification or poll interval
                try:
                    gen = listen_conn.notifies(timeout=self.poll_interval)
                    for _notify in gen:
                        break  # got notification, loop back to process
                except psycopg.OperationalError:
                    if not self._shutdown.is_set():
                        log.warning("LISTEN connection lost, reconnecting...")
                        time.sleep(1)
                        try:
                            listen_conn.close()
                        except Exception:
                            pass
                        return self.run()  # reconnect

        log.info("Tika worker shutting down")

    def _process_one(self):
        """Dequeue and process one job. Returns True if work was done."""
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Atomic dequeue: claim one pending job
                cur.execute(
                    "UPDATE text_extraction_queue SET "
                    "  status = 'processing', "
                    "  attempts = attempts + 1, "
                    "  updated_at = now() "
                    "WHERE id = ("
                    "  SELECT id FROM text_extraction_queue "
                    "  WHERE status = 'pending' "
                    "    AND attempts < max_attempts "
                    "  ORDER BY id "
                    "  FOR UPDATE SKIP LOCKED "
                    "  LIMIT 1"
                    ") RETURNING id, zoid, blob_zoid, tid, content_type"
                )
                row = cur.fetchone()
                if row is None:
                    conn.rollback()
                    return False
                conn.commit()

            job_id = row["id"]
            zoid = row["zoid"]
            blob_zoid = row["blob_zoid"] or row["zoid"]  # fallback for old rows
            tid = row["tid"]
            content_type = row["content_type"]

            try:
                text = self._extract(conn, blob_zoid, tid, content_type)
                self._update_searchable_text(conn, zoid, text)
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE text_extraction_queue SET "
                        "  status = 'done', error = NULL, updated_at = now() "
                        "WHERE id = %(id)s",
                        {"id": job_id},
                    )
                    conn.commit()
                log.info(
                    "Extracted text for zoid=%d blob_zoid=%d tid=%d (%d chars, job %d)",
                    zoid,
                    blob_zoid,
                    tid,
                    len(text) if text else 0,
                    job_id,
                )
            except Exception as exc:
                log.warning(
                    "Extraction failed for zoid=%d blob_zoid=%d tid=%d (job %d): %s",
                    zoid,
                    blob_zoid,
                    tid,
                    job_id,
                    exc,
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE text_extraction_queue SET "
                            "  status = CASE WHEN attempts >= max_attempts "
                            "    THEN 'failed' ELSE 'pending' END, "
                            "  error = %(error)s, updated_at = now() "
                            "WHERE id = %(id)s",
                            {"error": str(exc)[:1000], "id": job_id},
                        )
                        conn.commit()
                except Exception:
                    log.error("Failed to update queue status for job %d", job_id)
            return True

    def _extract(self, conn, zoid, tid, content_type):
        """Fetch blob and send to Tika, return extracted text."""
        blob_data = self._fetch_blob(conn, zoid, tid)
        if httpx is None:
            raise RuntimeError(MISSING_EXTRA_HINT)
        headers = {"Accept": "text/plain"}
        if content_type:
            headers["Content-Type"] = content_type
        with httpx.Client(timeout=self.http_timeout) as client:
            resp = client.put(
                f"{self.tika_url}/tika",
                content=blob_data,
                headers=headers,
            )
            resp.raise_for_status()
            return resp.text

    def _fetch_blob(self, conn, zoid, tid):
        """Fetch blob bytes from PG bytea or S3."""
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT data, s3_key FROM blob_state "
                "WHERE zoid = %(zoid)s AND tid = %(tid)s",
                {"zoid": zoid, "tid": tid},
            )
            row = cur.fetchone()
        if row is None:
            raise ValueError(f"No blob for zoid={zoid} tid={tid}")
        if row["data"] is not None:
            return bytes(row["data"])
        if row["s3_key"]:
            return self._fetch_from_s3(row["s3_key"])
        raise ValueError(f"Blob row has neither data nor s3_key for zoid={zoid}")

    def _fetch_from_s3(self, s3_key):
        """Download blob from S3."""
        import io

        client = self._get_s3_client()
        buf = io.BytesIO()
        client.download_fileobj(self.s3_config["bucket_name"], s3_key, buf)
        return buf.getvalue()

    def _get_s3_client(self):
        if self._s3_client is None:
            if not self.s3_config:
                raise ValueError("S3 blob requested but no S3 config provided")
            try:
                import boto3
            except ModuleNotFoundError:
                raise RuntimeError(
                    "S3-tiered blobs require the 'tika-s3' extra:\n"
                    "    pip install plone-pgcatalog[tika-s3]"
                ) from None

            # aws_access_key_id / aws_secret_access_key default to None when not
            # configured; boto3 treats None as "not provided" and falls back to
            # its default credential provider chain (env, ~/.aws, IAM role), so
            # this stays backward-compatible with ambient-credential setups.
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.s3_config.get("endpoint_url"),
                region_name=self.s3_config.get("region_name"),
                aws_access_key_id=self.s3_config.get("access_key"),
                aws_secret_access_key=self.s3_config.get("secret_key"),
            )
        return self._s3_client

    def _update_searchable_text(self, conn, zoid, extracted_text):
        """Merge extracted text into searchable_text via PL/pgSQL function."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pgcatalog_merge_extracted_text(%(zoid)s, %(text)s)",
                {"zoid": zoid, "text": extracted_text},
            )
            conn.commit()

    def shutdown(self):
        """Signal the worker to stop."""
        self._shutdown.set()


def main():
    """CLI entry point for standalone worker."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if httpx is None:
        logging.error(MISSING_EXTRA_HINT)
        sys.exit(1)

    dsn = os.environ.get("TIKA_WORKER_DSN")
    tika_url = os.environ.get("TIKA_WORKER_URL")
    if not dsn or not tika_url:
        logging.error(
            "TIKA_WORKER_DSN and TIKA_WORKER_URL environment variables are required.",
        )
        sys.exit(1)

    s3_config = None
    bucket = os.environ.get("TIKA_WORKER_S3_BUCKET")
    if bucket:
        s3_config = {
            "bucket_name": bucket,
            "endpoint_url": os.environ.get("TIKA_WORKER_S3_ENDPOINT_URL"),
            "region_name": os.environ.get("TIKA_WORKER_S3_REGION"),
            "access_key": os.environ.get("TIKA_WORKER_S3_ACCESS_KEY"),
            "secret_key": os.environ.get("TIKA_WORKER_S3_SECRET_KEY"),
        }

    poll_interval = int(os.environ.get("TIKA_WORKER_POLL_INTERVAL", "5"))
    http_timeout = float(os.environ.get("TIKA_WORKER_HTTP_TIMEOUT", "120"))

    worker = TikaWorker(
        dsn=dsn,
        tika_url=tika_url,
        s3_config=s3_config,
        poll_interval=poll_interval,
        http_timeout=http_timeout,
    )

    def handle_signal(_sig, _frame):
        log.info("Received shutdown signal")
        worker.shutdown()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    worker.run()


if __name__ == "__main__":
    main()
