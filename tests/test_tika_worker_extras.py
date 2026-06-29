"""Tests for graceful behaviour when the tika/tika-s3 extras are missing.

The ``pgcatalog-tika-worker`` console script is registered unconditionally,
but ``httpx`` (and ``boto3`` for the S3 path) ship only via the optional
``tika`` / ``tika-s3`` extras. Importing the worker module or running the
script without the extra must not crash with a bare ``ModuleNotFoundError``;
it must give a clear hint pointing at the extra. See issue #171.
"""

import subprocess
import sys
import textwrap


def _run_without_httpx(body):
    """Run ``body`` in a subprocess where importing httpx fails."""
    script = textwrap.dedent(
        """
            import sys
            # Make `import httpx` raise ModuleNotFoundError, simulating an
            # install without the [tika] / [tika-s3] extra.
            sys.modules["httpx"] = None
            """
    ) + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )


def test_module_imports_without_httpx():
    """The worker module imports cleanly even when httpx is absent."""
    result = _run_without_httpx(
        """
        from plone.pgcatalog import tika_worker
        assert tika_worker.httpx is None
        print("IMPORT_OK")
        """
    )
    assert result.returncode == 0, result.stderr
    assert "IMPORT_OK" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_main_exits_with_clear_hint_without_httpx():
    """Running the worker without the extra exits cleanly with a hint."""
    result = _run_without_httpx(
        """
        from plone.pgcatalog import tika_worker
        tika_worker.main()
        """
    )
    combined = result.stdout + result.stderr
    assert result.returncode == 1, combined
    # No bare traceback leaking the internal import failure ...
    assert "ModuleNotFoundError" not in combined
    assert "Traceback" not in combined
    # ... instead a clear, actionable hint mentioning the extra.
    assert "tika" in combined.lower()
    assert "pip install" in combined.lower()


# ---------------------------------------------------------------------------
# #178: the worker must forward S3 credentials to boto3, otherwise every
# S3-tiered blob fails extraction with "Unable to locate credentials".
# ---------------------------------------------------------------------------


def test_s3_client_forwards_credentials(monkeypatch):
    """_get_s3_client passes access/secret key through to boto3.client."""
    from plone.pgcatalog.tika_worker import TikaWorker

    import types

    captured = {}
    fake_boto3 = types.SimpleNamespace(
        client=lambda *a, **k: captured.update(service=a, kwargs=k) or "CLIENT"
    )
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    worker = TikaWorker(
        dsn="x",
        tika_url="y",
        s3_config={
            "bucket_name": "b",
            "endpoint_url": "http://endpoint",
            "region_name": "eu",
            "access_key": "AKIA",
            "secret_key": "SECRET",
        },
    )
    client = worker._get_s3_client()
    assert client == "CLIENT"
    kw = captured["kwargs"]
    assert kw["endpoint_url"] == "http://endpoint"
    assert kw["region_name"] == "eu"
    assert kw["aws_access_key_id"] == "AKIA"
    assert kw["aws_secret_access_key"] == "SECRET"


def test_s3_client_without_credentials_passes_none(monkeypatch):
    """Backward-compatible: no creds → boto3 falls back to its provider chain."""
    from plone.pgcatalog.tika_worker import TikaWorker

    import types

    captured = {}
    fake_boto3 = types.SimpleNamespace(
        client=lambda *a, **k: captured.update(k) or "CLIENT"
    )
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    worker = TikaWorker(
        dsn="x",
        tika_url="y",
        s3_config={"bucket_name": "b", "endpoint_url": None, "region_name": None},
    )
    worker._get_s3_client()
    # Keys are forwarded as None so boto3 uses its default credential chain.
    assert captured["aws_access_key_id"] is None
    assert captured["aws_secret_access_key"] is None


def test_main_reads_s3_credentials_from_env(monkeypatch):
    """main() reads TIKA_WORKER_S3_ACCESS_KEY / _SECRET_KEY into s3_config."""
    from plone.pgcatalog import tika_worker

    monkeypatch.setenv("TIKA_WORKER_DSN", "dsn")
    monkeypatch.setenv("TIKA_WORKER_URL", "http://tika")
    monkeypatch.setenv("TIKA_WORKER_S3_BUCKET", "bucket")
    monkeypatch.setenv("TIKA_WORKER_S3_ACCESS_KEY", "AKIA")
    monkeypatch.setenv("TIKA_WORKER_S3_SECRET_KEY", "SECRET")

    captured = {}

    class _FakeWorker:
        def __init__(self, **kw):
            captured.update(kw)

        def run(self):
            pass

    monkeypatch.setattr(tika_worker, "TikaWorker", _FakeWorker)
    monkeypatch.setattr(tika_worker, "httpx", object())  # pretend extra present
    tika_worker.main()

    s3 = captured["s3_config"]
    assert s3["access_key"] == "AKIA"
    assert s3["secret_key"] == "SECRET"
    assert s3["bucket_name"] == "bucket"


# ---------------------------------------------------------------------------
# #183 (B): worker Tika HTTP timeout must be configurable (OCR can exceed 120s).
# ---------------------------------------------------------------------------


def test_extract_uses_configured_http_timeout(monkeypatch):
    """_extract passes self.http_timeout to the httpx client."""
    from plone.pgcatalog import tika_worker

    import types

    captured = {}

    class _Resp:
        text = "extracted text"

        def raise_for_status(self):
            pass

    class _Client:
        def __init__(self, *a, **k):
            captured.update(k)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def put(self, *a, **k):
            return _Resp()

    monkeypatch.setattr(tika_worker, "httpx", types.SimpleNamespace(Client=_Client))
    worker = tika_worker.TikaWorker(dsn="x", tika_url="http://t", http_timeout=7.5)
    monkeypatch.setattr(worker, "_fetch_blob", lambda *a, **k: b"blob")
    out = worker._extract(conn=None, zoid=1, tid=1, content_type="application/pdf")
    assert out == "extracted text"
    assert captured["timeout"] == 7.5


def test_http_timeout_defaults_to_120(monkeypatch):
    from plone.pgcatalog import tika_worker

    worker = tika_worker.TikaWorker(dsn="x", tika_url="http://t")
    assert worker.http_timeout == 120.0


def test_main_reads_http_timeout_from_env(monkeypatch):
    from plone.pgcatalog import tika_worker

    monkeypatch.setenv("TIKA_WORKER_DSN", "dsn")
    monkeypatch.setenv("TIKA_WORKER_URL", "http://tika")
    monkeypatch.setenv("TIKA_WORKER_HTTP_TIMEOUT", "300")

    captured = {}

    class _FakeWorker:
        def __init__(self, **kw):
            captured.update(kw)

        def run(self):
            pass

    monkeypatch.setattr(tika_worker, "TikaWorker", _FakeWorker)
    monkeypatch.setattr(tika_worker, "httpx", object())
    tika_worker.main()

    assert captured["http_timeout"] == 300.0
