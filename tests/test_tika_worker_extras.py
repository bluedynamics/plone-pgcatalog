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
