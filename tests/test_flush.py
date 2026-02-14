"""Tests for catalog flush — making pending data visible within a transaction."""

from plone.pgcatalog.config import _auto_flush
from plone.pgcatalog.config import _clear_flush_state
from plone.pgcatalog.config import _do_flush
from plone.pgcatalog.config import _get_pending
from plone.pgcatalog.config import _has_new_objects
from plone.pgcatalog.config import _local
from plone.pgcatalog.config import _rollback_flush_if_active
from plone.pgcatalog.config import _rollback_flush_savepoint
from plone.pgcatalog.config import flush_catalog
from psycopg.types.json import Json
from tests.conftest import insert_object

import pytest


@pytest.fixture(autouse=True)
def clean_pending():
    """Clear pending data and flush state before/after each test."""
    _get_pending().clear()
    _clear_flush_state()
    # Reset generation counters
    _local._pending_gen = 0
    _local._flushed_gen = -1
    yield
    _get_pending().clear()
    _clear_flush_state()
    _local._pending_gen = 0
    _local._flushed_gen = -1


def _query_catalog_data(conn, zoid):
    """Query catalog columns for a specific zoid."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT path, parent_path, path_depth, idx, searchable_text "
            "FROM object_state WHERE zoid = %(zoid)s",
            {"zoid": zoid},
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Unit tests: flush mechanics
# ---------------------------------------------------------------------------


class TestFlushExistingObject:
    """Flush writes catalog data to an existing object_state row."""

    def test_flush_writes_catalog_columns(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=100, tid=1)

        # Start a transaction so SAVEPOINT works
        conn.execute("BEGIN")

        pending = {
            100: {
                "path": "/plone/doc",
                "idx": {"portal_type": "Document", "Title": "Hello"},
                "searchable_text": "hello world",
            }
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        row = _query_catalog_data(conn, 100)
        assert row["path"] == "/plone/doc"
        assert row["idx"]["portal_type"] == "Document"
        assert row["idx"]["Title"] == "Hello"
        assert row["searchable_text"] is not None  # tsvector present

        # Clean up
        _rollback_flush_savepoint()
        conn.execute("COMMIT")


class TestFlushNewObject:
    """Flush with UPDATE-only approach: new objects have no row to update."""

    def test_flush_new_object_is_noop(self, pg_conn_with_catalog):
        """UPDATE-only flush does NOT create rows for new objects.

        New objects become visible only after tpc_vote() writes the
        definitive data.  This avoids corrupting the ZODB state column
        with empty data (which breaks SearchableText extraction).
        """
        conn = pg_conn_with_catalog
        conn.execute("BEGIN")

        pending = {
            999: {
                "path": "/plone/new-doc",
                "idx": {"portal_type": "Document"},
                "searchable_text": None,
            }
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        # No row should exist — UPDATE matched nothing
        row = _query_catalog_data(conn, 999)
        assert row is None

        # Clean up
        _rollback_flush_savepoint()
        conn.execute("COMMIT")


class TestFlushUncatalog:
    """Flush with None sentinel NULLs catalog columns."""

    def test_flush_nulls_catalog_columns(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=200, tid=1)
        # Pre-populate catalog data
        conn.execute(
            "UPDATE object_state SET path = '/plone/doc', "
            "idx = %(idx)s WHERE zoid = 200",
            {"idx": Json({"portal_type": "Document"})},
        )
        conn.commit()

        conn.execute("BEGIN")
        pending = {200: None}
        _local._pending_gen = 1
        _do_flush(conn, pending)

        row = _query_catalog_data(conn, 200)
        assert row["path"] is None
        assert row["idx"] is None

        # Clean up
        _rollback_flush_savepoint()
        conn.execute("COMMIT")


class TestFlushRollback:
    """ROLLBACK TO SAVEPOINT undoes flush writes."""

    def test_rollback_restores_original_state(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=300, tid=1)

        conn.execute("BEGIN")
        pending = {
            300: {
                "path": "/plone/flushed",
                "idx": {"portal_type": "Page"},
                "searchable_text": None,
            }
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        # Verify flush wrote data
        row = _query_catalog_data(conn, 300)
        assert row["path"] == "/plone/flushed"

        # Roll back
        _rollback_flush_savepoint()

        # Verify data is gone
        row = _query_catalog_data(conn, 300)
        assert row["path"] is None

        conn.execute("COMMIT")


class TestFlushIdempotent:
    """Flush skips if generation hasn't changed."""

    def test_same_gen_skips_flush(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=400, tid=1)

        conn.execute("BEGIN")

        pending = {
            400: {
                "path": "/plone/doc",
                "idx": {"portal_type": "Document"},
                "searchable_text": None,
            }
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)
        assert getattr(_local, "_flush_active", False) is True

        # Roll back first flush
        _rollback_flush_savepoint()
        assert getattr(_local, "_flush_active", False) is False

        # Auto-flush should skip (flushed_gen == -1 after rollback, pending_gen == 1)
        # Actually, _clear_flush_state resets _flushed_gen to -1, so it WILL re-flush
        # Let's test the idempotent case properly:
        _do_flush(conn, pending)
        flushed_gen = getattr(_local, "_flushed_gen", -1)
        assert flushed_gen == 1

        # Now _auto_flush should skip since gen == flushed
        _auto_flush_skips = True

        class MockContext:
            pass

        # Simulate auto_flush check
        gen = getattr(_local, "_pending_gen", 0)
        flushed = getattr(_local, "_flushed_gen", -1)
        assert gen == flushed  # Same generation — would skip

        # Clean up
        _rollback_flush_savepoint()
        conn.execute("COMMIT")


class TestFlushReflush:
    """Flush again after new set_pending() calls."""

    def test_reflush_sees_new_data(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=500, tid=1)

        conn.execute("BEGIN")

        # First flush
        pending = _get_pending()
        pending[500] = {
            "path": "/plone/first",
            "idx": {"portal_type": "Document"},
            "searchable_text": None,
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        row = _query_catalog_data(conn, 500)
        assert row["path"] == "/plone/first"

        # New data
        pending[500] = {
            "path": "/plone/second",
            "idx": {"portal_type": "Page"},
            "searchable_text": None,
        }
        _local._pending_gen = 2

        # Second flush (rolls back first, re-flushes)
        _do_flush(conn, pending)

        row = _query_catalog_data(conn, 500)
        assert row["path"] == "/plone/second"
        assert row["idx"]["portal_type"] == "Page"

        # Clean up
        _rollback_flush_savepoint()
        conn.execute("COMMIT")


class TestAutoFlush:
    """_auto_flush() behavior."""

    def test_no_pending_is_noop(self, pg_conn_with_catalog):
        """auto_flush with empty pending does nothing."""

        class MockContext:
            _p_jar = None

        _auto_flush(MockContext())
        assert getattr(_local, "_flush_active", False) is False

    def test_no_storage_conn_is_noop(self, pg_conn_with_catalog):
        """auto_flush without storage connection does nothing."""
        pending = _get_pending()
        pending[1] = {"path": "/plone/x", "idx": {}, "searchable_text": None}
        _local._pending_gen = 1

        class MockContext:
            _p_jar = None  # No storage connection available

        _auto_flush(MockContext())
        assert getattr(_local, "_flush_active", False) is False


# ---------------------------------------------------------------------------
# Savepoint interaction tests
# ---------------------------------------------------------------------------


class TestSavepointInteraction:
    """PendingSavepoint.rollback() also rolls back PG flush savepoint."""

    def test_savepoint_rollback_clears_flush(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=600, tid=1)

        conn.execute("BEGIN")

        # Set pending data and flush
        pending = _get_pending()
        pending[600] = {
            "path": "/plone/saved",
            "idx": {"portal_type": "Document"},
            "searchable_text": None,
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        # Verify flushed data is visible
        row = _query_catalog_data(conn, 600)
        assert row["path"] == "/plone/saved"

        # Simulate ZODB savepoint (snapshot current pending)
        from plone.pgcatalog.config import PendingSavepoint

        snapshot = dict(pending)
        sp = PendingSavepoint(snapshot)

        # Modify pending after savepoint
        pending[600] = {
            "path": "/plone/modified",
            "idx": {"portal_type": "Page"},
            "searchable_text": None,
        }
        _local._pending_gen = 2

        # Rollback to savepoint — should also rollback PG flush
        sp.rollback()

        # Flush is rolled back
        assert getattr(_local, "_flush_active", False) is False

        # PG data should be back to original (no catalog data)
        row = _query_catalog_data(conn, 600)
        assert row["path"] is None

        # Pending dict should be restored to snapshot
        assert pending[600]["path"] == "/plone/saved"

        conn.execute("COMMIT")

    def test_savepoint_then_new_flush(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=700, tid=1)

        conn.execute("BEGIN")

        # Flush initial data
        pending = _get_pending()
        pending[700] = {
            "path": "/plone/initial",
            "idx": {"portal_type": "Document"},
            "searchable_text": None,
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        # Create savepoint snapshot
        from plone.pgcatalog.config import PendingSavepoint

        sp = PendingSavepoint(dict(pending))

        # Rollback to savepoint
        sp.rollback()

        # Set new pending data
        pending[700] = {
            "path": "/plone/after-rollback",
            "idx": {"portal_type": "Page"},
            "searchable_text": None,
        }
        _local._pending_gen = 2

        # Re-flush with new data
        _do_flush(conn, pending)

        row = _query_catalog_data(conn, 700)
        assert row["path"] == "/plone/after-rollback"
        assert row["idx"]["portal_type"] == "Page"

        # Clean up
        _rollback_flush_savepoint()
        conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# Integration tests: flush + search
# ---------------------------------------------------------------------------


class TestFlushThenSearch:
    """Flush makes data visible to _run_search."""

    def test_flush_data_found_by_search(self, pg_conn_with_catalog):
        from plone.pgcatalog.catalog import _run_search

        conn = pg_conn_with_catalog
        insert_object(conn, zoid=800, tid=1)
        # Pre-set allowedRolesAndUsers so security filter works
        conn.execute(
            "UPDATE object_state SET idx = %(idx)s WHERE zoid = 800",
            {"idx": Json({"allowedRolesAndUsers": ["Anonymous"]})},
        )
        conn.commit()

        conn.execute("BEGIN")

        pending = {
            800: {
                "path": "/plone/searchable",
                "idx": {
                    "portal_type": "Document",
                    "allowedRolesAndUsers": ["Anonymous"],
                    "path": "/plone/searchable",
                    "path_parent": "/plone",
                    "path_depth": 2,
                },
                "searchable_text": None,
            }
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        # Search should find the flushed data
        results = _run_search(conn, {"portal_type": "Document"})
        assert len(results) >= 1
        paths = [b.getPath() for b in results]
        assert "/plone/searchable" in paths

        # Clean up
        _rollback_flush_savepoint()
        conn.execute("COMMIT")


class TestFlushThenAbort:
    """Transaction abort rolls back flush."""

    def test_abort_rolls_back_flush(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=900, tid=1)

        conn.execute("BEGIN")

        pending = {
            900: {
                "path": "/plone/aborted",
                "idx": {"portal_type": "Document"},
                "searchable_text": None,
            }
        }
        _local._pending_gen = 1
        _do_flush(conn, pending)

        # Verify flush wrote data
        row = _query_catalog_data(conn, 900)
        assert row["path"] == "/plone/aborted"

        # Simulate abort: rollback flush + rollback transaction
        _rollback_flush_if_active()
        conn.execute("ROLLBACK")

        # Start fresh transaction to verify
        conn.execute("BEGIN")
        row = _query_catalog_data(conn, 900)
        assert row["path"] is None
        conn.execute("COMMIT")


# ---------------------------------------------------------------------------
# flush_catalog() public API
# ---------------------------------------------------------------------------


class TestFlushCatalogPublicAPI:
    """flush_catalog(context) public API."""

    def test_no_storage_conn_is_noop(self):
        """flush_catalog with context that has no _p_jar is a no-op."""

        class MockContext:
            _p_jar = None

        # Should not raise
        flush_catalog(MockContext())
        assert getattr(_local, "_flush_active", False) is False

    def test_export_available(self):
        """flush_catalog is importable from plone.pgcatalog."""
        from plone.pgcatalog import flush_catalog as fc

        assert fc is flush_catalog


# ---------------------------------------------------------------------------
# _has_new_objects detection
# ---------------------------------------------------------------------------


class TestHasNewObjects:
    """_has_new_objects() detection for create-vs-update flush strategy."""

    def test_all_existing(self, pg_conn_with_catalog):
        """All pending zoids have rows → returns False."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=1100, tid=1)
        insert_object(conn, zoid=1101, tid=1)
        conn.commit()

        pending = {
            1100: {"path": "/plone/a", "idx": {}, "searchable_text": None},
            1101: {"path": "/plone/b", "idx": {}, "searchable_text": None},
        }
        assert _has_new_objects(conn, pending) is False

    def test_some_missing(self, pg_conn_with_catalog):
        """One pending zoid has no row → returns True."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=1200, tid=1)
        conn.commit()

        pending = {
            1200: {"path": "/plone/exists", "idx": {}, "searchable_text": None},
            9999: {"path": "/plone/new", "idx": {}, "searchable_text": None},
        }
        assert _has_new_objects(conn, pending) is True

    def test_all_missing(self, pg_conn_with_catalog):
        """All pending zoids are new → returns True."""
        conn = pg_conn_with_catalog
        pending = {
            8888: {"path": "/plone/new1", "idx": {}, "searchable_text": None},
            8889: {"path": "/plone/new2", "idx": {}, "searchable_text": None},
        }
        assert _has_new_objects(conn, pending) is True

    def test_uncatalog_sentinel_ignored(self, pg_conn_with_catalog):
        """None sentinel (uncatalog) zoids are not checked for existence."""
        conn = pg_conn_with_catalog
        # Only uncatalog entries — no catalog zoids to check
        pending = {7777: None}
        assert _has_new_objects(conn, pending) is False

    def test_mixed_uncatalog_and_existing(self, pg_conn_with_catalog):
        """Uncatalog + existing catalog entries → returns False."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=1300, tid=1)
        conn.commit()

        pending = {
            7776: None,  # uncatalog — ignored
            1300: {"path": "/plone/exists", "idx": {}, "searchable_text": None},
        }
        assert _has_new_objects(conn, pending) is False

    def test_mixed_uncatalog_and_new(self, pg_conn_with_catalog):
        """Uncatalog + new catalog entry → returns True."""
        conn = pg_conn_with_catalog
        pending = {
            7775: None,  # uncatalog — ignored
            9998: {"path": "/plone/new", "idx": {}, "searchable_text": None},
        }
        assert _has_new_objects(conn, pending) is True
