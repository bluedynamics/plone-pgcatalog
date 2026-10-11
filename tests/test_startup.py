"""Tests for plone.pgcatalog startup hooks."""

from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.processor import CatalogStateProcessor
from plone.pgcatalog.startup import _ctype_is_c
from plone.pgcatalog.startup import _defer_index_actions
from plone.pgcatalog.startup import _index_set_version
from plone.pgcatalog.startup import _make_analyze_object_state_action
from plone.pgcatalog.startup import _make_autovacuum_settings_action
from plone.pgcatalog.startup import _schema_sql_version
from plone.pgcatalog.startup import _text_index_targets
from plone.pgcatalog.startup import check_lc_ctype
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Json
from tests.conftest import DSN

import hashlib
import os
import psycopg
import pytest


def _insert_catalog_rows(conn, count=5):
    """Insert minimal object_state rows with idx JSONB so ANALYZE has
    something to populate pg_stats_ext from.  Needs transaction_log
    to satisfy the foreign key.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transaction_log (tid) VALUES (1) ON CONFLICT DO NOTHING"
        )
        for i in range(count):
            cur.execute(
                "INSERT INTO object_state "
                "(zoid, tid, class_mod, class_name, state, state_size, refs, "
                " path, idx) "
                "VALUES (%s, 1, 'mod', 'cls', '{}'::jsonb, 0, '{}'::bigint[], "
                " %s, %s)",
                (
                    i + 100,
                    f"/plone/doc-{i}",
                    Json(
                        {
                            "portal_type": "Document",
                            "review_state": "published",
                            "path": f"/plone/doc-{i}",
                            "path_parent": "/plone",
                            "path_depth": 2,
                            "effective": "2026-01-01T00:00:00+00:00",
                            "expires": "2030-01-01T00:00:00+00:00",
                        }
                    ),
                ),
            )
    conn.commit()


class TestAnalyzeObjectStateAction:
    """Test the deferred ANALYZE object_state startup action."""

    def test_action_populates_pg_stats_ext(self, pg_conn_with_catalog):
        """After ANALYZE, pg_stats_ext has a row for each managed stats
        object once the table has at least one row to analyze.
        """
        _insert_catalog_rows(pg_conn_with_catalog)

        action = _make_analyze_object_state_action()
        action(DSN)

        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                "SELECT statistics_name FROM pg_stats_ext "
                "WHERE tablename = 'object_state' "
                "AND statistics_name = 'stts_os_type_state'"
            )
            row = cur.fetchone()
        assert row is not None, "Expected stats row after ANALYZE on populated table"

    def test_action_skips_when_already_populated(self, pg_conn_with_catalog):
        """Second call skips ANALYZE -- pg_stats_ext.n_distinct is no
        longer NULL, so the skip branch triggers.  Verified behaviourally:
        both calls complete without error.
        """
        _insert_catalog_rows(pg_conn_with_catalog)

        action = _make_analyze_object_state_action()
        action(DSN)  # populates
        action(DSN)  # should take the skip branch and return quickly

    def test_action_is_idempotent_on_empty_table(self, pg_conn_with_catalog):
        """On an empty table ANALYZE is a no-op.  Action must not raise."""
        action = _make_analyze_object_state_action()
        action(DSN)
        action(DSN)


def _reloptions(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reloptions FROM pg_class WHERE oid = 'object_state'::regclass"
        )
        return cur.fetchone()["reloptions"] or []


class TestAutovacuumSettingsAction:
    """Per-table autoanalyze for object_state (#224)."""

    def test_sets_analyze_thresholds(self, pg_conn_with_catalog):
        _make_autovacuum_settings_action()(DSN)

        assert sorted(_reloptions(pg_conn_with_catalog)) == [
            "autovacuum_analyze_scale_factor=0.01",
            "autovacuum_analyze_threshold=5000",
        ]

    def test_keeps_operator_settings(self, pg_conn_with_catalog):
        pg_conn_with_catalog.execute(
            "ALTER TABLE object_state SET (autovacuum_analyze_scale_factor = 0.05)"
        )
        pg_conn_with_catalog.commit()

        _make_autovacuum_settings_action()(DSN)

        assert _reloptions(pg_conn_with_catalog) == [
            "autovacuum_analyze_scale_factor=0.05"
        ]

    def test_gives_up_quickly_when_the_table_is_locked(
        self, pg_conn_with_catalog, caplog
    ):
        """A running autovacuum holds SHARE UPDATE EXCLUSIVE; don't wait."""
        import time

        with psycopg.connect(DSN) as blocker:
            blocker.execute("LOCK TABLE object_state IN SHARE UPDATE EXCLUSIVE MODE")
            started = time.monotonic()
            with caplog.at_level("WARNING", logger="plone.pgcatalog.startup"):
                _make_autovacuum_settings_action()(DSN)
            assert time.monotonic() - started < 10
            blocker.rollback()

        assert _reloptions(pg_conn_with_catalog) == []
        assert "autovacuum" in caplog.text


class _RecordingStorage:
    """Fake storage recording defer_startup_action calls (version-aware)."""

    def __init__(self):
        self.calls = []  # (name, version)

    def defer_startup_action(self, action, name, version=None):
        self.calls.append((name, version))


class _LegacyRecordingStorage:
    """Fake storage whose defer_startup_action predates the version kwarg."""

    def __init__(self):
        self.calls = []  # (name,)

    def defer_startup_action(self, action, name):
        self.calls.append((name,))


class TestVersionTaggedDeferral:
    """The three deferred startup actions must be version-tagged (#78)."""

    def test_all_actions_tagged_with_non_none_version(self):
        storage = _RecordingStorage()
        _defer_index_actions(storage, CatalogStateProcessor())

        names = [name for name, _ in storage.calls]
        assert names == [
            "ensure_text_indexes",
            "ensure_field_indexes",
            "analyze_object_state",
            "autovacuum_settings",
        ]
        assert all(version is not None for _, version in storage.calls)

    def test_analyze_version_tracks_schema_sql(self):
        processor = CatalogStateProcessor()
        storage = _RecordingStorage()
        _defer_index_actions(storage, processor)

        versions = dict(storage.calls)
        assert versions["analyze_object_state"] == _schema_sql_version(processor)

    def test_text_version_changes_when_registry_changes(self):
        before = _index_set_version(_text_index_targets())
        registry = get_registry()
        registry.register("GateExtraText", IndexType.TEXT, "GateExtraText")
        try:
            after = _index_set_version(_text_index_targets())
        finally:
            del registry._indexes["GateExtraText"]
        assert before != after

    def test_legacy_storage_without_version_kwarg_still_works(self):
        storage = _LegacyRecordingStorage()
        _defer_index_actions(storage, CatalogStateProcessor())

        names = [name for (name,) in storage.calls]
        assert names == [
            "ensure_text_indexes",
            "ensure_field_indexes",
            "analyze_object_state",
            "autovacuum_settings",
        ]

    def test_index_set_version_is_deterministic_and_order_independent(self):
        targets_a = [("b", "B"), ("a", "A")]
        targets_b = [("a", "A"), ("b", "B")]
        assert _index_set_version(targets_a) == _index_set_version(targets_b)
        assert _index_set_version([]).startswith("sha256:")
        expected_empty = "sha256:" + hashlib.sha256(b"").hexdigest()
        assert _index_set_version([]) == expected_empty


@pytest.fixture
def c_ctype_dsn():
    """A throw-away database created with LC_CTYPE 'C' (#228)."""
    name = f"pgcat_ctype_c_{os.getpid()}"
    try:
        admin = psycopg.connect(DSN, autocommit=True)
    except psycopg.Error:
        pytest.skip("no database connection")
    with admin:
        admin.execute(f"DROP DATABASE IF EXISTS {name}")
        try:
            admin.execute(
                f"CREATE DATABASE {name} TEMPLATE template0 ENCODING 'UTF8' "
                "LC_COLLATE 'C' LC_CTYPE 'C'"
            )
        except psycopg.Error as exc:
            pytest.skip(f"cannot create a test database: {exc}")
        try:
            yield make_conninfo(DSN, dbname=name)
        finally:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


class TestLcCtypeCheck:
    def test_c_ctype_database_is_reported(self, c_ctype_dsn, caplog):
        with caplog.at_level("ERROR", logger="plone.pgcatalog.startup"):
            assert check_lc_ctype(c_ctype_dsn) == "C"
        assert "lc_ctype" in caplog.text
        assert "C.UTF-8" in caplog.text

    def test_c_ctype_really_glues_punctuation(self, c_ctype_dsn):
        """Why the check exists: the parser keeps '»' in the lexeme."""
        with psycopg.connect(c_ctype_dsn) as conn:
            (vector,) = conn.execute(
                "SELECT to_tsvector('simple', 'Impulswochen »technik')::text"
            ).fetchone()
        assert "'»technik'" in vector

    def test_regular_database_passes(self, caplog):
        with caplog.at_level("ERROR", logger="plone.pgcatalog.startup"):
            assert check_lc_ctype(DSN) is None
        assert caplog.text == ""

    def test_posix_counts_as_c(self):
        assert _ctype_is_c("POSIX")
        assert _ctype_is_c("C")
        assert not _ctype_is_c("C.UTF-8")
        assert not _ctype_is_c("en_US.utf8")

    def test_unreachable_database_only_warns(self, caplog):
        with caplog.at_level("WARNING", logger="plone.pgcatalog.startup"):
            assert check_lc_ctype("host=127.0.0.1 port=1 connect_timeout=1") is None
        assert "lc_ctype" in caplog.text
