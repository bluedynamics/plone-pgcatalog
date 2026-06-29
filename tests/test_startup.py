"""Tests for plone.pgcatalog startup hooks."""

from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.processor import CatalogStateProcessor
from plone.pgcatalog.startup import _defer_index_actions
from plone.pgcatalog.startup import _index_set_version
from plone.pgcatalog.startup import _make_analyze_object_state_action
from plone.pgcatalog.startup import _schema_sql_version
from plone.pgcatalog.startup import _text_index_targets
from psycopg.types.json import Json
from tests.conftest import DSN

import hashlib


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
        ]

    def test_index_set_version_is_deterministic_and_order_independent(self):
        targets_a = [("b", "B"), ("a", "A")]
        targets_b = [("a", "A"), ("b", "B")]
        assert _index_set_version(targets_a) == _index_set_version(targets_b)
        assert _index_set_version([]).startswith("sha256:")
        expected_empty = "sha256:" + hashlib.sha256(b"").hexdigest()
        assert _index_set_version([]) == expected_empty
