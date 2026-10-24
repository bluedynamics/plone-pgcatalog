"""Unit tests for plone.pgcatalog.undo (#265) that need no Plone layer.

The undo flows themselves are tested on a history-preserving storage in
test_undo_integration.py.
"""

from plone.pgcatalog import maintenance
from plone.pgcatalog import undo
from plone.pgcatalog.schema import UNDO_QUEUE
from psycopg.types.json import Json
from tests.conftest import DSN
from tests.conftest import insert_object
from unittest import mock

import logging
import pytest


FOLDER = ("plone.app.contenttypes.content", "Folder")
BUCKET = ("BTrees.OOBTree", "OOBucket")


def _catalog_row(conn, zoid, path, cls=FOLDER, roles=("Anonymous",)):
    insert_object(conn, zoid, class_mod=cls[0], class_name=cls[1])
    if path is not None:
        conn.execute(
            "UPDATE object_state SET path = %s, idx = %s, allowed_roles = %s "
            "WHERE zoid = %s",
            (path, Json({"Title": path}), list(roles), zoid),
        )
    conn.commit()


def _rows(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT zoid, path, idx IS NOT NULL AS has_idx, allowed_roles "
            "FROM object_state ORDER BY zoid"
        )
        return {
            row["zoid"]: (row["path"], row["has_idx"], row["allowed_roles"])
            for row in cur.fetchall()
        }


def _queue(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kind, zoid, path, undone_tid FROM pgcatalog_undo_queue ORDER BY id"
        )
        return [tuple(row.values()) for row in cur.fetchall()]


@pytest.fixture
def undo_db(pg_conn_with_catalog):
    conn = pg_conn_with_catalog
    conn.execute("DELETE FROM pgcatalog_undo_queue")
    conn.commit()
    undo._local.followup_pending = False
    return conn


class TestHideUndone:
    def test_hides_restored_and_the_subtree(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 1, "/plone/f")
        _catalog_row(conn, 2, "/plone/f/a")
        _catalog_row(conn, 3, "/plone/f/a/b")
        _catalog_row(conn, 4, "/plone/other")
        _catalog_row(conn, 5, None, cls=BUCKET)

        with conn.cursor() as cur:
            assert undo.hide_undone(cur, 77, [1, 5], []) is True
        conn.commit()

        rows = _rows(conn)
        assert rows[1] == (None, False, None)
        assert rows[2] == ("/plone/f/a", True, None)
        assert rows[3] == ("/plone/f/a/b", True, None)
        assert rows[4] == ("/plone/other", True, ["Anonymous"])
        assert rows[5] == (None, False, None)
        assert _queue(conn) == [
            ("recatalog", 1, "/plone/f", 77),
            ("security", 2, "/plone/f/a", 77),
            ("security", 3, "/plone/f/a/b", 77),
        ]
        assert undo._local.followup_pending is True

    def test_subtree_of_a_deleted_object(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 1, "/plone/new")
        _catalog_row(conn, 2, "/plone/new/later")

        with conn.cursor() as cur:
            undo.hide_undone(cur, 5, [], [1])
        conn.commit()

        # the deleted row itself goes away with the undo's write
        assert _rows(conn)[1] == ("/plone/new", True, ["Anonymous"])
        assert _rows(conn)[2] == ("/plone/new/later", True, None)
        assert _queue(conn) == [("security", 2, "/plone/new/later", 5)]

    def test_like_wildcards_in_paths(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 1, "/plone/a_b")
        _catalog_row(conn, 2, "/plone/a_b/in")
        _catalog_row(conn, 3, "/plone/aXb/out")
        _catalog_row(conn, 4, "/plone/100%")
        _catalog_row(conn, 5, "/plone/100%/in")
        _catalog_row(conn, 6, "/plone/1000/out")

        with conn.cursor() as cur:
            undo.hide_undone(cur, 9, [1, 4], [])
        conn.commit()

        assert sorted(zoid for _, zoid, _, _ in _queue(conn)) == [1, 2, 4, 5]
        assert _rows(conn)[3][2] == ["Anonymous"]
        assert _rows(conn)[6][2] == ["Anonymous"]

    def test_a_later_undo_of_the_same_object_wins(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 1, "/plone/f")
        _catalog_row(conn, 2, "/plone/f/a")
        with conn.cursor() as cur:
            undo.hide_undone(cur, 10, [1], [])
            # second tid of the same undoMultiple: the row is hidden already
            undo.hide_undone(cur, 11, [1], [])
        conn.commit()

        assert _queue(conn) == [
            ("recatalog", 1, "/plone/f", 11),
            ("security", 2, "/plone/f/a", 10),
        ]

    def test_nothing_catalogable(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 5, None, cls=BUCKET)
        with conn.cursor() as cur:
            assert undo.hide_undone(cur, 1, [5], []) is False
            assert undo.hide_undone(cur, 1, [], []) is False
        assert _queue(conn) == []
        assert undo._local.followup_pending is False

    def test_processor_hook_flags_the_change_counter(self, undo_db):
        from plone.pgcatalog.processor import CatalogStateProcessor

        _catalog_row(undo_db, 1, "/plone/f")
        processor = CatalogStateProcessor()
        with undo_db.cursor() as cur:
            processor.undone(cur, 3, [1], [], {})
        assert processor._catalog_changed is True

    def test_processor_hook_without_changes(self, undo_db):
        from plone.pgcatalog.processor import CatalogStateProcessor

        processor = CatalogStateProcessor()
        with undo_db.cursor() as cur:
            processor.undone(cur, 3, [], [], {})
        assert processor._catalog_changed is False


def _rev(state, refs=(), cls=FOLDER):
    return {
        "class_mod": cls[0],
        "class_name": cls[1],
        "state": state,
        "refs": list(refs),
    }


def _entry(before, after, action="restore"):
    return {"action": action, "before": before, "after": after}


BASE_STATE = {
    "id": "f",
    "title": "Folder",
    "modification_date": {"@dt": "2026-10-12T10:00:00"},
    "_tree": {"@ref": "0000000000000064"},
}


def _decide(zoid, entries):
    return undo.needs_subtree_rule(zoid, entries, undo._relocated(entries))


class TestNeedsSubtreeRule:
    """The conservative rule: when undoing a root's change hides its subtree."""

    def _with(self, **changes):
        after = {**BASE_STATE, **changes}
        return {1: _entry(_rev(BASE_STATE), _rev(after))}

    def test_harmless_keys_only(self):
        entries = self._with(
            modification_date={"@dt": "2026-10-12T09:00:00"}, cmf_uid=7
        )
        assert _decide(1, entries) is False

    def test_identical_states(self):
        assert _decide(1, self._with()) is False

    @pytest.mark.parametrize(
        "changes",
        [
            {"__ac_local_roles__": {"bob": ["Reader"]}},
            {"__ac_local_roles_block__": True},
            {"_Access_contents_information_Permission": ["Manager"]},
            {"_View_Permission": {"@t": ["Manager"]}},
            {"__annotations__": {"@ref": "00000000000000c8"}},
            {"title": "Other title"},
            {"id": "renamed"},
        ],
    )
    def test_state_changes_trigger(self, changes):
        assert _decide(1, self._with(**changes)) is True

    def test_acquire_flag_alone_triggers(self):
        roles = ["Manager", "Reader"]
        before = {**BASE_STATE, "_View_Permission": {"@t": roles}}
        after = {**BASE_STATE, "_View_Permission": roles}
        assert _decide(1, {1: _entry(_rev(before), _rev(after))}) is True

    def test_removed_key_triggers(self):
        before = {**BASE_STATE, "__ac_local_roles_block__": True}
        assert _decide(1, {1: _entry(_rev(before), _rev(BASE_STATE))}) is True

    def test_class_change_triggers(self):
        after = _rev(BASE_STATE, cls=("plone.app.contenttypes.content", "Document"))
        assert _decide(1, {1: _entry(_rev(BASE_STATE), after)}) is True

    def test_relocation_triggers(self):
        """Same-id move: the object's state is identical, the buckets differ."""
        entries = {
            1: _entry(_rev(BASE_STATE), _rev(BASE_STATE)),
            50: _entry(
                _rev({}, refs=[1, 2], cls=BUCKET), _rev({}, refs=[2], cls=BUCKET)
            ),
            60: _entry(
                _rev({}, refs=[3], cls=BUCKET), _rev({}, refs=[1, 3], cls=BUCKET)
            ),
        }
        assert _decide(1, entries) is True
        assert undo._relocated(entries) == {1}

    def test_reference_by_a_deleted_object_counts_as_relocation(self):
        entries = {
            1: _entry(_rev(BASE_STATE), _rev(BASE_STATE)),
            9: _entry(_rev({}, refs=[1], cls=BUCKET), None, action="delete"),
        }
        assert _decide(1, entries) is True

    @pytest.mark.parametrize(
        "entries",
        [
            {},  # no entry for the root
            {1: _entry(_rev(BASE_STATE), None, action="delete")},
            {1: _entry(_rev("not a dict"), _rev(BASE_STATE))},
            {1: _entry(_rev(BASE_STATE), _rev(None))},
            {1: {"action": "restore"}},
        ],
    )
    def test_undecidable_falls_back_to_the_rule(self, entries):
        assert _decide(1, entries) is True

    def test_unreadable_entries_fall_back_to_the_rule(self):
        broken = {1: {"before": {"refs": None}, "after": None}}
        assert undo._relocated(broken) is None
        assert undo.needs_subtree_rule(1, broken, None) is True

    def test_no_entries_falls_back_to_the_rule(self):
        assert undo.needs_subtree_rule(1, None, None) is True


class TestHideUndoneWithEntries:
    def test_container_written_for_its_modification_date(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 1, "/plone/f")
        _catalog_row(conn, 2, "/plone/f/a")
        entries = {
            1: _entry(
                _rev(BASE_STATE),
                _rev({**BASE_STATE, "modification_date": {"@dt": "x"}}),
            )
        }

        with conn.cursor() as cur:
            assert undo.hide_undone(cur, 7, [1], [], entries) is True
        conn.commit()

        assert _rows(conn)[1] == (None, False, None)
        assert _rows(conn)[2] == ("/plone/f/a", True, ["Anonymous"])
        assert _queue(conn) == [("recatalog", 1, "/plone/f", 7)]

    def test_deleted_roots_keep_the_rule(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 1, "/plone/new")
        _catalog_row(conn, 2, "/plone/new/later")
        entries = {1: _entry(_rev(BASE_STATE), None, action="delete")}

        with conn.cursor() as cur:
            undo.hide_undone(cur, 5, [], [1], entries)
        conn.commit()

        assert _queue(conn) == [("security", 2, "/plone/new/later", 5)]

    def test_recatalog_rows_in_path_order(self, undo_db):
        conn = undo_db
        _catalog_row(conn, 3, "/plone/f/a")
        _catalog_row(conn, 1, "/plone/f")
        _catalog_row(conn, 2, None, cls=("plone.app.contenttypes.content", "Document"))
        entries = {z: _entry(_rev(BASE_STATE), _rev(BASE_STATE)) for z in (1, 2, 3)}

        with conn.cursor() as cur:
            undo.hide_undone(cur, 4, [2, 3, 1], [], entries)

        assert [zoid for _, zoid, _, _ in _queue(conn)] == [1, 3, 2]


class TestSubtreeRoots:
    def test_nested_roots_collapse(self):
        assert undo._subtree_roots(
            ["/plone/a/b", "/plone/a", "/plone/ab", "/plone/a", "/plone/c/d"]
        ) == ["/plone/a", "/plone/ab", "/plone/c/d"]


class TestQueueLength:
    def test_counts(self, undo_db):
        undo_db.execute(
            "INSERT INTO pgcatalog_undo_queue (kind, zoid, undone_tid) "
            "VALUES ('security', 1, 1)"
        )
        assert undo.queue_length(undo_db) == 1

    def test_no_table(self, undo_db):
        undo_db.execute("DROP TABLE pgcatalog_undo_queue")
        try:
            assert undo.queue_length(undo_db) is None
        finally:
            undo_db.execute(UNDO_QUEUE)
            undo_db.commit()


class TestWarnIfQueueNotEmpty:
    def test_warns_with_count(self, undo_db, caplog):
        undo_db.execute(
            "INSERT INTO pgcatalog_undo_queue (kind, zoid, undone_tid) "
            "VALUES ('security', 1, 1), ('recatalog', 2, 1)"
        )
        undo_db.commit()
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.undo"):
            assert undo.warn_if_queue_not_empty(DSN) == 2
        assert "2 catalog rows hidden by an undo" in caplog.text

    def test_quiet_when_empty(self, undo_db, caplog):
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.undo"):
            assert undo.warn_if_queue_not_empty(DSN) == 0
        assert caplog.text == ""

    def test_unreachable_database(self):
        assert undo.warn_if_queue_not_empty("host=/nonexistent dbname=x") is None


class _Storage:
    def __init__(self, history_preserving):
        self._history_preserving = history_preserving

    def supportsUndo(self):
        return self._history_preserving


class TestCheckHookSupport:
    @pytest.mark.parametrize(
        ("version", "covered"),
        [("1.18.0", True), ("1.19.2", True), ("2.0.0", True), ("1.17.1", False)],
    )
    def test_history_preserving(self, monkeypatch, caplog, version, covered):
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: version)
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.undo"):
            assert undo.check_hook_support(_Storage(True)) is covered
        assert ("has no undone() hook" in caplog.text) is not covered

    def test_history_free_needs_no_hook(self, monkeypatch, caplog):
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: "1.16.0")
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.undo"):
            assert undo.check_hook_support(_Storage(False)) is False
        assert caplog.text == ""

    def test_storage_without_undo_api(self):
        assert undo.check_hook_support(object()) is False


class TestStartupWiring:
    def test_register_runs_the_undo_checks(self):
        from plone.pgcatalog.startup import register_catalog_processor

        event = mock.Mock()
        storage = mock.Mock()
        storage._dsn = "dbname=x"
        event.database.storage = storage
        del storage._main

        with (
            mock.patch("plone.pgcatalog.startup._sync_registry_from_db"),
            mock.patch("plone.pgcatalog.startup.check_lc_ctype"),
            mock.patch("plone.pgcatalog.startup.detect_and_set_backend"),
            mock.patch.object(undo, "warn_if_queue_not_empty") as warn,
            mock.patch.object(undo, "check_hook_support") as check,
        ):
            register_catalog_processor(event)

        warn.assert_called_once_with("dbname=x")
        check.assert_called_once_with(storage)


class TestSubscriberRegistration:
    def test_zcml_registers_the_ipubsuccess_subscriber(self):
        from pathlib import Path

        import plone.pgcatalog

        zcml = (Path(plone.pgcatalog.__file__).parent / "configure.zcml").read_text()
        assert 'for="ZPublisher.interfaces.IPubSuccess"' in zcml
        assert 'handler=".undo.drain_after_undo"' in zcml
