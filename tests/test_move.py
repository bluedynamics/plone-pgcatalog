"""Unit tests for move/rename optimization.

Tests the bulk SQL path update, move context stack, and pending moves store.
Written TDD-style: these tests are written BEFORE the implementation.
"""

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.pending import _local
from tests.conftest import insert_object

import transaction


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_tree(conn):
    """Create a tree of cataloged objects for move testing.

    Tree structure:
        /plone                          (zoid=100)
        /plone/source                   (zoid=101)
        /plone/source/doc-a             (zoid=102)
        /plone/source/sub               (zoid=103)
        /plone/source/sub/deep-doc      (zoid=104)
        /plone/target                   (zoid=105)
        /plone/target/doc-b             (zoid=106)
    """
    tree = [
        (100, "/plone", {"Title": "Plone Site", "portal_type": "Plone Site"}),
        (101, "/plone/source", {"Title": "Source Folder", "portal_type": "Folder"}),
        (102, "/plone/source/doc-a", {"Title": "Doc A", "portal_type": "Document"}),
        (103, "/plone/source/sub", {"Title": "Subfolder", "portal_type": "Folder"}),
        (
            104,
            "/plone/source/sub/deep-doc",
            {"Title": "Deep Doc", "portal_type": "Document"},
        ),
        (105, "/plone/target", {"Title": "Target Folder", "portal_type": "Folder"}),
        (106, "/plone/target/doc-b", {"Title": "Doc B", "portal_type": "Document"}),
    ]
    for zoid, path, idx in tree:
        insert_object(conn, zoid=zoid)
        catalog_object(conn, zoid=zoid, path=path, idx=idx)
    conn.commit()


def _get_row(conn, zoid):
    """Fetch path, parent_path, path_depth, and idx for a zoid."""
    row = conn.execute(
        "SELECT path, parent_path, path_depth, idx FROM object_state WHERE zoid = %s",
        (zoid,),
    ).fetchone()
    return row


def _execute_bulk_path_update(conn, old_prefix, new_prefix, depth_delta):
    """Execute the bulk path update SQL (same as processor.finalize will use)."""
    conn.execute(
        """
        UPDATE object_state SET
            path = %(new)s || substring(path FROM length(%(old)s) + 1),
            parent_path = %(new)s || substring(parent_path FROM length(%(old)s) + 1),
            path_depth = path_depth + %(dd)s,
            idx = idx || jsonb_build_object(
                'path',
                %(new)s || substring(idx->>'path' FROM length(%(old)s) + 1),
                'path_parent',
                %(new)s || substring(idx->>'path_parent' FROM length(%(old)s) + 1),
                'path_depth',
                (idx->>'path_depth')::int + %(dd)s
            )
        WHERE path LIKE %(like)s
          AND idx IS NOT NULL
        """,
        {
            "old": old_prefix,
            "new": new_prefix,
            "dd": depth_delta,
            "like": old_prefix + "/%",
        },
    )
    conn.commit()


# ===========================================================================
# B1. Bulk SQL path update tests
# ===========================================================================


class TestBulkPathUpdateRename:
    """Test SQL: rename updates path/parent_path/idx for descendants."""

    def test_rename_updates_child_path(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        # Rename /plone/source -> /plone/source-renamed (depth_delta=0)
        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 102)  # was /plone/source/doc-a
        assert row["path"] == "/plone/source-renamed/doc-a"
        assert row["parent_path"] == "/plone/source-renamed"
        assert row["path_depth"] == 3  # unchanged

    def test_rename_updates_child_idx(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 102)
        assert row["idx"]["path"] == "/plone/source-renamed/doc-a"
        assert row["idx"]["path_parent"] == "/plone/source-renamed"
        assert row["idx"]["path_depth"] == 3

    def test_rename_updates_deep_descendant(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 104)  # was /plone/source/sub/deep-doc
        assert row["path"] == "/plone/source-renamed/sub/deep-doc"
        assert row["parent_path"] == "/plone/source-renamed/sub"
        assert row["idx"]["path"] == "/plone/source-renamed/sub/deep-doc"
        assert row["idx"]["path_parent"] == "/plone/source-renamed/sub"

    def test_rename_updates_intermediate_folder(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 103)  # was /plone/source/sub
        assert row["path"] == "/plone/source-renamed/sub"
        assert row["parent_path"] == "/plone/source-renamed"


class TestBulkPathUpdateCrossContainer:
    """Test SQL: move to different depth updates path_depth."""

    def test_cross_container_updates_paths(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        # Move /plone/source -> /plone/target/source (depth increases by 1)
        _execute_bulk_path_update(conn, "/plone/source", "/plone/target/source", 1)

        row = _get_row(conn, 102)  # was /plone/source/doc-a
        assert row["path"] == "/plone/target/source/doc-a"
        assert row["parent_path"] == "/plone/target/source"
        assert row["path_depth"] == 4  # was 3, +1

    def test_cross_container_updates_deep_depth(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/target/source", 1)

        row = _get_row(conn, 104)  # was /plone/source/sub/deep-doc
        assert row["path"] == "/plone/target/source/sub/deep-doc"
        assert row["path_depth"] == 5  # was 4, +1
        assert row["idx"]["path_depth"] == 5

    def test_move_up_decreases_depth(self, pg_conn_with_catalog):
        """Move from /plone/source/sub to /plone/sub (depth decreases by 1)."""
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source/sub", "/plone/sub", -1)

        row = _get_row(conn, 104)  # was /plone/source/sub/deep-doc
        assert row["path"] == "/plone/sub/deep-doc"
        assert row["path_depth"] == 3  # was 4, -1


class TestBulkPathUpdatePreservesOtherIdx:
    """Test SQL: non-path idx keys survive the JSONB merge."""

    def test_title_preserved(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 102)
        assert row["idx"]["Title"] == "Doc A"
        assert row["idx"]["portal_type"] == "Document"

    def test_all_idx_keys_preserved(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 103)
        assert row["idx"]["Title"] == "Subfolder"
        assert row["idx"]["portal_type"] == "Folder"
        # Path keys updated
        assert row["idx"]["path"] == "/plone/source-renamed/sub"


class TestParentNotMatchedByLike:
    """Test SQL: parent row excluded from bulk UPDATE."""

    def test_parent_unchanged(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        # Update descendants of /plone/source — should NOT touch /plone/source itself
        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 101)  # /plone/source (the parent)
        assert row["path"] == "/plone/source"  # unchanged!
        assert row["idx"]["path"] == "/plone/source"

    def test_sibling_unchanged(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 105)  # /plone/target — sibling, not descendant
        assert row["path"] == "/plone/target"

    def test_other_subtree_unchanged(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _execute_bulk_path_update(conn, "/plone/source", "/plone/source-renamed", 0)

        row = _get_row(conn, 106)  # /plone/target/doc-b
        assert row["path"] == "/plone/target/doc-b"


# ===========================================================================
# B1.5 Move context stack tests
# ===========================================================================


class TestMoveContextStack:
    """Test push/pop lifecycle and is_move_in_progress()."""

    def setup_method(self):
        """Clear any leftover state."""
        try:
            del _local.move_context_stack
        except AttributeError:
            pass

    def test_initially_not_in_progress(self):
        from plone.pgcatalog.move import is_move_in_progress

        assert is_move_in_progress() is False

    def test_push_sets_in_progress(self):
        from plone.pgcatalog.move import _push_move_context
        from plone.pgcatalog.move import is_move_in_progress
        from plone.pgcatalog.move import MoveContext

        _push_move_context(MoveContext(old_prefix="/plone/source", event_object=None))
        assert is_move_in_progress() is True

    def test_pop_clears_in_progress(self):
        from plone.pgcatalog.move import _pop_move_context
        from plone.pgcatalog.move import _push_move_context
        from plone.pgcatalog.move import is_move_in_progress
        from plone.pgcatalog.move import MoveContext

        _push_move_context(MoveContext(old_prefix="/plone/source", event_object=None))
        _pop_move_context()
        assert is_move_in_progress() is False

    def test_nesting_preserves_outer(self):
        from plone.pgcatalog.move import _pop_move_context
        from plone.pgcatalog.move import _push_move_context
        from plone.pgcatalog.move import is_move_in_progress
        from plone.pgcatalog.move import MoveContext

        _push_move_context(MoveContext(old_prefix="/plone/outer", event_object=None))
        _push_move_context(MoveContext(old_prefix="/plone/inner", event_object=None))
        assert is_move_in_progress() is True

        _pop_move_context()  # pop inner
        assert is_move_in_progress() is True  # outer still active

        _pop_move_context()  # pop outer
        assert is_move_in_progress() is False


# ===========================================================================
# B1.6 Pending moves store tests
# ===========================================================================


class TestPendingMoves:
    """Test add_pending_move/pop_all_pending_moves."""

    def setup_method(self):
        """Clear any leftover pending moves."""
        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

    def test_add_and_pop(self):
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        transaction.begin()
        add_pending_move("/plone/old", "/plone/new", 0)
        result = pop_all_pending_moves()
        assert result == [("/plone/old", "/plone/new", 0)]
        transaction.abort()

    def test_pop_clears(self):
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        transaction.begin()
        add_pending_move("/plone/old", "/plone/new", 0)
        pop_all_pending_moves()  # first pop
        result = pop_all_pending_moves()  # second pop
        assert result == []
        transaction.abort()

    def test_multiple_moves_preserve_order(self):
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        transaction.begin()
        add_pending_move("/plone/a", "/plone/a-new", 0)
        add_pending_move("/plone/b", "/plone/b-new", 0)
        add_pending_move("/plone/c", "/plone/c-new", 1)
        result = pop_all_pending_moves()
        assert len(result) == 3
        assert result[0] == ("/plone/a", "/plone/a-new", 0)
        assert result[1] == ("/plone/b", "/plone/b-new", 0)
        assert result[2] == ("/plone/c", "/plone/c-new", 1)
        transaction.abort()


class TestPendingMovesSavepoint:
    """Test savepoint restores pending_moves."""

    def test_savepoint_rollback_restores_moves(self):
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        txn = transaction.begin()
        add_pending_move("/plone/a", "/plone/a-new", 0)

        sp = txn.savepoint()
        add_pending_move("/plone/b", "/plone/b-new", 0)

        sp.rollback()
        result = pop_all_pending_moves()
        assert result == [("/plone/a", "/plone/a-new", 0)]
        transaction.abort()


class TestPendingMovesTransactionClear:
    """Test abort/finish clears pending_moves."""

    def test_abort_clears(self):
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        transaction.begin()
        add_pending_move("/plone/old", "/plone/new", 0)
        transaction.abort()

        result = pop_all_pending_moves()
        assert result == []

    def test_finish_clears(self):
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        txn = transaction.begin()
        add_pending_move("/plone/old", "/plone/new", 0)
        # Commit the transaction (tpc_finish clears pending)
        txn.commit()

        result = pop_all_pending_moves()
        assert result == []
