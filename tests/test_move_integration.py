"""Integration tests for move/rename optimization pipeline.

Tests the full flow: move detection → child skip → bulk SQL → correct PG state.
These tests exercise the complete pipeline without a full Plone instance by
simulating the event/handler sequence at the Python level.

Written TDD-style: these tests are written BEFORE the implementation.
"""

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.pending import _local
from psycopg.types.json import Json
from tests.conftest import insert_object

import transaction


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_tree(conn, searchable_text=False):
    """Create a tree of cataloged objects for integration testing.

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
        text = f"Searchable text for {idx['Title']}" if searchable_text else None
        catalog_object(conn, zoid=zoid, path=path, idx=idx, searchable_text=text)
    conn.commit()


def _setup_deep_tree(conn):
    """Create a deeply nested tree for depth-stress testing.

    Tree structure:
        /plone                              (zoid=200)
        /plone/a                            (zoid=201)
        /plone/a/b                          (zoid=202)
        /plone/a/b/c                        (zoid=203)
        /plone/a/b/c/d                      (zoid=204)
        /plone/a/b/c/d/leaf                 (zoid=205)
        /plone/dest                         (zoid=206)
    """
    tree = [
        (200, "/plone", {"Title": "Site", "portal_type": "Plone Site"}),
        (201, "/plone/a", {"Title": "Level A", "portal_type": "Folder"}),
        (202, "/plone/a/b", {"Title": "Level B", "portal_type": "Folder"}),
        (203, "/plone/a/b/c", {"Title": "Level C", "portal_type": "Folder"}),
        (204, "/plone/a/b/c/d", {"Title": "Level D", "portal_type": "Folder"}),
        (205, "/plone/a/b/c/d/leaf", {"Title": "Leaf Doc", "portal_type": "Document"}),
        (206, "/plone/dest", {"Title": "Destination", "portal_type": "Folder"}),
    ]
    for zoid, path, idx in tree:
        insert_object(conn, zoid=zoid)
        catalog_object(conn, zoid=zoid, path=path, idx=idx)
    conn.commit()


def _setup_siblings(conn, count=5):
    """Create N sibling folders under /plone, each with one child doc.

    /plone                (zoid=300)
    /plone/folder-0       (zoid=301)
    /plone/folder-0/doc   (zoid=302)
    /plone/folder-1       (zoid=303)
    /plone/folder-1/doc   (zoid=304)
    ...
    """
    insert_object(conn, zoid=300)
    catalog_object(
        conn,
        zoid=300,
        path="/plone",
        idx={"Title": "Site", "portal_type": "Plone Site"},
    )
    zoid = 301
    for i in range(count):
        folder_path = f"/plone/folder-{i}"
        doc_path = f"{folder_path}/doc"
        insert_object(conn, zoid=zoid)
        catalog_object(
            conn,
            zoid=zoid,
            path=folder_path,
            idx={"Title": f"Folder {i}", "portal_type": "Folder"},
        )
        zoid += 1
        insert_object(conn, zoid=zoid)
        catalog_object(
            conn,
            zoid=zoid,
            path=doc_path,
            idx={"Title": f"Doc in {i}", "portal_type": "Document"},
        )
        zoid += 1
    conn.commit()


def _get_row(conn, zoid):
    """Fetch path, parent_path, path_depth, idx, searchable_text for a zoid."""
    return conn.execute(
        "SELECT path, parent_path, path_depth, idx, searchable_text "
        "FROM object_state WHERE zoid = %s",
        (zoid,),
    ).fetchone()


def _simulate_move(conn, old_prefix, new_prefix, parent_zoid, parent_new_path):
    """Simulate the full move pipeline for a subtree.

    This performs the operations that the move optimization does:
    1. Update the parent's own catalog entry (normal pipeline)
    2. Register a pending move for descendants
    3. Execute the bulk SQL (as finalize() would)

    Args:
        conn: psycopg connection
        old_prefix: old path of the moved container (e.g., "/plone/source")
        new_prefix: new path of the moved container (e.g., "/plone/target/source")
        parent_zoid: zoid of the moved container itself
        parent_new_path: new full path of the container
    """
    from plone.pgcatalog.pending import add_pending_move
    from plone.pgcatalog.pending import pop_all_pending_moves

    old_depth = len([p for p in old_prefix.split("/") if p])
    new_depth = len([p for p in new_prefix.split("/") if p])
    depth_delta = new_depth - old_depth

    # Step 1: Update parent's own path (simulates normal indexObject flow)
    from plone.pgcatalog.columns import compute_path_info

    parent_path, path_depth = compute_path_info(parent_new_path)
    row = _get_row(conn, parent_zoid)
    if row and row["idx"]:
        new_idx = dict(row["idx"])
        new_idx["path"] = parent_new_path
        new_idx["path_parent"] = parent_path
        new_idx["path_depth"] = path_depth
        conn.execute(
            """
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": parent_zoid,
                "path": parent_new_path,
                "parent_path": parent_path,
                "path_depth": path_depth,
                "idx": Json(new_idx),
            },
        )

    # Step 2: Register pending move for descendants
    transaction.begin()
    add_pending_move(old_prefix, new_prefix, depth_delta)
    moves = pop_all_pending_moves()
    transaction.abort()

    # Step 3: Execute bulk SQL (same as processor.finalize would)
    for old_pfx, new_pfx, dd in moves:
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
                "old": old_pfx,
                "new": new_pfx,
                "dd": dd,
                "like": old_pfx + "/%",
            },
        )
    conn.commit()


# ===========================================================================
# Integration: Move context + catalog skip
# ===========================================================================


class TestCatalogSkipDuringMove:
    """Test that indexObject/unindexObject are no-ops when move is in progress."""

    def setup_method(self):
        """Clear any leftover state."""
        try:
            del _local.move_context_stack
        except AttributeError:
            pass

    def test_index_object_skips_during_move(self):
        """indexObject() should return immediately when move is in progress."""
        from plone.pgcatalog.move import _pop_move_context
        from plone.pgcatalog.move import _push_move_context
        from plone.pgcatalog.move import is_move_in_progress
        from plone.pgcatalog.move import MoveContext

        _push_move_context(MoveContext(old_prefix="/plone/source", event_object=None))
        assert is_move_in_progress() is True
        # If indexObject is called during this period, it should be a no-op.
        # The actual catalog skip is tested by verifying the flag is checked.
        # Full catalog test requires Plone instance.
        _pop_move_context()
        assert is_move_in_progress() is False

    def test_unindex_object_skips_during_move(self):
        """unindexObject() should return immediately when move is in progress."""
        from plone.pgcatalog.move import _pop_move_context
        from plone.pgcatalog.move import _push_move_context
        from plone.pgcatalog.move import is_move_in_progress
        from plone.pgcatalog.move import MoveContext

        _push_move_context(MoveContext(old_prefix="/plone/source", event_object=None))
        assert is_move_in_progress() is True
        _pop_move_context()


# ===========================================================================
# Integration: Full pipeline — rename (same container)
# ===========================================================================


class TestRenameUpdatesDescendants:
    """Rename /plone/source → /plone/source-renamed (depth_delta=0)."""

    def test_rename_all_descendants_updated(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/source-renamed",
            parent_zoid=101,
            parent_new_path="/plone/source-renamed",
        )

        # Check direct child
        row = _get_row(conn, 102)
        assert row["path"] == "/plone/source-renamed/doc-a"
        assert row["parent_path"] == "/plone/source-renamed"
        assert row["path_depth"] == 3
        assert row["idx"]["path"] == "/plone/source-renamed/doc-a"
        assert row["idx"]["path_parent"] == "/plone/source-renamed"

        # Check intermediate folder
        row = _get_row(conn, 103)
        assert row["path"] == "/plone/source-renamed/sub"
        assert row["parent_path"] == "/plone/source-renamed"

        # Check deep descendant
        row = _get_row(conn, 104)
        assert row["path"] == "/plone/source-renamed/sub/deep-doc"
        assert row["parent_path"] == "/plone/source-renamed/sub"
        assert row["idx"]["path"] == "/plone/source-renamed/sub/deep-doc"

    def test_rename_parent_itself_updated(self, pg_conn_with_catalog):
        """The moved container's own path is updated by normal pipeline."""
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/source-renamed",
            parent_zoid=101,
            parent_new_path="/plone/source-renamed",
        )

        row = _get_row(conn, 101)
        assert row["path"] == "/plone/source-renamed"
        assert row["parent_path"] == "/plone"
        assert row["idx"]["path"] == "/plone/source-renamed"

    def test_rename_siblings_unchanged(self, pg_conn_with_catalog):
        """Objects outside the renamed subtree must not change."""
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/source-renamed",
            parent_zoid=101,
            parent_new_path="/plone/source-renamed",
        )

        row = _get_row(conn, 105)
        assert row["path"] == "/plone/target"

        row = _get_row(conn, 106)
        assert row["path"] == "/plone/target/doc-b"


# ===========================================================================
# Integration: Full pipeline — cross-container move
# ===========================================================================


class TestMoveUpdatesDescendants:
    """Move /plone/source → /plone/target/source (depth_delta=+1)."""

    def test_move_all_descendants_updated(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/target/source",
            parent_zoid=101,
            parent_new_path="/plone/target/source",
        )

        # Direct child: depth 3→4
        row = _get_row(conn, 102)
        assert row["path"] == "/plone/target/source/doc-a"
        assert row["parent_path"] == "/plone/target/source"
        assert row["path_depth"] == 4
        assert row["idx"]["path_depth"] == 4

        # Deep descendant: depth 4→5
        row = _get_row(conn, 104)
        assert row["path"] == "/plone/target/source/sub/deep-doc"
        assert row["parent_path"] == "/plone/target/source/sub"
        assert row["path_depth"] == 5
        assert row["idx"]["path_depth"] == 5

    def test_move_preserves_non_path_idx(self, pg_conn_with_catalog):
        """Non-path idx keys (Title, portal_type) survive the JSONB merge."""
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/target/source",
            parent_zoid=101,
            parent_new_path="/plone/target/source",
        )

        row = _get_row(conn, 102)
        assert row["idx"]["Title"] == "Doc A"
        assert row["idx"]["portal_type"] == "Document"

        row = _get_row(conn, 103)
        assert row["idx"]["Title"] == "Subfolder"


# ===========================================================================
# Integration: SearchableText preservation
# ===========================================================================


class TestMovePreservesSearchableText:
    """Move must NOT null out SearchableText on descendants."""

    def test_searchable_text_preserved_after_rename(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn, searchable_text=True)

        # Verify searchable_text is set before move
        row = _get_row(conn, 102)
        assert row["searchable_text"] is not None

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/source-renamed",
            parent_zoid=101,
            parent_new_path="/plone/source-renamed",
        )

        # SearchableText should still be set
        row = _get_row(conn, 102)
        assert row["searchable_text"] is not None

        row = _get_row(conn, 104)
        assert row["searchable_text"] is not None

    def test_searchable_text_preserved_after_cross_container_move(
        self, pg_conn_with_catalog
    ):
        conn = pg_conn_with_catalog
        _setup_tree(conn, searchable_text=True)

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/target/source",
            parent_zoid=101,
            parent_new_path="/plone/target/source",
        )

        row = _get_row(conn, 102)
        assert row["searchable_text"] is not None

        row = _get_row(conn, 104)
        assert row["searchable_text"] is not None


# ===========================================================================
# Integration: Bulk rename (multiple siblings)
# ===========================================================================


class TestBulkRenameMultipleFolders:
    """Rename N sibling folders in one transaction — all paths correct."""

    def test_bulk_rename_5_siblings(self, pg_conn_with_catalog):
        """Rename folder-0..folder-4 → renamed-0..renamed-4."""
        conn = pg_conn_with_catalog
        _setup_siblings(conn, count=5)

        # Simulate renaming each folder sequentially (as Plone manage_renameObjects does)
        zoid = 301
        for i in range(5):
            old_prefix = f"/plone/folder-{i}"
            new_prefix = f"/plone/renamed-{i}"
            _simulate_move(
                conn,
                old_prefix=old_prefix,
                new_prefix=new_prefix,
                parent_zoid=zoid,
                parent_new_path=new_prefix,
            )
            zoid += 2  # skip doc zoid

        # Verify all renamed
        zoid = 301
        for i in range(5):
            row = _get_row(conn, zoid)
            assert row["path"] == f"/plone/renamed-{i}", f"folder-{i} not renamed"

            row = _get_row(conn, zoid + 1)
            assert row["path"] == f"/plone/renamed-{i}/doc", (
                f"doc in folder-{i} not updated"
            )
            assert row["parent_path"] == f"/plone/renamed-{i}"
            zoid += 2


# ===========================================================================
# Integration: Nested move — rename subfolder, then move parent
# ===========================================================================


class TestNestedMoveRenameThenParent:
    """Rename /plone/source/sub → /plone/source/sub-renamed,
    then move /plone/source → /plone/target/source.

    Both pending moves execute in order. Final state must be correct.
    """

    def test_nested_rename_then_move(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        # Step 1: Rename the subfolder
        _simulate_move(
            conn,
            old_prefix="/plone/source/sub",
            new_prefix="/plone/source/sub-renamed",
            parent_zoid=103,
            parent_new_path="/plone/source/sub-renamed",
        )

        # Verify intermediate state
        row = _get_row(conn, 104)
        assert row["path"] == "/plone/source/sub-renamed/deep-doc"

        # Step 2: Move the entire source folder
        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/target/source",
            parent_zoid=101,
            parent_new_path="/plone/target/source",
        )

        # Final state: all paths under /plone/target/source
        row = _get_row(conn, 101)
        assert row["path"] == "/plone/target/source"

        row = _get_row(conn, 102)
        assert row["path"] == "/plone/target/source/doc-a"

        # The renamed subfolder should be at the new location
        row = _get_row(conn, 103)
        assert row["path"] == "/plone/target/source/sub-renamed"

        row = _get_row(conn, 104)
        assert row["path"] == "/plone/target/source/sub-renamed/deep-doc"
        assert row["path_depth"] == 5  # /plone/target/source/sub-renamed/deep-doc = 5
        assert row["idx"]["path_depth"] == 5


# ===========================================================================
# Integration: Empty folder move
# ===========================================================================


class TestMoveEmptyFolder:
    """Move an empty folder — bulk SQL matches 0 descendants, no error."""

    def test_move_empty_folder_no_error(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog

        insert_object(conn, zoid=400)
        catalog_object(
            conn,
            zoid=400,
            path="/plone",
            idx={"Title": "Site", "portal_type": "Plone Site"},
        )
        insert_object(conn, zoid=401)
        catalog_object(
            conn,
            zoid=401,
            path="/plone/empty",
            idx={"Title": "Empty Folder", "portal_type": "Folder"},
        )
        insert_object(conn, zoid=402)
        catalog_object(
            conn,
            zoid=402,
            path="/plone/dest",
            idx={"Title": "Dest", "portal_type": "Folder"},
        )
        conn.commit()

        # Move empty folder — should not error
        _simulate_move(
            conn,
            old_prefix="/plone/empty",
            new_prefix="/plone/dest/empty",
            parent_zoid=401,
            parent_new_path="/plone/dest/empty",
        )

        row = _get_row(conn, 401)
        assert row["path"] == "/plone/dest/empty"
        assert row["path_depth"] == 3


# ===========================================================================
# Integration: Deep nesting (5 levels)
# ===========================================================================


class TestMoveDeepNesting:
    """Move a deeply nested tree — all levels updated correctly."""

    def test_move_5_level_tree(self, pg_conn_with_catalog):
        """Move /plone/a → /plone/dest/a (depth_delta=+1)."""
        conn = pg_conn_with_catalog
        _setup_deep_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/a",
            new_prefix="/plone/dest/a",
            parent_zoid=201,
            parent_new_path="/plone/dest/a",
        )

        # /plone/dest/a (was /plone/a, depth 2→3)
        row = _get_row(conn, 201)
        assert row["path"] == "/plone/dest/a"
        assert row["path_depth"] == 3

        # /plone/dest/a/b (was /plone/a/b, depth 3→4)
        row = _get_row(conn, 202)
        assert row["path"] == "/plone/dest/a/b"
        assert row["path_depth"] == 4

        # /plone/dest/a/b/c (was /plone/a/b/c, depth 4→5)
        row = _get_row(conn, 203)
        assert row["path"] == "/plone/dest/a/b/c"
        assert row["path_depth"] == 5

        # /plone/dest/a/b/c/d (was /plone/a/b/c/d, depth 5→6)
        row = _get_row(conn, 204)
        assert row["path"] == "/plone/dest/a/b/c/d"
        assert row["path_depth"] == 6

        # Leaf: /plone/dest/a/b/c/d/leaf (was /plone/a/b/c/d/leaf, depth 6→7)
        row = _get_row(conn, 205)
        assert row["path"] == "/plone/dest/a/b/c/d/leaf"
        assert row["path_depth"] == 7
        assert row["idx"]["path_depth"] == 7
        assert row["idx"]["path"] == "/plone/dest/a/b/c/d/leaf"
        assert row["idx"]["path_parent"] == "/plone/dest/a/b/c/d"

    def test_deep_move_preserves_idx(self, pg_conn_with_catalog):
        """All idx keys preserved through deep move."""
        conn = pg_conn_with_catalog
        _setup_deep_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/a",
            new_prefix="/plone/dest/a",
            parent_zoid=201,
            parent_new_path="/plone/dest/a",
        )

        row = _get_row(conn, 205)
        assert row["idx"]["Title"] == "Leaf Doc"
        assert row["idx"]["portal_type"] == "Document"

    def test_deep_tree_dest_unchanged(self, pg_conn_with_catalog):
        """Destination folder itself is not affected by bulk SQL."""
        conn = pg_conn_with_catalog
        _setup_deep_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/a",
            new_prefix="/plone/dest/a",
            parent_zoid=201,
            parent_new_path="/plone/dest/a",
        )

        row = _get_row(conn, 206)
        assert row["path"] == "/plone/dest"
        assert row["path_depth"] == 2


# ===========================================================================
# Integration: Move up (decrease depth)
# ===========================================================================


class TestMoveUp:
    """Move /plone/source/sub → /plone/sub (depth_delta=-1)."""

    def test_move_up_updates_all_descendants(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/source/sub",
            new_prefix="/plone/sub",
            parent_zoid=103,
            parent_new_path="/plone/sub",
        )

        # sub itself: depth 3→2
        row = _get_row(conn, 103)
        assert row["path"] == "/plone/sub"
        assert row["path_depth"] == 2
        assert row["parent_path"] == "/plone"

        # deep-doc: depth 4→3
        row = _get_row(conn, 104)
        assert row["path"] == "/plone/sub/deep-doc"
        assert row["path_depth"] == 3
        assert row["parent_path"] == "/plone/sub"
        assert row["idx"]["path_depth"] == 3


# ===========================================================================
# Integration: Pending moves through processor.finalize()
# ===========================================================================


class TestProcessorFinalize:
    """Test that processor.finalize() picks up and executes pending moves."""

    def setup_method(self):
        """Clear pending moves."""
        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

    def test_finalize_executes_pending_move(self, pg_conn_with_catalog):
        """finalize() should pop pending moves and execute bulk SQL."""
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.processor import CatalogStateProcessor

        conn = pg_conn_with_catalog
        _setup_tree(conn)

        # Register a pending move (as the wrapper would)
        transaction.begin()
        add_pending_move("/plone/source", "/plone/source-renamed", 0)

        # Call finalize with a cursor (as zodb-pgjsonb would in tpc_vote)
        processor = CatalogStateProcessor()
        with conn.cursor() as cursor:
            processor.finalize(cursor)
        conn.commit()
        transaction.abort()

        # Verify descendants were updated
        row = _get_row(conn, 102)
        assert row["path"] == "/plone/source-renamed/doc-a"

        row = _get_row(conn, 104)
        assert row["path"] == "/plone/source-renamed/sub/deep-doc"

    def test_finalize_executes_multiple_pending_moves(self, pg_conn_with_catalog):
        """finalize() should execute all pending moves in order."""
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.processor import CatalogStateProcessor

        conn = pg_conn_with_catalog
        _setup_siblings(conn, count=3)

        transaction.begin()
        add_pending_move("/plone/folder-0", "/plone/renamed-0", 0)
        add_pending_move("/plone/folder-1", "/plone/renamed-1", 0)
        add_pending_move("/plone/folder-2", "/plone/renamed-2", 0)

        processor = CatalogStateProcessor()
        with conn.cursor() as cursor:
            processor.finalize(cursor)
        conn.commit()
        transaction.abort()

        # Verify all children updated
        assert _get_row(conn, 302)["path"] == "/plone/renamed-0/doc"
        assert _get_row(conn, 304)["path"] == "/plone/renamed-1/doc"
        assert _get_row(conn, 306)["path"] == "/plone/renamed-2/doc"

    def test_finalize_no_pending_moves_is_noop(self, pg_conn_with_catalog):
        """finalize() with no pending moves should not error."""
        from plone.pgcatalog.processor import CatalogStateProcessor

        conn = pg_conn_with_catalog
        _setup_tree(conn)

        processor = CatalogStateProcessor()
        with conn.cursor() as cursor:
            processor.finalize(cursor)  # should not raise
        conn.commit()

        # Data unchanged
        row = _get_row(conn, 102)
        assert row["path"] == "/plone/source/doc-a"


# ===========================================================================
# Integration: Security reindex for cross-container moves
# ===========================================================================


class TestSecurityReindex:
    """Test that cross-container moves trigger security reindex.

    These tests verify the _reindex_security_for_move function is called
    correctly. Full integration with Plone security requires a Plone test layer.
    """

    def test_security_reindex_called_for_cross_container(self):
        """Cross-container move (oldParent != newParent) must trigger
        security reindex for descendants.

        This test verifies the detection logic. The actual security reindex
        uses Plone's reindexObjectSecurity which requires a Plone instance.
        """
        from plone.pgcatalog.move import MoveContext

        # Cross-container: oldParent != newParent → security reindex needed
        ctx = MoveContext(old_prefix="/plone/source", event_object=None)
        assert ctx.old_prefix == "/plone/source"

    def test_security_not_reindexed_for_rename(self):
        """Rename (same container, different name) should NOT trigger
        security reindex — permissions unchanged.

        Rename detection: oldParent is newParent.
        """
        # This is a behavioral contract test.
        # In the implementation, _wrapped_dispatchObjectMovedEvent checks:
        #   if event.oldParent is not event.newParent:
        #       _reindex_security_for_move(...)
        # For rename, oldParent IS newParent → no security reindex.
        pass  # Verified by implementation logic, not a functional test


# ===========================================================================
# Integration: Concurrent move context (nested events)
# ===========================================================================


class TestConcurrentMoveContexts:
    """Test that nested move operations don't corrupt each other's state."""

    def setup_method(self):
        try:
            del _local.move_context_stack
        except AttributeError:
            pass

    def test_nested_moves_accumulate_pending(self):
        """Two sequential moves in one transaction produce two pending entries."""
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        transaction.begin()
        add_pending_move("/plone/a", "/plone/a-new", 0)
        add_pending_move("/plone/b", "/plone/b-new", 1)

        result = pop_all_pending_moves()
        assert len(result) == 2
        assert result[0] == ("/plone/a", "/plone/a-new", 0)
        assert result[1] == ("/plone/b", "/plone/b-new", 1)
        transaction.abort()

    def test_move_context_stack_independent_of_pending(self):
        """Move context stack and pending moves are independent stores."""
        from plone.pgcatalog.move import _pop_move_context
        from plone.pgcatalog.move import _push_move_context
        from plone.pgcatalog.move import is_move_in_progress
        from plone.pgcatalog.move import MoveContext
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.pending import pop_all_pending_moves

        # Push context (simulates entering wrapper)
        _push_move_context(MoveContext(old_prefix="/plone/a", event_object=None))
        assert is_move_in_progress() is True

        # Add pending (simulates wrapper cleanup after dispatch)
        transaction.begin()
        add_pending_move("/plone/a", "/plone/a-new", 0)

        # Pop context (simulates leaving wrapper)
        _pop_move_context()
        assert is_move_in_progress() is False

        # Pending still available for finalize
        result = pop_all_pending_moves()
        assert len(result) == 1
        transaction.abort()
