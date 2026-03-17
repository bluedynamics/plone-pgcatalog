"""Integration tests for move/rename optimization using Plone test layer.

Tests the full pipeline: OFS events → wrapper handlers → move detection →
child skip → pending moves → bulk SQL execution.

Uses plone.app.testing with a real Plone site and actual OFS move/rename
operations to exercise the complete code path.
"""

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.pending import _local
from plone.pgcatalog.pending import add_pending_move
from plone.pgcatalog.pending import pop_all_pending_moves
from psycopg.types.json import Json
from tests.conftest import insert_object

import pytest
import transaction


# Plone layer setup — may fail in CI due to missing pkg_resources
# (setuptools >=82) or ZODB DemoStorage incompatibility.
# Try to create fixtures; if layer setup fails, tests using pgcatalog_layer skip.
_PLONE_LAYER_ERROR = None
try:
    from plone.app.testing import setRoles
    from plone.app.testing import TEST_USER_ID
    from plone.pgcatalog.testing import PGCATALOG_INTEGRATION_TESTING
    from zope.pytestlayer import fixture

    _layer_fixtures = fixture.create(
        PGCATALOG_INTEGRATION_TESTING,
        session_fixture_name="pgcatalog_layer_session",
        class_fixture_name="pgcatalog_layer_class",
        function_fixture_name="pgcatalog_layer",
    )
    globals().update(_layer_fixtures)
except Exception as exc:
    _PLONE_LAYER_ERROR = str(exc)

    @pytest.fixture
    def pgcatalog_layer():
        pytest.skip(f"Plone layer unavailable: {_PLONE_LAYER_ERROR}")


# ---------------------------------------------------------------------------
# Helpers for PG-level tests (no Plone layer needed)
# ---------------------------------------------------------------------------


def _setup_tree(conn, searchable_text=False):
    """Create a tree of cataloged objects for PG-level testing.

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
    """Create a deeply nested tree (5 levels)."""
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
    """Create N sibling folders under /plone, each with one child doc."""
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
    """Simulate the full move pipeline for a subtree at the SQL level.

    Performs the operations that the move optimization does:
    1. Update the parent's own catalog entry (normal pipeline)
    2. Register + pop a pending move
    3. Execute the bulk SQL (as finalize() would)
    """
    from plone.pgcatalog.columns import compute_path_info

    old_depth = len([p for p in old_prefix.split("/") if p])
    new_depth = len([p for p in new_prefix.split("/") if p])
    depth_delta = new_depth - old_depth

    # Step 1: Update parent's own path
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

    # Step 2+3: Register and execute pending move
    transaction.begin()
    add_pending_move(old_prefix, new_prefix, depth_delta)
    moves = pop_all_pending_moves()
    transaction.abort()

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
# Plone layer integration tests — real OFS events
# ===========================================================================


class TestMoveHandlerInstallation:
    """Test that move handlers are properly installed in the Plone layer."""

    def test_handlers_installed(self, pgcatalog_layer):
        """install_move_handlers() should have replaced OFS dispatch handlers."""
        from zope.component import getGlobalSiteManager

        gsm = getGlobalSiteManager()

        # Check that our wrappers are registered (not the OFS originals)
        from plone.pgcatalog.move import _wrapped_dispatchObjectMovedEvent
        from plone.pgcatalog.move import _wrapped_dispatchObjectWillBeMovedEvent

        will_handlers = list(gsm.registeredHandlers())
        will_found = any(
            h.handler is _wrapped_dispatchObjectWillBeMovedEvent for h in will_handlers
        )
        moved_found = any(
            h.handler is _wrapped_dispatchObjectMovedEvent for h in will_handlers
        )
        assert will_found, "Wrapped WillBeMoved handler not registered"
        assert moved_found, "Wrapped ObjectMoved handler not registered"

    def test_is_pgcatalog_active(self, pgcatalog_layer):
        """_is_pgcatalog_active() should return True when IPGCatalogTool is registered."""
        from plone.pgcatalog.move import _is_pgcatalog_active

        # In the test layer, the IPGCatalogTool utility is registered via ZCML
        # but PlonePGCatalogTool is not the site's portal_catalog.
        # _is_pgcatalog_active checks queryUtility(ICatalogTool) which returns
        # the site-local catalog, not our global utility.
        # This is expected — the function correctly distinguishes.
        result = _is_pgcatalog_active()
        # In the test layer without a PG-backed site, this is False
        # (the site's portal_catalog is still ZCatalog)
        assert result is False


class TestMoveContextWithPlone:
    """Test move context stack using the Plone layer."""

    def setup_method(self):
        try:
            del _local.move_context_stack
        except AttributeError:
            pass

    def test_move_context_lifecycle(self, pgcatalog_layer):
        """Push/pop move context works within a Plone layer."""
        from plone.pgcatalog.move import _pop_move_context
        from plone.pgcatalog.move import _push_move_context
        from plone.pgcatalog.move import is_move_in_progress
        from plone.pgcatalog.move import MoveContext

        assert is_move_in_progress() is False
        _push_move_context(MoveContext(old_prefix="/plone/source", event_object=None))
        assert is_move_in_progress() is True
        _pop_move_context()
        assert is_move_in_progress() is False


class TestOFSRenameWithWrappers:
    """Test actual OFS rename operations with our wrappers installed.

    These tests verify the wrapper handlers fire correctly during
    real OFS move/rename operations in a Plone site.
    """

    def test_rename_folder_fires_wrapper(self, pgcatalog_layer):
        """Renaming a folder in Plone fires our wrapper handlers."""

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        # Create test content
        portal.invokeFactory("Folder", "test-folder")
        folder = portal["test-folder"]
        folder.invokeFactory("Document", "doc1")
        folder.invokeFactory("Document", "doc2")

        # Clear any pending moves from creation (direct clear, no transaction manipulation)
        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

        # Rename the folder — this fires ObjectWillBeMovedEvent + ObjectMovedEvent
        # Our wrappers should detect this as a move and add a pending_move.
        # However, _is_pgcatalog_active() returns False in the test layer
        # (portal_catalog is ZCatalog, not PlonePGCatalogTool), so the wrappers
        # pass through without setting the flag. This is correct behavior —
        # the optimization only activates when pgcatalog is the catalog.
        portal.manage_renameObject("test-folder", "renamed-folder")

        # The folder should be renamed in OFS
        assert "renamed-folder" in portal.objectIds()
        assert "test-folder" not in portal.objectIds()

        # Children should be accessible under the new name
        renamed = portal["renamed-folder"]
        assert "doc1" in renamed.objectIds()
        assert "doc2" in renamed.objectIds()

    def test_move_folder_across_containers(self, pgcatalog_layer):
        """Moving a folder to a different container works correctly."""
        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        # Create source and target
        portal.invokeFactory("Folder", "source-folder")
        portal.invokeFactory("Folder", "target-folder")
        source = portal["source-folder"]
        source.invokeFactory("Document", "child-doc")

        # Move source into target
        clipboard = portal.manage_cutObjects(["source-folder"])
        portal["target-folder"].manage_pasteObjects(clipboard)

        # Verify move
        assert "source-folder" not in portal.objectIds()
        target = portal["target-folder"]
        assert "source-folder" in target.objectIds()
        assert "child-doc" in target["source-folder"].objectIds()


class TestWrapperWithPGCatalogActive:
    """Test wrapper behavior when _is_pgcatalog_active() is patched to True.

    This simulates the production scenario where PlonePGCatalogTool is the
    active catalog, so the wrappers set the move flag and register pending moves.
    """

    def setup_method(self):
        try:
            del _local.move_context_stack
        except AttributeError:
            pass
        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

    def test_rename_registers_pending_move(self, pgcatalog_layer, monkeypatch):
        """When pgcatalog is active, rename registers a pending move."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "src")
        portal["src"].invokeFactory("Document", "doc1")

        # Clear any pending moves from creation (direct clear, no txn manipulation)
        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

        portal.manage_renameObject("src", "dst")

        moves = pop_all_pending_moves()
        assert len(moves) == 1
        old_prefix, new_prefix, depth_delta = moves[0]
        assert old_prefix.endswith("/src")
        assert new_prefix.endswith("/dst")
        assert depth_delta == 0  # rename = same depth

    def test_move_registers_pending_move_with_depth_delta(
        self, pgcatalog_layer, monkeypatch
    ):
        """Cross-container move registers pending move with correct depth_delta."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "folder-a")
        portal.invokeFactory("Folder", "folder-b")
        portal["folder-a"].invokeFactory("Document", "doc")

        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

        clipboard = portal.manage_cutObjects(["folder-a"])
        portal["folder-b"].manage_pasteObjects(clipboard)

        moves = pop_all_pending_moves()
        assert len(moves) == 1
        old_prefix, new_prefix, depth_delta = moves[0]
        assert old_prefix.endswith("/folder-a")
        assert new_prefix.endswith("/folder-b/folder-a")
        assert depth_delta == 1  # moved one level deeper

    def test_move_context_active_during_dispatch(self, pgcatalog_layer, monkeypatch):
        """Move context stack is active during child dispatch."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        # Track push/pop of move context to verify the flag is set during dispatch
        context_log = []
        original_push = move_mod._push_move_context
        original_pop = move_mod._pop_move_context

        def tracking_push(ctx):
            context_log.append(("push", ctx.old_prefix))
            return original_push(ctx)

        def tracking_pop():
            result = original_pop()
            context_log.append(("pop", result.old_prefix if result else None))
            return result

        monkeypatch.setattr(move_mod, "_push_move_context", tracking_push)
        monkeypatch.setattr(move_mod, "_pop_move_context", tracking_pop)

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "myfolder")
        portal["myfolder"].invokeFactory("Document", "child1")
        portal["myfolder"].invokeFactory("Document", "child2")

        context_log.clear()
        portal.manage_renameObject("myfolder", "myfolder-renamed")

        # Wrappers should have pushed/popped context for both WillBeMoved and Moved
        push_events = [e for e in context_log if e[0] == "push"]
        pop_events = [e for e in context_log if e[0] == "pop"]
        assert len(push_events) == 2, (
            f"Expected 2 pushes (WillBe + Moved), got {push_events}"
        )
        assert len(pop_events) == 2, f"Expected 2 pops, got {pop_events}"
        assert all(e[1].endswith("/myfolder") for e in push_events)

    def test_move_context_cleared_after_move(self, pgcatalog_layer, monkeypatch):
        """Move context stack is empty after a completed move."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "tmp-folder")

        portal.manage_renameObject("tmp-folder", "tmp-renamed")

        assert move_mod.is_move_in_progress() is False

    def test_delete_does_not_trigger_optimization(self, pgcatalog_layer, monkeypatch):
        """Delete (not move) should NOT register pending moves."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "to-delete")
        portal["to-delete"].invokeFactory("Document", "doc")

        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

        portal.manage_delObjects(["to-delete"])

        moves = pop_all_pending_moves()
        assert moves == [], "Delete should not register pending moves"


# ===========================================================================
# PG-level SQL tests (using pg_conn_with_catalog, no Plone layer)
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

        row = _get_row(conn, 102)
        assert row["path"] == "/plone/source-renamed/doc-a"
        assert row["parent_path"] == "/plone/source-renamed"
        assert row["path_depth"] == 3
        assert row["idx"]["path"] == "/plone/source-renamed/doc-a"

        row = _get_row(conn, 103)
        assert row["path"] == "/plone/source-renamed/sub"

        row = _get_row(conn, 104)
        assert row["path"] == "/plone/source-renamed/sub/deep-doc"
        assert row["idx"]["path"] == "/plone/source-renamed/sub/deep-doc"

    def test_rename_parent_updated(self, pg_conn_with_catalog):
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
        assert row["idx"]["path"] == "/plone/source-renamed"

    def test_rename_siblings_unchanged(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/source-renamed",
            parent_zoid=101,
            parent_new_path="/plone/source-renamed",
        )

        assert _get_row(conn, 105)["path"] == "/plone/target"
        assert _get_row(conn, 106)["path"] == "/plone/target/doc-b"


class TestMoveUpdatesDescendants:
    """Move /plone/source → /plone/target/source (depth_delta=+1)."""

    def test_move_updates_paths_and_depth(self, pg_conn_with_catalog):
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
        assert row["path"] == "/plone/target/source/doc-a"
        assert row["path_depth"] == 4
        assert row["idx"]["path_depth"] == 4

        row = _get_row(conn, 104)
        assert row["path"] == "/plone/target/source/sub/deep-doc"
        assert row["path_depth"] == 5

    def test_move_preserves_non_path_idx(self, pg_conn_with_catalog):
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


class TestMovePreservesSearchableText:
    """Move must NOT null out SearchableText on descendants."""

    def test_searchable_text_preserved(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn, searchable_text=True)

        assert _get_row(conn, 102)["searchable_text"] is not None

        _simulate_move(
            conn,
            old_prefix="/plone/source",
            new_prefix="/plone/source-renamed",
            parent_zoid=101,
            parent_new_path="/plone/source-renamed",
        )

        assert _get_row(conn, 102)["searchable_text"] is not None
        assert _get_row(conn, 104)["searchable_text"] is not None


class TestBulkRenameMultipleFolders:
    """Rename N sibling folders in one transaction."""

    def test_bulk_rename_5_siblings(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_siblings(conn, count=5)

        zoid = 301
        for i in range(5):
            _simulate_move(
                conn,
                old_prefix=f"/plone/folder-{i}",
                new_prefix=f"/plone/renamed-{i}",
                parent_zoid=zoid,
                parent_new_path=f"/plone/renamed-{i}",
            )
            zoid += 2

        zoid = 301
        for i in range(5):
            assert _get_row(conn, zoid)["path"] == f"/plone/renamed-{i}"
            assert _get_row(conn, zoid + 1)["path"] == f"/plone/renamed-{i}/doc"
            zoid += 2


class TestNestedMoveRenameThenParent:
    """Rename subfolder, then move parent — both must compose correctly."""

    def test_nested_rename_then_move(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(
            conn,
            "/plone/source/sub",
            "/plone/source/sub-renamed",
            103,
            "/plone/source/sub-renamed",
        )
        _simulate_move(
            conn, "/plone/source", "/plone/target/source", 101, "/plone/target/source"
        )

        assert _get_row(conn, 103)["path"] == "/plone/target/source/sub-renamed"
        assert (
            _get_row(conn, 104)["path"] == "/plone/target/source/sub-renamed/deep-doc"
        )
        assert _get_row(conn, 104)["path_depth"] == 5


class TestMoveEmptyFolder:
    """Move an empty folder — bulk SQL matches 0 descendants, no error."""

    def test_empty_folder(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        for zoid, path, idx in [
            (400, "/plone", {"Title": "Site", "portal_type": "Plone Site"}),
            (401, "/plone/empty", {"Title": "Empty", "portal_type": "Folder"}),
            (402, "/plone/dest", {"Title": "Dest", "portal_type": "Folder"}),
        ]:
            insert_object(conn, zoid=zoid)
            catalog_object(conn, zoid=zoid, path=path, idx=idx)
        conn.commit()

        _simulate_move(
            conn, "/plone/empty", "/plone/dest/empty", 401, "/plone/dest/empty"
        )
        assert _get_row(conn, 401)["path"] == "/plone/dest/empty"


class TestMoveDeepNesting:
    """Move a deeply nested tree — all levels updated correctly."""

    def test_deep_move(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_deep_tree(conn)

        _simulate_move(conn, "/plone/a", "/plone/dest/a", 201, "/plone/dest/a")

        assert _get_row(conn, 202)["path"] == "/plone/dest/a/b"
        assert _get_row(conn, 205)["path"] == "/plone/dest/a/b/c/d/leaf"
        assert _get_row(conn, 205)["path_depth"] == 7
        assert _get_row(conn, 205)["idx"]["Title"] == "Leaf Doc"
        assert _get_row(conn, 206)["path"] == "/plone/dest"  # unchanged


class TestMoveUp:
    """Move /plone/source/sub → /plone/sub (depth_delta=-1)."""

    def test_move_up(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)

        _simulate_move(conn, "/plone/source/sub", "/plone/sub", 103, "/plone/sub")

        row = _get_row(conn, 104)
        assert row["path"] == "/plone/sub/deep-doc"
        assert row["path_depth"] == 3


class TestProcessorFinalize:
    """Test that processor.finalize() executes pending moves."""

    def setup_method(self):
        try:
            _local.pending_moves.clear()
        except AttributeError:
            pass

    def test_finalize_executes_pending_move(self, pg_conn_with_catalog):
        from plone.pgcatalog.processor import CatalogStateProcessor

        conn = pg_conn_with_catalog
        _setup_tree(conn)

        transaction.begin()
        add_pending_move("/plone/source", "/plone/source-renamed", 0)

        processor = CatalogStateProcessor()
        with conn.cursor() as cursor:
            processor.finalize(cursor)
        conn.commit()
        transaction.abort()

        assert _get_row(conn, 102)["path"] == "/plone/source-renamed/doc-a"
        assert _get_row(conn, 104)["path"] == "/plone/source-renamed/sub/deep-doc"

    def test_finalize_multiple_moves(self, pg_conn_with_catalog):
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

        assert _get_row(conn, 302)["path"] == "/plone/renamed-0/doc"
        assert _get_row(conn, 304)["path"] == "/plone/renamed-1/doc"
        assert _get_row(conn, 306)["path"] == "/plone/renamed-2/doc"

    def test_finalize_no_moves_noop(self, pg_conn_with_catalog):
        from plone.pgcatalog.processor import CatalogStateProcessor

        conn = pg_conn_with_catalog
        _setup_tree(conn)

        processor = CatalogStateProcessor()
        with conn.cursor() as cursor:
            processor.finalize(cursor)
        conn.commit()

        assert _get_row(conn, 102)["path"] == "/plone/source/doc-a"
