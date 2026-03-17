"""End-to-end integration tests using PG-backed Plone layer.

Tests the full catalog pipeline: content creation → ZODB commit →
PG catalog columns → SQL catalog queries. Uses PGJsonbStorage
instead of DemoStorage so catalog writes actually reach PostgreSQL.

These tests verify what DemoStorage-based tests cannot: that data
actually arrives in PostgreSQL with correct catalog columns, paths,
and searchable text.
"""

from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from plone.pgcatalog.testing import PGCATALOG_PG_FIXTURE
from zope.pytestlayer import fixture

import transaction


# Use the PG fixture directly (NOT wrapped in FunctionalTesting).
# FunctionalTesting stacks a DemoStorage which intercepts writes,
# defeating the purpose of PG-backed testing.  PGCatalogPGFixture
# handles per-test isolation itself via PGTestDB.restore().
globals().update(
    fixture.create(
        PGCATALOG_PG_FIXTURE,
        session_fixture_name="pg_functional_session",
        class_fixture_name="pg_functional_class",
        function_fixture_name="pg_functional",
    )
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _query_pg(pg_functional, sql, params=None):
    """Execute a SQL query against the test database and return all rows."""
    test_db = pg_functional["pgTestDB"]
    with test_db.connection.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def _query_object_by_path_suffix(pg_functional, suffix):
    """Find an object_state row where path ends with the given suffix."""
    rows = _query_pg(
        pg_functional,
        "SELECT path, parent_path, path_depth, idx, searchable_text "
        "FROM object_state WHERE path LIKE %s AND idx IS NOT NULL",
        (f"%{suffix}",),
    )
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# Layer basics
# ---------------------------------------------------------------------------


class TestPGLayerBasics:
    """Verify the PG-backed layer sets up correctly."""

    def test_portal_exists(self, pg_functional):
        """Plone site is accessible in the PG-backed layer."""
        portal = pg_functional["portal"]
        assert portal is not None
        assert portal.getId() == "plone"

    def test_app_exists(self, pg_functional):
        """Zope app is accessible."""
        app = pg_functional["app"]
        assert app is not None

    def test_pg_test_db_exposed(self, pg_functional):
        """pgTestDB resource is available for child layers."""
        test_db = pg_functional["pgTestDB"]
        assert test_db is not None
        assert test_db.depth >= 1  # At least one snapshot level


# ---------------------------------------------------------------------------
# Content creation → PG
# ---------------------------------------------------------------------------


class TestContentCreationInPG:
    """Test that content creation writes to PostgreSQL."""

    def test_create_document_and_commit(self, pg_functional):
        """Creating and committing a Document writes to PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "test-doc", title="Test Document")
        transaction.commit()

        assert "test-doc" in portal.objectIds()

    def test_created_content_has_oid(self, pg_functional):
        """Committed content gets a ZODB OID (stored in PG)."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "oid-test", title="OID Test")
        transaction.commit()

        doc = portal["oid-test"]
        assert doc._p_oid is not None


# ---------------------------------------------------------------------------
# Test isolation
# ---------------------------------------------------------------------------


class TestIsolationBetweenTests:
    """Verify that per-test snapshot restore provides isolation."""

    def test_create_content_a(self, pg_functional):
        """Create content in test A — should not leak to test B."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "isolation-a", title="Isolation A")
        transaction.commit()
        assert "isolation-a" in portal.objectIds()

    def test_content_a_not_visible(self, pg_functional):
        """Content from test A should NOT be visible in test B."""
        portal = pg_functional["portal"]
        assert "isolation-a" not in portal.objectIds()

    def test_pg_rows_also_isolated(self, pg_functional):
        """PG rows from test A are also rolled back for test B."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "pg-iso-check", title="PG Isolation")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/pg-iso-check")
        assert row is not None

    def test_pg_rows_from_previous_not_visible(self, pg_functional):
        """PG rows from previous test should NOT be visible."""
        row = _query_object_by_path_suffix(pg_functional, "/pg-iso-check")
        assert row is None


# ---------------------------------------------------------------------------
# Catalog columns in PG
# ---------------------------------------------------------------------------


class TestCatalogColumnsInPG:
    """Test that catalog columns are written to PG object_state."""

    def test_path_column_written(self, pg_functional):
        """Committed content has path column set in PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "path-doc", title="Path Test")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/path-doc")
        assert row is not None
        path, parent_path, path_depth, idx, _ = row
        assert path.endswith("/path-doc")
        assert parent_path.endswith("/plone")
        assert path_depth >= 2

    def test_idx_contains_title(self, pg_functional):
        """idx JSONB column contains Title from the content object."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "title-doc", title="My Unique Title")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/title-doc")
        assert row is not None
        _, _, _, idx, _ = row
        assert idx["Title"] == "My Unique Title"

    def test_idx_contains_portal_type(self, pg_functional):
        """idx JSONB column contains portal_type."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "type-doc", title="Type Test")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/type-doc")
        assert row is not None
        _, _, _, idx, _ = row
        assert idx["portal_type"] == "Document"

    def test_idx_contains_uid(self, pg_functional):
        """idx JSONB column contains UID."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "uid-doc", title="UID Test")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/uid-doc")
        assert row is not None
        _, _, _, idx, _ = row
        assert "UID" in idx
        assert idx["UID"] is not None

    def test_folder_is_folderish(self, pg_functional):
        """Folders have is_folderish=True in idx."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "folder-test", title="Folder Test")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/folder-test")
        assert row is not None
        _, _, _, idx, _ = row
        assert idx.get("is_folderish") is True

    def test_nested_content_paths(self, pg_functional):
        """Nested content has correct path hierarchy in PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "parent-f", title="Parent")
        portal["parent-f"].invokeFactory("Document", "child-d", title="Child")
        transaction.commit()

        parent_row = _query_object_by_path_suffix(pg_functional, "/parent-f")
        child_row = _query_object_by_path_suffix(pg_functional, "/parent-f/child-d")

        assert parent_row is not None
        assert child_row is not None

        # Child's parent_path should be parent's path
        child_path, child_parent, child_depth, _, _ = child_row
        parent_path = parent_row[0]
        assert child_parent == parent_path
        assert child_depth == parent_row[2] + 1

    def test_searchable_text_written(self, pg_functional):
        """searchable_text column populated for content with title."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "search-doc", title="Searchable Content")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/search-doc")
        assert row is not None
        _, _, _, _, searchable_text = row
        assert searchable_text is not None
        assert "Searchable" in searchable_text or "searchable" in searchable_text

    def test_keyword_index_in_idx(self, pg_functional):
        """Subject keywords are stored in idx JSONB."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "kw-doc", title="Keywords")
        doc = portal["kw-doc"]
        doc.setSubject(["python", "testing"])
        doc.reindexObject()
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/kw-doc")
        assert row is not None
        _, _, _, idx, _ = row
        subject = idx.get("Subject")
        assert subject is not None
        assert "python" in subject
        assert "testing" in subject


# ---------------------------------------------------------------------------
# Move / rename → PG path updates
# ---------------------------------------------------------------------------


class TestRenameUpdatesPGPaths:
    """Rename content and verify paths are updated in PostgreSQL."""

    def test_rename_folder_updates_path(self, pg_functional, monkeypatch):
        """Renaming a folder updates its path in PG."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "old-name", title="Rename Me")
        transaction.commit()

        # Verify initial path
        row = _query_object_by_path_suffix(pg_functional, "/old-name")
        assert row is not None

        portal.manage_renameObject("old-name", "new-name")
        transaction.commit()

        # Old path gone, new path present
        assert _query_object_by_path_suffix(pg_functional, "/old-name") is None
        row = _query_object_by_path_suffix(pg_functional, "/new-name")
        assert row is not None
        assert row[0].endswith("/new-name")

    def test_rename_updates_descendant_paths(self, pg_functional, monkeypatch):
        """Renaming a folder updates all descendant paths in PG."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "top")
        portal["top"].invokeFactory("Folder", "mid")
        portal["top"]["mid"].invokeFactory("Document", "leaf", title="Leaf")
        transaction.commit()

        portal.manage_renameObject("top", "top-renamed")
        transaction.commit()

        # All descendants should have updated paths
        row = _query_object_by_path_suffix(pg_functional, "/top-renamed/mid")
        assert row is not None

        row = _query_object_by_path_suffix(pg_functional, "/top-renamed/mid/leaf")
        assert row is not None
        _, _, _, idx, _ = row
        assert idx["Title"] == "Leaf"  # non-path idx preserved

    def test_rename_preserves_searchable_text(self, pg_functional, monkeypatch):
        """Rename must NOT null out searchable_text on descendants."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "st-folder")
        portal["st-folder"].invokeFactory("Document", "st-doc", title="Preserved Text")
        transaction.commit()

        # Verify searchable_text exists before rename
        row = _query_object_by_path_suffix(pg_functional, "/st-folder/st-doc")
        assert row is not None
        assert row[4] is not None  # searchable_text

        portal.manage_renameObject("st-folder", "st-folder-renamed")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/st-folder-renamed/st-doc")
        assert row is not None
        assert row[4] is not None  # searchable_text preserved


class TestMoveUpdatesPGPaths:
    """Move content between containers and verify PG paths."""

    def test_move_folder_to_subfolder(self, pg_functional, monkeypatch):
        """Moving a folder into another updates paths and depth in PG."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "source")
        portal["source"].invokeFactory("Document", "doc-a", title="Doc A")
        portal.invokeFactory("Folder", "target")
        transaction.commit()

        clipboard = portal.manage_cutObjects(["source"])
        portal["target"].manage_pasteObjects(clipboard)
        transaction.commit()

        # Source should be under target now
        row = _query_object_by_path_suffix(pg_functional, "/target/source")
        assert row is not None

        # Child doc should also be updated
        row = _query_object_by_path_suffix(pg_functional, "/target/source/doc-a")
        assert row is not None
        path, parent_path, path_depth, idx, _ = row
        assert path.endswith("/target/source/doc-a")
        assert parent_path.endswith("/target/source")
        assert idx["Title"] == "Doc A"

    def test_move_up_decreases_depth(self, pg_functional, monkeypatch):
        """Moving from deep to shallow decreases path_depth in PG."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "deep1")
        portal["deep1"].invokeFactory("Folder", "deep2")
        portal["deep1"]["deep2"].invokeFactory("Folder", "item")
        portal["deep1"]["deep2"]["item"].invokeFactory(
            "Document", "leaf", title="Deep Leaf"
        )
        transaction.commit()

        # Get original depth
        row = _query_object_by_path_suffix(pg_functional, "/deep1/deep2/item/leaf")
        assert row is not None
        original_depth = row[2]

        # Move item to portal root
        clipboard = portal["deep1"]["deep2"].manage_cutObjects(["item"])
        portal.manage_pasteObjects(clipboard)
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/item/leaf")
        assert row is not None
        assert row[2] < original_depth  # depth decreased

    def test_move_does_not_affect_siblings(self, pg_functional, monkeypatch):
        """Moving one folder does not affect its siblings in PG."""
        import plone.pgcatalog.move as move_mod

        monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "sibling-a")
        portal.invokeFactory("Folder", "sibling-b")
        portal["sibling-b"].invokeFactory("Document", "b-doc", title="B Doc")
        portal.invokeFactory("Folder", "dest")
        transaction.commit()

        clipboard = portal.manage_cutObjects(["sibling-a"])
        portal["dest"].manage_pasteObjects(clipboard)
        transaction.commit()

        # sibling-b should be unchanged
        row = _query_object_by_path_suffix(pg_functional, "/sibling-b/b-doc")
        assert row is not None
        _, _, _, idx, _ = row
        assert idx["Title"] == "B Doc"


# ---------------------------------------------------------------------------
# SQL queryability
# ---------------------------------------------------------------------------


class TestSQLQueryability:
    """Verify that catalog data in PG is queryable with SQL."""

    def test_query_by_portal_type(self, pg_functional):
        """Can find objects by portal_type via SQL JSONB query."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "sql-doc", title="SQL Test")
        portal.invokeFactory("Folder", "sql-folder", title="SQL Folder")
        transaction.commit()

        rows = _query_pg(
            pg_functional,
            "SELECT path FROM object_state "
            'WHERE idx @> \'{"portal_type": "Document"}\' '
            "AND path LIKE %s",
            ("%/sql-doc",),
        )
        assert len(rows) == 1
        assert rows[0][0].endswith("/sql-doc")

    def test_query_by_path_prefix(self, pg_functional):
        """Can find descendant objects by path prefix via SQL."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "tree")
        portal["tree"].invokeFactory("Document", "d1", title="D1")
        portal["tree"].invokeFactory("Document", "d2", title="D2")
        transaction.commit()

        site_path = "/".join(portal.getPhysicalPath())
        rows = _query_pg(
            pg_functional,
            "SELECT path FROM object_state "
            "WHERE path LIKE %s AND idx IS NOT NULL "
            "AND path != %s",
            (f"{site_path}/tree%", f"{site_path}/tree"),
        )
        assert len(rows) == 2
        paths = {r[0] for r in rows}
        assert any(p.endswith("/d1") for p in paths)
        assert any(p.endswith("/d2") for p in paths)
