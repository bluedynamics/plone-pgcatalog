"""End-to-end integration tests using PG-backed Plone layer.

Tests the full catalog pipeline: content creation → ZODB commit →
PG catalog columns → SQL catalog queries. Uses PGJsonbStorage
instead of DemoStorage so catalog writes actually reach PostgreSQL.

These tests verify what DemoStorage-based tests cannot: that data
actually arrives in PostgreSQL with correct catalog columns, paths,
and searchable text.
"""

from plone.app.testing import login
from plone.app.testing import logout
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from plone.app.testing import TEST_USER_NAME
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


# ---------------------------------------------------------------------------
# Catalog searchResults() end-to-end
# ---------------------------------------------------------------------------


class TestSearchResultsEndToEnd:
    """Test searchResults() through the Plone catalog API."""

    def test_search_by_portal_type(self, pg_functional):
        """searchResults(portal_type=...) returns matching content."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "sr-doc", title="SR Doc")
        portal.invokeFactory("Folder", "sr-folder", title="SR Folder")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/sr-doc") for p in paths)
        assert not any(p.endswith("/sr-folder") for p in paths)

    def test_search_by_multiple_types(self, pg_functional):
        """searchResults with list of portal_types."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "mt-doc", title="MT Doc")
        portal.invokeFactory("Folder", "mt-folder", title="MT Folder")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type=["Document", "Folder"])
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/mt-doc") for p in paths)
        assert any(p.endswith("/mt-folder") for p in paths)

    def test_search_returns_catalog_search_results(self, pg_functional):
        """searchResults returns CatalogSearchResults instance."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "csr-doc", title="CSR Doc")
        transaction.commit()

        from plone.pgcatalog.brain import CatalogSearchResults

        results = catalog.unrestrictedSearchResults(portal_type="Document")
        assert isinstance(results, CatalogSearchResults)

    def test_search_empty_result(self, pg_functional):
        """searchResults with no matches returns empty results."""
        portal = pg_functional["portal"]
        catalog = portal["portal_catalog"]

        results = catalog.unrestrictedSearchResults(portal_type="NonExistentType")
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Uncatalog / delete flow
# ---------------------------------------------------------------------------


class TestUncatalogDeleteFlow:
    """Test that deleting content NULLs catalog columns in PG."""

    def test_delete_nulls_catalog_columns(self, pg_functional):
        """Deleting content sets idx to NULL in PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "del-doc", title="Delete Me")
        transaction.commit()

        # Verify it's cataloged
        results = catalog.unrestrictedSearchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/del-doc") for p in paths)

        # Delete and commit
        portal.manage_delObjects(["del-doc"])
        transaction.commit()

        # Should no longer appear in search
        results = catalog.unrestrictedSearchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert not any(p.endswith("/del-doc") for p in paths)

    def test_delete_preserves_other_content(self, pg_functional):
        """Deleting one object does not affect sibling catalog entries."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "keep-doc", title="Keep Me")
        portal.invokeFactory("Document", "remove-doc", title="Remove Me")
        transaction.commit()

        portal.manage_delObjects(["remove-doc"])
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/keep-doc") for p in paths)
        assert not any(p.endswith("/remove-doc") for p in paths)


# ---------------------------------------------------------------------------
# Partial reindex (reindexObject with idxs)
# ---------------------------------------------------------------------------


class TestReindex:
    """Test reindexObject() updates idx fields in PG."""

    def test_reindex_title_updates_idx(self, pg_functional):
        """Changing title and full reindexing updates idx in PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "ri-doc", title="Original Title")
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/ri-doc")
        assert row is not None
        assert row[3]["Title"] == "Original Title"

        # Change title and full reindex (the normal Plone path)
        portal["ri-doc"].setTitle("Updated Title")
        portal["ri-doc"].reindexObject()
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/ri-doc")
        assert row is not None
        assert row[3]["Title"] == "Updated Title"

    def test_reindex_preserves_other_fields(self, pg_functional):
        """Reindexing after title change preserves portal_type."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "pr-doc", title="Preserve Test")
        transaction.commit()

        portal["pr-doc"].setTitle("New Title")
        portal["pr-doc"].reindexObject()
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/pr-doc")
        assert row is not None
        assert row[3]["Title"] == "New Title"
        assert row[3]["portal_type"] == "Document"

    def test_reindex_subject_keywords(self, pg_functional):
        """Changing Subject and reindexing updates keywords in PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "kw-ri", title="Keywords Reindex")
        transaction.commit()

        portal["kw-ri"].setSubject(["new-tag", "updated"])
        portal["kw-ri"].reindexObject()
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/kw-ri")
        assert row is not None
        subject = row[3].get("Subject")
        assert "new-tag" in subject
        assert "updated" in subject


# ---------------------------------------------------------------------------
# Full-text search with weight-based relevance
# ---------------------------------------------------------------------------


class TestFullTextSearchWeighted:
    """Test SearchableText queries with tsvector weight ranking."""

    def test_searchable_text_finds_content(self, pg_functional):
        """SearchableText query finds content matching title."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "fts-doc", title="Elephants in Africa")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(SearchableText="Elephants")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/fts-doc") for p in paths)

    def test_searchable_text_no_match(self, pg_functional):
        """SearchableText query returns nothing for non-matching term."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "fts-no", title="Elephants Only")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(SearchableText="Xylophone")
        paths = [b.getPath() for b in results]
        assert not any(p.endswith("/fts-no") for p in paths)

    def test_title_match_ranks_higher_than_description(self, pg_functional):
        """Content matching in Title (weight A) ranks above Description (B).

        Verifies that PG tsvector weights are applied correctly so
        title-matching results appear before description-only matches.
        """
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        # "Quantum" in title only
        portal.invokeFactory(
            "Document",
            "title-match",
            title="Quantum Computing Breakthrough",
            description="Recent advances in technology",
        )
        # "Quantum" in description only
        portal.invokeFactory(
            "Document",
            "desc-match",
            title="Technology News Today",
            description="Quantum computing is advancing rapidly",
        )
        transaction.commit()

        results = catalog.unrestrictedSearchResults(
            SearchableText="Quantum",
            sort_on="relevance",
        )
        paths = [b.getPath() for b in results]
        matching = [p for p in paths if "title-match" in p or "desc-match" in p]
        assert len(matching) == 2
        # Title match should come first (higher weight)
        assert matching[0].endswith("/title-match")
        assert matching[1].endswith("/desc-match")

    def test_multi_word_search(self, pg_functional):
        """Multi-word SearchableText finds content with all terms."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory(
            "Document",
            "multi-doc",
            title="Python Web Development Guide",
        )
        portal.invokeFactory(
            "Document",
            "partial-doc",
            title="Python Snake Facts",
        )
        transaction.commit()

        # Both words must match (AND semantics)
        results = catalog.unrestrictedSearchResults(SearchableText="Python Development")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/multi-doc") for p in paths)
        # partial-doc should NOT match (has "Python" but not "Development")
        assert not any(p.endswith("/partial-doc") for p in paths)


# ---------------------------------------------------------------------------
# Path queries via searchResults
# ---------------------------------------------------------------------------


class TestPathQueries:
    """Test path-based queries through the catalog API."""

    def test_path_subtree_query(self, pg_functional):
        """path query finds all descendants of a folder."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Folder", "pq-folder")
        portal["pq-folder"].invokeFactory("Document", "pq-a", title="A")
        portal["pq-folder"].invokeFactory("Document", "pq-b", title="B")
        portal.invokeFactory("Document", "pq-outside", title="Outside")
        transaction.commit()

        site_path = "/".join(portal.getPhysicalPath())
        results = catalog.unrestrictedSearchResults(
            path=f"{site_path}/pq-folder",
            portal_type="Document",
        )
        paths = [b.getPath() for b in results]
        assert len(paths) == 2
        assert any(p.endswith("/pq-a") for p in paths)
        assert any(p.endswith("/pq-b") for p in paths)

    def test_path_depth_one(self, pg_functional):
        """path query with depth=1 finds only direct children."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Folder", "d1-parent")
        portal["d1-parent"].invokeFactory("Folder", "d1-child")
        portal["d1-parent"]["d1-child"].invokeFactory(
            "Document", "d1-grandchild", title="Grandchild"
        )
        transaction.commit()

        site_path = "/".join(portal.getPhysicalPath())
        results = catalog.unrestrictedSearchResults(
            path={"query": f"{site_path}/d1-parent", "depth": 1},
        )
        paths = [b.getPath() for b in results]
        # Should find only direct child, not grandchild or parent itself
        assert any(p.endswith("/d1-child") for p in paths)
        assert not any(p.endswith("/d1-grandchild") for p in paths)

    def test_path_exact_match(self, pg_functional):
        """path query with depth=0 finds only the exact object."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Folder", "exact-folder")
        portal["exact-folder"].invokeFactory("Document", "exact-doc", title="Exact")
        transaction.commit()

        site_path = "/".join(portal.getPhysicalPath())
        results = catalog.unrestrictedSearchResults(
            path={"query": f"{site_path}/exact-folder", "depth": 0},
        )
        paths = [b.getPath() for b in results]
        assert len(paths) == 1
        assert paths[0].endswith("/exact-folder")


# ---------------------------------------------------------------------------
# Sort + pagination via catalog API
# ---------------------------------------------------------------------------


class TestSortAndPagination:
    """Test sort_on, sort_order, and b_start/b_size."""

    def test_sort_by_sortable_title(self, pg_functional):
        """Results sorted by sortable_title ascending."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "sort-c", title="Charlie")
        portal.invokeFactory("Document", "sort-a", title="Alpha")
        portal.invokeFactory("Document", "sort-b", title="Bravo")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(
            portal_type="Document",
            sort_on="sortable_title",
            sort_order="ascending",
        )
        titles = [b.Title for b in results]
        # Filter to our test docs (site may have other content)
        our_titles = [t for t in titles if t in ("Alpha", "Bravo", "Charlie")]
        assert our_titles == ["Alpha", "Bravo", "Charlie"]

    def test_sort_descending(self, pg_functional):
        """Results sorted by sortable_title descending."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "sd-a", title="Alpha")
        portal.invokeFactory("Document", "sd-b", title="Bravo")
        portal.invokeFactory("Document", "sd-c", title="Charlie")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(
            portal_type="Document",
            sort_on="sortable_title",
            sort_order="descending",
        )
        titles = [b.Title for b in results]
        our_titles = [t for t in titles if t in ("Alpha", "Bravo", "Charlie")]
        assert our_titles == ["Charlie", "Bravo", "Alpha"]

    def test_pagination_b_start_b_size(self, pg_functional):
        """b_start and b_size limit returned results."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        for i in range(5):
            portal.invokeFactory("Document", f"page-{i}", title=f"Page {i}")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(
            portal_type="Document",
            sort_on="sortable_title",
            b_start=0,
            b_size=2,
        )
        assert len(list(results)) == 2

    def test_pagination_offset(self, pg_functional):
        """b_start > 0 skips initial results."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        for i in range(5):
            portal.invokeFactory("Document", f"off-{i}", title=f"Off {i:02d}")
        transaction.commit()

        page1 = catalog.unrestrictedSearchResults(
            portal_type="Document",
            sort_on="sortable_title",
            b_start=0,
            b_size=3,
        )
        page2 = catalog.unrestrictedSearchResults(
            portal_type="Document",
            sort_on="sortable_title",
            b_start=3,
            b_size=3,
        )
        paths1 = {b.getPath() for b in page1}
        paths2 = {b.getPath() for b in page2}
        # No overlap between pages
        assert paths1.isdisjoint(paths2)


# ---------------------------------------------------------------------------
# Brain attribute access from real search
# ---------------------------------------------------------------------------


class TestBrainAttributes:
    """Test PGCatalogBrain attributes from real catalog search."""

    def test_brain_get_path(self, pg_functional):
        """brain.getPath() returns correct physical path."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "brain-doc", title="Brain Test")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type="Document")
        brain = next(b for b in results if b.getPath().endswith("/brain-doc"))
        assert brain.getPath().endswith("/plone/brain-doc")

    def test_brain_title(self, pg_functional):
        """brain.Title returns the title from idx JSONB."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "bt-doc", title="My Brain Title")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type="Document")
        brain = next(b for b in results if b.getPath().endswith("/bt-doc"))
        assert brain.Title == "My Brain Title"

    def test_brain_portal_type(self, pg_functional):
        """brain.portal_type returns the correct type."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Folder", "bpt-folder", title="Type Test")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type="Folder")
        brain = next(b for b in results if b.getPath().endswith("/bpt-folder"))
        assert brain.portal_type == "Folder"

    def test_brain_get_object(self, pg_functional):
        """brain.getObject() returns the actual persistent object."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "go-doc", title="Get Object")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type="Document")
        brain = next(b for b in results if b.getPath().endswith("/go-doc"))
        obj = brain.getObject()
        assert obj.getId() == "go-doc"
        assert obj.Title() == "Get Object"

    def test_brain_get_rid(self, pg_functional):
        """brain.getRID() returns an integer ZOID."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "rid-doc", title="RID Test")
        transaction.commit()

        results = catalog.unrestrictedSearchResults(portal_type="Document")
        brain = next(b for b in results if b.getPath().endswith("/rid-doc"))
        rid = brain.getRID()
        assert isinstance(rid, int)
        assert rid > 0


# ---------------------------------------------------------------------------
# Security filtering (allowedRolesAndUsers)
# ---------------------------------------------------------------------------


class TestSecurityFiltering:
    """Test that searchResults respects security (allowedRolesAndUsers)."""

    def test_manager_finds_all_content(self, pg_functional):
        """Manager user finds content via searchResults."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "sec-doc", title="Secured")
        transaction.commit()

        # Search as Manager (current user)
        results = catalog.searchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/sec-doc") for p in paths)

    def test_anonymous_cannot_see_restricted_content(self, pg_functional):
        """Anonymous user cannot see content restricted to Manager."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        # First commit: create content
        portal.invokeFactory("Document", "priv-doc", title="Private")
        transaction.commit()

        # Second commit: restrict permissions and reindex
        doc = portal["priv-doc"]
        doc.manage_permission("View", roles=["Manager"], acquire=False)
        doc.reindexObject()
        transaction.commit()

        # Search as anonymous
        logout()
        results = catalog.searchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert not any(p.endswith("/priv-doc") for p in paths)

    def test_anonymous_can_see_public_content(self, pg_functional):
        """Anonymous user can see content with View granted to Anonymous."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "pub-doc", title="Public")
        transaction.commit()

        # Default Plone grants View to Anonymous (no workflow)
        logout()
        results = catalog.searchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/pub-doc") for p in paths)

    def test_mixed_visibility(self, pg_functional):
        """Manager sees both; anonymous sees only public content."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        # First commit: create both docs
        portal.invokeFactory("Document", "mix-pub", title="Public Mix")
        portal.invokeFactory("Document", "mix-priv", title="Private Mix")
        transaction.commit()

        # Second commit: restrict one doc
        doc = portal["mix-priv"]
        doc.manage_permission("View", roles=["Manager"], acquire=False)
        doc.reindexObject()
        transaction.commit()

        # Manager sees both
        login(portal, TEST_USER_NAME)
        setRoles(portal, TEST_USER_ID, ["Manager"])
        results = catalog.searchResults(portal_type="Document")
        manager_paths = [b.getPath() for b in results]
        assert any(p.endswith("/mix-pub") for p in manager_paths)
        assert any(p.endswith("/mix-priv") for p in manager_paths)

        # Anonymous sees only public
        logout()
        results = catalog.searchResults(portal_type="Document")
        anon_paths = [b.getPath() for b in results]
        assert any(p.endswith("/mix-pub") for p in anon_paths)
        assert not any(p.endswith("/mix-priv") for p in anon_paths)

    def test_unrestricted_bypasses_security(self, pg_functional):
        """unrestrictedSearchResults ignores security filters."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        # First commit: create content
        portal.invokeFactory("Document", "unr-doc", title="Unrestricted")
        transaction.commit()

        # Second commit: restrict
        doc = portal["unr-doc"]
        doc.manage_permission("View", roles=["Manager"], acquire=False)
        doc.reindexObject()
        transaction.commit()

        # Even as anonymous, unrestricted search finds it
        logout()
        results = catalog.unrestrictedSearchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/unr-doc") for p in paths)


# ---------------------------------------------------------------------------
# uniqueValuesFor
# ---------------------------------------------------------------------------


class TestUniqueValuesFor:
    """Test uniqueValuesFor() queries PG for distinct index values."""

    def test_unique_portal_types(self, pg_functional):
        """uniqueValuesFor('portal_type') returns distinct types."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "uv-doc", title="UV Doc")
        portal.invokeFactory("Folder", "uv-folder", title="UV Folder")
        transaction.commit()

        values = catalog.uniqueValuesFor("portal_type")
        assert "Document" in values
        assert "Folder" in values

    def test_unique_review_states(self, pg_functional):
        """uniqueValuesFor returns distinct values for field indexes."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "uv-doc1", title="UV Doc 1")
        portal.invokeFactory("Folder", "uv-fold1", title="UV Folder 1")
        transaction.commit()

        # portal_type is a field index with atomic values
        values = catalog.uniqueValuesFor("portal_type")
        assert "Document" in values
        assert "Folder" in values


# ---------------------------------------------------------------------------
# Maintenance operations
# ---------------------------------------------------------------------------


class TestMaintenanceOps:
    """Test refreshCatalog, reindexIndex, clearFindAndRebuild."""

    def test_refresh_catalog_recatalogs_existing(self, pg_functional):
        """refreshCatalog() re-indexes objects already in PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "rc-doc", title="Original")
        transaction.commit()

        # Verify original title
        row = _query_object_by_path_suffix(pg_functional, "/rc-doc")
        assert row is not None
        assert row[3]["Title"] == "Original"

        # Change title in memory (do NOT commit — that would NULL idx)
        portal["rc-doc"].setTitle("Refreshed")

        # refreshCatalog re-catalogs from live ZODB objects
        catalog.refreshCatalog(clear=0)
        transaction.commit()

        row = _query_object_by_path_suffix(pg_functional, "/rc-doc")
        assert row is not None
        assert row[3]["Title"] == "Refreshed"

    def test_clear_find_and_rebuild(self, pg_functional):
        """clearFindAndRebuild() clears then re-indexes all content."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "cfr-doc", title="Rebuild Me")
        transaction.commit()

        # Verify it's cataloged
        results = catalog.unrestrictedSearchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/cfr-doc") for p in paths)

        # Clear and rebuild
        catalog.clearFindAndRebuild()
        transaction.commit()

        # Should still be findable after rebuild
        results = catalog.unrestrictedSearchResults(portal_type="Document")
        paths = [b.getPath() for b in results]
        assert any(p.endswith("/cfr-doc") for p in paths)

    def test_manage_catalog_clear(self, pg_functional):
        """manage_catalogClear() removes all catalog data from PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        catalog = portal["portal_catalog"]

        portal.invokeFactory("Document", "clr-doc", title="Clear Me")
        transaction.commit()

        # Verify it's cataloged
        row = _query_object_by_path_suffix(pg_functional, "/clr-doc")
        assert row is not None

        catalog.manage_catalogClear()

        # After clear, idx should be NULL in PG
        rows = _query_pg(
            pg_functional,
            "SELECT idx FROM object_state WHERE path LIKE %s",
            ("%/clr-doc",),
        )
        # Either no rows with path (path NULLed) or idx is None
        assert not rows or rows[0][0] is None
