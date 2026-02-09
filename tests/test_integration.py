"""Integration tests for PGCatalogTool — full catalog lifecycle.

Tests the catalog tool against real PostgreSQL: catalog, search, reindex,
uncatalog, refresh, and maintenance operations.
"""


from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain
from plone.pgcatalog.catalog import PGCatalogTool
from tests.conftest import insert_object


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def _make_catalog(conn):
    return PGCatalogTool(conn)


def _setup_objects(conn, catalog):
    """Insert and catalog test objects."""
    objects = [
        {
            "zoid": 500,
            "path": "/plone/doc1",
            "idx": {
                "portal_type": "Document",
                "review_state": "published",
                "Title": "First Document",
                "sortable_title": "first document",
                "Subject": ["Python"],
                "is_folderish": False,
                "allowedRolesAndUsers": ["Anonymous"],
                "effective": "2025-01-01T00:00:00+00:00",
                "expires": None,
            },
            "text": "This is the first document about Python",
        },
        {
            "zoid": 501,
            "path": "/plone/doc2",
            "idx": {
                "portal_type": "Document",
                "review_state": "private",
                "Title": "Second Document",
                "sortable_title": "second document",
                "Subject": ["Zope"],
                "is_folderish": False,
                "allowedRolesAndUsers": ["Manager"],
                "effective": "2025-01-01T00:00:00+00:00",
                "expires": None,
            },
            "text": "This is the second document about Zope",
        },
        {
            "zoid": 502,
            "path": "/plone/folder",
            "idx": {
                "portal_type": "Folder",
                "review_state": "published",
                "Title": "A Folder",
                "sortable_title": "a folder",
                "Subject": ["Python", "Zope"],
                "is_folderish": True,
                "allowedRolesAndUsers": ["Anonymous"],
                "effective": "2025-01-01T00:00:00+00:00",
                "expires": None,
            },
            "text": None,
        },
    ]
    for obj in objects:
        insert_object(conn, zoid=obj["zoid"])
        catalog.catalog_object(
            zoid=obj["zoid"],
            path=obj["path"],
            idx=obj["idx"],
            searchable_text=obj["text"],
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Catalog lifecycle
# ---------------------------------------------------------------------------


class TestCatalogObject:

    def test_catalog_and_search(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(portal_type="Document")
        assert len(results) == 2
        assert isinstance(results, CatalogSearchResults)

    def test_brains_have_correct_type(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(portal_type="Document")
        for brain in results:
            assert isinstance(brain, PGCatalogBrain)

    def test_brain_attributes(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(
            portal_type="Document",
            sort_on="sortable_title",
        )
        brain = results[0]
        assert brain.getPath() == "/plone/doc1"
        assert brain.getRID() == 500
        assert brain.Title == "First Document"
        assert brain.portal_type == "Document"

    def test_callable_alias(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat(portal_type="Folder")
        assert len(results) == 1
        assert results[0].getPath() == "/plone/folder"


class TestUncatalogObject:

    def test_uncatalog_removes_from_search(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        # Verify it's there
        assert len(cat.searchResults(portal_type="Folder")) == 1

        # Uncatalog
        cat.uncatalog_object(502)
        conn.commit()

        # Gone from search
        assert len(cat.searchResults(portal_type="Folder")) == 0


class TestReindexObject:

    def test_reindex_updates_field(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        # Change review_state
        cat.reindex_object(500, idx_updates={"review_state": "private"})
        conn.commit()

        # Old query returns nothing
        results = cat.searchResults(
            portal_type="Document", review_state="published"
        )
        zoids = {b.getRID() for b in results}
        assert 500 not in zoids

        # New query finds it
        results = cat.searchResults(
            portal_type="Document", review_state="private"
        )
        zoids = {b.getRID() for b in results}
        assert 500 in zoids

    def test_reindex_preserves_other_fields(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        cat.reindex_object(500, idx_updates={"review_state": "pending"})
        conn.commit()

        results = cat.searchResults(portal_type="Document")
        brain = next(b for b in results if b.getRID() == 500)
        assert brain.Title == "First Document"  # preserved
        assert brain.review_state == "pending"  # updated

    def test_reindex_with_searchable_text(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        cat.reindex_object(
            500,
            idx_updates={"Title": "Updated Title"},
            searchable_text="updated full text content",
        )
        conn.commit()

        # Full-text search should find the new text
        results = cat.searchResults(SearchableText="updated")
        assert len(results) == 1
        assert results[0].getRID() == 500

    def test_reindex_without_searchable_text_preserves_tsvector(self, pg_conn_with_catalog):
        """Reindex without searchable_text leaves existing tsvector unchanged."""
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        # Object 500 has searchable_text set. Reindex idx only.
        cat.reindex_object(500, idx_updates={"review_state": "draft"})
        conn.commit()

        # The old text should still be searchable (unchanged)
        results = cat.searchResults(SearchableText="Python")
        zoids = {b.getRID() for b in results}
        assert 500 in zoids

    def test_reindex_clear_searchable_text_directly(self, pg_conn_with_catalog):
        """Low-level reindex with searchable_text=None clears the tsvector."""
        from plone.pgcatalog.indexing import reindex_object as sql_reindex

        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        # Explicitly pass searchable_text=None to clear it
        sql_reindex(conn, zoid=500, idx_updates={}, searchable_text=None)
        conn.commit()

        # Full-text search should no longer find it
        results = cat.searchResults(SearchableText="Python")
        zoids = {b.getRID() for b in results}
        assert 500 not in zoids


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


class TestSecuredSearch:

    def test_secured_anonymous(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(
            portal_type="Document",
            secured=True,
            roles=["Anonymous"],
            show_inactive=True,
        )
        zoids = {b.getRID() for b in results}
        assert 500 in zoids  # public
        assert 501 not in zoids  # Manager only

    def test_unrestricted_search(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.unrestrictedSearchResults(portal_type="Document")
        assert len(results) == 2  # both docs, no security filter


# ---------------------------------------------------------------------------
# Sort + Pagination with actual_result_count
# ---------------------------------------------------------------------------


class TestResultCount:

    def test_actual_result_count_without_limit(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults()
        assert results.actual_result_count == len(results) == 3

    def test_actual_result_count_with_limit(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(
            sort_on="sortable_title", sort_limit=2
        )
        assert len(results) == 2
        assert results.actual_result_count == 3


# ---------------------------------------------------------------------------
# Maintenance operations
# ---------------------------------------------------------------------------


class TestRefreshCatalog:

    def test_refresh_returns_count(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        count = cat.refreshCatalog()
        assert count == 3


class TestReindexIndex:

    def test_reindex_specific_key(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        count = cat.reindexIndex("portal_type")
        assert count == 3  # all 3 objects have portal_type

    def test_reindex_missing_key(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        count = cat.reindexIndex("nonexistent_key")
        assert count == 0


class TestClearFindAndRebuild:

    def test_clears_all_catalog_data(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        # All 3 objects are cataloged
        assert len(cat.searchResults()) == 3

        count = cat.clearFindAndRebuild()
        conn.commit()
        assert count == 3

        # No cataloged objects remain
        assert len(cat.searchResults()) == 0

    def test_preserves_base_rows(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        cat.clearFindAndRebuild()
        conn.commit()

        # Base object_state rows still exist (just idx/path cleared)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM object_state WHERE zoid IN (500, 501, 502)")
            assert cur.fetchone()["cnt"] == 3

    def test_returns_zero_when_nothing_cataloged(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        assert cat.clearFindAndRebuild() == 0


class TestWindowFunctionResultCount:

    def test_limit_returns_actual_count(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(sort_on="sortable_title", sort_limit=1)
        assert len(results) == 1
        assert results.actual_result_count == 3

    def test_limit_with_offset(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(
            sort_on="sortable_title", sort_limit=1, b_start=1
        )
        assert len(results) == 1
        assert results.actual_result_count == 3

    def test_no_limit_no_window_function(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults()
        # actual_result_count == len when no limit
        assert results.actual_result_count == len(results) == 3

    def test_limit_no_results_returns_zero(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(
            portal_type="NonExistent", sort_limit=10
        )
        assert len(results) == 0
        assert results.actual_result_count == 0


# ---------------------------------------------------------------------------
# Non-cataloged objects
# ---------------------------------------------------------------------------


class TestNonCatalogedObjects:

    def test_non_cataloged_excluded(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        # Insert an object without cataloging it
        insert_object(conn, zoid=599)

        results = cat.searchResults()
        zoids = {b.getRID() for b in results}
        assert 599 not in zoids  # not cataloged → not in results


# ---------------------------------------------------------------------------
# Full-text via catalog tool
# ---------------------------------------------------------------------------


class TestCatalogFullText:

    def test_search_text(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(SearchableText="Python")
        assert len(results) == 1
        assert results[0].getRID() == 500

    def test_combined_text_and_type(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        cat = _make_catalog(conn)
        _setup_objects(conn, cat)

        results = cat.searchResults(
            SearchableText="document", portal_type="Document"
        )
        assert len(results) == 2
