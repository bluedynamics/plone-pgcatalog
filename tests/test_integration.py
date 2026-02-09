"""Integration tests for PGCatalogTool — full catalog lifecycle.

Tests the catalog tool against real PostgreSQL: catalog, search, reindex,
uncatalog, refresh, and maintenance operations.
"""

from datetime import datetime
from datetime import timezone

from tests.conftest import insert_object

from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain
from plone.pgcatalog.catalog import PGCatalogTool


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
        brain = [b for b in results if b.getRID() == 500][0]
        assert brain.Title == "First Document"  # preserved
        assert brain.review_state == "pending"  # updated


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
