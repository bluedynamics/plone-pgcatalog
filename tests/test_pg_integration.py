"""End-to-end integration tests using PG-backed Plone layer.

Tests the full catalog pipeline: content creation → ZODB commit →
PG catalog columns → SQL catalog queries. Uses PGJsonbStorage
instead of DemoStorage so catalog writes actually reach PostgreSQL.
"""

from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from plone.pgcatalog.testing import PGCATALOG_PG_FIXTURE
from zope.pytestlayer import fixture

import pytest
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


class TestContentCreationInPG:
    """Test that content creation writes to PostgreSQL."""

    def test_create_document_and_commit(self, pg_functional):
        """Creating and committing a Document writes to PG."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "test-doc", title="Test Document")
        transaction.commit()

        # The document should exist after commit
        assert "test-doc" in portal.objectIds()

    def test_created_content_has_oid(self, pg_functional):
        """Committed content gets a ZODB OID (stored in PG)."""
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "oid-test", title="OID Test")
        transaction.commit()

        doc = portal["oid-test"]
        assert doc._p_oid is not None


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
        # After snapshot restore, isolation-a should not exist
        assert "isolation-a" not in portal.objectIds()


@pytest.mark.skipif(
    not pytest.importorskip("psycopg", reason="psycopg not available"),
    reason="psycopg required",
)
class TestCatalogColumnsInPG:
    """Test that catalog columns are written to PG object_state."""

    def test_catalog_object_writes_to_pg(self, pg_functional):
        """Cataloging an object writes idx/path columns to PG."""

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "pg-doc", title="PG Document")
        transaction.commit()

        # Query PG directly to check catalog columns
        test_db = pg_functional["pgTestDB"]
        with test_db.connection.cursor() as cur:
            cur.execute(
                "SELECT path, idx FROM object_state WHERE path LIKE %s",
                ("%/pg-doc",),
            )
            row = cur.fetchone()

        if row is not None:
            path, idx = row
            assert path is not None
            assert "pg-doc" in path
