"""Tests for plone.pgcatalog.indexing — PG write operations."""

from tests.conftest import insert_object

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.indexing import reindex_object
from plone.pgcatalog.indexing import uncatalog_object


class TestCatalogObject:
    """Test full catalog write."""

    def test_writes_path_and_idx(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=10)

        idx = {"portal_type": "Document", "review_state": "published", "Title": "Hello"}
        catalog_object(conn, zoid=10, path="/plone/hello", idx=idx)
        conn.commit()

        row = _get_row(conn, 10)
        assert row["path"] == "/plone/hello"
        assert row["parent_path"] == "/plone"
        assert row["path_depth"] == 2
        assert row["idx"]["portal_type"] == "Document"
        assert row["idx"]["review_state"] == "published"
        assert row["idx"]["Title"] == "Hello"

    def test_writes_correct_parent_path(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=11)

        catalog_object(conn, zoid=11, path="/plone/folder/subfolder/doc", idx={"portal_type": "Document"})
        conn.commit()

        row = _get_row(conn, 11)
        assert row["parent_path"] == "/plone/folder/subfolder"
        assert row["path_depth"] == 4

    def test_writes_root_level_path(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=12)

        catalog_object(conn, zoid=12, path="/plone", idx={"portal_type": "Plone Site"})
        conn.commit()

        row = _get_row(conn, 12)
        assert row["parent_path"] == "/"
        assert row["path_depth"] == 1

    def test_writes_searchable_text(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=13)

        catalog_object(
            conn, zoid=13, path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="The quick brown fox",
        )
        conn.commit()

        row = _get_row(conn, 13)
        assert row["searchable_text"] is not None

        # Verify full-text search works
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'fox')"
            )
            zoids = [r["zoid"] for r in cur.fetchall()]
        assert 13 in zoids

    def test_no_searchable_text_leaves_null(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=14)

        catalog_object(conn, zoid=14, path="/plone/doc", idx={"portal_type": "Document"})
        conn.commit()

        row = _get_row(conn, 14)
        assert row["searchable_text"] is None

    def test_overwrite_existing_catalog_data(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=15)

        # First catalog
        catalog_object(conn, zoid=15, path="/plone/old", idx={"portal_type": "Document"})
        conn.commit()

        # Re-catalog with new data
        catalog_object(conn, zoid=15, path="/plone/new", idx={"portal_type": "Page", "Title": "New"})
        conn.commit()

        row = _get_row(conn, 15)
        assert row["path"] == "/plone/new"
        assert row["idx"]["portal_type"] == "Page"
        assert row["idx"]["Title"] == "New"
        # Old keys not present
        assert "Document" not in str(row["idx"])

    def test_idx_with_various_value_types(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=16)

        idx = {
            "portal_type": "Document",
            "is_folderish": False,
            "Subject": ["Python", "Zope"],
            "getObjPositionInParent": 3,
            "created": "2025-01-15T10:30:00+00:00",
            "expires": None,
        }
        catalog_object(conn, zoid=16, path="/plone/doc", idx=idx)
        conn.commit()

        row = _get_row(conn, 16)
        assert row["idx"]["is_folderish"] is False
        assert row["idx"]["Subject"] == ["Python", "Zope"]
        assert row["idx"]["getObjPositionInParent"] == 3
        assert row["idx"]["created"] == "2025-01-15T10:30:00+00:00"
        assert row["idx"]["expires"] is None

    def test_base_columns_preserved(self, pg_conn_with_catalog):
        """Cataloging doesn't touch the base object_state columns."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=17, class_mod="plone.app", class_name="Document",
                       state={"title": "Original"})

        catalog_object(conn, zoid=17, path="/plone/doc", idx={"portal_type": "Document"})
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT class_mod, class_name, state FROM object_state WHERE zoid = 17"
            )
            row = cur.fetchone()
        assert row["class_mod"] == "plone.app"
        assert row["class_name"] == "Document"
        assert row["state"]["title"] == "Original"


class TestUncatalogObject:
    """Test catalog data clearing."""

    def test_clears_all_catalog_columns(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=20)

        # Catalog first
        catalog_object(conn, zoid=20, path="/plone/doc", idx={"portal_type": "Document"},
                        searchable_text="hello world")
        conn.commit()
        row = _get_row(conn, 20)
        assert row["path"] is not None
        assert row["idx"] is not None

        # Uncatalog
        uncatalog_object(conn, zoid=20)
        conn.commit()

        row = _get_row(conn, 20)
        assert row["path"] is None
        assert row["parent_path"] is None
        assert row["path_depth"] is None
        assert row["idx"] is None
        assert row["searchable_text"] is None

    def test_preserves_base_row(self, pg_conn_with_catalog):
        """Uncataloging does not delete the object_state row."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=21, class_mod="myapp", class_name="Doc")

        catalog_object(conn, zoid=21, path="/plone/doc", idx={"portal_type": "Document"})
        conn.commit()

        uncatalog_object(conn, zoid=21)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT class_mod, class_name FROM object_state WHERE zoid = 21")
            row = cur.fetchone()
        assert row is not None
        assert row["class_mod"] == "myapp"


class TestReindexObject:
    """Test partial reindex (idx merge)."""

    def test_merges_into_existing_idx(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=30)

        # Full catalog first
        catalog_object(conn, zoid=30, path="/plone/doc",
                        idx={"portal_type": "Document", "review_state": "private"})
        conn.commit()

        # Partial reindex: update review_state only
        reindex_object(conn, zoid=30, idx_updates={"review_state": "published"})
        conn.commit()

        row = _get_row(conn, 30)
        assert row["idx"]["review_state"] == "published"
        assert row["idx"]["portal_type"] == "Document"  # preserved

    def test_adds_new_keys(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=31)

        catalog_object(conn, zoid=31, path="/plone/doc", idx={"portal_type": "Document"})
        conn.commit()

        reindex_object(conn, zoid=31, idx_updates={"Title": "New Title"})
        conn.commit()

        row = _get_row(conn, 31)
        assert row["idx"]["Title"] == "New Title"
        assert row["idx"]["portal_type"] == "Document"  # preserved

    def test_does_not_touch_searchable_text_by_default(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=32)

        catalog_object(conn, zoid=32, path="/plone/doc", idx={"portal_type": "Document"},
                        searchable_text="original text")
        conn.commit()

        # Reindex idx only — searchable_text should be preserved
        reindex_object(conn, zoid=32, idx_updates={"Title": "Updated"})
        conn.commit()

        # Verify text search still works with original text
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'original')"
            )
            zoids = [r["zoid"] for r in cur.fetchall()]
        assert 32 in zoids

    def test_updates_searchable_text_when_provided(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=33)

        catalog_object(conn, zoid=33, path="/plone/doc", idx={"portal_type": "Document"},
                        searchable_text="old text")
        conn.commit()

        reindex_object(conn, zoid=33, idx_updates={"Title": "New"},
                        searchable_text="new searchable content")
        conn.commit()

        with conn.cursor() as cur:
            # Old text no longer matches
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'old')"
            )
            assert cur.fetchone() is None

            # New text matches
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'searchable')"
            )
            assert cur.fetchone()["zoid"] == 33

    def test_path_unchanged_by_reindex(self, pg_conn_with_catalog):
        """reindex_object does not change path/parent_path/path_depth."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=34)

        catalog_object(conn, zoid=34, path="/plone/folder/doc",
                        idx={"portal_type": "Document"})
        conn.commit()

        reindex_object(conn, zoid=34, idx_updates={"review_state": "published"})
        conn.commit()

        row = _get_row(conn, 34)
        assert row["path"] == "/plone/folder/doc"
        assert row["parent_path"] == "/plone/folder"
        assert row["path_depth"] == 3

    def test_reindex_on_uncataloged_object(self, pg_conn_with_catalog):
        """reindex_object on object with no idx creates idx from scratch."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=35)

        reindex_object(conn, zoid=35, idx_updates={"portal_type": "Document"})
        conn.commit()

        row = _get_row(conn, 35)
        assert row["idx"]["portal_type"] == "Document"


class TestSearchableTextLanguage:
    """Test multilingual full-text search."""

    def test_german_stemming(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=40)

        catalog_object(conn, zoid=40, path="/plone/doc",
                        idx={"portal_type": "Document"},
                        searchable_text="Die Katzen spielen im Garten",
                        language="german")
        conn.commit()

        # German stemmer should match "Katze" (singular) for "Katzen" (plural)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('german', 'Katze')"
            )
            result = cur.fetchone()
        assert result is not None
        assert result["zoid"] == 40

    def test_simple_language_no_stemming(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=41)

        catalog_object(conn, zoid=41, path="/plone/doc",
                        idx={"portal_type": "Document"},
                        searchable_text="Die Katzen spielen im Garten",
                        language="simple")
        conn.commit()

        # "simple" config: no stemming, "Katze" won't match "Katzen"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'Katze')"
            )
            assert cur.fetchone() is None

            # But exact word matches
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'Katzen')"
            )
            assert cur.fetchone()["zoid"] == 41


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_row(conn, zoid):
    """Read full catalog data for a zoid."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT path, parent_path, path_depth, idx, searchable_text "
            "FROM object_state WHERE zoid = %(zoid)s",
            {"zoid": zoid},
        )
        return cur.fetchone()
