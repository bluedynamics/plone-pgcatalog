"""Tests for PGIndex wrappers — ZCatalog internal API compatibility.

Tests that getpath(), getrid(), Indexes["UID"]._index.get(), and
uniqueValues() work correctly with PG-backed data.
"""

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.pgindex import _PGIndexMapping
from plone.pgcatalog.pgindex import PGIndex
from tests.conftest import insert_object
from unittest import mock

import pytest


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------


def _catalog_objects(conn):
    """Insert and catalog several test objects.

    Returns a dict of {zoid: {path, uid}} for reference.
    """
    objects = {
        100: {
            "path": "/plone/doc1",
            "idx": {
                "portal_type": "Document",
                "UID": "uid-aaa-100",
                "Title": "First Document",
                "review_state": "published",
            },
        },
        101: {
            "path": "/plone/doc2",
            "idx": {
                "portal_type": "Document",
                "UID": "uid-bbb-101",
                "Title": "Second Document",
                "review_state": "private",
            },
        },
        102: {
            "path": "/plone/folder1",
            "idx": {
                "portal_type": "Folder",
                "UID": "uid-ccc-102",
                "Title": "A Folder",
                "review_state": "published",
            },
        },
    }
    for zoid, data in objects.items():
        insert_object(conn, zoid)
        catalog_object(conn, zoid=zoid, path=data["path"], idx=data["idx"])
    conn.commit()
    return objects


# ---------------------------------------------------------------------------
# _PGIndexMapping tests
# ---------------------------------------------------------------------------


class TestPGIndexMapping:
    def test_get_returns_zoid_for_existing_value(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        assert mapping.get("uid-aaa-100") == 100

    def test_get_returns_default_for_missing_value(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        assert mapping.get("nonexistent-uid") is None
        assert mapping.get("nonexistent-uid", -1) == -1

    def test_contains(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        assert "uid-aaa-100" in mapping
        assert "nonexistent" not in mapping

    def test_keys(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        keys = mapping.keys()
        assert set(keys) == {"uid-aaa-100", "uid-bbb-101", "uid-ccc-102"}

    def test_get_handles_connection_error(self):
        def bad_conn():
            raise RuntimeError("no connection")

        mapping = _PGIndexMapping("UID", bad_conn)
        assert mapping.get("anything") is None

    def test_keys_handles_connection_error(self):
        def bad_conn():
            raise RuntimeError("no connection")

        mapping = _PGIndexMapping("UID", bad_conn)
        assert mapping.keys() == []


# ---------------------------------------------------------------------------
# PGIndex tests
# ---------------------------------------------------------------------------


class TestPGIndex:
    def test_index_property_returns_mapping(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        wrapped = mock.Mock()
        wrapped.id = "UID"
        idx = PGIndex(wrapped, "UID", lambda: pg_conn_with_catalog)
        assert isinstance(idx._index, _PGIndexMapping)
        assert idx._index.get("uid-aaa-100") == 100

    def test_unique_values_without_lengths(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        wrapped = mock.Mock()
        wrapped.id = "portal_type"
        idx = PGIndex(wrapped, "portal_type", lambda: pg_conn_with_catalog)
        values = set(idx.uniqueValues())
        assert values == {"Document", "Folder"}

    def test_unique_values_with_lengths(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        wrapped = mock.Mock()
        wrapped.id = "portal_type"
        idx = PGIndex(wrapped, "portal_type", lambda: pg_conn_with_catalog)
        result = dict(idx.uniqueValues(withLengths=True))
        assert result == {"Document": 2, "Folder": 1}

    def test_unique_values_wrong_name_returns_nothing(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        wrapped = mock.Mock()
        wrapped.id = "portal_type"
        idx = PGIndex(wrapped, "portal_type", lambda: pg_conn_with_catalog)
        result = list(idx.uniqueValues(name="other_index") or [])
        assert result == []

    def test_delegates_unknown_attrs_to_wrapped(self):
        wrapped = mock.Mock()
        wrapped.id = "UID"
        wrapped.meta_type = "UUIDIndex"
        wrapped.some_method.return_value = 42

        def no_conn():
            raise RuntimeError("no conn")

        idx = PGIndex(wrapped, "UID", no_conn)
        assert idx.meta_type == "UUIDIndex"
        assert idx.some_method() == 42

    def test_unique_values_handles_connection_error(self):
        wrapped = mock.Mock()
        wrapped.id = "portal_type"

        def bad_conn():
            raise RuntimeError("no connection")

        idx = PGIndex(wrapped, "portal_type", bad_conn)
        result = list(idx.uniqueValues() or [])
        assert result == []


# ---------------------------------------------------------------------------
# PlonePGCatalogTool.getpath / .getrid / .Indexes integration tests
# ---------------------------------------------------------------------------


def _make_catalog(conn):
    """Create a PlonePGCatalogTool wired to a real PG connection.

    Bypasses CatalogTool.__init__ (needs full Zope) and patches
    _get_pg_read_connection to return the test connection.
    """
    from plone.pgcatalog.catalog import PlonePGCatalogTool

    catalog = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
    catalog._get_pg_read_connection = lambda: conn
    # ZCatalog needs _catalog.indexes for the Indexes container
    catalog._catalog = mock.Mock()
    catalog._catalog.indexes = {}
    return catalog


class TestCatalogGetpath:
    def test_returns_path_for_known_zoid(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        assert catalog.getpath(100) == "/plone/doc1"

    def test_returns_path_for_different_zoid(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        assert catalog.getpath(101) == "/plone/doc2"

    def test_raises_keyerror_for_unknown_zoid(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        with pytest.raises(KeyError):
            catalog.getpath(99999)

    def test_raises_keyerror_for_uncataloged_object(self, pg_conn_with_catalog):
        """Object exists in PG but has no path (not cataloged)."""
        insert_object(pg_conn_with_catalog, 200)
        pg_conn_with_catalog.commit()
        catalog = _make_catalog(pg_conn_with_catalog)
        with pytest.raises(KeyError):
            catalog.getpath(200)


class TestCatalogGetrid:
    def test_returns_zoid_for_known_path(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        assert catalog.getrid("/plone/doc1") == 100

    def test_returns_default_for_unknown_path(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        assert catalog.getrid("/plone/nonexistent") is None

    def test_returns_custom_default(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        assert catalog.getrid("/plone/nonexistent", default=-1) == -1


class TestCatalogIndexesWrapper:
    """Test that catalog.Indexes[name] returns PGIndex wrappers."""

    def _make_catalog_with_indexes(self, conn):
        """Catalog with a real UID index in _catalog.indexes."""
        catalog = _make_catalog(conn)
        # Simulate a UUIDIndex in _catalog.indexes
        uid_index = mock.Mock()
        uid_index.id = "UID"
        uid_index.meta_type = "UUIDIndex"
        portal_type_index = mock.Mock()
        portal_type_index.id = "portal_type"
        portal_type_index.meta_type = "FieldIndex"
        catalog._catalog.indexes = {
            "UID": uid_index,
            "portal_type": portal_type_index,
        }
        return catalog

    def test_indexes_uid_returns_pgindex(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = self._make_catalog_with_indexes(pg_conn_with_catalog)
        index = catalog.Indexes._getOb("UID")
        assert isinstance(index, PGIndex)

    def test_indexes_uid_index_get_returns_zoid(self, pg_conn_with_catalog):
        """Exact pattern used by plone.app.uuid.uuidToPhysicalPath."""
        _catalog_objects(pg_conn_with_catalog)
        catalog = self._make_catalog_with_indexes(pg_conn_with_catalog)
        index = catalog.Indexes._getOb("UID")
        rid = index._index.get("uid-aaa-100")
        assert rid == 100

    def test_indexes_uid_index_get_returns_none_for_missing(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = self._make_catalog_with_indexes(pg_conn_with_catalog)
        index = catalog.Indexes._getOb("UID")
        assert index._index.get("nonexistent-uid") is None

    def test_indexes_portal_type_unique_values(self, pg_conn_with_catalog):
        """Pattern used by plone.app.dexterity and plone.restapi."""
        _catalog_objects(pg_conn_with_catalog)
        catalog = self._make_catalog_with_indexes(pg_conn_with_catalog)
        index = catalog.Indexes._getOb("portal_type")
        result = dict(index.uniqueValues(withLengths=True))
        assert result == {"Document": 2, "Folder": 1}

    def test_indexes_delegates_meta_type(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = self._make_catalog_with_indexes(pg_conn_with_catalog)
        index = catalog.Indexes._getOb("UID")
        assert index.meta_type == "UUIDIndex"


class TestUUIDToPathFlow:
    """Integration test simulating the exact plone.app.uuid code path.

    plone.app.uuid.uuidToPhysicalPath() does:

        index = catalog.Indexes["UID"]
        rid = index._index.get(uuid)
        if not rid:
            return
        path = catalog.getpath(rid)
        return path

    This test verifies the full flow works end-to-end.
    """

    def test_uuid_to_physical_path(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        uid_index = mock.Mock()
        uid_index.id = "UID"
        catalog._catalog.indexes = {"UID": uid_index}

        # Simulate uuidToPhysicalPath
        index = catalog.Indexes._getOb("UID")
        rid = index._index.get("uid-aaa-100")
        assert rid is not None
        path = catalog.getpath(rid)
        assert path == "/plone/doc1"

    def test_uuid_to_physical_path_returns_none_for_missing(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)
        uid_index = mock.Mock()
        uid_index.id = "UID"
        catalog._catalog.indexes = {"UID": uid_index}

        index = catalog.Indexes._getOb("UID")
        rid = index._index.get("nonexistent-uid")
        assert rid is None
        # plone.app.uuid checks `if not rid: return`

    def test_getrid_for_vocabularies(self, pg_conn_with_catalog):
        """plone.app.vocabularies checks catalog.getrid(path) is not None."""
        _catalog_objects(pg_conn_with_catalog)
        catalog = _make_catalog(pg_conn_with_catalog)

        # Existing path → returns zoid (truthy)
        assert catalog.getrid("/plone/doc1") is not None
        # Missing path → returns None
        assert catalog.getrid("/plone/nonexistent") is None
