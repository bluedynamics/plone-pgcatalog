"""Tests for ZMI helper methods on PlonePGCatalogTool.

Tests manage_get_catalog_summary, manage_get_catalog_objects,
manage_get_object_detail, manage_get_indexes_and_metadata.
"""

from plone.pgcatalog.backends import BM25Backend
from plone.pgcatalog.backends import TsvectorBackend
from plone.pgcatalog.columns import IndexRegistry
from plone.pgcatalog.columns import IndexType
from unittest import mock

import pytest


@pytest.fixture()
def _mock_registry():
    """Populate the module-level IndexRegistry with test data."""
    from plone.pgcatalog import columns

    old = columns._registry
    reg = IndexRegistry()
    reg.register("Title", IndexType.TEXT, "Title", ["Title"])
    reg.register("portal_type", IndexType.FIELD, "portal_type", ["portal_type"])
    reg.register("SearchableText", IndexType.TEXT, None, ["SearchableText"])
    reg.register("path", IndexType.PATH, None, ["path"])
    reg.register("UID", IndexType.UUID, "uid", ["UID"])
    reg.add_metadata("Title")
    reg.add_metadata("Description")
    reg.add_metadata("portal_type")
    columns._registry = reg
    yield reg
    columns._registry = old


@pytest.fixture()
def catalog_tool():
    """Create a minimal PlonePGCatalogTool mock with PG connection."""
    from plone.pgcatalog.catalog import PlonePGCatalogTool

    tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
    return tool


class TestManageGetIndexesAndMetadata:
    """Tests for manage_get_indexes_and_metadata()."""

    def test_returns_indexes_sorted_by_name(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        names = [idx["name"] for idx in result["indexes"]]
        assert names == sorted(names)

    def test_index_count_matches(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        assert result["index_count"] == len(result["indexes"])
        assert result["index_count"] == 5

    def test_metadata_count_matches(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        assert result["metadata_count"] == len(result["metadata"])
        assert result["metadata_count"] == 3

    def test_metadata_sorted(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        assert result["metadata"] == sorted(result["metadata"])

    def test_special_index_has_dedicated_storage(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        special = next(i for i in result["indexes"] if i["name"] == "SearchableText")
        assert special["is_special"] is True
        assert special["storage"] == "dedicated column"
        assert special["idx_key"] == ""

    def test_regular_index_has_jsonb_storage(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        regular = next(i for i in result["indexes"] if i["name"] == "portal_type")
        assert regular["is_special"] is False
        assert regular["storage"] == "idx JSONB"
        assert regular["idx_key"] == "portal_type"

    def test_index_type_is_string(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        title_idx = next(i for i in result["indexes"] if i["name"] == "Title")
        assert title_idx["index_type"] == "ZCTextIndex"

    def test_source_attrs_joined(self, catalog_tool, _mock_registry):
        result = catalog_tool.manage_get_indexes_and_metadata()
        uid_idx = next(i for i in result["indexes"] if i["name"] == "UID")
        assert uid_idx["source_attrs"] == "UID"

    def test_empty_registry(self, catalog_tool):
        """With empty registry, returns empty lists."""
        from plone.pgcatalog import columns

        old = columns._registry
        columns._registry = IndexRegistry()
        try:
            result = catalog_tool.manage_get_indexes_and_metadata()
            assert result["indexes"] == []
            assert result["metadata"] == []
            assert result["index_count"] == 0
            assert result["metadata_count"] == 0
        finally:
            columns._registry = old


class TestManageGetCatalogSummary:
    """Tests for manage_get_catalog_summary()."""

    def test_tsvector_backend(self, catalog_tool, _mock_registry):
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = {"cnt": 42}
        with (
            mock.patch.object(
                catalog_tool, "_get_pg_read_connection", return_value=mock_conn
            ),
            mock.patch(
                "plone.pgcatalog.catalog.get_backend", return_value=TsvectorBackend()
            ),
        ):
            result = catalog_tool.manage_get_catalog_summary()
        assert result["object_count"] == 42
        assert result["backend_name"] == "Tsvector"
        assert result["has_bm25"] is False
        assert result["bm25_languages"] == []
        assert result["index_count"] == 5
        assert result["metadata_count"] == 3

    def test_bm25_backend(self, catalog_tool, _mock_registry):
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = {"cnt": 100}
        bm25 = BM25Backend.__new__(BM25Backend)
        bm25.languages = ["en", "de"]
        with (
            mock.patch.object(
                catalog_tool, "_get_pg_read_connection", return_value=mock_conn
            ),
            mock.patch("plone.pgcatalog.catalog.get_backend", return_value=bm25),
        ):
            result = catalog_tool.manage_get_catalog_summary()
        assert result["backend_name"] == "BM25"
        assert result["has_bm25"] is True
        assert result["bm25_languages"] == ["en", "de"]

    def test_empty_db(self, catalog_tool, _mock_registry):
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = {"cnt": 0}
        with (
            mock.patch.object(
                catalog_tool, "_get_pg_read_connection", return_value=mock_conn
            ),
            mock.patch(
                "plone.pgcatalog.catalog.get_backend", return_value=TsvectorBackend()
            ),
        ):
            result = catalog_tool.manage_get_catalog_summary()
        assert result["object_count"] == 0


class TestManageGetCatalogObjects:
    """Tests for manage_get_catalog_objects()."""

    def test_returns_objects_with_total(self, catalog_tool):
        rows = [
            {"zoid": 1, "path": "/Plone", "portal_type": "Plone Site", "_total": 100},
            {"zoid": 2, "path": "/Plone/doc", "portal_type": "Document", "_total": 100},
        ]
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchall.return_value = rows
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_catalog_objects()
        assert result["total"] == 100
        assert len(result["objects"]) == 2
        assert result["objects"][0]["path"] == "/Plone"
        assert result["objects"][1]["portal_type"] == "Document"

    def test_empty_result(self, catalog_tool):
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchall.return_value = []
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_catalog_objects()
        assert result["total"] == 0
        assert result["objects"] == []

    def test_null_portal_type_becomes_empty_string(self, catalog_tool):
        rows = [{"zoid": 1, "path": "/x", "portal_type": None, "_total": 1}]
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchall.return_value = rows
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_catalog_objects()
        assert result["objects"][0]["portal_type"] == ""

    def test_filterpath_escapes_wildcards(self, catalog_tool):
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchall.return_value = []
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            catalog_tool.manage_get_catalog_objects(filterpath="/Plone/100%_done")
        # Verify the LIKE parameter was escaped
        call_args = mock_conn.cursor().__enter__().execute.call_args
        params = call_args[0][1]
        assert params["prefix"] == "/Plone/100\\%\\_done%"

    def test_batch_start_passed_as_offset(self, catalog_tool):
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchall.return_value = []
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            catalog_tool.manage_get_catalog_objects(batch_start=40)
        call_args = mock_conn.cursor().__enter__().execute.call_args
        params = call_args[0][1]
        assert params["offset"] == 40


class TestManageGetObjectDetail:
    """Tests for manage_get_object_detail()."""

    def test_returns_sorted_idx_items(self, catalog_tool):
        row = {
            "path": "/Plone/doc",
            "idx": {"Title": "Hello", "portal_type": "Document", "UID": "abc-123"},
            "has_searchable_text": True,
            "searchable_text_preview": "'hello':1",
        }
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = row
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_object_detail(zoid=42)
        assert result["path"] == "/Plone/doc"
        keys = [item["key"] for item in result["idx_items"]]
        assert keys == ["Title", "UID", "portal_type"]

    def test_none_value_marked(self, catalog_tool):
        row = {
            "path": "/x",
            "idx": {"review_state": None},
            "has_searchable_text": False,
            "searchable_text_preview": None,
        }
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = row
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_object_detail(zoid=1)
        item = result["idx_items"][0]
        assert item["is_none"] is True
        assert item["value"] == ""

    def test_list_value_joined(self, catalog_tool):
        row = {
            "path": "/x",
            "idx": {"Subject": ["python", "zope"]},
            "has_searchable_text": False,
            "searchable_text_preview": None,
        }
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = row
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_object_detail(zoid=1)
        item = result["idx_items"][0]
        assert item["value"] == "python, zope"

    def test_bool_value_display(self, catalog_tool):
        row = {
            "path": "/x",
            "idx": {"is_folderish": True},
            "has_searchable_text": False,
            "searchable_text_preview": None,
        }
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = row
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_object_detail(zoid=1)
        assert result["idx_items"][0]["value"] == "True"

    def test_not_found_returns_none(self, catalog_tool):
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = None
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_object_detail(zoid=999)
        assert result is None

    def test_empty_idx_returns_empty_items(self, catalog_tool):
        row = {
            "path": "/x",
            "idx": None,
            "has_searchable_text": False,
            "searchable_text_preview": None,
        }
        mock_conn = mock.MagicMock()
        mock_conn.cursor().__enter__().fetchone.return_value = row
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_object_detail(zoid=1)
        assert result["idx_items"] == []
