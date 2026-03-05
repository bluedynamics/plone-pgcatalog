"""Tests for ZMI helper methods on PlonePGCatalogTool.

Tests manage_get_catalog_summary, manage_get_catalog_objects,
manage_get_object_detail, manage_get_indexes_and_metadata,
manage_get_blob_stats.
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


class TestFormatSize:
    """Tests for the _format_size() helper."""

    def test_zero(self):
        from plone.pgcatalog.catalog import _format_size

        assert _format_size(0) == "0 B"

    def test_bytes(self):
        from plone.pgcatalog.catalog import _format_size

        assert _format_size(500) == "500 B"

    def test_kilobytes(self):
        from plone.pgcatalog.catalog import _format_size

        assert _format_size(1536) == "1.5 KB"

    def test_megabytes(self):
        from plone.pgcatalog.catalog import _format_size

        assert _format_size(10485760) == "10.0 MB"

    def test_gigabytes(self):
        from plone.pgcatalog.catalog import _format_size

        assert _format_size(1073741824) == "1.0 GB"

    def test_terabytes(self):
        from plone.pgcatalog.catalog import _format_size

        assert _format_size(1099511627776) == "1.0 TB"


class TestManageGetBlobStats:
    """Tests for manage_get_blob_stats()."""

    def _make_mock_conn(self, fetchone_returns):
        """Create a mock connection with sequential fetchone results."""
        mock_conn = mock.MagicMock()
        cursor_ctx = mock_conn.cursor().__enter__()
        cursor_ctx.fetchone.side_effect = fetchone_returns
        return mock_conn

    def test_table_not_exists(self, catalog_tool):
        mock_conn = self._make_mock_conn([{"exists": False}])
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_stats()
        assert result == {"available": False}

    def test_empty_table(self, catalog_tool):
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {
                    "total_blobs": 0,
                    "unique_objects": 0,
                    "total_size": 0,
                    "pg_count": 0,
                    "pg_size": 0,
                    "s3_count": 0,
                    "s3_size": 0,
                    "largest_blob": 0,
                    "avg_blob_size": 0,
                },
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_stats()
        assert result["available"] is True
        assert result["total_blobs"] == 0
        assert result["unique_objects"] == 0
        assert result["total_size_display"] == "0 B"
        assert result["avg_versions"] == 0

    def test_pg_only_blobs(self, catalog_tool):
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {
                    "total_blobs": 100,
                    "unique_objects": 50,
                    "total_size": 10485760,
                    "pg_count": 100,
                    "pg_size": 10485760,
                    "s3_count": 0,
                    "s3_size": 0,
                    "largest_blob": 1048576,
                    "avg_blob_size": 104857,
                },
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_stats()
        assert result["available"] is True
        assert result["total_blobs"] == 100
        assert result["unique_objects"] == 50
        assert result["pg_count"] == 100
        assert result["s3_count"] == 0
        assert result["avg_versions"] == 2.0
        assert result["total_size_display"] == "10.0 MB"
        assert result["largest_blob_display"] == "1.0 MB"

    def test_mixed_tiers(self, catalog_tool):
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {
                    "total_blobs": 200,
                    "unique_objects": 150,
                    "total_size": 1073741824,
                    "pg_count": 80,
                    "pg_size": 73741824,
                    "s3_count": 120,
                    "s3_size": 1000000000,
                    "largest_blob": 52428800,
                    "avg_blob_size": 5368709,
                },
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_stats()
        assert result["pg_count"] == 80
        assert result["s3_count"] == 120
        assert result["total_size_display"] == "1.0 GB"

    def test_avg_versions_single_version(self, catalog_tool):
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {
                    "total_blobs": 10,
                    "unique_objects": 10,
                    "total_size": 1024,
                    "pg_count": 10,
                    "pg_size": 1024,
                    "s3_count": 0,
                    "s3_size": 0,
                    "largest_blob": 512,
                    "avg_blob_size": 102,
                },
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_stats()
        assert result["avg_versions"] == 1.0


class TestManageGetBlobHistogram:
    """Tests for manage_get_blob_histogram() — logarithmic buckets."""

    def _make_mock_conn(self, fetchone_sequence):
        """Create mock conn with sequential fetchone results."""
        mock_conn = mock.MagicMock()
        cursor_ctx = mock_conn.cursor().__enter__()
        cursor_ctx.fetchone.side_effect = fetchone_sequence
        return mock_conn

    def test_table_not_exists(self, catalog_tool):
        mock_conn = self._make_mock_conn([{"exists": False}])
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert result == []

    def test_empty_table(self, catalog_tool):
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {"min_size": None, "max_size": None, "cnt": 0},
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert result == []

    def test_small_blobs_single_bucket(self, catalog_tool):
        """All blobs < 10 KB → single bucket 0–10 KB."""
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {"min_size": 10, "max_size": 500, "cnt": 42},
                # Boundaries: [0, 10240] — next boundary > 500 is 10240
                {"bucket_0": 42},
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert len(result) == 1
        assert result[0]["count"] == 42
        assert result[0]["pct"] == 100
        assert "10.0 KB" in result[0]["label"]

    def test_spans_multiple_decades(self, catalog_tool):
        """Blobs from 500 B to 5 MB → 4 buckets: 0–10KB, 10–100KB,
        100KB–1MB, 1MB–10MB."""
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {"min_size": 500, "max_size": 5_000_000, "cnt": 100},
                # Boundaries: [0, 10KB, 100KB, 1MB, 10MB]
                {"bucket_0": 10, "bucket_1": 30, "bucket_2": 25, "bucket_3": 35},
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert len(result) == 4
        # Largest bucket (35) gets pct=100
        assert result[3]["count"] == 35
        assert result[3]["pct"] == 100
        # Last bucket label ends with "10.0 MB" (clean boundary)
        assert "10.0 MB" in result[3]["label"]

    def test_pct_relative_to_max(self, catalog_tool):
        """pct should be relative to the largest bucket."""
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {"min_size": 100, "max_size": 2000, "cnt": 30},
                # Boundaries: [0, 10240] — next > 2000 is 10240, single bucket
                {"bucket_0": 30},
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert len(result) == 1
        assert result[0]["pct"] == 100

    def test_labels_use_format_size(self, catalog_tool):
        """Labels should use human-readable sizes with clean boundaries."""
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {"min_size": 500, "max_size": 50_000, "cnt": 10},
                # Boundaries: [0, 10KB, 100KB] — next > 50000 is 102400
                {"bucket_0": 5, "bucket_1": 5},
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert result[0]["label"] == "0 B – 10.0 KB"
        assert "100.0 KB" in result[1]["label"]

    def test_tier_without_s3(self, catalog_tool):
        """Without S3, tier should be empty string."""
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                {"min_size": 100, "max_size": 2000, "cnt": 10},
                {"bucket_0": 10},
            ]
        )
        with mock.patch.object(
            catalog_tool, "_get_pg_read_connection", return_value=mock_conn
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert result[0]["tier"] == ""

    def test_tier_with_s3_threshold(self, catalog_tool):
        """With S3, buckets should be classified as pg/s3/mixed."""
        mock_conn = self._make_mock_conn(
            [
                {"exists": True},
                # Range spans 0 to 5 MB
                {"min_size": 500, "max_size": 5_000_000, "cnt": 100},
                # Boundaries: [0, 10KB, 100KB, 1MB, 10MB]
                {"bucket_0": 10, "bucket_1": 20, "bucket_2": 30, "bucket_3": 40},
            ]
        )
        # threshold = 1 MB (1048576)
        with (
            mock.patch.object(
                catalog_tool, "_get_pg_read_connection", return_value=mock_conn
            ),
            mock.patch.object(
                catalog_tool, "_get_blob_threshold", return_value=1048576
            ),
        ):
            result = catalog_tool.manage_get_blob_histogram()
        assert len(result) == 4
        # 0-10KB: entirely below 1MB -> pg
        assert result[0]["tier"] == "pg"
        # 10KB-100KB: entirely below 1MB -> pg
        assert result[1]["tier"] == "pg"
        # 100KB-1MB: hi == threshold -> pg (hi <= threshold)
        assert result[2]["tier"] == "pg"
        # 1MB-10MB: lo >= threshold -> s3
        assert result[3]["tier"] == "s3"
