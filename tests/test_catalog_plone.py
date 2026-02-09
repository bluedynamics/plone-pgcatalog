"""Tests for PlonePGCatalogTool — helper methods and write path."""

from contextlib import contextmanager
from plone.pgcatalog.catalog import PlonePGCatalogTool
from plone.pgcatalog.interfaces import IPGCatalogTool
from unittest import mock


class TestImplementsInterface:

    def test_implements_ipgcatalogtool(self):
        assert IPGCatalogTool.implementedBy(PlonePGCatalogTool)


class TestObjToZoid:

    def test_with_p_oid(self):
        obj = mock.Mock()
        obj._p_oid = b"\x00\x00\x00\x00\x00\x00\x01\x00"
        assert PlonePGCatalogTool._obj_to_zoid(obj) == 256

    def test_with_zero_oid(self):
        obj = mock.Mock()
        obj._p_oid = b"\x00\x00\x00\x00\x00\x00\x00\x00"
        assert PlonePGCatalogTool._obj_to_zoid(obj) == 0

    def test_without_p_oid(self):
        obj = mock.Mock(spec=[])
        assert PlonePGCatalogTool._obj_to_zoid(obj) is None


class TestExtractSearchableText:

    def test_extracts_string(self):
        wrapper = mock.Mock()
        wrapper.SearchableText = "hello world"
        assert PlonePGCatalogTool._extract_searchable_text(wrapper) == "hello world"

    def test_extracts_from_callable(self):
        wrapper = mock.Mock()
        wrapper.SearchableText.return_value = "hello callable"
        assert PlonePGCatalogTool._extract_searchable_text(wrapper) == "hello callable"

    def test_returns_none_for_non_string(self):
        wrapper = mock.Mock()
        wrapper.SearchableText = 42
        assert PlonePGCatalogTool._extract_searchable_text(wrapper) is None

    def test_returns_none_on_missing_attr(self):
        wrapper = mock.Mock(spec=[])
        assert PlonePGCatalogTool._extract_searchable_text(wrapper) is None

    def test_returns_none_on_exception(self):
        wrapper = mock.Mock()
        type(wrapper).SearchableText = mock.PropertyMock(side_effect=RuntimeError)
        assert PlonePGCatalogTool._extract_searchable_text(wrapper) is None


class TestExtractIdx:

    def _make_tool(self):
        # PlonePGCatalogTool.__init__ chains to CatalogTool which does a lot;
        # bypass by calling _extract_idx as unbound method via the class.
        return PlonePGCatalogTool.__new__(PlonePGCatalogTool)

    def test_extracts_known_indexes(self):
        tool = self._make_tool()
        wrapper = mock.Mock()
        wrapper.portal_type = "Document"
        wrapper.review_state = "published"
        wrapper.Title = "Hello"
        wrapper.is_folderish = False

        idx = tool._extract_idx(wrapper)
        assert idx["portal_type"] == "Document"
        assert idx["review_state"] == "published"
        assert idx["Title"] == "Hello"

    def test_partial_reindex(self):
        tool = self._make_tool()
        wrapper = mock.Mock()
        wrapper.portal_type = "Document"
        wrapper.review_state = "published"

        idx = tool._extract_idx(wrapper, idxs=["portal_type"])
        assert "portal_type" in idx
        assert "review_state" not in idx

    def test_skips_on_exception(self):
        tool = self._make_tool()
        wrapper = mock.Mock()
        # Make portal_type a callable that raises (simulating a broken indexer)
        wrapper.portal_type.side_effect = RuntimeError("indexer error")
        wrapper.review_state = "published"

        idx = tool._extract_idx(wrapper)
        assert "portal_type" not in idx
        assert idx["review_state"] == "published"


class TestWrapObject:

    def test_wraps_with_adapter(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        wrapped = mock.Mock()
        with mock.patch(
            "zope.component.queryMultiAdapter",
            return_value=wrapped,
        ):
            result = tool._wrap_object(obj)
            assert result is wrapped

    def test_returns_obj_if_no_adapter(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        with mock.patch(
            "zope.component.queryMultiAdapter",
            return_value=None,
        ):
            result = tool._wrap_object(obj)
            assert result is obj


def _mock_pg_connection(mock_conn):
    """Create a mock _pg_connection context manager that yields mock_conn."""

    @contextmanager
    def _cm(_self):
        yield mock_conn

    return _cm


class TestCatalogObjectWritePath:

    def test_skips_pg_without_p_oid(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock(spec=["getPhysicalPath"])
        obj.getPhysicalPath.return_value = ("", "plone", "doc")
        # No _p_oid → skip PG write
        mock_conn = mock.Mock()
        with mock.patch.object(
            PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
        ), mock.patch.object(
            PlonePGCatalogTool.__bases__[0], "catalog_object"
        ), mock.patch(
            "plone.pgcatalog.catalog._sql_catalog"
        ) as sql_mock:
            tool.catalog_object(obj)
            sql_mock.assert_not_called()

    def test_skips_pg_on_exception(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc")
        obj._p_oid = b"\x00\x00\x00\x00\x00\x00\x00\x01"

        mock_conn = mock.Mock()

        with mock.patch.object(
            PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
        ), mock.patch.object(
            PlonePGCatalogTool, "_wrap_object", return_value=obj
        ), mock.patch.object(
            PlonePGCatalogTool, "_extract_idx", return_value={}
        ), mock.patch.object(
            PlonePGCatalogTool, "_extract_searchable_text", return_value=None
        ), mock.patch(
            "plone.pgcatalog.catalog._sql_catalog",
            side_effect=RuntimeError("PG down"),
        ), mock.patch.object(
            PlonePGCatalogTool.__bases__[0], "catalog_object"
        ) as parent_mock:
            tool.catalog_object(obj)
            # Parent is still called even if PG fails
            parent_mock.assert_called_once()

    def test_falls_back_to_parent_without_path(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock(spec=[])  # no getPhysicalPath
        with mock.patch.object(
            PlonePGCatalogTool.__bases__[0], "catalog_object"
        ) as parent_mock:
            tool.catalog_object(obj)
            parent_mock.assert_called_once()


class TestUncatalogObjectWritePath:

    def test_uncatalog_calls_parent(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = mock.Mock(return_value=False)
        mock_cursor.fetchone.return_value = {"zoid": 42}

        with mock.patch.object(
            PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
        ), mock.patch(
            "plone.pgcatalog.catalog._sql_uncatalog"
        ) as uncatalog_mock, mock.patch.object(
            PlonePGCatalogTool.__bases__[0], "uncatalog_object"
        ) as parent_mock:
            tool.uncatalog_object("/plone/doc")
            uncatalog_mock.assert_called_once_with(mock_conn, zoid=42)
            parent_mock.assert_called_once_with("/plone/doc")
