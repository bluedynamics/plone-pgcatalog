"""Tests for PlonePGCatalogTool — helper methods and write path."""

from contextlib import contextmanager
from plone.pgcatalog.catalog import PlonePGCatalogTool
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.interfaces import IPGCatalogTool
from unittest import mock

import pytest


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

    def test_does_not_call_parent_catalog_object(self):
        """catalog_object() must NOT delegate to parent (no BTree writes)."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc")
        obj._p_oid = b"\x00\x00\x00\x00\x00\x00\x00\x01"

        with mock.patch.object(
            PlonePGCatalogTool, "_wrap_object", return_value=obj
        ), mock.patch.object(
            PlonePGCatalogTool, "_extract_idx", return_value={}
        ), mock.patch.object(
            PlonePGCatalogTool, "_extract_searchable_text", return_value=None
        ), mock.patch(
            "plone.pgcatalog.config.set_pending"
        ) as pending_mock, mock.patch.object(
            PlonePGCatalogTool.__bases__[0], "catalog_object"
        ) as parent_mock:
            tool.catalog_object(obj)
            # PG annotation was set
            pending_mock.assert_called_once()
            # Parent was NOT called (no BTree writes)
            parent_mock.assert_not_called()

    def test_noop_without_path(self):
        """catalog_object() is a no-op when object has no getPhysicalPath."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock(spec=[])  # no getPhysicalPath
        with mock.patch(
            "plone.pgcatalog.config.set_pending"
        ) as pending_mock, mock.patch.object(
            PlonePGCatalogTool.__bases__[0], "catalog_object"
        ) as parent_mock:
            tool.catalog_object(obj)
            # No PG annotation (no physical path)
            pending_mock.assert_not_called()
            # No parent call (no BTree writes)
            parent_mock.assert_not_called()


class TestUncatalogObjectWritePath:

    def test_uncatalog_does_not_call_parent(self):
        """uncatalog_object() must NOT delegate to parent (no BTree writes)."""
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
            # Parent was NOT called (no BTree writes)
            parent_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Dynamic extraction tests (IndexRegistry-based)
# ---------------------------------------------------------------------------


class TestExtractIdxDynamic:
    """Test _extract_idx with dynamic IndexRegistry instead of static KNOWN_INDEXES."""

    def _make_tool(self):
        return PlonePGCatalogTool.__new__(PlonePGCatalogTool)

    def test_extracts_dynamically_registered_index(self, populated_registry):
        """A dynamically registered FieldIndex should be extracted."""
        registry = get_registry()
        registry.register("my_addon_field", IndexType.FIELD, "my_addon_field")
        try:
            tool = self._make_tool()
            wrapper = mock.Mock()
            wrapper.my_addon_field = "addon_value"
            wrapper.portal_type = "Document"

            idx = tool._extract_idx(wrapper)
            assert idx["my_addon_field"] == "addon_value"
            assert idx["portal_type"] == "Document"
        finally:
            # Clean up dynamic registration
            registry._indexes.pop("my_addon_field", None)

    def test_uses_source_attrs_not_index_name(self, populated_registry):
        """When source_attrs differs from index name, extraction uses source_attrs."""
        registry = get_registry()
        # Register an index "myIndex" that reads from attribute "other_attr"
        registry.register(
            "myIndex", IndexType.FIELD, "myIndex", source_attrs=["other_attr"]
        )
        try:
            tool = self._make_tool()
            wrapper = mock.Mock(spec=["other_attr"])
            wrapper.other_attr = "from_other"

            idx = tool._extract_idx(wrapper)
            assert idx["myIndex"] == "from_other"
        finally:
            registry._indexes.pop("myIndex", None)

    def test_source_attrs_multi_uses_first_non_none(self, populated_registry):
        """With multiple source_attrs, use first non-None value."""
        registry = get_registry()
        registry.register(
            "multiAttr", IndexType.FIELD, "multiAttr",
            source_attrs=["attr_a", "attr_b"],
        )
        try:
            tool = self._make_tool()
            wrapper = mock.Mock(spec=["attr_a", "attr_b"])
            wrapper.attr_a = None
            wrapper.attr_b = "fallback_value"

            idx = tool._extract_idx(wrapper)
            assert idx["multiAttr"] == "fallback_value"
        finally:
            registry._indexes.pop("multiAttr", None)

    def test_extracts_metadata_columns(self, populated_registry):
        """Metadata-only columns should also be extracted into idx."""
        registry = get_registry()
        registry.add_metadata("custom_meta")
        try:
            tool = self._make_tool()
            wrapper = mock.Mock()
            wrapper.custom_meta = "meta_value"

            idx = tool._extract_idx(wrapper)
            assert idx["custom_meta"] == "meta_value"
        finally:
            registry._metadata.discard("custom_meta")

    def test_partial_reindex_with_dynamic_index(self, populated_registry):
        """Partial reindex (idxs param) should work with dynamic indexes."""
        registry = get_registry()
        registry.register("dyn_a", IndexType.FIELD, "dyn_a")
        registry.register("dyn_b", IndexType.FIELD, "dyn_b")
        try:
            tool = self._make_tool()
            wrapper = mock.Mock()
            wrapper.dyn_a = "val_a"
            wrapper.dyn_b = "val_b"

            idx = tool._extract_idx(wrapper, idxs=["dyn_a"])
            assert "dyn_a" in idx
            assert "dyn_b" not in idx
        finally:
            registry._indexes.pop("dyn_a", None)
            registry._indexes.pop("dyn_b", None)

    def test_skips_special_indexes(self, populated_registry):
        """Indexes with idx_key=None (special) should be skipped in extraction."""
        tool = self._make_tool()
        wrapper = mock.Mock()

        idx = tool._extract_idx(wrapper)
        # SearchableText, effectiveRange, path have idx_key=None → not in idx
        assert "SearchableText" not in idx
        assert "effectiveRange" not in idx
        assert "path" not in idx

    def test_callable_value_extracted(self, populated_registry):
        """Callable attributes (indexers) should be called for their value."""
        registry = get_registry()
        registry.register("callable_idx", IndexType.FIELD, "callable_idx")
        try:
            tool = self._make_tool()
            wrapper = mock.Mock()
            wrapper.callable_idx.return_value = "called_value"

            idx = tool._extract_idx(wrapper)
            assert idx["callable_idx"] == "called_value"
        finally:
            registry._indexes.pop("callable_idx", None)

    def test_exception_in_extraction_skipped(self, populated_registry):
        """If an indexer raises, that field is skipped gracefully."""
        registry = get_registry()
        registry.register("broken_idx", IndexType.FIELD, "broken_idx")
        try:
            tool = self._make_tool()
            wrapper = mock.Mock()
            wrapper.broken_idx.side_effect = RuntimeError("broken")
            wrapper.portal_type = "Document"

            idx = tool._extract_idx(wrapper)
            assert "broken_idx" not in idx
            assert idx["portal_type"] == "Document"
        finally:
            registry._indexes.pop("broken_idx", None)


class TestIPGIndexTranslatorExtract:
    """IPGIndexTranslator.extract() fallback in _extract_idx."""

    def _make_tool(self):
        return PlonePGCatalogTool.__new__(PlonePGCatalogTool)

    def test_translator_extract_called(self, populated_registry):
        """Custom translator.extract() is called during indexing."""
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import getSiteManager

        translator = mock.Mock()
        translator.extract.return_value = {
            "event_start": "2025-01-15T10:00:00",
            "event_end": "2025-01-20T18:00:00",
        }

        sm = getSiteManager()
        sm.registerUtility(translator, IPGIndexTranslator, name="event_range")
        try:
            tool = self._make_tool()
            wrapper = mock.Mock()

            idx = tool._extract_idx(wrapper)
            translator.extract.assert_called_once_with(wrapper, "event_range")
            assert idx["event_start"] == "2025-01-15T10:00:00"
            assert idx["event_end"] == "2025-01-20T18:00:00"
        finally:
            sm.unregisterUtility(provided=IPGIndexTranslator, name="event_range")

    def test_translator_extract_exception_skipped(self, populated_registry):
        """If translator.extract() raises, it's skipped gracefully."""
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import getSiteManager

        translator = mock.Mock()
        translator.extract.side_effect = RuntimeError("broken translator")

        sm = getSiteManager()
        sm.registerUtility(translator, IPGIndexTranslator, name="broken_range")
        try:
            tool = self._make_tool()
            wrapper = mock.Mock()
            wrapper.portal_type = "Document"

            idx = tool._extract_idx(wrapper)
            # Doesn't crash, regular indexes still extracted
            assert idx["portal_type"] == "Document"
        finally:
            sm.unregisterUtility(provided=IPGIndexTranslator, name="broken_range")
