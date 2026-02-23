"""Tests for PlonePGCatalogTool — helper methods and write path."""

from contextlib import contextmanager
from plone.pgcatalog.catalog import _path_value_to_string
from plone.pgcatalog.catalog import PlonePGCatalogTool
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.interfaces import IPGCatalogTool
from unittest import mock


class TestImplementsInterface:
    def test_implements_ipgcatalogtool(self):
        assert IPGCatalogTool.implementedBy(PlonePGCatalogTool)


class TestSecurityDeclarations:
    """Verify Zope security declarations on PlonePGCatalogTool."""

    def test_unrestricted_search_is_private(self):
        # declarePrivate sets MethodName__roles__ = () (empty tuple = private)
        roles = getattr(PlonePGCatalogTool, "unrestrictedSearchResults__roles__", None)
        assert roles == (), f"Expected () for private, got {roles!r}"

    def test_refresh_catalog_is_protected(self):
        # declareProtected sets MethodName__roles__ = PermissionRole(...)
        roles = getattr(PlonePGCatalogTool, "refreshCatalog__roles__", None)
        assert roles is not None and roles != ()

    def test_reindex_index_is_protected(self):
        roles = getattr(PlonePGCatalogTool, "reindexIndex__roles__", None)
        assert roles is not None and roles != ()

    def test_clear_find_and_rebuild_is_protected(self):
        roles = getattr(PlonePGCatalogTool, "clearFindAndRebuild__roles__", None)
        assert roles is not None and roles != ()

    def test_ac_permissions_includes_manage_entries(self):
        # __ac_permissions__ maps permission → method names
        perms = PlonePGCatalogTool.__ac_permissions__
        manage_entries = None
        for perm_name, methods in perms:
            if perm_name == "Manage ZCatalog Entries":
                manage_entries = methods
                break
        assert manage_entries is not None, (
            "Manage ZCatalog Entries not in __ac_permissions__"
        )
        assert "refreshCatalog" in manage_entries
        assert "reindexIndex" in manage_entries
        assert "clearFindAndRebuild" in manage_entries


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
        with (
            mock.patch.object(
                PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
            ),
            mock.patch("plone.pgcatalog.catalog._sql_catalog") as sql_mock,
        ):
            tool.catalog_object(obj)
            sql_mock.assert_not_called()

    def test_sets_pending_annotation(self):
        """catalog_object() sets PG pending annotation (no BTree writes)."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc")
        obj._p_oid = b"\x00\x00\x00\x00\x00\x00\x00\x01"

        with (
            mock.patch.object(PlonePGCatalogTool, "_wrap_object", return_value=obj),
            mock.patch.object(PlonePGCatalogTool, "_extract_idx", return_value={}),
            mock.patch.object(
                PlonePGCatalogTool, "_extract_searchable_text", return_value=None
            ),
            mock.patch("plone.pgcatalog.catalog.set_pending") as pending_mock,
        ):
            tool.catalog_object(obj)
            # PG annotation was set
            pending_mock.assert_called_once()

    def test_noop_without_path(self):
        """catalog_object() is a no-op when object has no getPhysicalPath."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock(spec=[])  # no getPhysicalPath
        with mock.patch("plone.pgcatalog.catalog.set_pending") as pending_mock:
            tool.catalog_object(obj)
            # No PG annotation (no physical path)
            pending_mock.assert_not_called()


class TestUncatalogObjectWritePath:
    def test_uncatalog_uses_pg(self):
        """uncatalog_object() calls PG uncatalog (no BTree writes)."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = mock.Mock(return_value=False)
        mock_cursor.fetchone.return_value = {"zoid": 42}

        with (
            mock.patch.object(
                PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
            ),
            mock.patch("plone.pgcatalog.catalog._sql_uncatalog") as uncatalog_mock,
        ):
            tool.uncatalog_object("/plone/doc")
            uncatalog_mock.assert_called_once_with(mock_conn, zoid=42)


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
            "multiAttr",
            IndexType.FIELD,
            "multiAttr",
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


# ---------------------------------------------------------------------------
# _path_value_to_string tests
# ---------------------------------------------------------------------------


class TestPathValueToString:
    def test_none_returns_none(self):
        assert _path_value_to_string(None) is None

    def test_non_empty_string(self):
        assert _path_value_to_string("/plone/doc") == "/plone/doc"

    def test_empty_string_returns_none(self):
        assert _path_value_to_string("") is None

    def test_list_of_components(self):
        assert (
            _path_value_to_string(["uuid1", "uuid2", "uuid3"]) == "/uuid1/uuid2/uuid3"
        )

    def test_tuple_of_components(self):
        assert _path_value_to_string(("a", "b")) == "/a/b"

    def test_empty_list_returns_none(self):
        assert _path_value_to_string([]) is None

    def test_empty_tuple_returns_none(self):
        assert _path_value_to_string(()) is None

    def test_other_type_returns_str(self):
        assert _path_value_to_string(42) == "42"


# ---------------------------------------------------------------------------
# _pg_connection context manager tests
# ---------------------------------------------------------------------------


class TestPgConnection:
    def test_reuses_request_scoped_connection(self):
        """_pg_connection reuses an existing request-scoped connection."""
        import plone.pgcatalog.pending as pending_mod

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        mock_conn.closed = False

        pending_mod._local.pgcat_conn = mock_conn
        try:
            with tool._pg_connection() as conn:
                assert conn is mock_conn
        finally:
            pending_mod._local.pgcat_conn = None
            pending_mod._local.pgcat_pool = None

    def test_borrows_from_pool_when_no_request_conn(self):
        """_pg_connection borrows from pool when no request-scoped conn."""
        import plone.pgcatalog.pending as pending_mod

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_pool = mock.Mock()
        mock_conn = mock.Mock()
        mock_pool.getconn.return_value = mock_conn

        pending_mod._local.pgcat_conn = None
        try:
            with (
                mock.patch("plone.pgcatalog.catalog.get_pool", return_value=mock_pool),
            ):
                with tool._pg_connection() as conn:
                    assert conn is mock_conn
                mock_pool.putconn.assert_called_once_with(mock_conn)
        finally:
            pending_mod._local.pgcat_conn = None
            pending_mod._local.pgcat_pool = None


# ---------------------------------------------------------------------------
# indexObject / reindexObject tests
# ---------------------------------------------------------------------------


class TestIndexObjectReindexObject:
    def test_indexObject_calls_set_pg_annotation(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        with mock.patch.object(tool, "_set_pg_annotation") as ann_mock:
            tool.indexObject(obj)
            ann_mock.assert_called_once_with(obj)

    def test_reindexObject_calls_set_pg_annotation(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        with mock.patch.object(tool, "_set_pg_annotation") as ann_mock:
            tool.reindexObject(obj, uid="/plone/doc")
            ann_mock.assert_called_once_with(obj, "/plone/doc")


# ---------------------------------------------------------------------------
# uncatalog_object — persistent object branch
# ---------------------------------------------------------------------------


class TestUncatalogObjectPersistent:
    def test_uncatalog_with_persistent_obj(self):
        """uncatalog_object sets None pending when object has _p_oid."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        obj = mock.Mock()
        obj._p_oid = b"\x00\x00\x00\x00\x00\x00\x00\x05"

        with (
            mock.patch.object(
                PlonePGCatalogTool, "unrestrictedTraverse", return_value=obj
            ),
            mock.patch("plone.pgcatalog.catalog.set_pending") as pending_mock,
        ):
            tool.uncatalog_object("/plone/doc")
            pending_mock.assert_called_once_with(5, None)
            assert obj._p_changed is True

    def test_uncatalog_traverse_exception(self):
        """uncatalog_object handles unrestrictedTraverse raising."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)

        with (
            mock.patch.object(
                PlonePGCatalogTool,
                "unrestrictedTraverse",
                side_effect=RuntimeError("traverse failed"),
            ),
            mock.patch.object(
                PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock.Mock())
            ),
        ):
            # Should not raise
            tool.uncatalog_object("/plone/missing")


# ---------------------------------------------------------------------------
# searchResults / unrestrictedSearchResults
# ---------------------------------------------------------------------------


class TestSearchResults:
    def test_searchResults_uses_storage_connection(self):
        """searchResults uses the ZODB storage instance connection."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        mock_results = mock.Mock()

        with (
            mock.patch("AccessControl.getSecurityManager") as sm_mock,
            mock.patch.object(
                tool, "_listAllowedRolesAndUsers", return_value=["Anonymous"]
            ),
            mock.patch(
                "plone.pgcatalog.catalog.get_storage_connection",
                return_value=mock_conn,
            ),
            mock.patch(
                "plone.pgcatalog.catalog._run_search", return_value=mock_results
            ) as run_mock,
            mock.patch(
                "plone.pgcatalog.catalog.apply_security_filters",
                side_effect=lambda q, r, **kw: q,
            ),
        ):
            sm_mock.return_value.checkPermission.return_value = False
            result = tool.searchResults({"portal_type": "Document"})
            assert result is mock_results
            run_mock.assert_called_once()
            # Verify lazy_conn is the storage connection
            call_kwargs = run_mock.call_args
            assert call_kwargs.kwargs.get("lazy_conn") is mock_conn

    def test_searchResults_falls_back_to_pool(self):
        """searchResults falls back to pool when no storage connection."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_pool = mock.Mock()
        mock_conn = mock.Mock()
        mock_results = mock.Mock()

        with (
            mock.patch("AccessControl.getSecurityManager") as sm_mock,
            mock.patch.object(
                tool, "_listAllowedRolesAndUsers", return_value=["Anonymous"]
            ),
            mock.patch(
                "plone.pgcatalog.catalog.get_storage_connection", return_value=None
            ),
            mock.patch("plone.pgcatalog.catalog.get_pool", return_value=mock_pool),
            mock.patch(
                "plone.pgcatalog.catalog.get_request_connection", return_value=mock_conn
            ),
            mock.patch(
                "plone.pgcatalog.catalog._run_search", return_value=mock_results
            ) as run_mock,
            mock.patch(
                "plone.pgcatalog.catalog.apply_security_filters",
                side_effect=lambda q, r, **kw: q,
            ),
        ):
            sm_mock.return_value.checkPermission.return_value = False
            result = tool.searchResults({"portal_type": "Document"})
            assert result is mock_results
            run_mock.assert_called_once()

    def test_unrestrictedSearchResults_uses_storage_connection(self):
        """unrestrictedSearchResults uses the ZODB storage instance connection."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        mock_results = mock.Mock()

        with (
            mock.patch(
                "plone.pgcatalog.catalog.get_storage_connection",
                return_value=mock_conn,
            ),
            mock.patch(
                "plone.pgcatalog.catalog._run_search", return_value=mock_results
            ) as run_mock,
        ):
            result = tool.unrestrictedSearchResults(portal_type="Document")
            assert result is mock_results
            run_mock.assert_called_once()
            assert run_mock.call_args.kwargs.get("lazy_conn") is mock_conn


# ---------------------------------------------------------------------------
# Maintenance methods
# ---------------------------------------------------------------------------


class TestMaintenanceMethods:
    def test_refreshCatalog_no_clear(self):
        """refreshCatalog(clear=0) queries PG for cataloged paths and re-catalogs."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        # Simulate PG returning two rows
        mock_cursor = mock.Mock()
        mock_cursor.fetchall.return_value = [
            {"path": "/plone/doc1"},
            {"path": "/plone/doc2"},
        ]
        mock_conn.cursor.return_value.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = mock.Mock(return_value=False)
        with (
            mock.patch.object(
                PlonePGCatalogTool, "_get_pg_read_connection", return_value=mock_conn
            ),
            mock.patch.object(
                PlonePGCatalogTool, "unrestrictedTraverse", return_value=mock.Mock()
            ),
            mock.patch.object(PlonePGCatalogTool, "catalog_object") as cat_mock,
        ):
            result = tool.refreshCatalog()
            assert result == 2
            assert cat_mock.call_count == 2

    def test_refreshCatalog_with_clear(self):
        """refreshCatalog(clear=1) delegates to clearFindAndRebuild."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        with mock.patch.object(PlonePGCatalogTool, "clearFindAndRebuild") as cfr_mock:
            tool.refreshCatalog(clear=1)
            cfr_mock.assert_called_once()

    def test_refreshCatalog_accepts_pghandler(self):
        """refreshCatalog accepts pghandler for ZCatalog API compat."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        with mock.patch.object(PlonePGCatalogTool, "clearFindAndRebuild"):
            # Should not raise TypeError
            tool.refreshCatalog(clear=1, pghandler=mock.Mock())

    def test_reindexIndex(self):
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        with (
            mock.patch.object(
                PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
            ),
            mock.patch(
                "plone.pgcatalog.catalog.reindex_index", return_value=3
            ) as reindex_mock,
        ):
            result = tool.reindexIndex("portal_type")
            assert result == 3
            reindex_mock.assert_called_once_with(mock_conn, "portal_type")

    def test_reindexIndex_accepts_pghandler(self):
        """reindexIndex accepts pghandler kwarg (ZCatalog compat)."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        with (
            mock.patch.object(
                PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
            ),
            mock.patch("plone.pgcatalog.catalog.reindex_index", return_value=0),
        ):
            # Should not raise TypeError
            tool.reindexIndex("portal_type", None, pghandler=mock.Mock())

    def test_reindexIndex_three_positional_args(self):
        """reindexIndex accepts 3 positional args (manage_reindexIndex compat)."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        handler = mock.Mock()
        with (
            mock.patch.object(
                PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
            ),
            mock.patch("plone.pgcatalog.catalog.reindex_index", return_value=0),
        ):
            # Should not raise TypeError — ZMI calls with 3 positional args
            tool.reindexIndex("portal_type", None, handler)

    def test_clearFindAndRebuild(self):
        """clearFindAndRebuild clears PG data then walks portal tree."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        mock_conn = mock.Mock()
        mock_portal = mock.Mock()
        with (
            mock.patch.object(
                PlonePGCatalogTool, "_pg_connection", _mock_pg_connection(mock_conn)
            ),
            mock.patch(
                "plone.pgcatalog.catalog.clear_catalog_data", return_value=10
            ) as clear_mock,
            mock.patch("plone.pgcatalog.catalog.aq_parent", return_value=mock_portal),
            mock.patch("plone.pgcatalog.catalog.aq_inner", return_value=tool),
        ):
            tool.clearFindAndRebuild()
            clear_mock.assert_called_once_with(mock_conn)
            mock_portal.ZopeFindAndApply.assert_called_once()


# ---------------------------------------------------------------------------
# _run_search lazy mode tests
# ---------------------------------------------------------------------------


class TestRunSearchLazyMode:
    def test_lazy_mode_wires_result_set(self):
        """_run_search with lazy_conn wires brain._result_set for lazy loading."""
        from plone.pgcatalog.catalog import _run_search

        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = mock.Mock(return_value=False)
        mock_cursor.fetchall.return_value = [
            {"zoid": 1, "path": "/plone/a"},
            {"zoid": 2, "path": "/plone/b"},
        ]

        with mock.patch("plone.pgcatalog.catalog.build_query") as bq:
            bq.return_value = {
                "where": "TRUE",
                "params": {},
                "order_by": None,
                "limit": None,
                "offset": None,
            }
            results = _run_search(mock_conn, {}, lazy_conn=mock_conn)

        assert len(results) == 2
        # Brains should have _result_set wired
        for brain in results:
            assert brain._result_set is results


# ---------------------------------------------------------------------------
# _extract_idx — metadata callable extraction
# ---------------------------------------------------------------------------


class TestExtractIdxMetadataCallable:
    def test_metadata_callable_is_called(self, populated_registry):
        """Metadata columns with callable values should be called."""
        registry = get_registry()
        registry.add_metadata("dynamic_meta")
        try:
            tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
            wrapper = mock.Mock()
            wrapper.dynamic_meta.return_value = "called_meta_value"

            idx = tool._extract_idx(wrapper)
            assert idx["dynamic_meta"] == "called_meta_value"
        finally:
            registry._metadata.discard("dynamic_meta")

    def test_metadata_exception_skipped(self, populated_registry):
        """If a metadata accessor raises, it's skipped."""
        registry = get_registry()
        registry.add_metadata("broken_meta")
        try:
            tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
            wrapper = mock.Mock()
            wrapper.broken_meta.side_effect = RuntimeError("broken")
            wrapper.portal_type = "Document"

            idx = tool._extract_idx(wrapper)
            assert "broken_meta" not in idx
            assert idx["portal_type"] == "Document"
        finally:
            registry._metadata.discard("broken_meta")


# ---------------------------------------------------------------------------
# PATH-type index extraction tests
# ---------------------------------------------------------------------------


class TestExtractIdxPathType:
    def test_path_index_extracts_parent_and_depth(self, populated_registry):
        """PATH-type indexes store path + _parent + _depth in idx JSONB."""
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        wrapper = mock.Mock()
        wrapper.tgpath = "/plone/folder/doc"

        idx = tool._extract_idx(wrapper)
        assert idx["tgpath"] == "/plone/folder/doc"
        assert idx["tgpath_parent"] == "/plone/folder"
        assert idx["tgpath_depth"] == 3
