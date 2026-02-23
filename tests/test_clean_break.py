"""Tests for the clean-break PlonePGCatalogTool (no ZCatalog inheritance)."""

from plone.pgcatalog.catalog import _CatalogCompat
from plone.pgcatalog.catalog import PlonePGCatalogTool
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.schema import install_catalog_schema
from psycopg.rows import dict_row
from tests.conftest import DSN
from tests.conftest import insert_object
from tests.conftest import TABLES_TO_DROP
from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

import psycopg
import pytest
import warnings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tool():
    """Bare PlonePGCatalogTool instance (no ZODB, no PG)."""
    return PlonePGCatalogTool()


@pytest.fixture
def pg_conn():
    """Fresh database connection with base schema + catalog extension."""
    c = psycopg.connect(DSN, row_factory=dict_row)
    with c.cursor() as cur:
        cur.execute(TABLES_TO_DROP)
    c.commit()
    c.execute(HISTORY_FREE_SCHEMA)
    c.commit()
    install_catalog_schema(c)
    c.commit()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Base class / interface tests
# ---------------------------------------------------------------------------


class TestCleanBreakInheritance:
    """Verify PlonePGCatalogTool no longer inherits from ZCatalog."""

    def test_no_zcatalog_in_mro(self):
        mro_names = [cls.__name__ for cls in PlonePGCatalogTool.__mro__]
        assert "ZCatalog" not in mro_names
        assert "CatalogTool" not in mro_names  # CMFPlone CatalogTool

    def test_has_folder_in_mro(self):
        mro_names = [cls.__name__ for cls in PlonePGCatalogTool.__mro__]
        assert "Folder" in mro_names
        assert "ObjectManager" in mro_names

    def test_has_unique_object_in_mro(self):
        mro_names = [cls.__name__ for cls in PlonePGCatalogTool.__mro__]
        assert "UniqueObject" in mro_names

    def test_implements_izcatalog(self):
        from Products.ZCatalog.interfaces import IZCatalog

        assert IZCatalog.implementedBy(PlonePGCatalogTool)

    def test_implements_ipgcatalogtool(self):
        assert IPGCatalogTool.implementedBy(PlonePGCatalogTool)

    def test_implements_iplonecatalogtool(self):
        from plone.base.interfaces import IPloneCatalogTool

        assert IPloneCatalogTool.implementedBy(PlonePGCatalogTool)

    def test_instance_provides_ipgcatalogtool(self, tool):
        assert IPGCatalogTool.providedBy(tool)

    def test_id(self, tool):
        assert tool.id == "portal_catalog"

    def test_meta_type(self, tool):
        assert tool.meta_type == "PG Catalog Tool"


# ---------------------------------------------------------------------------
# _CatalogCompat shim tests
# ---------------------------------------------------------------------------


class TestCatalogCompat:
    """Test the _catalog compatibility shim."""

    def test_new_instance_has_catalog(self, tool):
        assert hasattr(tool, "_catalog")
        assert isinstance(tool._catalog, _CatalogCompat)

    def test_indexes_is_mapping(self, tool):
        assert hasattr(tool._catalog, "indexes")
        assert hasattr(tool._catalog.indexes, "keys")

    def test_schema_is_mapping(self, tool):
        assert hasattr(tool._catalog, "schema")
        assert hasattr(tool._catalog.schema, "keys")

    def test_get_index(self, tool):
        # Add an index object, verify getIndex works
        tool._catalog.indexes["test_idx"] = object()
        result = tool._catalog.getIndex("test_idx")
        assert result is tool._catalog.indexes["test_idx"]

    def test_get_index_missing_raises(self, tool):
        with pytest.raises(KeyError):
            tool._catalog.getIndex("nonexistent")


# ---------------------------------------------------------------------------
# indexes() / schema() / addColumn / delColumn
# ---------------------------------------------------------------------------


class TestIndexesAndSchema:
    """Test index and metadata management."""

    def test_indexes_empty_initially(self, tool):
        assert tool.indexes() == []

    def test_schema_from_registry(self, tool):
        # Registry is populated by conftest.py's populated_registry fixture
        result = tool.schema()
        assert isinstance(result, list)
        # Should include standard Plone metadata
        assert "getObjSize" in result

    def test_add_column(self, tool):
        tool.addColumn("custom_meta")
        assert "custom_meta" in get_registry().metadata

    def test_del_column(self, tool):
        tool.addColumn("temp_meta")
        assert "temp_meta" in get_registry().metadata
        tool.delColumn("temp_meta")
        assert "temp_meta" not in get_registry().metadata

    def test_del_column_nonexistent(self, tool):
        # Should not raise
        tool.delColumn("nonexistent_column_xyz")


# ---------------------------------------------------------------------------
# getIndexDataForRID
# ---------------------------------------------------------------------------


class TestGetIndexDataForRID:
    """Test PG-backed getIndexDataForRID."""

    def test_returns_idx_dict(self, pg_conn, tool):
        insert_object(pg_conn, zoid=100)
        catalog_object(
            pg_conn,
            zoid=100,
            path="/plone/test",
            idx={"portal_type": "Document", "review_state": "published"},
        )
        pg_conn.commit()

        # Monkey-patch read connection for testing
        tool._get_pg_read_connection = lambda: pg_conn
        result = tool.getIndexDataForRID(100)
        assert result["portal_type"] == "Document"
        assert result["review_state"] == "published"

    def test_missing_zoid_returns_empty(self, pg_conn, tool):
        tool._get_pg_read_connection = lambda: pg_conn
        result = tool.getIndexDataForRID(99999)
        assert result == {}


# ---------------------------------------------------------------------------
# getCounter / _increment_counter
# ---------------------------------------------------------------------------


class TestCounter:
    """Test catalog change counter."""

    def test_initial_counter_is_zero(self, tool):
        assert tool.getCounter() == 0

    def test_increment_counter(self, tool):
        tool._increment_counter()
        assert tool.getCounter() == 1

    def test_increment_multiple(self, tool):
        tool._increment_counter()
        tool._increment_counter()
        tool._increment_counter()
        assert tool.getCounter() == 3


# ---------------------------------------------------------------------------
# _listAllowedRolesAndUsers
# ---------------------------------------------------------------------------


class TestAllowedRolesAndUsers:
    """Test security filtering method."""

    def test_anonymous_user(self, tool):

        class _FakeUser:
            def getRoles(self):
                return ("Anonymous",)

        result = tool._listAllowedRolesAndUsers(_FakeUser())
        assert result == ["Anonymous"]

    def test_authenticated_user_with_groups(self, tool):

        class _FakeUser:
            def getRoles(self):
                return ("Member", "Authenticated")

            def getGroups(self):
                return ("AuthenticatedUsers", "Reviewers")

            def getId(self):
                return "testuser"

        result = tool._listAllowedRolesAndUsers(_FakeUser())
        assert result[0] == "user:testuser"
        assert "Member" in result
        assert "Authenticated" in result
        assert "user:AuthenticatedUsers" in result
        assert "user:Reviewers" in result
        assert result[-1] == "Anonymous"


# ---------------------------------------------------------------------------
# Deprecated proxy methods
# ---------------------------------------------------------------------------


class TestDeprecatedProxies:
    """Test search() and uniqueValuesFor() deprecation warnings."""

    def test_search_emits_deprecation_warning(self, tool):
        # Patch searchResults to avoid needing a real PG connection
        called = []
        tool.searchResults = lambda *a, **kw: called.append((a, kw)) or []

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            tool.search(portal_type="Document")

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "search()" in str(w[0].message)
        assert "searchResults()" in str(w[0].message)
        assert len(called) == 1  # proxied to searchResults

    def test_uniquevaluesfor_emits_deprecation_warning(self, tool, pg_conn):
        # Set up a mock index via _catalog.indexes
        class _FakeIndex:
            def __init__(self):
                self.id = "portal_type"
                self.meta_type = "FieldIndex"

            def uniqueValues(self):
                return iter(["Document", "Folder"])

        tool._catalog.indexes["portal_type"] = _FakeIndex()

        # PGCatalogIndexes wraps via _getOb → needs catalog as parent.
        # For unit test, bypass by monkeypatching Indexes._getOb.
        original_getOb = tool.Indexes.__class__._getOb

        def _mock_getOb(self_idx, name, default=None):
            idx = tool._catalog.indexes.get(name)
            if idx is not None:
                return idx
            return default

        tool.Indexes.__class__._getOb = _mock_getOb
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = tool.uniqueValuesFor("portal_type")

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "uniqueValuesFor()" in str(w[0].message)
            assert set(result) == {"Document", "Folder"}
        finally:
            tool.Indexes.__class__._getOb = original_getOb


# ---------------------------------------------------------------------------
# Blocked methods (NotImplementedError)
# ---------------------------------------------------------------------------


class TestBlockedMethods:
    """Test that unsupported ZCatalog methods raise NotImplementedError."""

    @pytest.mark.parametrize(
        "method_name",
        [
            "getAllBrains",
            "searchAll",
            "getobject",
            "getMetadataForUID",
            "getMetadataForRID",
            "getIndexDataForUID",
            "index_objects",
        ],
    )
    def test_unsupported_method_raises(self, tool, method_name):
        method = getattr(tool, method_name)
        with pytest.raises(NotImplementedError) as exc_info:
            method()
        assert method_name in str(exc_info.value)


# ---------------------------------------------------------------------------
# manage_options
# ---------------------------------------------------------------------------


class TestManageOptions:
    """Test ZMI manage_options are defined directly."""

    def test_manage_options_defined(self, tool):
        labels = [opt["label"] for opt in tool.manage_options]
        assert "Catalog" in labels
        assert "Advanced" in labels
        assert "Indexes & Metadata" in labels

    def test_no_query_report_tab(self, tool):
        actions = [opt["action"] for opt in tool.manage_options]
        assert "manage_catalogReport" not in actions
        assert "manage_catalogPlan" not in actions


# ---------------------------------------------------------------------------
# addIndex type resolution (via ObjectManager.all_meta_types)
# ---------------------------------------------------------------------------


class TestAddIndexTypeResolution:
    """Test that addIndex resolves type strings via Products.meta_types."""

    @pytest.fixture(autouse=True)
    def _register_plugin_indexes(self):
        """Ensure Products.meta_types is populated with PluginIndexes.

        In test env without full Zope product initialization, the global
        Products.meta_types tuple is empty.  We manually register the
        standard index types so addIndex can resolve type strings.
        """
        from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
        from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
        from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
        from Products.PluginIndexes.interfaces import IPluggableIndex
        from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex

        import Products

        saved = getattr(Products, "meta_types", ())
        entries = []
        for klass in (FieldIndex, KeywordIndex, BooleanIndex, DateIndex):
            entries.append(
                {
                    "name": klass.meta_type,
                    "instance": klass,
                    "interfaces": (IPluggableIndex,),
                    "visibility": "Global",
                }
            )
        Products.meta_types = saved + tuple(entries)
        yield
        Products.meta_types = saved

    def test_add_index_with_string_type(self, tool):
        """addIndex('foo', 'FieldIndex') should resolve to a FieldIndex instance."""
        tool.addIndex("test_field", "FieldIndex")
        idx = tool._catalog.indexes["test_field"]
        assert idx.meta_type == "FieldIndex"

    def test_add_index_with_keyword_type(self, tool):
        tool.addIndex("test_kw", "KeywordIndex")
        idx = tool._catalog.indexes["test_kw"]
        assert idx.meta_type == "KeywordIndex"

    def test_add_index_with_boolean_type(self, tool):
        tool.addIndex("test_bool", "BooleanIndex")
        idx = tool._catalog.indexes["test_bool"]
        assert idx.meta_type == "BooleanIndex"

    def test_add_index_with_date_type(self, tool):
        tool.addIndex("test_date", "DateIndex")
        idx = tool._catalog.indexes["test_date"]
        assert idx.meta_type == "DateIndex"

    def test_add_index_unknown_type_raises(self, tool):
        with pytest.raises(ValueError, match="Unknown index type"):
            tool.addIndex("bad", "NoSuchIndex")

    def test_add_index_with_object(self, tool):
        """addIndex with an IPluggableIndex object should store it directly."""
        from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex

        idx = FieldIndex("direct_idx")
        tool.addIndex("direct_idx", idx)
        assert tool._catalog.indexes["direct_idx"] is idx

    def test_add_index_syncs_registry(self, tool):
        """addIndex should register the index in IndexRegistry."""
        tool.addIndex("synced_field", "FieldIndex")
        reg = get_registry()
        assert "synced_field" in dict(reg.items())


# ---------------------------------------------------------------------------
# ZCTextIndex + lexicon containment (generic fix via Folder base class)
# ---------------------------------------------------------------------------


class TestZCTextIndexCreation:
    """Test that ZCTextIndex can be created with lexicon stored in ObjectManager."""

    @pytest.fixture(autouse=True)
    def _register_plugin_indexes(self):
        """Register ZCTextIndex and PLexicon in Products.meta_types."""
        from Products.PluginIndexes.interfaces import IPluggableIndex
        from Products.ZCTextIndex.ZCTextIndex import PLexicon
        from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex

        import Products

        saved = getattr(Products, "meta_types", ())
        entries = [
            {
                "name": ZCTextIndex.meta_type,
                "instance": ZCTextIndex,
                "interfaces": (IPluggableIndex,),
                "visibility": "Global",
            },
            {
                "name": PLexicon.meta_type,
                "instance": PLexicon,
                "visibility": "Global",
            },
        ]
        Products.meta_types = saved + tuple(entries)
        yield
        Products.meta_types = saved

    def test_catalog_supports_setobject(self, tool):
        """Folder base class provides _setObject for sub-object containment."""
        assert hasattr(tool, "_setObject")
        assert hasattr(tool, "_getOb")
        assert hasattr(tool, "objectIds")

    def test_lexicon_creation_and_lookup(self, tool):
        """PLexicon can be stored in catalog and found via getattr."""
        from Products.ZCTextIndex.ZCTextIndex import PLexicon

        lexicon = PLexicon("test_lexicon")
        tool._setObject("test_lexicon", lexicon)
        assert "test_lexicon" in tool.objectIds()
        found = getattr(tool, "test_lexicon", None)
        assert found is not None

    def test_add_zctextindex_with_lexicon(self, tool):
        """ZCTextIndex creation should work when lexicon is in catalog's ObjectManager."""
        from Products.ZCTextIndex.ZCTextIndex import PLexicon

        # First add the lexicon as a sub-object (as GenericSetup _initObjects does)
        lexicon = PLexicon("plone_lexicon")
        tool._setObject("plone_lexicon", lexicon)

        # Now create a ZCTextIndex referencing the lexicon
        class _Extra:
            lexicon_id = "plone_lexicon"
            index_type = "Okapi BM25 Rank"

        tool.addIndex("SearchableText", "ZCTextIndex", extra=_Extra())
        idx = tool._catalog.indexes["SearchableText"]
        assert idx.meta_type == "ZCTextIndex"

    def test_add_zctextindex_syncs_registry(self, tool):
        """ZCTextIndex should be registered as TEXT type in IndexRegistry."""
        from Products.ZCTextIndex.ZCTextIndex import PLexicon

        lexicon = PLexicon("plone_lexicon")
        tool._setObject("plone_lexicon", lexicon)

        class _Extra:
            lexicon_id = "plone_lexicon"
            index_type = "Okapi BM25 Rank"

        tool.addIndex("test_text_idx", "ZCTextIndex", extra=_Extra())
        reg = get_registry()
        entry = reg.get("test_text_idx")
        assert entry is not None
        assert entry[0] == IndexType.TEXT


# ---------------------------------------------------------------------------
# CMFCore IndexQueue processor contract (_indexObject, _reindexObject, etc.)
# ---------------------------------------------------------------------------


class TestCMFCoreProcessorContract:
    """Test underscore-prefixed methods called by CMFCore.indexing.PortalCatalogProcessor."""

    def test_has_index_object(self, tool):
        assert hasattr(tool, "_indexObject")
        assert callable(tool._indexObject)

    def test_has_reindex_object(self, tool):
        assert hasattr(tool, "_reindexObject")
        assert callable(tool._reindexObject)

    def test_has_unindex_object(self, tool):
        assert hasattr(tool, "_unindexObject")
        assert callable(tool._unindexObject)

    def test_has_public_unindex_object(self, tool):
        assert hasattr(tool, "unindexObject")
        assert callable(tool.unindexObject)

    def test_has_url_method(self, tool):
        # __url is name-mangled to _PlonePGCatalogTool__url
        assert hasattr(tool, "_PlonePGCatalogTool__url")


# ---------------------------------------------------------------------------
# addColumn persists to _catalog.schema
# ---------------------------------------------------------------------------


class TestAddColumnPersistence:
    """Test that addColumn/delColumn persist to _catalog.schema for sync_from_catalog."""

    def test_add_column_writes_schema(self, tool):
        tool.addColumn("test_col")
        assert "test_col" in tool._catalog.schema

    def test_del_column_removes_schema(self, tool):
        tool.addColumn("temp_col")
        assert "temp_col" in tool._catalog.schema
        tool.delColumn("temp_col")
        assert "temp_col" not in tool._catalog.schema

    def test_schema_survives_for_sync(self, tool):
        """Schema entries should be available for sync_from_catalog to read."""
        tool.addColumn("sync_test_col")
        # Verify it's accessible the same way sync_from_catalog reads it
        schema = tool._catalog.schema
        assert "sync_test_col" in schema


# ---------------------------------------------------------------------------
# _PendingBrain helper
# ---------------------------------------------------------------------------


class TestPendingBrain:
    """Test the _PendingBrain synthetic brain used for pending objects."""

    def test_getpath_returns_path(self):
        from plone.pgcatalog.catalog import _PendingBrain

        brain = _PendingBrain("/plone/doc", object())
        assert brain.getPath() == "/plone/doc"

    def test_unrestrictedgetobject_returns_obj(self):
        from plone.pgcatalog.catalog import _PendingBrain

        obj = object()
        brain = _PendingBrain("/plone/doc", obj)
        assert brain._unrestrictedGetObject() is obj


# ---------------------------------------------------------------------------
# addIndex edge cases
# ---------------------------------------------------------------------------


class TestAddIndexEdgeCases:
    """Test addIndex branches not covered by the basic type resolution tests."""

    def test_add_index_caller_only_constructor(self, tool):
        """Test addIndex with an index whose __init__ accepts 'caller' but not 'extra'."""
        from Products.PluginIndexes.interfaces import IPluggableIndex
        from zope.interface import implementer

        import Products

        @implementer(IPluggableIndex)
        class _CallerOnlyIndex:
            meta_type = "CallerOnlyIndex"

            def __init__(self, name, caller=None):
                self.id = name
                self.caller = caller

        saved = Products.meta_types
        Products.meta_types = (
            *saved,
            {
                "name": "CallerOnlyIndex",
                "instance": _CallerOnlyIndex,
                "interfaces": (IPluggableIndex,),
                "visibility": "Global",
            },
        )
        try:
            tool.addIndex("test_caller", "CallerOnlyIndex")
            assert "test_caller" in tool._catalog.indexes
        finally:
            Products.meta_types = saved

    def test_add_index_simple_constructor(self, tool):
        """Test addIndex with an index whose __init__ accepts only 'name'."""
        from Products.PluginIndexes.interfaces import IPluggableIndex
        from zope.interface import implementer

        import Products

        @implementer(IPluggableIndex)
        class _SimpleIndex:
            meta_type = "SimpleIndex"

            def __init__(self, name):
                self.id = name

        saved = Products.meta_types
        Products.meta_types = (
            *saved,
            {
                "name": "SimpleIndex",
                "instance": _SimpleIndex,
                "interfaces": (IPluggableIndex,),
                "visibility": "Global",
            },
        )
        try:
            tool.addIndex("test_simple", "SimpleIndex")
            assert "test_simple" in tool._catalog.indexes
        finally:
            Products.meta_types = saved

    def test_add_index_invalid_type_raises(self, tool):
        """addIndex with a non-string non-IPluggableIndex raises ValueError."""
        with pytest.raises(ValueError, match="Invalid index_type"):
            tool.addIndex("bad", 12345)

    def test_add_index_getindexsourcenames_exception(self, tool):
        """addIndex handles getIndexSourceNames() raising an exception."""
        from Products.PluginIndexes.interfaces import IPluggableIndex
        from zope.interface import implementer

        @implementer(IPluggableIndex)
        class _BrokenSourceIndex:
            meta_type = "FieldIndex"

            def __init__(self, name, extra=None, caller=None):
                self.id = name

            def getIndexSourceNames(self):
                raise RuntimeError("broken")

        idx_obj = _BrokenSourceIndex("broken_src")
        tool.addIndex("broken_src", idx_obj)
        assert "broken_src" in tool._catalog.indexes
        # Falls back to [name] as source attrs
        reg = get_registry()
        entry = reg.get("broken_src")
        assert entry is not None
        assert entry[2] == ["broken_src"]


# ---------------------------------------------------------------------------
# delIndex
# ---------------------------------------------------------------------------


class TestDelIndex:
    """Test delIndex method."""

    def test_del_existing_index(self, tool):
        from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex

        tool._catalog.indexes["to_delete"] = FieldIndex("to_delete")
        tool.delIndex("to_delete")
        assert "to_delete" not in tool._catalog.indexes

    def test_del_nonexistent_index_noop(self, tool):
        """Deleting a non-existent index is a silent no-op."""
        tool.delIndex("nonexistent")  # should not raise


# ---------------------------------------------------------------------------
# getIndexObjects
# ---------------------------------------------------------------------------


class TestGetIndexObjects:
    """Test getIndexObjects via PGCatalogIndexes wrapping."""

    def test_returns_index_objects(self, tool):
        """getIndexObjects returns indexes from _catalog.indexes."""
        from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex

        idx = FieldIndex("my_field")
        tool._catalog.indexes["my_field"] = idx

        # Monkeypatch Indexes._getOb to return from _catalog.indexes
        original = tool.Indexes.__class__._getOb

        def _mock(self_idx, name, default=None):
            return tool._catalog.indexes.get(name, default)

        tool.Indexes.__class__._getOb = _mock
        try:
            result = tool.getIndexObjects()
            assert len(result) == 1
            assert result[0] is idx
        finally:
            tool.Indexes.__class__._getOb = original


# ---------------------------------------------------------------------------
# CMFCore processor methods (actual behavior, not just hasattr)
# ---------------------------------------------------------------------------


class TestCMFCoreProcessorBehavior:
    """Test _indexObject, _unindexObject, _reindexObject actual behavior."""

    def test_url_helper(self, tool):
        """__url builds path from getPhysicalPath."""
        from unittest import mock

        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc1")
        url = tool._PlonePGCatalogTool__url(obj)
        assert url == "/plone/doc1"

    def test_index_object_calls_catalog_object(self, tool):
        """_indexObject calls catalog_object with url."""
        from unittest import mock

        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc1")
        with mock.patch.object(tool, "catalog_object") as cat_mock:
            tool._indexObject(obj)
            cat_mock.assert_called_once_with(obj, "/plone/doc1")

    def test_unindex_object_calls_uncatalog(self, tool):
        """_unindexObject calls uncatalog_object with url."""
        from unittest import mock

        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc1")
        with mock.patch.object(tool, "uncatalog_object") as uncat_mock:
            tool._unindexObject(obj)
            uncat_mock.assert_called_once_with("/plone/doc1")

    def test_public_unindex_object(self, tool):
        """unindexObject calls uncatalog_object with url."""
        from unittest import mock

        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc1")
        with mock.patch.object(tool, "uncatalog_object") as uncat_mock:
            tool.unindexObject(obj)
            uncat_mock.assert_called_once_with("/plone/doc1")

    def test_reindex_object_calls_catalog_object(self, tool):
        """_reindexObject calls catalog_object with url and idxs."""
        from unittest import mock

        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc1")
        tool._catalog.indexes["portal_type"] = mock.Mock()
        with mock.patch.object(tool, "catalog_object") as cat_mock:
            tool._reindexObject(obj, idxs=["portal_type"])
            cat_mock.assert_called_once_with(obj, "/plone/doc1", ["portal_type"], 1)

    def test_reindex_object_filters_unknown_idxs(self, tool):
        """_reindexObject filters out indexes not in _catalog.indexes."""
        from unittest import mock

        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc1")
        tool._catalog.indexes["portal_type"] = mock.Mock()
        with mock.patch.object(tool, "catalog_object") as cat_mock:
            tool._reindexObject(obj, idxs=["portal_type", "nonexistent"])
            args = cat_mock.call_args
            assert args[0][2] == ["portal_type"]

    def test_reindex_object_default_uid(self, tool):
        """_reindexObject uses __url when uid is None."""
        from unittest import mock

        obj = mock.Mock()
        obj.getPhysicalPath.return_value = ("", "plone", "doc1")
        with mock.patch.object(tool, "catalog_object") as cat_mock:
            tool._reindexObject(obj)
            args = cat_mock.call_args
            assert args[0][1] == "/plone/doc1"

    def test_unrestricted_search_results_proxy(self, tool):
        """_unrestrictedSearchResults delegates to unrestrictedSearchResults."""
        from unittest import mock

        with mock.patch.object(
            tool, "unrestrictedSearchResults", return_value=[]
        ) as usr_mock:
            result = tool._unrestrictedSearchResults(portal_type="Document")
            usr_mock.assert_called_once_with(None, portal_type="Document")
            assert result == []
