"""Tests for plone.pgcatalog.columns — IndexRegistry and dynamic index support.

TDD: these tests are written BEFORE the implementation.
"""

from plone.pgcatalog.columns import IndexType

import pytest


# ---------------------------------------------------------------------------
# Mock ZCatalog index objects for testing sync_from_catalog
# ---------------------------------------------------------------------------


class MockIndex:
    """Minimal mock of a ZCatalog PluginIndex object."""

    def __init__(self, meta_type, indexed_attrs=None):
        self.meta_type = meta_type
        self.id = "mock"
        self._indexed_attrs = indexed_attrs

    def getIndexSourceNames(self):
        if self._indexed_attrs is not None:
            return self._indexed_attrs
        return [self.id]


class MockCatalog:
    """Minimal mock of a ZCatalog tool with _catalog attribute."""

    def __init__(self, indexes=None, schema=None):
        self._catalog = MockInternalCatalog(indexes or {}, schema or {})


class MockInternalCatalog:
    """Minimal mock of Products.ZCatalog.Catalog.Catalog."""

    def __init__(self, indexes, schema):
        self.indexes = indexes
        self.schema = schema

    def getIndex(self, name):
        return self.indexes[name]


# ---------------------------------------------------------------------------
# META_TYPE_MAP tests
# ---------------------------------------------------------------------------


class TestMetaTypeMap:
    """META_TYPE_MAP maps ZCatalog meta_type strings to IndexType enum."""

    def test_exists(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert isinstance(META_TYPE_MAP, dict)

    def test_field_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["FieldIndex"] == IndexType.FIELD

    def test_keyword_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["KeywordIndex"] == IndexType.KEYWORD

    def test_date_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["DateIndex"] == IndexType.DATE

    def test_boolean_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["BooleanIndex"] == IndexType.BOOLEAN

    def test_date_range_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["DateRangeIndex"] == IndexType.DATE_RANGE

    def test_uuid_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["UUIDIndex"] == IndexType.UUID

    def test_zctext_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["ZCTextIndex"] == IndexType.TEXT

    def test_extended_path_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["ExtendedPathIndex"] == IndexType.PATH

    def test_path_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["PathIndex"] == IndexType.PATH

    def test_gopip_index(self):
        from plone.pgcatalog.columns import META_TYPE_MAP

        assert META_TYPE_MAP["GopipIndex"] == IndexType.GOPIP

    def test_all_index_types_covered(self):
        """Every IndexType enum value has at least one meta_type mapping."""
        from plone.pgcatalog.columns import META_TYPE_MAP

        covered = set(META_TYPE_MAP.values())
        for idx_type in IndexType:
            assert idx_type in covered, (
                f"IndexType.{idx_type.name} has no META_TYPE_MAP entry"
            )


# ---------------------------------------------------------------------------
# SPECIAL_INDEXES tests
# ---------------------------------------------------------------------------


class TestSpecialIndexes:
    """SPECIAL_INDEXES defines indexes that need idx_key=None (hardcoded handling)."""

    def test_exists(self):
        from plone.pgcatalog.columns import SPECIAL_INDEXES

        assert isinstance(SPECIAL_INDEXES, (set, frozenset))

    def test_searchable_text(self):
        from plone.pgcatalog.columns import SPECIAL_INDEXES

        assert "SearchableText" in SPECIAL_INDEXES

    def test_effective_range(self):
        from plone.pgcatalog.columns import SPECIAL_INDEXES

        assert "effectiveRange" in SPECIAL_INDEXES

    def test_path(self):
        from plone.pgcatalog.columns import SPECIAL_INDEXES

        assert "path" in SPECIAL_INDEXES


# ---------------------------------------------------------------------------
# IndexRegistry tests
# ---------------------------------------------------------------------------


class TestIndexRegistryInit:
    """IndexRegistry starts empty."""

    def test_starts_empty(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        assert len(list(registry.keys())) == 0

    def test_contains_nothing(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        assert "portal_type" not in registry

    def test_get_returns_default(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        assert registry.get("portal_type") is None
        assert registry.get("portal_type", "default") == "default"

    def test_metadata_starts_empty(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        assert len(registry.metadata) == 0


class TestIndexRegistrySyncFromCatalog:
    """sync_from_catalog() populates from ZCatalog's _catalog.indexes."""

    def test_basic_field_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("FieldIndex")
        idx.id = "portal_type"
        catalog = MockCatalog(indexes={"portal_type": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "portal_type" in registry
        idx_type, idx_key, source_attrs = registry["portal_type"]
        assert idx_type == IndexType.FIELD
        assert idx_key == "portal_type"
        assert source_attrs == ["portal_type"]

    def test_keyword_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("KeywordIndex")
        idx.id = "Subject"
        catalog = MockCatalog(indexes={"Subject": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        idx_type, idx_key, source_attrs = registry["Subject"]
        assert idx_type == IndexType.KEYWORD

    def test_date_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("DateIndex")
        idx.id = "modified"
        catalog = MockCatalog(indexes={"modified": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        idx_type, idx_key, source_attrs = registry["modified"]
        assert idx_type == IndexType.DATE

    def test_boolean_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("BooleanIndex")
        idx.id = "is_folderish"
        catalog = MockCatalog(indexes={"is_folderish": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        idx_type, _, _ = registry["is_folderish"]
        assert idx_type == IndexType.BOOLEAN

    def test_uuid_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("UUIDIndex")
        idx.id = "UID"
        catalog = MockCatalog(indexes={"UID": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        idx_type, _, _ = registry["UID"]
        assert idx_type == IndexType.UUID

    def test_gopip_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("GopipIndex")
        idx.id = "getObjPositionInParent"
        catalog = MockCatalog(indexes={"getObjPositionInParent": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        idx_type, _, _ = registry["getObjPositionInParent"]
        assert idx_type == IndexType.GOPIP

    def test_multiple_indexes(self):
        from plone.pgcatalog.columns import IndexRegistry

        indexes = {}
        for name, meta_type in [
            ("portal_type", "FieldIndex"),
            ("Subject", "KeywordIndex"),
            ("modified", "DateIndex"),
        ]:
            idx = MockIndex(meta_type)
            idx.id = name
            indexes[name] = idx

        catalog = MockCatalog(indexes=indexes)
        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert len(list(registry.keys())) == 3
        assert "portal_type" in registry
        assert "Subject" in registry
        assert "modified" in registry

    def test_special_index_gets_none_idx_key(self):
        """SearchableText, effectiveRange, path get idx_key=None."""
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("ZCTextIndex")
        idx.id = "SearchableText"
        catalog = MockCatalog(indexes={"SearchableText": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        idx_type, idx_key, _ = registry["SearchableText"]
        assert idx_type == IndexType.TEXT
        assert idx_key is None

    def test_effective_range_gets_none_idx_key(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("DateRangeIndex")
        idx.id = "effectiveRange"
        catalog = MockCatalog(indexes={"effectiveRange": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        _, idx_key, _ = registry["effectiveRange"]
        assert idx_key is None

    def test_path_gets_none_idx_key(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("ExtendedPathIndex")
        idx.id = "path"
        catalog = MockCatalog(indexes={"path": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        _, idx_key, _ = registry["path"]
        assert idx_key is None

    def test_non_special_index_has_name_as_idx_key(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("FieldIndex")
        idx.id = "portal_type"
        catalog = MockCatalog(indexes={"portal_type": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        _, idx_key, _ = registry["portal_type"]
        assert idx_key == "portal_type"


class TestIndexRegistrySourceAttrs:
    """sync_from_catalog reads getIndexSourceNames() for source_attrs."""

    def test_default_source_attr_is_index_name(self):
        """When no indexed_attr is specified, source_attrs defaults to [name]."""
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("FieldIndex")
        idx.id = "Creator"
        catalog = MockCatalog(indexes={"Creator": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        _, _, source_attrs = registry["Creator"]
        assert source_attrs == ["Creator"]

    def test_custom_indexed_attr(self):
        """When indexed_attr differs from index name, source_attrs reflects it."""
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("FieldIndex", indexed_attrs=["some_other_attr"])
        idx.id = "my_custom_index"
        catalog = MockCatalog(indexes={"my_custom_index": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        _, idx_key, source_attrs = registry["my_custom_index"]
        assert idx_key == "my_custom_index"  # JSONB key is still the index name
        assert source_attrs == ["some_other_attr"]  # extraction uses indexed_attr

    def test_multiple_indexed_attrs(self):
        """An index can have multiple indexed_attrs."""
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("FieldIndex", indexed_attrs=["title", "short_title"])
        idx.id = "sortable_title"
        catalog = MockCatalog(indexes={"sortable_title": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        _, _, source_attrs = registry["sortable_title"]
        assert source_attrs == ["title", "short_title"]


class TestIndexRegistryUnknownMetaType:
    """Unknown meta_type values are silently skipped."""

    def test_unknown_meta_type_skipped(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("CustomFancyIndex")
        idx.id = "my_fancy"
        catalog = MockCatalog(indexes={"my_fancy": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "my_fancy" not in registry

    def test_known_indexes_still_registered_alongside_unknown(self):
        from plone.pgcatalog.columns import IndexRegistry

        indexes = {
            "portal_type": MockIndex("FieldIndex"),
            "my_fancy": MockIndex("CustomFancyIndex"),
        }
        indexes["portal_type"].id = "portal_type"
        indexes["my_fancy"].id = "my_fancy"
        catalog = MockCatalog(indexes=indexes)

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "portal_type" in registry
        assert "my_fancy" not in registry


class TestIndexRegistryMetadata:
    """sync_from_catalog reads catalog._catalog.schema for metadata columns."""

    def test_metadata_from_schema(self):
        from plone.pgcatalog.columns import IndexRegistry

        catalog = MockCatalog(
            indexes={},
            schema={"getObjSize": 0, "image_scales": 1, "Title": 2},
        )
        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "getObjSize" in registry.metadata
        assert "image_scales" in registry.metadata
        assert "Title" in registry.metadata

    def test_metadata_not_in_index_lookup(self):
        """Metadata columns are NOT queryable indexes."""
        from plone.pgcatalog.columns import IndexRegistry

        catalog = MockCatalog(
            indexes={},
            schema={"getObjSize": 0},
        )
        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "getObjSize" not in registry  # not an index
        assert "getObjSize" in registry.metadata  # is metadata


class TestIndexRegistryProgrammatic:
    """register() and add_metadata() for programmatic use."""

    def test_register_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        registry.register("my_field", IndexType.FIELD, "my_field")

        assert "my_field" in registry
        idx_type, idx_key, source_attrs = registry["my_field"]
        assert idx_type == IndexType.FIELD
        assert idx_key == "my_field"
        assert source_attrs == ["my_field"]

    def test_register_with_custom_source_attrs(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        registry.register(
            "my_index",
            IndexType.FIELD,
            "my_index",
            source_attrs=["other_attr"],
        )

        _, _, source_attrs = registry["my_index"]
        assert source_attrs == ["other_attr"]

    def test_register_defaults_source_attrs_to_idx_key(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        registry.register("my_field", IndexType.FIELD, "my_field")

        _, _, source_attrs = registry["my_field"]
        assert source_attrs == ["my_field"]

    def test_add_metadata(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        registry.add_metadata("getObjSize")

        assert "getObjSize" in registry.metadata

    def test_add_metadata_not_an_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        registry.add_metadata("getObjSize")

        assert "getObjSize" not in registry  # not queryable


class TestIndexRegistryDictAPI:
    """IndexRegistry has dict-like API."""

    def _make_registry(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        registry.register("portal_type", IndexType.FIELD, "portal_type")
        registry.register("Subject", IndexType.KEYWORD, "Subject")
        return registry

    def test_contains(self):
        registry = self._make_registry()
        assert "portal_type" in registry
        assert "nonexistent" not in registry

    def test_getitem(self):
        registry = self._make_registry()
        idx_type, idx_key, source_attrs = registry["portal_type"]
        assert idx_type == IndexType.FIELD

    def test_getitem_raises_keyerror(self):
        registry = self._make_registry()
        with pytest.raises(KeyError):
            registry["nonexistent"]

    def test_get_with_default(self):
        registry = self._make_registry()
        assert registry.get("nonexistent", "fallback") == "fallback"

    def test_items(self):
        registry = self._make_registry()
        items = dict(registry.items())
        assert "portal_type" in items
        assert "Subject" in items

    def test_keys(self):
        registry = self._make_registry()
        assert set(registry.keys()) == {"portal_type", "Subject"}

    def test_len(self):
        registry = self._make_registry()
        assert len(registry) == 2


class TestIndexRegistryResync:
    """Re-sync picks up newly added indexes."""

    def test_resync_adds_new_index(self):
        from plone.pgcatalog.columns import IndexRegistry

        # First sync with one index
        idx1 = MockIndex("FieldIndex")
        idx1.id = "portal_type"
        catalog = MockCatalog(indexes={"portal_type": idx1})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)
        assert "portal_type" in registry
        assert "review_state" not in registry

        # Add another index and re-sync
        idx2 = MockIndex("FieldIndex")
        idx2.id = "review_state"
        catalog._catalog.indexes["review_state"] = idx2

        registry.sync_from_catalog(catalog)
        assert "portal_type" in registry
        assert "review_state" in registry

    def test_resync_adds_new_metadata(self):
        from plone.pgcatalog.columns import IndexRegistry

        catalog = MockCatalog(indexes={}, schema={"Title": 0})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)
        assert "Title" in registry.metadata

        catalog._catalog.schema["getObjSize"] = 1
        registry.sync_from_catalog(catalog)
        assert "getObjSize" in registry.metadata


class TestValidateIdentifier:
    """validate_identifier() rejects unsafe SQL identifiers."""

    def test_accepts_simple_name(self):
        from plone.pgcatalog.columns import validate_identifier

        validate_identifier("portal_type")  # no exception

    def test_accepts_underscore_prefix(self):
        from plone.pgcatalog.columns import validate_identifier

        validate_identifier("_private")

    def test_accepts_uppercase(self):
        from plone.pgcatalog.columns import validate_identifier

        validate_identifier("Subject")

    def test_accepts_alphanumeric(self):
        from plone.pgcatalog.columns import validate_identifier

        validate_identifier("field_2")

    def test_rejects_single_quote(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("foo'bar")

    def test_rejects_semicolon(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("foo;DROP TABLE")

    def test_rejects_dash(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("my-index")

    def test_rejects_space(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("my index")

    def test_rejects_dot(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("schema.table")

    def test_rejects_leading_digit(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("1field")

    def test_rejects_sql_injection_payload(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("'; DROP TABLE object_state; --")

    def test_rejects_empty_string(self):
        from plone.pgcatalog.columns import validate_identifier

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("")


class TestIndexRegistryRejectsUnsafeNames:
    """register() and sync_from_catalog() reject unsafe SQL identifiers."""

    def test_register_rejects_unsafe_idx_key(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        with pytest.raises(ValueError, match="Invalid identifier"):
            registry.register("my_index", IndexType.FIELD, "foo'bar")

    def test_register_allows_none_idx_key(self):
        from plone.pgcatalog.columns import IndexRegistry

        registry = IndexRegistry()
        registry.register("SearchableText", IndexType.TEXT, None)
        assert "SearchableText" in registry

    def test_sync_skips_unsafe_index_name(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("FieldIndex")
        idx.id = "bad-name"
        catalog = MockCatalog(indexes={"bad-name": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "bad-name" not in registry

    def test_sync_skips_sql_injection_name(self):
        from plone.pgcatalog.columns import IndexRegistry

        idx = MockIndex("FieldIndex")
        idx.id = "'; DROP TABLE x; --"
        catalog = MockCatalog(indexes={"'; DROP TABLE x; --": idx})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "'; DROP TABLE x; --" not in registry

    def test_sync_accepts_safe_alongside_unsafe(self):
        from plone.pgcatalog.columns import IndexRegistry

        safe = MockIndex("FieldIndex")
        safe.id = "portal_type"
        unsafe = MockIndex("FieldIndex")
        unsafe.id = "bad-name"
        catalog = MockCatalog(indexes={"portal_type": safe, "bad-name": unsafe})

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "portal_type" in registry
        assert "bad-name" not in registry


class TestGetRegistry:
    """get_registry() returns module-level singleton."""

    def test_returns_registry(self):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexRegistry

        registry = get_registry()
        assert isinstance(registry, IndexRegistry)

    def test_returns_same_instance(self):
        from plone.pgcatalog.columns import get_registry

        r1 = get_registry()
        r2 = get_registry()
        assert r1 is r2
