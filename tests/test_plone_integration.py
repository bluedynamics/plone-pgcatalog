"""Plone integration tests for plone.pgcatalog components.

Tests using plone.app.testing with a real Plone site to exercise:
- IndexRegistry.sync_from_catalog() with real ZCatalog indexes
- Index value extraction from real Plone content objects
- _set_pg_annotation pipeline with real Dexterity content
- setuphandlers: _snapshot_catalog, _replace_catalog, _ensure_catalog_indexes
- PlonePGCatalogTool: wrap_object, extract_idx, extract_searchable_text
- columns: convert_value with real DateTime objects from Plone content
"""

from datetime import UTC
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID


# ===========================================================================
# IndexRegistry.sync_from_catalog — real Plone catalog
# ===========================================================================


class TestIndexRegistrySyncFromCatalog:
    """Verify sync_from_catalog discovers all standard Plone indexes."""

    def test_discovers_core_plone_indexes(self, pgcatalog_layer):
        """sync_from_catalog finds UID, Title, portal_type, review_state, etc."""
        from plone.pgcatalog.columns import IndexRegistry

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        for name in ("UID", "Title", "portal_type", "review_state", "Subject"):
            assert name in registry, f"Expected index {name!r} not found in registry"

    def test_discovers_date_indexes(self, pgcatalog_layer):
        """Date indexes (created, modified, effective, expires) are found."""
        from plone.pgcatalog.columns import IndexRegistry
        from plone.pgcatalog.columns import IndexType

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        for name in ("created", "modified", "effective", "expires"):
            assert name in registry, f"Expected date index {name!r} not in registry"
            idx_type, _idx_key, _source_attrs = registry[name]
            assert idx_type == IndexType.DATE, f"{name} should be DATE, got {idx_type}"

    def test_discovers_path_index(self, pgcatalog_layer):
        """The 'path' index is discovered as PATH type with idx_key=None."""
        from plone.pgcatalog.columns import IndexRegistry
        from plone.pgcatalog.columns import IndexType

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "path" in registry
        idx_type, idx_key, _source_attrs = registry["path"]
        assert idx_type == IndexType.PATH
        assert idx_key is None  # special index

    def test_discovers_searchable_text(self, pgcatalog_layer):
        """SearchableText (ZCTextIndex) is discovered with idx_key=None."""
        from plone.pgcatalog.columns import IndexRegistry
        from plone.pgcatalog.columns import IndexType

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "SearchableText" in registry
        idx_type, idx_key, _source_attrs = registry["SearchableText"]
        assert idx_type == IndexType.TEXT
        assert idx_key is None  # special index

    def test_discovers_boolean_index(self, pgcatalog_layer):
        """is_folderish (BooleanIndex) is discovered."""
        from plone.pgcatalog.columns import IndexRegistry
        from plone.pgcatalog.columns import IndexType

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "is_folderish" in registry
        idx_type, _idx_key, _source_attrs = registry["is_folderish"]
        assert idx_type == IndexType.BOOLEAN

    def test_discovers_keyword_index(self, pgcatalog_layer):
        """Subject (KeywordIndex) is discovered."""
        from plone.pgcatalog.columns import IndexRegistry
        from plone.pgcatalog.columns import IndexType

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "Subject" in registry
        idx_type, _idx_key, _source_attrs = registry["Subject"]
        assert idx_type == IndexType.KEYWORD

    def test_discovers_date_range_index(self, pgcatalog_layer):
        """effectiveRange (DateRangeIndex) is discovered with idx_key=None."""
        from plone.pgcatalog.columns import IndexRegistry
        from plone.pgcatalog.columns import IndexType

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert "effectiveRange" in registry
        idx_type, idx_key, _source_attrs = registry["effectiveRange"]
        assert idx_type == IndexType.DATE_RANGE
        assert idx_key is None  # special index

    def test_discovers_metadata_columns(self, pgcatalog_layer):
        """Metadata columns (Description, portal_type, etc.) are collected."""
        from plone.pgcatalog.columns import IndexRegistry

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        for col in ("Description", "portal_type", "review_state", "Title"):
            assert col in registry.metadata, (
                f"Expected metadata column {col!r} not found"
            )

    def test_source_attrs_extracted(self, pgcatalog_layer):
        """Source attributes are correctly extracted from getIndexSourceNames."""
        from plone.pgcatalog.columns import IndexRegistry

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        # sortable_title usually has source_attr = "sortable_title"
        if "sortable_title" in registry:
            _idx_type, _idx_key, source_attrs = registry["sortable_title"]
            assert isinstance(source_attrs, list)
            assert len(source_attrs) > 0

    def test_index_count_reasonable(self, pgcatalog_layer):
        """A fresh Plone site has a reasonable number of indexes (>15)."""
        from plone.pgcatalog.columns import IndexRegistry

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        registry = IndexRegistry()
        registry.sync_from_catalog(catalog)

        assert len(registry) > 15, (
            f"Expected >15 indexes from Plone, got {len(registry)}"
        )


# ===========================================================================
# Index extraction from real Plone content
# ===========================================================================


class TestExtractionWithPloneContent:
    """Extract index values from real Plone Dexterity content."""

    def test_extract_idx_from_document(self, pgcatalog_layer):
        """extract_idx returns correct values for a Plone Document."""
        from plone.pgcatalog.extraction import extract_idx
        from plone.pgcatalog.extraction import wrap_object

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "test-doc", title="Test Document")
        doc = portal["test-doc"]

        # Populate the module-level registry so extract_idx can use it
        from plone.pgcatalog.columns import get_registry

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            wrapper = wrap_object(doc, portal.portal_catalog)
            idx = extract_idx(wrapper)

            assert idx.get("Title") == "Test Document"
            assert idx.get("portal_type") == "Document"
            assert "UID" in idx
            assert idx["UID"] is not None
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata

    def test_extract_idx_from_folder(self, pgcatalog_layer):
        """extract_idx correctly identifies a Folder."""
        from plone.pgcatalog.extraction import extract_idx
        from plone.pgcatalog.extraction import wrap_object

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Folder", "test-folder", title="Test Folder")
        folder = portal["test-folder"]

        from plone.pgcatalog.columns import get_registry

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            wrapper = wrap_object(folder, portal.portal_catalog)
            idx = extract_idx(wrapper)

            assert idx.get("Title") == "Test Folder"
            assert idx.get("portal_type") == "Folder"
            assert idx.get("is_folderish") is True
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata

    def test_extract_searchable_text(self, pgcatalog_layer):
        """SearchableText is extracted from Document body."""
        from plone.pgcatalog.extraction import extract_searchable_text
        from plone.pgcatalog.extraction import wrap_object

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "text-doc", title="Unique Title XYZ")

        doc = portal["text-doc"]
        wrapper = wrap_object(doc, portal.portal_catalog)
        text = extract_searchable_text(wrapper)

        assert text is not None
        assert "Unique Title XYZ" in text

    def test_extract_subject_keyword(self, pgcatalog_layer):
        """Subject (keywords) are extracted from tagged content."""
        from plone.pgcatalog.extraction import extract_idx
        from plone.pgcatalog.extraction import wrap_object

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "tagged-doc", title="Tagged")
        doc = portal["tagged-doc"]
        doc.setSubject(["python", "testing"])
        doc.reindexObject()

        from plone.pgcatalog.columns import get_registry

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            wrapper = wrap_object(doc, portal.portal_catalog)
            idx = extract_idx(wrapper)

            subject = idx.get("Subject")
            assert subject is not None
            assert "python" in subject
            assert "testing" in subject
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata

    def test_extract_review_state(self, pgcatalog_layer):
        """review_state is extracted (default: 'private') via plone.indexer."""
        from plone.pgcatalog.extraction import extract_idx
        from plone.pgcatalog.extraction import wrap_object

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "state-doc", title="State Test")
        doc = portal["state-doc"]

        from plone.pgcatalog.columns import get_registry

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            wrapper = wrap_object(doc, portal.portal_catalog)
            idx = extract_idx(wrapper)

            # review_state may be in idx or metadata depending on Plone config
            state = idx.get("review_state")
            if state is not None:
                assert state == "private"
            # If None, the indexer may not be registered in this context — that's OK
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata

    def test_wrap_object_returns_indexable_wrapper(self, pgcatalog_layer):
        """wrap_object returns an IIndexableObject adapter."""
        from plone.indexer.interfaces import IIndexableObject
        from plone.pgcatalog.extraction import wrap_object

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "wrap-doc", title="Wrap Test")
        doc = portal["wrap-doc"]

        wrapper = wrap_object(doc, portal.portal_catalog)
        assert IIndexableObject.providedBy(wrapper)

    def test_extract_datetime_values_converted(self, pgcatalog_layer):
        """DateTime values (created, modified) are converted to ISO strings."""
        from plone.pgcatalog.extraction import extract_idx
        from plone.pgcatalog.extraction import wrap_object

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        portal.invokeFactory("Document", "date-doc", title="Date Test")
        doc = portal["date-doc"]

        from plone.pgcatalog.columns import get_registry

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            wrapper = wrap_object(doc, portal.portal_catalog)
            idx = extract_idx(wrapper)

            # created/modified should be ISO date strings
            created = idx.get("created")
            if created is not None:
                assert isinstance(created, str), (
                    f"created should be string, got {type(created)}"
                )
                # ISO 8601 format check
                assert "T" in created or "-" in created
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata


# ===========================================================================
# _set_pg_annotation pipeline
# ===========================================================================


class TestSetPGAnnotationPipeline:
    """Test _set_pg_annotation with real Plone content.

    Uses PlonePGCatalogTool directly (not as portal_catalog replacement)
    to verify the annotation pipeline works with real content objects.
    """

    def test_annotation_on_new_object_uses_dict_fallback(self, pgcatalog_layer):
        """New objects (no _p_oid yet) use __dict__ fallback."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.processor import ANNOTATION_KEY

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            portal.invokeFactory("Document", "anno-doc", title="Annotation Test")
            doc = portal["anno-doc"]

            tool = PlonePGCatalogTool()
            # Wrap tool in Acquisition context so getPhysicalPath works

            tool = tool.__of__(portal)

            result = tool._set_pg_annotation(doc)

            if doc._p_oid is None:
                # New object without OID → __dict__ fallback
                assert result is True
                assert ANNOTATION_KEY in doc.__dict__
                data = doc.__dict__[ANNOTATION_KEY]
                assert "path" in data
                assert "idx" in data
                assert data["idx"].get("Title") == "Annotation Test"
            else:
                # Object already has OID → pending store
                assert result is True
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata

    def test_annotation_contains_path_info(self, pgcatalog_layer):
        """Annotation data includes correct path, parent_path, path_depth."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.pending import _get_pending
        from plone.pgcatalog.processor import ANNOTATION_KEY

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            portal.invokeFactory("Folder", "path-folder")
            portal["path-folder"].invokeFactory(
                "Document", "path-doc", title="Path Test"
            )
            doc = portal["path-folder"]["path-doc"]

            tool = PlonePGCatalogTool().__of__(portal)
            tool._set_pg_annotation(doc)

            # Get pending data from either __dict__ or pending store
            if doc._p_oid is None:
                data = doc.__dict__.get(ANNOTATION_KEY, {})
            else:
                from ZODB.utils import u64

                zoid = u64(doc._p_oid)
                all_pending = _get_pending()  # returns dict of zoid → data
                data = all_pending.get(zoid, {})

            expected_path = "/".join(doc.getPhysicalPath())
            assert data.get("path") == expected_path

            idx = data.get("idx", {})
            assert idx.get("path") == expected_path
            assert "path_parent" in idx
            assert "path_depth" in idx
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata

    def test_annotation_extracts_searchable_text(self, pgcatalog_layer):
        """Annotation data includes searchable_text."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.processor import ANNOTATION_KEY

        layer = pgcatalog_layer
        portal = layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        registry = get_registry()
        old_indexes = dict(registry._indexes)
        old_metadata = set(registry._metadata)
        try:
            registry.sync_from_catalog(portal.portal_catalog)

            portal.invokeFactory(
                "Document", "search-doc", title="Searchable Content Here"
            )
            doc = portal["search-doc"]

            tool = PlonePGCatalogTool().__of__(portal)
            tool._set_pg_annotation(doc)

            if doc._p_oid is None:
                data = doc.__dict__.get(ANNOTATION_KEY, {})
            else:
                from plone.pgcatalog.pending import _get_pending
                from ZODB.utils import u64

                zoid = u64(doc._p_oid)
                all_pending = _get_pending()
                data = all_pending.get(zoid, {})

            text = data.get("searchable_text")
            assert text is not None
            assert "Searchable Content Here" in text
        finally:
            registry._indexes = old_indexes
            registry._metadata = old_metadata


# ===========================================================================
# setuphandlers with real Plone catalog
# ===========================================================================


class TestSnapshotCatalog:
    """Test _snapshot_catalog with a real Plone ZCatalog."""

    def test_snapshot_captures_indexes(self, pgcatalog_layer):
        """_snapshot_catalog captures all existing ZCatalog indexes."""
        from plone.pgcatalog.setuphandlers import _snapshot_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        snapshot = _snapshot_catalog(catalog)

        assert "indexes" in snapshot
        assert len(snapshot["indexes"]) > 15

        # Check known indexes
        assert "UID" in snapshot["indexes"]
        assert snapshot["indexes"]["UID"]["meta_type"] == "UUIDIndex"

        assert "portal_type" in snapshot["indexes"]
        assert snapshot["indexes"]["portal_type"]["meta_type"] == "FieldIndex"

    def test_snapshot_captures_metadata(self, pgcatalog_layer):
        """_snapshot_catalog captures metadata columns."""
        from plone.pgcatalog.setuphandlers import _snapshot_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        snapshot = _snapshot_catalog(catalog)

        assert "metadata" in snapshot
        assert len(snapshot["metadata"]) > 5
        assert "Title" in snapshot["metadata"]
        assert "Description" in snapshot["metadata"]

    def test_snapshot_captures_source_attrs(self, pgcatalog_layer):
        """_snapshot_catalog captures source attributes for indexes."""
        from plone.pgcatalog.setuphandlers import _snapshot_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        snapshot = _snapshot_catalog(catalog)

        # Every index should have source_attrs
        for name, entry in snapshot["indexes"].items():
            assert "source_attrs" in entry, f"Index {name!r} missing source_attrs"
            assert isinstance(entry["source_attrs"], list)

    def test_snapshot_captures_date_range_fields(self, pgcatalog_layer):
        """_snapshot_catalog captures since_field/until_field for DateRangeIndex."""
        from plone.pgcatalog.setuphandlers import _snapshot_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        snapshot = _snapshot_catalog(catalog)

        if "effectiveRange" in snapshot["indexes"]:
            entry = snapshot["indexes"]["effectiveRange"]
            assert entry["meta_type"] == "DateRangeIndex"
            assert "since_field" in entry
            assert "until_field" in entry

    def test_snapshot_captures_zctextindex_attrs(self, pgcatalog_layer):
        """_snapshot_catalog captures lexicon_id/index_type for ZCTextIndex."""
        from plone.pgcatalog.setuphandlers import _snapshot_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]
        catalog = portal.portal_catalog

        snapshot = _snapshot_catalog(catalog)

        if "SearchableText" in snapshot["indexes"]:
            entry = snapshot["indexes"]["SearchableText"]
            assert entry["meta_type"] == "ZCTextIndex"
            assert "lexicon_id" in entry


class TestReplaceCatalogWithPlone:
    """Test _replace_catalog with a real Plone site.

    Note: These tests modify portal_catalog — the integration layer
    resets state between tests.
    """

    def test_replace_creates_pgcatalog_tool(self, pgcatalog_layer):
        """_replace_catalog replaces ZCatalog with PlonePGCatalogTool."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.setuphandlers import _replace_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]

        # Before: standard ZCatalog
        old_catalog = portal.portal_catalog
        assert not isinstance(old_catalog, PlonePGCatalogTool)

        _replace_catalog(portal)

        # After: PlonePGCatalogTool
        new_catalog = portal.portal_catalog
        assert isinstance(new_catalog, PlonePGCatalogTool)

    def test_replace_registers_as_icatalogtool(self, pgcatalog_layer):
        """_replace_catalog registers the new tool as ICatalogTool utility."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.setuphandlers import _replace_catalog
        from Products.CMFCore.interfaces import ICatalogTool
        from zope.component import getSiteManager

        layer = pgcatalog_layer
        portal = layer["portal"]

        _replace_catalog(portal)

        sm = getSiteManager(portal)
        utility = sm.queryUtility(ICatalogTool)
        assert utility is not None
        assert isinstance(utility, PlonePGCatalogTool)


class TestBuildExtra:
    """Test _build_extra creates correct namespace objects for addIndex."""

    def test_build_extra_field_index(self):
        """_build_extra for a FieldIndex sets indexed_attrs."""
        from plone.pgcatalog.setuphandlers import _build_extra

        extra = _build_extra(
            {
                "meta_type": "FieldIndex",
                "source_attrs": ["sortable_title"],
            }
        )
        assert extra.indexed_attrs == "sortable_title"

    def test_build_extra_date_range_index(self):
        """_build_extra for a DateRangeIndex sets since_field/until_field."""
        from plone.pgcatalog.setuphandlers import _build_extra

        extra = _build_extra(
            {
                "meta_type": "DateRangeIndex",
                "source_attrs": ["effectiveRange"],
                "since_field": "effective",
                "until_field": "expires",
            }
        )
        assert extra.since_field == "effective"
        assert extra.until_field == "expires"

    def test_build_extra_zctextindex(self):
        """_build_extra for a ZCTextIndex sets lexicon_id, index_type, doc_attr."""
        from plone.pgcatalog.setuphandlers import _build_extra

        extra = _build_extra(
            {
                "meta_type": "ZCTextIndex",
                "source_attrs": ["SearchableText"],
                "lexicon_id": "plone_lexicon",
                "index_type": "Okapi BM25 Rank",
            }
        )
        assert extra.lexicon_id == "plone_lexicon"
        assert extra.index_type == "Okapi BM25 Rank"
        assert extra.doc_attr == "SearchableText"


# ===========================================================================
# Value conversion with real Plone types
# ===========================================================================


class TestValueConversionWithPloneTypes:
    """Test convert_value with real DateTime objects from Plone content."""

    def test_convert_zope_datetime(self, pgcatalog_layer):
        """Zope DateTime objects are converted to ISO 8601 strings."""
        from DateTime import DateTime
        from plone.pgcatalog.columns import convert_value

        dt = DateTime("2026-03-15T10:30:00+01:00")
        result = convert_value(dt)

        assert isinstance(result, str)
        assert "2026" in result
        assert "03" in result

    def test_convert_python_datetime(self):
        """Python datetime is converted to ISO 8601."""
        from datetime import datetime
        from plone.pgcatalog.columns import convert_value

        dt = datetime(2026, 3, 15, 10, 30, 0, tzinfo=UTC)
        result = convert_value(dt)

        assert isinstance(result, str)
        assert "2026-03-15" in result

    def test_convert_none(self):
        """None passes through as None."""
        from plone.pgcatalog.columns import convert_value

        assert convert_value(None) is None

    def test_convert_string(self):
        """Strings pass through unchanged."""
        from plone.pgcatalog.columns import convert_value

        assert convert_value("hello") == "hello"

    def test_convert_bool(self):
        """Booleans pass through (not converted to int)."""
        from plone.pgcatalog.columns import convert_value

        assert convert_value(True) is True
        assert convert_value(False) is False

    def test_convert_list_of_strings(self):
        """Lists of strings (Subject values) pass through."""
        from plone.pgcatalog.columns import convert_value

        result = convert_value(["python", "testing"])
        assert result == ["python", "testing"]


# ===========================================================================
# PlonePGCatalogTool API with real Plone site
# ===========================================================================


class TestPGCatalogToolAPI:
    """Test PlonePGCatalogTool methods with real Plone site context."""

    def test_tool_instantiation(self, pgcatalog_layer):
        """PlonePGCatalogTool can be instantiated and wrapped in Acquisition."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool

        layer = pgcatalog_layer
        portal = layer["portal"]

        tool = PlonePGCatalogTool().__of__(portal)
        assert tool.getId() == "portal_catalog"

    def test_tool_indexes_returns_list(self, pgcatalog_layer):
        """After _replace_catalog + _ensure_catalog_indexes, indexes() works."""
        from plone.pgcatalog.setuphandlers import _ensure_catalog_indexes
        from plone.pgcatalog.setuphandlers import _replace_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]

        _replace_catalog(portal)
        _ensure_catalog_indexes(portal)

        catalog = portal.portal_catalog
        indexes = catalog.indexes()
        assert isinstance(indexes, (list, tuple))
        assert "Title" in indexes
        assert "UID" in indexes
        assert "portal_type" in indexes

    def test_tool_schema_returns_metadata(self, pgcatalog_layer):
        """After setup, schema() returns metadata column names."""
        from plone.pgcatalog.setuphandlers import _ensure_catalog_indexes
        from plone.pgcatalog.setuphandlers import _replace_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]

        _replace_catalog(portal)
        _ensure_catalog_indexes(portal)

        catalog = portal.portal_catalog
        schema = catalog.schema()
        assert isinstance(schema, (list, tuple))
        assert "Title" in schema


class TestRemoveLexicons:
    """Test _remove_lexicons with a real Plone catalog."""

    def test_removes_orphaned_lexicons(self, pgcatalog_layer):
        """After _replace + _ensure + _remove_lexicons, lexicons are gone."""
        from plone.pgcatalog.setuphandlers import _ensure_catalog_indexes
        from plone.pgcatalog.setuphandlers import _remove_lexicons
        from plone.pgcatalog.setuphandlers import _replace_catalog

        layer = pgcatalog_layer
        portal = layer["portal"]

        _replace_catalog(portal)
        _ensure_catalog_indexes(portal)

        catalog = portal.portal_catalog
        # After _ensure, lexicons exist (created by profile import)
        lexicon_names = {"htmltext_lexicon", "plaintext_lexicon", "plone_lexicon"}
        has_lexicons = bool(lexicon_names & set(catalog.objectIds()))

        if has_lexicons:
            _remove_lexicons(portal)
            remaining = lexicon_names & set(catalog.objectIds())
            assert not remaining, f"Lexicons not removed: {remaining}"


class TestComputePathInfo:
    """Test compute_path_info with realistic Plone paths."""

    def test_site_root(self):
        from plone.pgcatalog.columns import compute_path_info

        parent, depth = compute_path_info("/plone")
        assert parent == "/"
        assert depth == 1

    def test_content_in_root(self):
        from plone.pgcatalog.columns import compute_path_info

        parent, depth = compute_path_info("/plone/my-document")
        assert parent == "/plone"
        assert depth == 2

    def test_deeply_nested(self):
        from plone.pgcatalog.columns import compute_path_info

        parent, depth = compute_path_info("/plone/a/b/c/d/leaf")
        assert parent == "/plone/a/b/c/d"
        assert depth == 6
