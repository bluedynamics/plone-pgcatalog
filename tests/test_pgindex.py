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
# KeywordIndex-specific uniqueValues behavior (#143)
# ---------------------------------------------------------------------------


def _catalog_keyword_objects(conn):
    """Insert three docs with an array-valued ``Subject`` (KeywordIndex)."""
    objects = {
        200: {
            "path": "/plone/tag-doc-1",
            "idx": {
                "portal_type": "Document",
                "Subject": ["Werkvortrag", "Tirol", "Aktuelles"],
            },
        },
        201: {
            "path": "/plone/tag-doc-2",
            "idx": {
                "portal_type": "Document",
                "Subject": ["Tirol", "AUSSCHREIBUNG"],
            },
        },
        202: {
            "path": "/plone/tag-doc-3",
            "idx": {
                "portal_type": "Folder",
                "Subject": ["Aktuelles"],
            },
        },
    }
    for zoid, data in objects.items():
        insert_object(conn, zoid)
        catalog_object(conn, zoid=zoid, path=data["path"], idx=data["idx"])
    conn.commit()
    return objects


class TestPGIndexKeyword:
    """Regression tests for #143 — KeywordIndex must expose individual
    keywords, not serialized JSON arrays, in ``uniqueValues()``.
    """

    def _make_keyword_pg_index(self, idx_key, get_conn):
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.pgindex import PGIndex

        wrapped = mock.Mock()
        wrapped.id = idx_key
        return PGIndex(wrapped, idx_key, get_conn, index_type=IndexType.KEYWORD)

    def test_keyword_unique_values_returns_individual_tags(self, pg_conn_with_catalog):
        _catalog_keyword_objects(pg_conn_with_catalog)
        idx = self._make_keyword_pg_index("Subject", lambda: pg_conn_with_catalog)

        values = set(idx.uniqueValues())

        assert values == {"Werkvortrag", "Tirol", "Aktuelles", "AUSSCHREIBUNG"}
        for v in values:
            assert not v.startswith("[")

    def test_keyword_unique_values_with_lengths_counts_elements(
        self, pg_conn_with_catalog
    ):
        _catalog_keyword_objects(pg_conn_with_catalog)
        idx = self._make_keyword_pg_index("Subject", lambda: pg_conn_with_catalog)

        result = dict(idx.uniqueValues(withLengths=True))

        assert result == {
            "Werkvortrag": 1,
            "Tirol": 2,
            "Aktuelles": 2,
            "AUSSCHREIBUNG": 1,
        }

    def test_keyword_unique_values_survives_mixed_scalar_row(
        self, pg_conn_with_catalog
    ):
        """Defensive branch: if some row's idx->'Subject' is a scalar
        (corrupted legacy state) the query must not crash —
        ``jsonb_array_elements_text`` would otherwise raise
        ``cannot extract elements from a scalar``.  Treat the scalar as
        a single-element "pseudo-keyword" to keep the query going.
        """
        _catalog_keyword_objects(pg_conn_with_catalog)
        insert_object(pg_conn_with_catalog, 203)
        catalog_object(
            pg_conn_with_catalog,
            zoid=203,
            path="/plone/legacy-scalar",
            idx={"portal_type": "Document", "Subject": "Legacy"},
        )
        pg_conn_with_catalog.commit()

        idx = self._make_keyword_pg_index("Subject", lambda: pg_conn_with_catalog)

        values = set(idx.uniqueValues())

        assert "Legacy" in values
        assert "Tirol" in values

    def test_scalar_field_index_still_returns_scalars(self, pg_conn_with_catalog):
        """Regression guard: FieldIndex-backed uniqueValues must keep
        returning the scalar values (not wrapped in arrays).
        """
        _catalog_keyword_objects(pg_conn_with_catalog)
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.pgindex import PGIndex

        wrapped = mock.Mock()
        wrapped.id = "portal_type"
        idx = PGIndex(
            wrapped,
            "portal_type",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.FIELD,
        )
        assert set(idx.uniqueValues()) == {"Document", "Folder"}

    def test_default_index_type_is_scalar(self, pg_conn_with_catalog):
        """PGIndex without an explicit ``index_type`` keeps the
        pre-#143 scalar SQL path.  Preserves behavior for any caller
        that builds PGIndex directly without specifying the type.
        """
        _catalog_objects(pg_conn_with_catalog)
        wrapped = mock.Mock()
        wrapped.id = "portal_type"
        idx = PGIndex(wrapped, "portal_type", lambda: pg_conn_with_catalog)
        assert set(idx.uniqueValues()) == {"Document", "Folder"}


class TestPGIndexMappingKeyword:
    """``_PGIndexMapping`` must expose individual keywords to callers
    that iterate it directly — ``plone.app.vocabularies.Keywords``
    iterates ``index._index`` to build the tag-autocomplete vocabulary
    (edit-form ``Schlagwort`` widget).  Before the fix, the mapping
    was not iterable and ``keys()`` returned serialized JSON arrays;
    the widget produced no suggestions.  See follow-up to #143.
    """

    def _make_mapping(self, idx_key, get_conn, index_type=None):
        from plone.pgcatalog.pgindex import _PGIndexMapping

        return _PGIndexMapping(idx_key, get_conn, index_type=index_type)

    def test_mapping_is_iterable_for_scalar_index(self, pg_conn_with_catalog):
        """Pre-existing callers iterating a scalar ``_index`` must keep
        seeing scalar values.
        """
        _catalog_objects(pg_conn_with_catalog)
        mapping = self._make_mapping("portal_type", lambda: pg_conn_with_catalog)
        assert set(iter(mapping)) == {"Document", "Folder"}

    def test_mapping_iterates_individual_keywords(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_keyword_objects(pg_conn_with_catalog)
        mapping = self._make_mapping(
            "Subject",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.KEYWORD,
        )
        values = set(iter(mapping))
        assert values == {"Werkvortrag", "Tirol", "Aktuelles", "AUSSCHREIBUNG"}
        for v in values:
            assert not v.startswith("[")

    def test_mapping_keys_returns_individual_keywords(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_keyword_objects(pg_conn_with_catalog)
        mapping = self._make_mapping(
            "Subject",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.KEYWORD,
        )
        assert set(mapping.keys()) == {
            "Werkvortrag",
            "Tirol",
            "Aktuelles",
            "AUSSCHREIBUNG",
        }

    def test_vocabulary_autocomplete_picks_substring(self, pg_conn_with_catalog):
        """End-to-end-ish: plone.app.vocabularies.Keywords.all_keywords
        iterates ``index._index`` and filters by substring.  Build a
        ``PGIndex`` wired to the PG connection and reproduce the
        ``safe_simplevocabulary_from_values(index._index, query=...)``
        call pattern.
        """
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.pgindex import PGIndex

        _catalog_keyword_objects(pg_conn_with_catalog)
        wrapped = mock.Mock()
        wrapped.id = "Subject"
        pg_index = PGIndex(
            wrapped,
            "Subject",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.KEYWORD,
        )
        # Substring "AT" should not match any of our test keywords;
        # "Tir" should match "Tirol".
        matches_tir = [i for i in pg_index._index if "Tir" in i]
        assert matches_tir == ["Tirol"]

        matches_ak = [i for i in pg_index._index if "Ak" in i]
        assert matches_ak == ["Aktuelles"]


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

    def test_catalog_getindex_keywords_vocabulary_flow(self, pg_conn_with_catalog):
        """End-to-end: simulate the KeywordsVocabulary code path.

        plone.app.vocabularies.catalog.KeywordsVocabulary.all_keywords()::

            index = self.catalog._catalog.getIndex(self.keyword_index)
            return safe_simplevocabulary_from_values(index._index, ...)

        The ``index._index`` lookup must return PG-backed data, not the
        empty ZCatalog BTree.  Regression test for empty Subjects/Tags
        dropdowns.
        """
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat

        _catalog_objects(pg_conn_with_catalog)

        # Build a catalog with a real _CatalogCompat (not a mock) so that
        # _CatalogCompat.getIndex() can walk the Acquisition chain and
        # reach _maybe_wrap_index.
        catalog = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        catalog._get_pg_read_connection = lambda: pg_conn_with_catalog

        compat = _CatalogCompat()
        portal_type_index = mock.Mock()
        portal_type_index.id = "portal_type"
        portal_type_index.meta_type = "FieldIndex"
        compat.indexes["portal_type"] = portal_type_index
        catalog._catalog = compat

        # portal_type is pre-registered in the IndexRegistry by the
        # session-scoped populated_registry fixture in conftest.py.

        # This is the exact line KeywordsVocabulary runs — accessed via
        # catalog._catalog (Acquisition-wrapped) so aq_parent returns catalog.
        index = catalog._catalog.getIndex("portal_type")

        # Verify the wrapping actually produced a PG-backed mapping.
        assert isinstance(index._index, _PGIndexMapping)
        # And uniqueValues() as CMFPlone.browser.search does
        values = list(index.uniqueValues())
        # _catalog_objects creates Document and Folder rows
        assert "Document" in values
        assert "Folder" in values


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


class TestMaybeWrapIndex:
    """Test the _maybe_wrap_index() helper."""

    def test_wraps_field_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.interfaces import IPGCatalogTool
        from plone.pgcatalog.pgindex import _maybe_wrap_index
        from unittest import mock
        from zope.interface import directlyProvides

        catalog = mock.Mock()
        directlyProvides(catalog, IPGCatalogTool)
        catalog._get_pg_read_connection = lambda: pg_conn_with_catalog
        raw_index = mock.Mock()
        raw_index.id = "portal_type"
        raw_index.meta_type = "FieldIndex"

        wrapped = _maybe_wrap_index(catalog, "portal_type", raw_index)
        assert isinstance(wrapped, PGIndex)

    def test_returns_raw_for_non_pg_catalog(self):
        """Non-IPGCatalogTool catalogs get the raw index back."""
        from plone.pgcatalog.pgindex import _maybe_wrap_index
        from unittest import mock

        # A catalog that does NOT implement IPGCatalogTool
        from zope.interface import Interface

        class IOtherCatalog(Interface):
            pass

        catalog = mock.Mock()
        from zope.interface import directlyProvides

        directlyProvides(catalog, IOtherCatalog)
        raw_index = mock.Mock()

        wrapped = _maybe_wrap_index(catalog, "portal_type", raw_index)
        assert wrapped is raw_index

    def test_returns_raw_for_none_index(self):
        """None stays None."""
        from plone.pgcatalog.pgindex import _maybe_wrap_index

        assert _maybe_wrap_index(object(), "x", None) is None

    def test_special_index_not_wrapped(self, pg_conn_with_catalog):
        """Indexes registered with idx_key=None (SearchableText,
        path, effectiveRange) return the raw index unchanged.
        """
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.pgindex import _maybe_wrap_index
        from unittest import mock

        # Register SearchableText with idx_key=None
        registry = get_registry()
        registry.register(
            name="SearchableText",
            idx_type=IndexType.TEXT,
            idx_key=None,
            source_attrs=[],
        )

        from plone.pgcatalog.interfaces import IPGCatalogTool
        from zope.interface import directlyProvides

        catalog = mock.Mock()
        directlyProvides(catalog, IPGCatalogTool)
        catalog._get_pg_read_connection = lambda: pg_conn_with_catalog
        raw_index = mock.Mock()

        wrapped = _maybe_wrap_index(catalog, "SearchableText", raw_index)
        assert wrapped is raw_index


# ---------------------------------------------------------------------------
# _PGIndexMapping: __getitem__, __len__, items/values NotImplementedError
# ---------------------------------------------------------------------------


class TestPGIndexMappingNewMethods:
    def test_getitem_returns_zoid_for_existing_value(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        assert mapping["uid-aaa-100"] == 100

    def test_getitem_raises_keyerror_on_miss(self, pg_conn_with_catalog):
        import pytest

        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        with pytest.raises(KeyError, match="nonexistent-uid"):
            _ = mapping["nonexistent-uid"]

    def test_len_scalar_index(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)  # 2 Documents, 1 Folder
        mapping = _PGIndexMapping("portal_type", lambda: pg_conn_with_catalog)
        assert len(mapping) == 2  # distinct values

    def test_len_keyword_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_keyword_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping(
            "Subject",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.KEYWORD,
        )
        # distinct keywords across the three fixture docs
        assert len(mapping) == 4

    def test_len_keyword_with_legacy_scalar_row(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_keyword_objects(pg_conn_with_catalog)
        insert_object(pg_conn_with_catalog, 299)
        catalog_object(
            pg_conn_with_catalog,
            zoid=299,
            path="/plone/legacy-scalar",
            idx={"portal_type": "Document", "Subject": "Legacy"},
        )
        pg_conn_with_catalog.commit()
        mapping = _PGIndexMapping(
            "Subject",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.KEYWORD,
        )
        assert len(mapping) == 5  # 4 array keywords + "Legacy" scalar
