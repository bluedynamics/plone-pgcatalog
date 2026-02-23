"""Tests for eea.facetednavigation compatibility adapter (PGFacetedCatalog).

These tests verify that ``PGFacetedCatalog._pg_apply_index`` correctly
queries the ``idx`` JSONB column in object_state to produce frozensets
of matching ZOIDs for faceted counting.

All tests are skipped when ``eea.facetednavigation`` is not installed.
"""

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.schema import install_catalog_schema
from psycopg.rows import dict_row
from tests.conftest import DSN
from tests.conftest import insert_object
from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

import psycopg
import pytest


try:
    from eea.facetednavigation.search.catalog import FacetedCatalog
    from eea.facetednavigation.search.interfaces import IFacetedCatalog

    HAS_EEA = True
except ImportError:
    HAS_EEA = False

pytestmark = pytest.mark.skipif(
    not HAS_EEA, reason="eea.facetednavigation not installed"
)

if HAS_EEA:
    from plone.pgcatalog.addons_compat.eeafacetednavigation import _pg_apply_index
    from plone.pgcatalog.addons_compat.eeafacetednavigation import PGFacetedCatalog


# ---------------------------------------------------------------------------
# Mock index object (mimics ZCatalog index interface)
# ---------------------------------------------------------------------------


class _MockIndex:
    def __init__(self, name, meta_type="FieldIndex"):
        self._id = name
        self.meta_type = meta_type

    def getId(self):
        return self._id


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TABLES_TO_DROP = (
    "DROP TABLE IF EXISTS blob_state, object_state, transaction_log CASCADE"
)


@pytest.fixture
def pg_conn_with_data():
    """Fresh database with schema and three cataloged test objects.

    Objects:
        zoid=1: Document, published, Subject=[news, tech]
        zoid=2: Document, private, Subject=[news]
        zoid=3: Folder, published, Subject=[tech], is_folderish=true
    """
    conn = psycopg.connect(DSN, row_factory=dict_row)
    with conn.cursor() as cur:
        cur.execute(TABLES_TO_DROP)
    conn.commit()

    # Install base schema + catalog extension
    conn.execute(HISTORY_FREE_SCHEMA)
    conn.commit()
    install_catalog_schema(conn)
    conn.commit()

    # Insert base object_state rows
    insert_object(conn, zoid=1)
    insert_object(conn, zoid=2)
    insert_object(conn, zoid=3)

    # Write catalog data
    catalog_object(
        conn,
        zoid=1,
        path="/plone/doc1",
        idx={
            "portal_type": "Document",
            "review_state": "published",
            "Subject": ["news", "tech"],
            "is_folderish": False,
            "UID": "uid-1",
            "Creator": "admin",
        },
    )
    catalog_object(
        conn,
        zoid=2,
        path="/plone/doc2",
        idx={
            "portal_type": "Document",
            "review_state": "private",
            "Subject": ["news"],
            "is_folderish": False,
            "UID": "uid-2",
            "Creator": "editor",
        },
    )
    catalog_object(
        conn,
        zoid=3,
        path="/plone/folder1",
        idx={
            "portal_type": "Folder",
            "review_state": "published",
            "Subject": ["tech"],
            "is_folderish": True,
            "UID": "uid-3",
            "Creator": "admin",
        },
    )
    conn.commit()

    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Interface and inheritance tests
# ---------------------------------------------------------------------------


class TestInterfaceAndInheritance:
    def test_implements_interface(self):
        """PGFacetedCatalog provides IFacetedCatalog."""
        assert IFacetedCatalog.providedBy(PGFacetedCatalog())

    def test_subclasses_faceted_catalog(self):
        """PGFacetedCatalog is a subclass of FacetedCatalog."""
        assert issubclass(PGFacetedCatalog, FacetedCatalog)


# ---------------------------------------------------------------------------
# _pg_apply_index tests (FieldIndex)
# ---------------------------------------------------------------------------


class TestPGApplyFieldMatch:
    def test_pg_apply_field_match(self, pg_conn_with_data):
        """FieldIndex exact match on portal_type='Document' returns zoids 1, 2."""
        index = _MockIndex("portal_type", meta_type="FieldIndex")
        result = _pg_apply_index(pg_conn_with_data, "portal_type", index, "Document")
        assert result == frozenset({1, 2})


# ---------------------------------------------------------------------------
# _pg_apply_index tests (KeywordIndex)
# ---------------------------------------------------------------------------


class TestPGApplyKeyword:
    def test_pg_apply_keyword_single(self, pg_conn_with_data):
        """KeywordIndex with single value 'news' returns zoids 1, 2."""
        index = _MockIndex("Subject", meta_type="KeywordIndex")
        result = _pg_apply_index(pg_conn_with_data, "Subject", index, "news")
        assert result == frozenset({1, 2})

    def test_pg_apply_keyword_multi(self, pg_conn_with_data):
        """KeywordIndex with multiple values returns union (OR semantics)."""
        index = _MockIndex("Subject", meta_type="KeywordIndex")
        result = _pg_apply_index(pg_conn_with_data, "Subject", index, ["news", "tech"])
        # news: zoid 1, 2; tech: zoid 1, 3 -> union = {1, 2, 3}
        assert result == frozenset({1, 2, 3})


# ---------------------------------------------------------------------------
# _pg_apply_index tests (BooleanIndex)
# ---------------------------------------------------------------------------


class TestPGApplyBoolean:
    def test_pg_apply_boolean(self, pg_conn_with_data):
        """BooleanIndex with True returns only zoid 3 (the folder)."""
        index = _MockIndex("is_folderish", meta_type="BooleanIndex")
        result = _pg_apply_index(pg_conn_with_data, "is_folderish", index, True)
        assert result == frozenset({3})


# ---------------------------------------------------------------------------
# _pg_apply_index tests (UUIDIndex)
# ---------------------------------------------------------------------------


class TestPGApplyUUID:
    def test_pg_apply_uuid(self, pg_conn_with_data):
        """UUIDIndex exact match on 'uid-1' returns zoid 1."""
        index = _MockIndex("UID", meta_type="UUIDIndex")
        result = _pg_apply_index(pg_conn_with_data, "UID", index, "uid-1")
        assert result == frozenset({1})


# ---------------------------------------------------------------------------
# _pg_apply_index tests (dict query)
# ---------------------------------------------------------------------------


class TestPGApplyDictQuery:
    def test_pg_apply_dict_query(self, pg_conn_with_data):
        """Dict-style query {'query': 'Folder'} extracts the value."""
        index = _MockIndex("portal_type", meta_type="FieldIndex")
        result = _pg_apply_index(
            pg_conn_with_data, "portal_type", index, {"query": "Folder"}
        )
        assert result == frozenset({3})


# ---------------------------------------------------------------------------
# _pg_apply_index tests (unknown index)
# ---------------------------------------------------------------------------


class TestPGApplyUnknown:
    def test_pg_apply_unknown_returns_empty(self, pg_conn_with_data):
        """Unknown index type returns empty frozenset."""
        index = _MockIndex("nonexistent", meta_type="SomeWeirdIndex")
        result = _pg_apply_index(pg_conn_with_data, "nonexistent", index, "whatever")
        assert result == frozenset()


# ---------------------------------------------------------------------------
# frozenset intersection test
# ---------------------------------------------------------------------------


class TestFrozensetIntersection:
    def test_frozenset_intersection(self, pg_conn_with_data):
        """Result frozenset intersects correctly with another set."""
        # Get Documents (zoids 1, 2)
        field_index = _MockIndex("portal_type", meta_type="FieldIndex")
        documents = _pg_apply_index(
            pg_conn_with_data, "portal_type", field_index, "Document"
        )

        # Get published items (zoids 1, 3)
        state_index = _MockIndex("review_state", meta_type="FieldIndex")
        published = _pg_apply_index(
            pg_conn_with_data, "review_state", state_index, "published"
        )

        # Intersection: published Documents -> zoid 1 only
        assert documents & published == frozenset({1})


# ---------------------------------------------------------------------------
# _pg_apply_index tests (DateIndex)
# ---------------------------------------------------------------------------


class TestPGApplyDate:
    def test_pg_apply_date_returns_frozenset(self, pg_conn_with_data):
        """DateIndex returns empty frozenset (unsupported in faceted apply)."""
        from plone.pgcatalog.addons_compat.eeafacetednavigation import _dispatch_by_type
        from plone.pgcatalog.columns import IndexType

        # DATE_RANGE is not supported, returns empty
        result = _dispatch_by_type(
            pg_conn_with_data, IndexType.DATE_RANGE, "effectiveRange", "2024-01-01"
        )
        assert result == frozenset()

    def test_pg_apply_date_match(self, pg_conn_with_data):
        """DateIndex exact match works via jsonb containment."""
        # Add a DateIndex entry
        catalog_object(
            pg_conn_with_data,
            zoid=1,
            path="/plone/doc1",
            idx={"created": "2024-01-15"},
        )
        pg_conn_with_data.commit()

        from plone.pgcatalog.addons_compat.eeafacetednavigation import _dispatch_by_type
        from plone.pgcatalog.columns import IndexType

        result = _dispatch_by_type(
            pg_conn_with_data, IndexType.DATE, "created", "2024-01-15"
        )
        assert 1 in result


# ---------------------------------------------------------------------------
# _pg_apply_index tests (special indexes → empty)
# ---------------------------------------------------------------------------


class TestPGApplySpecialIndex:
    def test_special_index_returns_empty(self, pg_conn_with_data):
        """Special indexes (idx_key=None) return empty frozenset."""
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        # Register SearchableText as special (idx_key=None)
        get_registry().register(
            "SearchableText", IndexType.TEXT, None, ["SearchableText"]
        )
        index = _MockIndex("SearchableText", meta_type="ZCTextIndex")
        result = _pg_apply_index(pg_conn_with_data, "SearchableText", index, "volcano")
        assert result == frozenset()


# ---------------------------------------------------------------------------
# PGFacetedCatalog.apply_index tests
# ---------------------------------------------------------------------------


class TestPGFacetedCatalogApplyIndex:
    """Test the adapter's apply_index method."""

    def test_apply_index_non_pg_catalog_falls_back(self):
        """When catalog is not IPGCatalogTool, falls back to super."""
        from unittest import mock

        adapter = PGFacetedCatalog()
        context = mock.Mock()
        # getToolByName returns a catalog that doesn't provide IPGCatalogTool
        catalog = mock.Mock()
        catalog.__class__ = type("FakeCatalog", (), {})
        with (
            mock.patch(
                "plone.pgcatalog.addons_compat.eeafacetednavigation.getToolByName",
                return_value=catalog,
            ),
            mock.patch.object(
                FacetedCatalog, "apply_index", return_value=(set(), ())
            ) as super_mock,
        ):
            adapter.apply_index(context, _MockIndex("portal_type"), "Document")
            super_mock.assert_called_once()

    def test_apply_index_no_catalog_falls_back(self):
        """When getToolByName returns None, falls back to super."""
        from unittest import mock

        adapter = PGFacetedCatalog()
        context = mock.Mock()
        with (
            mock.patch(
                "plone.pgcatalog.addons_compat.eeafacetednavigation.getToolByName",
                return_value=None,
            ),
            mock.patch.object(
                FacetedCatalog, "apply_index", return_value=(set(), ())
            ) as super_mock,
        ):
            adapter.apply_index(context, _MockIndex("portal_type"), "Document")
            super_mock.assert_called_once()

    def test_apply_index_pg_exception_falls_back(self):
        """When PG query raises, falls back to super."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from unittest import mock

        adapter = PGFacetedCatalog()
        context = mock.Mock()
        catalog = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        with (
            mock.patch(
                "plone.pgcatalog.addons_compat.eeafacetednavigation.getToolByName",
                return_value=catalog,
            ),
            mock.patch(
                "plone.pgcatalog.addons_compat.eeafacetednavigation.get_pool",
                side_effect=RuntimeError("no pool"),
            ),
            mock.patch.object(
                FacetedCatalog, "apply_index", return_value=(set(), ())
            ) as super_mock,
        ):
            adapter.apply_index(context, _MockIndex("portal_type"), "Document")
            super_mock.assert_called_once()
