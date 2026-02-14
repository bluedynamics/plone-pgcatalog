"""Tests for plone.pgcatalog.brain — PGCatalogBrain + CatalogSearchResults."""

from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain


# ---------------------------------------------------------------------------
# Sample rows
# ---------------------------------------------------------------------------


def _make_row(zoid=1, path="/plone/doc", idx=None):
    return {
        "zoid": zoid,
        "path": path,
        "idx": idx
        or {"portal_type": "Document", "Title": "Hello", "is_folderish": False},
    }


# ---------------------------------------------------------------------------
# PGCatalogBrain
# ---------------------------------------------------------------------------


class TestBrainBasics:
    def test_get_path(self):
        brain = PGCatalogBrain(_make_row(path="/plone/folder/doc"))
        assert brain.getPath() == "/plone/folder/doc"

    def test_get_url_without_catalog(self):
        brain = PGCatalogBrain(_make_row(path="/plone/doc"))
        assert brain.getURL() == "/plone/doc"

    def test_get_url_with_request(self):
        from unittest import mock

        catalog = mock.Mock()
        catalog.REQUEST.physicalPathToURL.return_value = "http://example.com/plone/doc"
        brain = PGCatalogBrain(_make_row(path="/plone/doc"), catalog=catalog)
        assert brain.getURL() == "http://example.com/plone/doc"

    def test_get_url_catalog_without_request(self):
        from unittest import mock

        catalog = mock.Mock(spec=[])
        brain = PGCatalogBrain(_make_row(path="/plone/doc"), catalog=catalog)
        assert brain.getURL() == "/plone/doc"

    def test_get_rid(self):
        brain = PGCatalogBrain(_make_row(zoid=42))
        assert brain.getRID() == 42

    def test_data_record_id(self):
        brain = PGCatalogBrain(_make_row(zoid=42))
        assert brain.data_record_id_ == 42

    def test_get_object_without_catalog(self):
        brain = PGCatalogBrain(_make_row())
        assert brain.getObject() is None

    def test_get_object_with_catalog(self):
        from unittest import mock

        obj = mock.Mock()
        catalog = mock.Mock()
        catalog.restrictedTraverse.return_value = obj
        brain = PGCatalogBrain(_make_row(path="/plone/doc"), catalog=catalog)
        assert brain.getObject() is obj

    def test_get_object_traversal_error(self):
        from unittest import mock

        catalog = mock.Mock()
        catalog.restrictedTraverse.side_effect = KeyError("not found")
        brain = PGCatalogBrain(_make_row(path="/plone/doc"), catalog=catalog)
        assert brain.getObject() is None

    def test_unrestricted_get_object_without_catalog(self):
        brain = PGCatalogBrain(_make_row())
        assert brain._unrestrictedGetObject() is None

    def test_unrestricted_get_object_with_catalog(self):
        from unittest import mock

        obj = mock.Mock()
        catalog = mock.Mock()
        catalog.unrestrictedTraverse.return_value = obj
        brain = PGCatalogBrain(_make_row(path="/plone/doc"), catalog=catalog)
        assert brain._unrestrictedGetObject() is obj

    def test_unrestricted_get_object_traversal_error(self):
        from unittest import mock

        catalog = mock.Mock()
        catalog.unrestrictedTraverse.side_effect = AttributeError("nope")
        brain = PGCatalogBrain(_make_row(path="/plone/doc"), catalog=catalog)
        assert brain._unrestrictedGetObject() is None


class TestBrainAttributeAccess:
    def test_idx_attribute(self):
        brain = PGCatalogBrain(_make_row(idx={"portal_type": "Document"}))
        assert brain.portal_type == "Document"

    def test_idx_title(self):
        brain = PGCatalogBrain(_make_row(idx={"Title": "My Page"}))
        assert brain.Title == "My Page"

    def test_idx_boolean(self):
        brain = PGCatalogBrain(_make_row(idx={"is_folderish": False}))
        assert brain.is_folderish is False

    def test_idx_list(self):
        brain = PGCatalogBrain(_make_row(idx={"Subject": ["Python", "Zope"]}))
        assert brain.Subject == ["Python", "Zope"]

    def test_idx_none_value(self):
        brain = PGCatalogBrain(_make_row(idx={"expires": None}))
        assert brain.expires is None

    def test_attribute_error_for_unknown(self):
        import pytest

        brain = PGCatalogBrain(_make_row(idx={}))
        with pytest.raises(AttributeError):
            _ = brain.nonexistent_attribute

    def test_private_attrs_raise_attribute_error(self):
        import pytest

        brain = PGCatalogBrain(_make_row())
        with pytest.raises(AttributeError):
            _ = brain._private

    def test_repr(self):
        brain = PGCatalogBrain(_make_row(zoid=42, path="/plone/doc"))
        r = repr(brain)
        assert "42" in r
        assert "/plone/doc" in r


class TestBrainContains:
    def test_contains_idx_key(self):
        brain = PGCatalogBrain(_make_row(idx={"portal_type": "Document"}))
        assert "portal_type" in brain

    def test_contains_special_key(self):
        brain = PGCatalogBrain(_make_row())
        assert "path" in brain
        assert "zoid" in brain

    def test_not_contains_unknown(self):
        brain = PGCatalogBrain(_make_row(idx={}))
        assert "nonexistent" not in brain

    def test_has_key(self):
        brain = PGCatalogBrain(_make_row(idx={"portal_type": "Document"}))
        assert brain.has_key("portal_type") is True
        assert brain.has_key("nonexistent") is False


# ---------------------------------------------------------------------------
# CatalogSearchResults
# ---------------------------------------------------------------------------


class TestCatalogSearchResults:
    def _make_results(self, count=5, actual=None):
        brains = [
            PGCatalogBrain(_make_row(zoid=i, path=f"/plone/doc{i}"))
            for i in range(count)
        ]
        return CatalogSearchResults(brains, actual_result_count=actual)

    def test_len(self):
        results = self._make_results(5)
        assert len(results) == 5

    def test_iter(self):
        results = self._make_results(3)
        paths = [b.getPath() for b in results]
        assert len(paths) == 3
        assert "/plone/doc0" in paths

    def test_indexing(self):
        results = self._make_results(3)
        assert results[0].getRID() == 0
        assert results[2].getRID() == 2

    def test_negative_indexing(self):
        results = self._make_results(3)
        assert results[-1].getRID() == 2

    def test_slicing(self):
        results = self._make_results(5, actual=100)
        sliced = results[1:3]
        assert isinstance(sliced, CatalogSearchResults)
        assert len(sliced) == 2
        assert sliced.actual_result_count == 100  # preserved from original

    def test_actual_result_count_default(self):
        results = self._make_results(5)
        assert results.actual_result_count == 5

    def test_actual_result_count_with_limit(self):
        results = self._make_results(10, actual=500)
        assert len(results) == 10
        assert results.actual_result_count == 500

    def test_bool_true(self):
        results = self._make_results(1)
        assert bool(results) is True

    def test_bool_false(self):
        results = CatalogSearchResults([])
        assert bool(results) is False

    def test_repr(self):
        results = self._make_results(3, actual=50)
        r = repr(results)
        assert "len=3" in r
        assert "actual=50" in r

    def test_empty_results(self):
        results = CatalogSearchResults([])
        assert len(results) == 0
        assert results.actual_result_count == 0
        assert list(results) == []


# ---------------------------------------------------------------------------
# Lazy idx batch loading
# ---------------------------------------------------------------------------


def _make_lazy_row(zoid=1, path="/plone/doc"):
    """Row WITHOUT idx — simulates lazy mode SELECT."""
    return {"zoid": zoid, "path": path}


class _MockConn:
    def __init__(self, idx_data):
        self._idx_data = idx_data

    def cursor(self):
        return _MockCursor(self._idx_data)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class _MockCursor:
    def __init__(self, idx_data):
        self._idx_data = idx_data
        self._rows = []

    def execute(self, sql, params=None, **kwargs):
        zoids = params.get("zoids", []) if params else []
        self._rows = [
            {"zoid": z, "idx": self._idx_data[z]} for z in zoids if z in self._idx_data
        ]

    def __iter__(self):
        return iter(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestLazyIdxLoading:
    def _make_lazy_results(self, count=3):
        """Create brains without idx, wired to a mock connection."""
        idx_data = {
            i: {"portal_type": "Document", "Title": f"Doc {i}"} for i in range(count)
        }
        conn = _MockConn(idx_data)
        brains = [
            PGCatalogBrain(_make_lazy_row(zoid=i, path=f"/plone/doc{i}"))
            for i in range(count)
        ]
        results = CatalogSearchResults(brains, conn=conn)
        for brain in brains:
            brain._result_set = results
        return results, conn

    def test_lazy_idx_not_loaded_until_metadata_access(self):
        results, conn = self._make_lazy_results(3)
        assert not results._idx_loaded

    def test_len_does_not_trigger_idx_load(self):
        results, conn = self._make_lazy_results(3)
        assert len(results) == 3
        assert not results._idx_loaded

    def test_getPath_does_not_trigger_idx_load(self):
        results, conn = self._make_lazy_results(3)
        assert results[0].getPath() == "/plone/doc0"
        assert not results._idx_loaded

    def test_getRID_does_not_trigger_idx_load(self):
        results, conn = self._make_lazy_results(3)
        assert results[0].getRID() == 0
        assert not results._idx_loaded

    def test_lazy_idx_batch_loads_all_at_once(self):
        results, conn = self._make_lazy_results(3)
        # Access metadata on one brain
        title = results[0].Title
        assert title == "Doc 0"
        # Batch loaded all brains
        assert results._idx_loaded
        # Other brains also have idx now
        assert results[1].Title == "Doc 1"
        assert results[2].Title == "Doc 2"

    def test_contains_triggers_idx_load(self):
        results, conn = self._make_lazy_results(3)
        assert "portal_type" in results[0]
        assert results._idx_loaded

    def test_attribute_error_after_lazy_load(self):
        import pytest

        results, conn = self._make_lazy_results(1)
        with pytest.raises(AttributeError):
            _ = results[0].nonexistent

    def test_slice_preserves_lazy_loading(self):
        results, conn = self._make_lazy_results(5)
        sliced = results[1:3]
        assert isinstance(sliced, CatalogSearchResults)
        assert not sliced._idx_loaded
        # Access metadata on sliced brain triggers batch load for slice
        assert sliced[0].Title == "Doc 1"
        assert sliced._idx_loaded
        # Original result set NOT loaded (sliced brains rewired)
        assert not results._idx_loaded

    def test_eager_mode_still_works(self):
        """No conn → eager mode, idx in row, no lazy loading."""
        row = _make_row(zoid=1, path="/plone/doc", idx={"Title": "Hello"})
        brain = PGCatalogBrain(row)
        results = CatalogSearchResults([brain])
        assert results._idx_loaded  # True because no conn
        assert brain.Title == "Hello"
