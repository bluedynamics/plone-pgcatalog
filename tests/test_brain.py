"""Tests for plone.pgcatalog.brain — PGCatalogBrain + CatalogSearchResults."""

from plone.pgcatalog.brain import CatalogSearchResults
from plone.pgcatalog.brain import PGCatalogBrain


# ---------------------------------------------------------------------------
# Sample rows
# ---------------------------------------------------------------------------

def _make_row(zoid=1, path="/plone/doc", idx=None, state=None):
    return {
        "zoid": zoid,
        "path": path,
        "idx": idx or {"portal_type": "Document", "Title": "Hello", "is_folderish": False},
        "state": state or {"title": "Hello", "description": "A doc"},
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

    def test_get_rid(self):
        brain = PGCatalogBrain(_make_row(zoid=42))
        assert brain.getRID() == 42

    def test_data_record_id(self):
        brain = PGCatalogBrain(_make_row(zoid=42))
        assert brain.data_record_id_ == 42

    def test_get_object_without_catalog(self):
        brain = PGCatalogBrain(_make_row())
        assert brain.getObject() is None

    def test_unrestricted_get_object_without_catalog(self):
        brain = PGCatalogBrain(_make_row())
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

    def test_state_fallback(self):
        """Attributes not in idx fall back to state JSONB."""
        brain = PGCatalogBrain(_make_row(
            idx={"portal_type": "Document"},
            state={"description": "A document"},
        ))
        assert brain.description == "A document"

    def test_idx_takes_precedence_over_state(self):
        """idx values override state values of the same name."""
        brain = PGCatalogBrain(_make_row(
            idx={"Title": "Computed Title"},
            state={"Title": "Raw Title"},
        ))
        assert brain.Title == "Computed Title"

    def test_attribute_error_for_unknown(self):
        import pytest
        brain = PGCatalogBrain(_make_row(idx={}, state={}))
        with pytest.raises(AttributeError):
            brain.nonexistent_attribute

    def test_private_attrs_raise_attribute_error(self):
        import pytest
        brain = PGCatalogBrain(_make_row())
        with pytest.raises(AttributeError):
            brain._private

    def test_repr(self):
        brain = PGCatalogBrain(_make_row(zoid=42, path="/plone/doc"))
        r = repr(brain)
        assert "42" in r
        assert "/plone/doc" in r


class TestBrainContains:

    def test_contains_idx_key(self):
        brain = PGCatalogBrain(_make_row(idx={"portal_type": "Document"}))
        assert "portal_type" in brain

    def test_contains_state_key(self):
        brain = PGCatalogBrain(_make_row(idx={}, state={"description": "hi"}))
        assert "description" in brain

    def test_contains_special_key(self):
        brain = PGCatalogBrain(_make_row())
        assert "path" in brain
        assert "zoid" in brain

    def test_not_contains_unknown(self):
        brain = PGCatalogBrain(_make_row(idx={}, state={}))
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
