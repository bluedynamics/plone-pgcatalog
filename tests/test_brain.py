"""Tests for plone.pgcatalog.brain — PGCatalogBrain + CatalogSearchResults."""

from datetime import UTC
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

    def test_get_url_uses_globalrequest(self):
        """getURL resolves the request via ``zope.globalrequest.getRequest()``
        — the catalog tool is intentionally NOT consulted, so brains can
        be cached/pickled/re-queued without dragging the acquisition
        chain along.
        """
        from unittest import mock

        fake_request = mock.Mock()
        fake_request.physicalPathToURL.return_value = "http://example.com/plone/doc"
        brain = PGCatalogBrain(_make_row(path="/plone/doc"))

        with mock.patch("plone.pgcatalog.brain.getRequest", return_value=fake_request):
            assert brain.getURL() == "http://example.com/plone/doc"

    def test_get_url_without_request_returns_path(self):
        """Standalone / script mode: no request available, return the
        raw path.
        """
        from unittest import mock

        brain = PGCatalogBrain(_make_row(path="/plone/doc"))
        with mock.patch("plone.pgcatalog.brain.getRequest", return_value=None):
            assert brain.getURL() == "/plone/doc"

    def test_get_url_ignores_catalog_request(self):
        """Regression guard: even if the catalog carries a REQUEST
        attribute, getURL must not consult it — it goes exclusively
        through getRequest().  Prevents the acquisition-based lookup
        from sneaking back in via copy-paste.
        """
        from unittest import mock

        catalog = mock.Mock()
        # Deliberately different URL so the test fails loudly if the
        # code ever reads catalog.REQUEST.
        catalog.REQUEST.physicalPathToURL.return_value = "http://WRONG-via-catalog/doc"
        brain = PGCatalogBrain(_make_row(path="/plone/doc"), catalog=catalog)
        with mock.patch("plone.pgcatalog.brain.getRequest", return_value=None):
            # No global request → path, NOT the catalog's mocked URL.
            assert brain.getURL() == "/plone/doc"

    def test_get_rid(self):
        brain = PGCatalogBrain(_make_row(zoid=42))
        assert brain.getRID() == 42

    def test_data_record_id(self):
        brain = PGCatalogBrain(_make_row(zoid=42))
        assert brain.data_record_id_ == 42

    def _mock_root(self, root):
        """Patch ``_traversal_root`` to return *root*."""
        from unittest import mock

        return mock.patch("plone.pgcatalog.brain._traversal_root", return_value=root)

    def test_get_object_without_traversal_root(self):
        """No site hook and no request — getObject returns None."""
        brain = PGCatalogBrain(_make_row())
        with self._mock_root(None):
            assert brain.getObject() is None

    def test_get_object_with_traversal_root(self):
        """getObject traverses the parent unrestricted and the leaf restricted.

        Matches upstream ``AbstractCatalogBrain.getObject``: the catalog
        filter already authorized access to the target, so intermediate
        containers are bypassed — only the final object is permission-
        checked.  The traversal root is resolved at call time via
        ``_traversal_root()`` (``getSite().getPhysicalRoot()`` +
        ``getRequest()`` fallback) — brains do NOT hold a reference
        to the catalog tool (cache-friendly).
        """
        from unittest import mock

        obj = mock.Mock()
        parent = mock.Mock()
        parent.restrictedTraverse.return_value = obj
        root = mock.Mock()
        root.unrestrictedTraverse.return_value = parent
        brain = PGCatalogBrain(_make_row(path="/plone/kalender/event-xyz"))

        with self._mock_root(root):
            assert brain.getObject() is obj
        root.unrestrictedTraverse.assert_called_once_with(["", "plone", "kalender"])
        parent.restrictedTraverse.assert_called_once_with("event-xyz")

    def test_get_object_restricted_only_on_leaf(self):
        """Regression for #141 — parent folder with stricter permissions."""
        from AccessControl.unauthorized import Unauthorized
        from unittest import mock

        obj = mock.Mock()
        parent = mock.Mock()
        parent.restrictedTraverse.return_value = obj
        root = mock.Mock()
        # restrictedTraverse on the parent path would raise Unauthorized
        # — the buggy implementation called restrictedTraverse on the
        # full path, which bubbles through ``kalender``.
        root.restrictedTraverse.side_effect = Unauthorized(
            "You are not allowed to access 'kalender'"
        )
        root.unrestrictedTraverse.return_value = parent
        brain = PGCatalogBrain(_make_row(path="/plone/kalender/event-xyz"))

        with self._mock_root(root):
            assert brain.getObject() is obj

    def test_get_object_traversal_error(self):
        from unittest import mock

        root = mock.Mock()
        root.unrestrictedTraverse.side_effect = KeyError("not found")
        brain = PGCatalogBrain(_make_row(path="/plone/doc"))
        with self._mock_root(root):
            assert brain.getObject() is None

    def test_get_object_site_root_path(self):
        """Path ``/plone`` splits to ``['', 'plone']`` — root is the
        "parent" (traversed unrestricted to ``['']``), leaf is ``plone``
        (restricted).  Matches upstream ZCatalog semantics.
        """
        from unittest import mock

        obj = mock.Mock()
        root_via = mock.Mock()
        root_via.restrictedTraverse.return_value = obj
        root = mock.Mock()
        root.unrestrictedTraverse.return_value = root_via
        brain = PGCatalogBrain(_make_row(path="/plone"))

        with self._mock_root(root):
            assert brain.getObject() is obj
        root.unrestrictedTraverse.assert_called_once_with([""])
        root_via.restrictedTraverse.assert_called_once_with("plone")

    def test_unrestricted_get_object_without_traversal_root(self):
        brain = PGCatalogBrain(_make_row())
        with self._mock_root(None):
            assert brain._unrestrictedGetObject() is None

    def test_unrestricted_get_object_with_traversal_root(self):
        from unittest import mock

        obj = mock.Mock()
        root = mock.Mock()
        root.unrestrictedTraverse.return_value = obj
        brain = PGCatalogBrain(_make_row(path="/plone/doc"))
        with self._mock_root(root):
            assert brain._unrestrictedGetObject() is obj

    def test_unrestricted_get_object_traversal_error(self):
        from unittest import mock

        root = mock.Mock()
        root.unrestrictedTraverse.side_effect = AttributeError("nope")
        brain = PGCatalogBrain(_make_row(path="/plone/doc"))
        with self._mock_root(root):
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

    def test_known_field_missing_from_idx_returns_none(self):
        """Known catalog fields absent from idx return None (MV behavior).

        Matches ZCatalog's Missing Value behavior: known indexes and metadata
        columns return None when not stored in idx for this object.
        """
        brain = PGCatalogBrain(_make_row(idx={}))
        # Language is a registered index (via conftest populated_registry)
        assert brain.Language is None
        # mime_type is registered metadata (not an index)
        assert brain.mime_type is None

    def test_unknown_field_raises_attribute_error(self):
        """Unknown fields raise AttributeError for fallback to getObject().

        This enables CatalogContentListingObject.__getattr__ to fall through
        to getObject() for attributes not in the catalog schema (e.g.
        content_type).
        """
        import pytest

        brain = PGCatalogBrain(_make_row(idx={"portal_type": "Document"}))
        with pytest.raises(AttributeError):
            _ = brain.content_type

    def test_unknown_field_with_getattr_default(self):
        """getattr(brain, unknown, default) returns the default sentinel.

        This is the exact pattern used by CatalogContentListingObject:
            brain_name = getattr(aq_base(self._brain), name, missing)
        """
        sentinel = object()
        brain = PGCatalogBrain(_make_row(idx={"portal_type": "Document"}))
        assert getattr(brain, "content_type", sentinel) is sentinel

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

    def test_known_field_returns_none_after_lazy_load(self):
        """Known catalog field absent from idx returns None after lazy load."""
        results, conn = self._make_lazy_results(1)
        # Language is registered but not in the idx_data
        assert results[0].Language is None

    def test_unknown_field_raises_after_lazy_load(self):
        """Unknown field raises AttributeError after lazy load."""
        import pytest

        results, conn = self._make_lazy_results(1)
        with pytest.raises(AttributeError):
            _ = results[0].content_type

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


# ---------------------------------------------------------------------------
# @meta codec round-trip — metadata type preservation
# ---------------------------------------------------------------------------


def _encode_meta(meta_dict):
    """Helper: pickle a dict and convert through codec, as extraction.py does."""
    from zodb_json_codec import pickle_to_dict

    import pickle

    return pickle_to_dict(pickle.dumps(meta_dict, protocol=3))


class TestBrainMetaDecoding:
    """Test that brain attribute access restores non-JSON-native types
    stored in idx["@meta"] via the Rust codec."""

    def test_json_native_from_idx(self):
        """JSON-native values come from top-level idx as before."""
        brain = PGCatalogBrain(
            _make_row(idx={"Title": "Hello", "portal_type": "Document"})
        )
        assert brain.Title == "Hello"
        assert brain.portal_type == "Document"

    def test_datetime_from_meta(self):
        """Zope DateTime in @meta is restored to the original type."""
        from DateTime import DateTime

        dt = DateTime("2008/03/17 08:03:00 GMT+1")
        coded = _encode_meta({"effective": dt})
        brain = PGCatalogBrain(_make_row(idx={"@meta": coded}))
        result = brain.effective
        assert isinstance(result, DateTime)
        assert result == dt

    def test_python_datetime_from_meta(self):
        """stdlib datetime in @meta is restored."""
        from datetime import datetime

        dt = datetime(2024, 1, 15, 12, 30, tzinfo=UTC)
        coded = _encode_meta({"EffectiveDate": dt})
        brain = PGCatalogBrain(_make_row(idx={"@meta": coded}))
        result = brain.EffectiveDate
        assert isinstance(result, datetime)
        assert result == dt

    def test_python_date_from_meta(self):
        """stdlib date in @meta is restored."""
        from datetime import date

        d = date(2024, 6, 15)
        coded = _encode_meta({"CreationDate": d})
        brain = PGCatalogBrain(_make_row(idx={"@meta": coded}))
        result = brain.CreationDate
        assert isinstance(result, date)
        assert result == d

    def test_mixed_idx_and_meta(self):
        """Brain with both top-level idx and @meta resolves both correctly."""
        from DateTime import DateTime

        dt = DateTime("2024/01/01")
        coded = _encode_meta({"effective": dt})
        idx = {"portal_type": "Document", "Title": "Hello", "@meta": coded}
        brain = PGCatalogBrain(_make_row(idx=idx))
        assert brain.portal_type == "Document"
        assert brain.Title == "Hello"
        assert isinstance(brain.effective, DateTime)
        assert brain.effective == dt

    def test_meta_decode_cached(self):
        """Decoding @meta is cached — second access reuses the cached dict."""
        from DateTime import DateTime

        dt1 = DateTime("2024/01/01")
        dt2 = DateTime("2024/12/31")
        coded = _encode_meta({"effective": dt1, "expires": dt2})
        row = _make_row(idx={"@meta": coded})
        brain = PGCatalogBrain(row)

        # First access triggers decode
        assert brain.effective == dt1
        assert "_meta_decoded" in row

        # Second access uses cache
        cached = row["_meta_decoded"]
        assert brain.expires == dt2
        assert row["_meta_decoded"] is cached  # same dict object

    def test_no_meta_key_backward_compat(self):
        """Old-style idx without @meta returns values from top-level (backward compat)."""
        brain = PGCatalogBrain(
            _make_row(idx={"effective": "2008-03-17T08:03:00+01:00"})
        )
        # Returns the string — no crash, backward compatible
        assert brain.effective == "2008-03-17T08:03:00+01:00"

    def test_field_in_both_idx_and_meta(self):
        """When field exists in both top-level idx and @meta, @meta wins."""
        from DateTime import DateTime

        dt = DateTime("2008/03/17 08:03:00 GMT+1")
        coded = _encode_meta({"effective": dt})
        idx = {"effective": "2008-03-17T08:03:00+01:00", "@meta": coded}
        brain = PGCatalogBrain(_make_row(idx=idx))
        result = brain.effective
        assert isinstance(result, DateTime)
        assert result == dt

    def test_unknown_field_raises_attributeerror(self):
        """Unknown fields still raise AttributeError with @meta present."""
        import pytest

        coded = _encode_meta({"effective": "2024-01-01"})
        brain = PGCatalogBrain(_make_row(idx={"@meta": coded}))
        with pytest.raises(AttributeError):
            _ = brain.content_type

    def test_known_field_missing_returns_none(self):
        """Registered metadata field not in idx or @meta returns None."""
        brain = PGCatalogBrain(_make_row(idx={"@meta": _encode_meta({})}))
        # mime_type is registered metadata (in conftest)
        assert brain.mime_type is None

    def test_contains_meta_field(self):
        """'in' operator finds fields in @meta."""
        from DateTime import DateTime

        coded = _encode_meta({"effective": DateTime("2024/01/01")})
        brain = PGCatalogBrain(_make_row(idx={"@meta": coded}))
        assert "effective" in brain
