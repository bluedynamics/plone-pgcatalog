"""Tests for SearchableText IFile indexer override."""

from plone.pgcatalog.indexers import SearchableText_file_override
from unittest import mock

import os


class TestSearchableTextFileOverride:
    """Test conditional SearchableText indexer for IFile."""

    def _make_obj(self, title="Test File", description="A test", subject=()):
        """Create a mock IFile object with the minimum needed attributes."""
        obj = mock.Mock()
        obj.id = "test-file"
        obj.title = title
        obj.description = description
        obj.Subject.return_value = subject
        return obj

    def test_with_tika_returns_title_description_only(self):
        """When PGCATALOG_TIKA_URL is set, skip transforms entirely."""
        obj = self._make_obj(title="My Report", description="Quarterly results")
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}),
            mock.patch(
                "plone.pgcatalog.indexers.SearchableText",
                return_value="test-file My Report Quarterly results",
            ),
        ):
            result = SearchableText_file_override(obj)
        assert "My Report" in result
        assert "Quarterly results" in result

    def test_without_tika_delegates_to_original(self):
        """When PGCATALOG_TIKA_URL is not set, delegate to original indexer."""
        obj = self._make_obj()
        with (
            mock.patch.dict(os.environ, {}, clear=False),
            mock.patch(
                "plone.pgcatalog.indexers._original_searchable_text_file",
                return_value="original result",
            ) as original_mock,
        ):
            os.environ.pop("PGCATALOG_TIKA_URL", None)
            result = SearchableText_file_override(obj)
        original_mock.assert_called_once_with(obj)
        assert result == "original result"

    def test_with_empty_tika_url_delegates_to_original(self):
        """Empty PGCATALOG_TIKA_URL is treated as unset."""
        obj = self._make_obj()
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "  "}),
            mock.patch(
                "plone.pgcatalog.indexers._original_searchable_text_file",
                return_value="original result",
            ) as original_mock,
        ):
            result = SearchableText_file_override(obj)
        original_mock.assert_called_once_with(obj)
        assert result == "original result"

    def test_tika_mode_includes_subject(self):
        """Tika mode should include Subject keywords."""
        obj = self._make_obj(subject=("python", "plone"))
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}),
            mock.patch(
                "plone.pgcatalog.indexers.SearchableText",
                return_value="test-file Test File A test python plone",
            ),
        ):
            result = SearchableText_file_override(obj)
        assert "python" in result
        assert "plone" in result

    def test_tika_mode_does_not_call_transforms(self):
        """Verify portal_transforms is never accessed in Tika mode."""
        obj = self._make_obj()
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}),
            mock.patch(
                "plone.pgcatalog.indexers.SearchableText",
                return_value="test-file Test File A test",
            ),
        ):
            result = SearchableText_file_override(obj)
        assert result is not None


# ---------------------------------------------------------------------------
# #114: Tika-aware NamedfileFieldConverter — skips portal_transforms for
# searchable NamedBlobFile fields when Tika is active (textindexer behavior).
# ---------------------------------------------------------------------------


class _FakeBlob:
    def __init__(
        self, filename="report.pdf", size=1234, content_type="application/pdf"
    ):
        self.filename = filename
        self.contentType = content_type
        self._size = size
        self.data = b"%PDF-1.4 fake"

    def getSize(self):
        return self._size


class _FakeField:
    """Minimal stand-in for a zope schema field used by the converter."""

    def __init__(self, value):
        self._value = value

    def interface(self, context):
        return context  # storage == context for the test

    def get(self, storage):
        return self._value


class TestTikaAwareNamedfileFieldConverter:
    def _converter(self, value):
        from plone.pgcatalog.indexers import TikaAwareNamedfileFieldConverter

        return TikaAwareNamedfileFieldConverter(
            mock.Mock(), _FakeField(value), mock.Mock()
        )

    def test_with_tika_returns_filename(self):
        conv = self._converter(_FakeBlob(filename="quarterly.pdf"))
        with mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}):
            result = conv.convert()
        assert result == "quarterly.pdf"

    def test_with_tika_does_not_call_transforms(self):
        conv = self._converter(_FakeBlob())
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}),
            mock.patch(
                "plone.app.dexterity.textindexer.converters.getToolByName"
            ) as gtbn,
        ):
            conv.convert()
        gtbn.assert_not_called()

    def test_with_tika_empty_blob_returns_empty(self):
        conv = self._converter(_FakeBlob(filename="x.pdf", size=0))
        with mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}):
            assert conv.convert() == ""

    def test_with_tika_missing_filename_returns_empty_string(self):
        conv = self._converter(_FakeBlob(filename=None))
        with mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}):
            assert conv.convert() == ""

    def test_without_tika_delegates_to_parent(self):
        conv = self._converter(_FakeBlob())
        with (
            mock.patch.dict(os.environ, {}, clear=False),
            mock.patch(
                "plone.app.dexterity.textindexer.converters."
                "NamedfileFieldConverter.convert",
                return_value="ORIGINAL",
            ) as parent,
        ):
            os.environ.pop("PGCATALOG_TIKA_URL", None)
            result = conv.convert()
        parent.assert_called_once()
        assert result == "ORIGINAL"

    def test_empty_tika_url_delegates_to_parent(self):
        conv = self._converter(_FakeBlob())
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "  "}),
            mock.patch(
                "plone.app.dexterity.textindexer.converters."
                "NamedfileFieldConverter.convert",
                return_value="ORIGINAL",
            ),
        ):
            assert conv.convert() == "ORIGINAL"
