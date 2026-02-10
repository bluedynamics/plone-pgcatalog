"""Tests for plone.pgcatalog.columns — value conversion + path utilities."""

from datetime import date
from datetime import datetime
from datetime import UTC
from plone.pgcatalog.columns import compute_path_info
from plone.pgcatalog.columns import convert_value


class TestConvertValue:
    """Verify value conversion for idx JSONB storage."""

    def test_none(self):
        assert convert_value(None) is None

    def test_bool_true(self):
        assert convert_value(True) is True

    def test_bool_false(self):
        assert convert_value(False) is False

    def test_int(self):
        assert convert_value(42) == 42

    def test_float(self):
        assert convert_value(3.14) == 3.14

    def test_string(self):
        assert convert_value("hello") == "hello"

    def test_datetime_with_tz(self):
        dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        result = convert_value(dt)
        assert result == "2025-01-15T10:30:00+00:00"

    def test_datetime_naive(self):
        dt = datetime(2025, 1, 15, 10, 30, 0)
        result = convert_value(dt)
        assert result == "2025-01-15T10:30:00"

    def test_date(self):
        d = date(2025, 1, 15)
        assert convert_value(d) == "2025-01-15"

    def test_list(self):
        assert convert_value(["a", "b"]) == ["a", "b"]

    def test_tuple(self):
        assert convert_value(("a", "b")) == ["a", "b"]

    def test_set(self):
        result = convert_value({"a"})
        assert result == ["a"]

    def test_nested_list(self):
        assert convert_value([1, [2, 3]]) == [1, [2, 3]]

    def test_dict(self):
        assert convert_value({"key": "val"}) == {"key": "val"}

    def test_zope_datetime(self):
        """Zope DateTime objects use ISO8601() method."""

        class FakeDateTime:
            def ISO8601(self):
                return "2025-01-15T10:30:00+00:00"

        result = convert_value(FakeDateTime())
        assert result == "2025-01-15T10:30:00+00:00"

    def test_unknown_type_falls_back_to_str(self):
        class Custom:
            def __str__(self):
                return "custom-repr"

        assert convert_value(Custom()) == "custom-repr"

    def test_bool_not_treated_as_int(self):
        """bool is subclass of int — must return bool, not int."""
        assert convert_value(True) is True
        assert not isinstance(convert_value(True), int) or isinstance(
            convert_value(True), bool
        )


class TestComputePathInfo:
    """Verify path decomposition."""

    def test_root_level(self):
        parent, depth = compute_path_info("/plone")
        assert parent == "/"
        assert depth == 1

    def test_two_levels(self):
        parent, depth = compute_path_info("/plone/folder")
        assert parent == "/plone"
        assert depth == 2

    def test_three_levels(self):
        parent, depth = compute_path_info("/plone/folder/doc")
        assert parent == "/plone/folder"
        assert depth == 3

    def test_deep_path(self):
        parent, depth = compute_path_info("/plone/a/b/c/d")
        assert parent == "/plone/a/b/c"
        assert depth == 5

    def test_root_path(self):
        parent, depth = compute_path_info("/")
        assert parent == "/"
        assert depth == 0

    def test_trailing_slash_ignored(self):
        """Trailing slashes produce empty components, which are filtered."""
        parent, depth = compute_path_info("/plone/folder/")
        assert parent == "/plone"
        assert depth == 2
