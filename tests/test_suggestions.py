"""Tests for suggest_indexes() — pure unit tests, no PG needed."""

from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.suggestions import suggest_indexes

import pytest


class FakeRegistry:
    """Minimal IndexRegistry stand-in for unit tests."""

    def __init__(self, indexes=None):
        self._indexes = indexes or {}

    def items(self):
        return self._indexes.items()


def _reg(**kwargs):
    """Build a FakeRegistry from name=IndexType pairs."""
    indexes = {}
    for name, idx_type in kwargs.items():
        indexes[name] = (idx_type, name, [name])
    return FakeRegistry(indexes)


class TestSuggestIndexes:
    """Test the pure suggestion engine."""

    def test_single_field_returns_single_btree(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type"], registry, {})
        assert len(result) == 1
        assert result[0]["status"] == "new"
        assert "(idx->>'portal_type')" in result[0]["ddl"]
        assert "idx_os_sug_" in result[0]["ddl"]

    def test_two_fields_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Creator=IndexType.FIELD,
        )
        result = suggest_indexes(["portal_type", "Creator"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert "portal_type" in new[0]["ddl"]
        assert "Creator" in new[0]["ddl"]

    def test_max_three_fields_in_composite(self):
        registry = _reg(
            a=IndexType.FIELD,
            b=IndexType.FIELD,
            c=IndexType.FIELD,
            d=IndexType.FIELD,
        )
        result = suggest_indexes(["a", "b", "c", "d"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        # Composite should have max 3 fields
        for s in new:
            assert len(s["fields"]) <= 3

    def test_keyword_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Subject=IndexType.KEYWORD,
        )
        result = suggest_indexes(["portal_type", "Subject"], registry, {})
        # KEYWORD gets its own suggestion, not mixed into composite
        for s in result:
            if "Subject" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("KEYWORD should not be in a composite")

    def test_text_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Title=IndexType.TEXT,
        )
        result = suggest_indexes(["portal_type", "Title"], registry, {})
        for s in result:
            if "Title" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("TEXT should not be in a composite")

    def test_date_uses_timestamptz(self):
        registry = _reg(modified=IndexType.DATE)
        result = suggest_indexes(["modified"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("pgcatalog_to_timestamptz" in s["ddl"] for s in new)

    def test_boolean_uses_cast(self):
        registry = _reg(is_folderish=IndexType.BOOLEAN)
        result = suggest_indexes(["is_folderish"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("::boolean" in s["ddl"] for s in new)

    def test_uuid_uses_text_expression(self):
        registry = _reg(UID=IndexType.UUID)
        result = suggest_indexes(["UID"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("(idx->>'UID')" in s["ddl"] for s in new)

    def test_path_uses_text_pattern_ops(self):
        registry = _reg(tgpath=IndexType.PATH)
        result = suggest_indexes(["tgpath"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("text_pattern_ops" in s["ddl"] for s in new)

    def test_keyword_gets_own_gin(self):
        registry = _reg(Subject=IndexType.KEYWORD)
        result = suggest_indexes(["Subject"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("GIN" in s["ddl"].upper() or "gin" in s["ddl"] for s in new)

    def test_non_idx_fields_filtered(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type", "sort_on", "b_size"], registry, {})
        for s in result:
            assert "sort_on" not in s["fields"]
            assert "b_size" not in s["fields"]

    def test_unknown_field_skipped(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type", "unknown_field"], registry, {})
        for s in result:
            assert "unknown_field" not in s["fields"]

    def test_already_covered_by_existing_index(self):
        registry = _reg(portal_type=IndexType.FIELD)
        existing = {
            "idx_os_cat_portal_type": (
                "CREATE INDEX idx_os_cat_portal_type ON object_state "
                "((idx->>'portal_type')) WHERE idx IS NOT NULL"
            )
        }
        result = suggest_indexes(["portal_type"], registry, existing)
        assert all(s["status"] == "already_covered" for s in result)

    def test_dedicated_column_already_covered(self):
        registry = _reg(
            allowedRolesAndUsers=IndexType.KEYWORD,
        )
        result = suggest_indexes(["allowedRolesAndUsers"], registry, {})
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1
        assert "dedicated column" in covered[0]["reason"].lower()

    def test_empty_keys_returns_empty(self):
        registry = _reg()
        result = suggest_indexes([], registry, {})
        assert result == []

    def test_all_filtered_keys_returns_empty(self):
        registry = _reg()
        result = suggest_indexes(["sort_on", "b_size"], registry, {})
        assert result == []

    def test_selectivity_ordering(self):
        """UUID fields should come first in composites (most selective)."""
        registry = _reg(
            review_state=IndexType.FIELD,
            UID=IndexType.UUID,
        )
        result = suggest_indexes(["review_state", "UID"], registry, {})
        composites = [s for s in result if len(s["fields"]) > 1]
        if composites:
            assert composites[0]["fields"][0] == "UID"

    def test_naming_convention(self):
        """Generated index names use idx_os_sug_ prefix."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        for s in new:
            assert "idx_os_sug_" in s["ddl"]

    def test_date_range_excluded(self):
        """DATE_RANGE (effectiveRange) should be filtered by _NON_IDX_FIELDS."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type", "effectiveRange"], registry, {})
        for s in result:
            assert "effectiveRange" not in s["fields"]

    def test_gopip_skipped(self):
        """GopipIndex fields are skipped (no meaningful PG index type)."""
        registry = _reg(getObjPositionInParent=IndexType.GOPIP)
        result = suggest_indexes(["getObjPositionInParent"], registry, {})
        assert result == []
