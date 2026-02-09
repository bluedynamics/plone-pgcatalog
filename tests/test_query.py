"""Unit tests for plone.pgcatalog.query — query translation (no PG needed)."""

from datetime import datetime
from datetime import UTC
from plone.pgcatalog.query import build_query
from psycopg.types.json import Json


# ---------------------------------------------------------------------------
# FieldIndex
# ---------------------------------------------------------------------------


class TestFieldIndex:

    def test_exact_match(self):
        qr = build_query({"portal_type": "Document"})
        assert "idx @>" in qr["where"]
        assert "::jsonb" in qr["where"]
        # Param should be a Json wrapper with the containment dict
        param = _find_json_param(qr["params"])
        assert param.obj == {"portal_type": "Document"}

    def test_exact_match_explicit_query(self):
        qr = build_query({"portal_type": {"query": "Document"}})
        param = _find_json_param(qr["params"])
        assert param.obj == {"portal_type": "Document"}

    def test_multi_value(self):
        qr = build_query({"portal_type": {"query": ["Document", "News Item"]}})
        assert "= ANY(" in qr["where"]
        vals = _find_list_param(qr["params"])
        assert set(vals) == {"Document", "News Item"}

    def test_range_min(self):
        qr = build_query({"sortable_title": {"query": "b", "range": "min"}})
        assert "idx->>'sortable_title' >=" in qr["where"]

    def test_range_max(self):
        qr = build_query({"sortable_title": {"query": "m", "range": "max"}})
        assert "idx->>'sortable_title' <=" in qr["where"]

    def test_range_min_max(self):
        qr = build_query(
            {"sortable_title": {"query": ["b", "m"], "range": "min:max"}}
        )
        assert "idx->>'sortable_title' >=" in qr["where"]
        assert "idx->>'sortable_title' <=" in qr["where"]

    def test_not_single(self):
        qr = build_query({"portal_type": {"not": "Document"}})
        assert "idx->>'portal_type' !=" in qr["where"]

    def test_not_list(self):
        qr = build_query({"portal_type": {"not": ["Document", "News Item"]}})
        assert "NOT (idx->>'portal_type' = ANY(" in qr["where"]

    def test_query_and_not(self):
        qr = build_query(
            {"portal_type": {"query": "Document", "not": ["News Item"]}}
        )
        assert "idx @>" in qr["where"]
        assert "NOT (idx->>'portal_type' = ANY(" in qr["where"]


# ---------------------------------------------------------------------------
# KeywordIndex
# ---------------------------------------------------------------------------


class TestKeywordIndex:

    def test_single_value(self):
        qr = build_query({"Subject": "Python"})
        # Single value gets wrapped in list → overlap
        assert "?|" in qr["where"]

    def test_or_operator(self):
        qr = build_query(
            {"Subject": {"query": ["Python", "Zope"], "operator": "or"}}
        )
        assert "?|" in qr["where"]
        vals = _find_list_param(qr["params"])
        assert set(vals) == {"Python", "Zope"}

    def test_and_operator(self):
        qr = build_query(
            {"Subject": {"query": ["Python", "Zope"], "operator": "and"}}
        )
        assert "idx @>" in qr["where"]
        param = _find_json_param(qr["params"])
        assert param.obj == {"Subject": ["Python", "Zope"]}

    def test_default_operator_is_or(self):
        qr = build_query({"Subject": {"query": ["Python", "Zope"]}})
        assert "?|" in qr["where"]


# ---------------------------------------------------------------------------
# BooleanIndex
# ---------------------------------------------------------------------------


class TestBooleanIndex:

    def test_true(self):
        qr = build_query({"is_folderish": True})
        param = _find_json_param(qr["params"])
        assert param.obj == {"is_folderish": True}

    def test_false(self):
        qr = build_query({"is_folderish": False})
        param = _find_json_param(qr["params"])
        assert param.obj == {"is_folderish": False}

    def test_truthy_coerced(self):
        qr = build_query({"is_folderish": 1})
        param = _find_json_param(qr["params"])
        assert param.obj == {"is_folderish": True}


# ---------------------------------------------------------------------------
# DateIndex
# ---------------------------------------------------------------------------


class TestDateIndex:

    def test_exact(self):
        dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        qr = build_query({"created": dt})
        assert "pgcatalog_to_timestamptz(idx->>'created') =" in qr["where"]

    def test_range_min(self):
        dt = datetime(2025, 1, 1, tzinfo=UTC)
        qr = build_query({"modified": {"query": dt, "range": "min"}})
        assert "pgcatalog_to_timestamptz(idx->>'modified') >=" in qr["where"]

    def test_range_max(self):
        dt = datetime(2025, 12, 31, tzinfo=UTC)
        qr = build_query({"modified": {"query": dt, "range": "max"}})
        assert "pgcatalog_to_timestamptz(idx->>'modified') <=" in qr["where"]

    def test_range_min_max(self):
        dt_min = datetime(2025, 1, 1, tzinfo=UTC)
        dt_max = datetime(2025, 12, 31, tzinfo=UTC)
        qr = build_query(
            {"modified": {"query": [dt_min, dt_max], "range": "min:max"}}
        )
        where = qr["where"]
        assert "pgcatalog_to_timestamptz(idx->>'modified') >=" in where
        assert "pgcatalog_to_timestamptz(idx->>'modified') <=" in where

    def test_zope_datetime(self):
        """Zope DateTime objects (duck-typed via asdatetime)."""

        class FakeDateTime:
            def asdatetime(self):
                return datetime(2025, 6, 15, tzinfo=UTC)

        qr = build_query({"created": FakeDateTime()})
        assert "pgcatalog_to_timestamptz" in qr["where"]
        val = next(iter(qr["params"].values()))
        assert val == datetime(2025, 6, 15, tzinfo=UTC)


# ---------------------------------------------------------------------------
# DateRangeIndex (effectiveRange)
# ---------------------------------------------------------------------------


class TestDateRangeIndex:

    def test_effective_range(self):
        now = datetime(2025, 6, 15, tzinfo=UTC)
        qr = build_query({"effectiveRange": now})
        where = qr["where"]
        assert "idx->>'effective'" in where
        assert "idx->>'expires'" in where
        assert "IS NULL" in where

    def test_effective_range_sql_structure(self):
        now = datetime(2025, 6, 15, tzinfo=UTC)
        qr = build_query({"effectiveRange": now})
        # Should have: effective <= now AND (expires >= now OR expires IS NULL)
        where = qr["where"]
        assert "<=" in where  # effective <= now
        assert ">=" in where  # expires >= now
        assert "OR idx->>'expires' IS NULL" in where


# ---------------------------------------------------------------------------
# UUIDIndex
# ---------------------------------------------------------------------------


class TestUUIDIndex:

    def test_exact_match(self):
        qr = build_query({"UID": "abc123def456"})
        param = _find_json_param(qr["params"])
        assert param.obj == {"UID": "abc123def456"}


# ---------------------------------------------------------------------------
# SearchableText (ZCTextIndex)
# ---------------------------------------------------------------------------


class TestSearchableText:

    def test_full_text_search(self):
        qr = build_query({"SearchableText": "quick fox"})
        assert "searchable_text @@ plainto_tsquery" in qr["where"]
        assert "::regconfig" in qr["where"]
        # Should use 'simple' language by default
        assert "simple" in list(qr["params"].values())

    def test_text_field_with_idx_key(self):
        """Title/Description with idx_key → containment match."""
        qr = build_query({"Title": "Hello"})
        param = _find_json_param(qr["params"])
        assert param.obj == {"Title": "Hello"}


# ---------------------------------------------------------------------------
# PathIndex
# ---------------------------------------------------------------------------


class TestPathIndex:

    def test_subtree_default(self):
        qr = build_query({"path": "/plone/folder"})
        assert "path =" in qr["where"]
        assert "path LIKE" in qr["where"]
        # LIKE pattern should end with /%
        like_val = [v for v in qr["params"].values() if isinstance(v, str) and v.endswith("/%")]
        assert like_val

    def test_exact_depth_0(self):
        qr = build_query({"path": {"query": "/plone/folder", "depth": 0}})
        assert "path =" in qr["where"]
        assert "LIKE" not in qr["where"]

    def test_children_depth_1(self):
        qr = build_query({"path": {"query": "/plone/folder", "depth": 1}})
        assert "parent_path =" in qr["where"]

    def test_limited_depth(self):
        qr = build_query({"path": {"query": "/plone/folder", "depth": 2}})
        assert "path LIKE" in qr["where"]
        assert "path_depth <=" in qr["where"]

    def test_navtree_depth_1(self):
        qr = build_query(
            {"path": {"query": "/plone/folder/doc", "navtree": True, "depth": 1}}
        )
        assert "parent_path = ANY(" in qr["where"]

    def test_navtree_depth_0_breadcrumbs(self):
        qr = build_query(
            {"path": {"query": "/plone/folder/doc", "navtree": True, "depth": 0}}
        )
        assert "path = ANY(" in qr["where"]

    def test_navtree_start(self):
        qr = build_query(
            {
                "path": {
                    "query": "/plone/folder/doc",
                    "navtree": True,
                    "depth": 1,
                    "navtree_start": 1,
                }
            }
        )
        # With navtree_start=1, should skip root level
        parents = _find_list_param(qr["params"])
        assert "/" not in parents

    def test_multiple_paths_subtree(self):
        qr = build_query(
            {"path": {"query": ["/plone/folder1", "/plone/folder2"]}}
        )
        assert "OR" in qr["where"]

    def test_invalid_path_rejected(self):
        import pytest

        with pytest.raises(ValueError, match="Invalid path"):
            build_query({"path": "no-leading-slash"})

    def test_sql_injection_in_path_rejected(self):
        import pytest

        with pytest.raises(ValueError, match="Invalid path"):
            build_query({"path": "/plone'; DROP TABLE object_state;--"})


# ---------------------------------------------------------------------------
# Sort
# ---------------------------------------------------------------------------


class TestSort:

    def test_sort_ascending(self):
        qr = build_query({"portal_type": "Document", "sort_on": "sortable_title"})
        assert qr["order_by"] == "idx->>'sortable_title' ASC"

    def test_sort_descending(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": "modified",
                "sort_order": "descending",
            }
        )
        assert "DESC" in qr["order_by"]
        assert "pgcatalog_to_timestamptz" in qr["order_by"]

    def test_sort_reverse(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": "modified",
                "sort_order": "reverse",
            }
        )
        assert "DESC" in qr["order_by"]

    def test_sort_with_limit(self):
        qr = build_query(
            {"portal_type": "Document", "sort_on": "modified", "sort_limit": 50}
        )
        assert qr["limit"] == 50

    def test_sort_gopip_integer_cast(self):
        qr = build_query(
            {"portal_type": "Document", "sort_on": "getObjPositionInParent"}
        )
        assert "::integer" in qr["order_by"]

    def test_unknown_sort_ignored(self):
        qr = build_query({"portal_type": "Document", "sort_on": "nonexistent"})
        assert qr["order_by"] is None


# ---------------------------------------------------------------------------
# Batch / Pagination
# ---------------------------------------------------------------------------


class TestBatch:

    def test_b_start(self):
        qr = build_query({"portal_type": "Document", "b_start": 10})
        assert qr["offset"] == 10

    def test_b_size(self):
        qr = build_query({"portal_type": "Document", "b_size": 30})
        assert qr["limit"] == 30

    def test_b_start_and_b_size(self):
        qr = build_query({"portal_type": "Document", "b_start": 20, "b_size": 10})
        assert qr["offset"] == 20
        assert qr["limit"] == 10

    def test_sort_limit_overrides_b_size(self):
        qr = build_query(
            {"portal_type": "Document", "sort_limit": 50, "b_size": 30}
        )
        assert qr["limit"] == 50


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:

    def test_empty_query(self):
        qr = build_query({})
        assert "idx IS NOT NULL" in qr["where"]
        assert qr["order_by"] is None
        assert qr["limit"] is None
        assert qr["offset"] == 0

    def test_unknown_index_skipped(self):
        qr = build_query({"nonexistent_index": "value"})
        # Should only have the base clause
        assert qr["where"] == "idx IS NOT NULL"

    def test_combined_query(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "review_state": "published",
                "is_folderish": False,
            }
        )
        assert qr["where"].count("AND") >= 3  # base + 3 conditions

    def test_all_params_parameterized(self):
        """No user values should appear directly in the WHERE clause."""
        qr = build_query(
            {
                "portal_type": "Document",
                "Subject": {"query": ["Python"], "operator": "or"},
                "SearchableText": "hello world",
            }
        )
        # "Document", "Python", "hello world" should NOT be in the SQL string
        assert "Document" not in qr["where"]
        assert "Python" not in qr["where"]
        assert "hello world" not in qr["where"]


# ---------------------------------------------------------------------------
# None query values (early return branches)
# ---------------------------------------------------------------------------


class TestNoneQueryValues:

    def test_keyword_none_query(self):
        qr = build_query({"Subject": {"query": None}})
        assert "?|" not in qr["where"]

    def test_date_none_query(self):
        qr = build_query({"modified": {"query": None}})
        assert "pgcatalog_to_timestamptz" not in qr["where"]

    def test_boolean_none_query(self):
        qr = build_query({"is_folderish": {"query": None}})
        assert "is_folderish" not in qr["where"]

    def test_date_range_none_query(self):
        qr = build_query({"effectiveRange": {"query": None}})
        assert "effective" not in qr["where"]

    def test_uuid_none_query(self):
        qr = build_query({"UID": {"query": None}})
        assert "UID" not in qr["where"]

    def test_path_none_query(self):
        qr = build_query({"path": {"query": None}})
        assert qr["where"] == "idx IS NOT NULL"


class TestSortEdgeCases:

    def test_sort_boolean_index(self):
        qr = build_query({
            "portal_type": "Document",
            "sort_on": "is_folderish",
        })
        assert "::boolean" in qr["order_by"]

    def test_sort_composite_index_ignored(self):
        """effectiveRange has idx_key=None — can't sort on it."""
        qr = build_query({
            "portal_type": "Document",
            "sort_on": "effectiveRange",
        })
        assert qr["order_by"] is None


class TestDateCoercion:

    def test_date_object_coerced_to_datetime(self):
        from datetime import date
        qr = build_query({"modified": {"query": date(2025, 6, 15), "range": "min"}})
        assert len(qr["params"]) >= 1

    def test_iso8601_duck_type(self):
        class FakeDateTime:
            def ISO8601(self):
                return "2025-06-15T00:00:00+00:00"
        qr = build_query({"modified": {"query": FakeDateTime(), "range": "min"}})
        param_vals = list(qr["params"].values())
        assert "2025-06-15" in str(param_vals)

    def test_string_date_passthrough(self):
        qr = build_query({"modified": {"query": "2025-06-15", "range": "min"}})
        param_vals = list(qr["params"].values())
        assert "2025-06-15" in str(param_vals)


class TestPathValidation:

    def test_invalid_path_type_raises(self):
        from plone.pgcatalog.query import _validate_path

        import pytest
        with pytest.raises(ValueError, match="must be a string"):
            _validate_path(123)


class TestNavtreeEdgeCases:

    def test_navtree_breadcrumbs_empty(self):
        """navtree_start beyond path length produces FALSE clause."""
        qr = build_query({
            "path": {"query": "/a", "navtree": True, "depth": 0, "navtree_start": 10},
        })
        assert "FALSE" in qr["where"]

    def test_navtree_parents_empty(self):
        """navtree_start beyond path length with depth=1 produces FALSE clause."""
        qr = build_query({
            "path": {"query": "/a", "navtree": True, "depth": 1, "navtree_start": 10},
        })
        assert "FALSE" in qr["where"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_json_param(params):
    """Find the first Json parameter value."""
    for v in params.values():
        if isinstance(v, Json):
            return v
    return None


def _find_list_param(params):
    """Find the first list parameter value."""
    for v in params.values():
        if isinstance(v, list):
            return v
    return None
