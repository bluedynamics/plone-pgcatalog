"""Unit tests for plone.pgcatalog.query — query translation (no PG needed)."""

from datetime import datetime
from datetime import UTC
from plone.pgcatalog.query import _bool_to_lower_str
from plone.pgcatalog.query import build_query
from psycopg.types.json import Json
from unittest import mock


# ---------------------------------------------------------------------------
# FieldIndex
# ---------------------------------------------------------------------------


class TestFieldIndex:
    def test_exact_match(self):
        qr = build_query({"portal_type": "Document"})
        assert "idx->>'portal_type' =" in qr["where"]
        # Param should be a plain string (btree-friendly operator)
        param = _find_str_param(qr["params"])
        assert param == "Document"

    def test_exact_match_explicit_query(self):
        qr = build_query({"portal_type": {"query": "Document"}})
        param = _find_str_param(qr["params"])
        assert param == "Document"

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
        qr = build_query({"sortable_title": {"query": ["b", "m"], "range": "min:max"}})
        assert "idx->>'sortable_title' >=" in qr["where"]
        assert "idx->>'sortable_title' <=" in qr["where"]

    def test_not_single(self):
        qr = build_query({"portal_type": {"not": "Document"}})
        assert "idx->>'portal_type' !=" in qr["where"]

    def test_not_list(self):
        qr = build_query({"portal_type": {"not": ["Document", "News Item"]}})
        assert "NOT (idx->>'portal_type' = ANY(" in qr["where"]

    def test_query_and_not(self):
        qr = build_query({"portal_type": {"query": "Document", "not": ["News Item"]}})
        assert "idx->>'portal_type' =" in qr["where"]
        assert "NOT (idx->>'portal_type' = ANY(" in qr["where"]


# ---------------------------------------------------------------------------
# KeywordIndex
# ---------------------------------------------------------------------------


class TestKeywordIndex:
    def test_single_value(self):
        qr = build_query({"Subject": "Python"})
        # Single value uses @> containment (GIN-friendly, #80)
        assert "idx @>" in qr["where"]

    def test_or_operator(self):
        qr = build_query({"Subject": {"query": ["Python", "Zope"], "operator": "or"}})
        assert "?|" in qr["where"]
        vals = _find_list_param(qr["params"])
        assert set(vals) == {"Python", "Zope"}

    def test_and_operator(self):
        qr = build_query({"Subject": {"query": ["Python", "Zope"], "operator": "and"}})
        assert "idx @>" in qr["where"]
        param = _find_json_param(qr["params"])
        assert param.obj == {"Subject": ["Python", "Zope"]}

    def test_default_operator_is_or(self):
        qr = build_query({"Subject": {"query": ["Python", "Zope"]}})
        assert "?|" in qr["where"]

    def test_non_iterable_non_str_coerced_to_single_value(self):
        """Regression for #152: a caller accidentally passing a non-string,
        non-iterable value (e.g. a Zope ``DateTime``, a number, etc.) must
        not crash the query builder with ``TypeError: object is not
        iterable``.  Observed on aaf-6 prod where an addon passed a
        ``DateTime`` through a keyword criterion by mistake.
        """

        class _NotIterable:
            """Mimics Zope ``DateTime`` — has ``__str__`` but no
            ``__iter__`` and isn't a ``str``."""

            def __str__(self):
                return "2026-04-20"

        qr = build_query({"Subject": _NotIterable()})
        # Falls into the single-value branch — containment (`@>`).
        assert "idx @>" in qr["where"]
        # Value was coerced to its str form.
        param = _find_json_param(qr["params"])
        assert param.obj == {"Subject": ["2026-04-20"]}

    def test_zope_datetime_as_keyword_does_not_crash(self):
        """The specific shape that surfaced in production: a Zope
        ``DateTime`` passed as a keyword-index query value.  It should
        be coerced via ``str()`` into a single-element list, not crash.
        """
        from datetime import datetime
        from datetime import UTC

        # Python ``datetime`` is also not iterable — exactly the same
        # bug class.
        dt = datetime(2026, 4, 20, tzinfo=UTC)
        qr = build_query({"Subject": dt})
        assert "idx @>" in qr["where"]
        param = _find_json_param(qr["params"])
        assert len(param.obj["Subject"]) == 1


# ---------------------------------------------------------------------------
# BooleanIndex
# ---------------------------------------------------------------------------


class TestBooleanIndex:
    def test_true(self):
        qr = build_query({"is_folderish": True})
        assert "(idx->>'is_folderish')::boolean =" in qr["where"]
        param = _find_bool_param(qr["params"])
        assert param is True

    def test_false(self):
        qr = build_query({"is_folderish": False})
        assert "(idx->>'is_folderish')::boolean =" in qr["where"]
        param = _find_bool_param(qr["params"])
        assert param is False

    def test_truthy_coerced(self):
        qr = build_query({"is_folderish": 1})
        assert "(idx->>'is_folderish')::boolean =" in qr["where"]
        param = _find_bool_param(qr["params"])
        assert param is True


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
        qr = build_query({"modified": {"query": [dt_min, dt_max], "range": "min:max"}})
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
        assert "idx->>'UID' =" in qr["where"]
        param = _find_str_param(qr["params"])
        assert param == "abc123def456"

    def test_list_uses_any(self):
        """UID=list should use ``= ANY`` so plone.app.querystring
        ``list.contains`` operator matches any element in the list.
        Regression test for empty @@getVocabulary results.
        """
        qr = build_query({"UID": ["uid-a", "uid-b", "uid-c"]})
        assert "idx->>'UID' = ANY(" in qr["where"]
        vals = _find_list_param(qr["params"])
        assert vals == ["uid-a", "uid-b", "uid-c"]

    def test_tuple_uses_any(self):
        qr = build_query({"UID": ("uid-a", "uid-b")})
        assert "idx->>'UID' = ANY(" in qr["where"]
        vals = _find_list_param(qr["params"])
        assert vals == ["uid-a", "uid-b"]

    def test_single_element_list(self):
        """Single-element list still uses ANY (consistent with _handle_field)."""
        qr = build_query({"UID": ["only-one"]})
        assert "idx->>'UID' = ANY(" in qr["where"]
        vals = _find_list_param(qr["params"])
        assert vals == ["only-one"]


# ---------------------------------------------------------------------------
# SearchableText (ZCTextIndex)
# ---------------------------------------------------------------------------


class TestSearchableText:
    def test_full_text_search(self):
        qr = build_query({"SearchableText": "quick fox"})
        assert "searchable_text @@ plainto_tsquery" in qr["where"]
        assert "pgcatalog_lang_to_regconfig" in qr["where"]
        assert "::regconfig" in qr["where"]

    def test_searchable_text_without_language(self):
        """Without Language in query, empty string → 'simple' via SQL function."""
        qr = build_query({"SearchableText": "hello"})
        # Language param should be empty string (function maps to 'simple')
        lang_params = [v for v in qr["params"].values() if v == ""]
        assert lang_params

    def test_searchable_text_with_language_filter(self):
        """When Language is in query dict, it's passed to the SQL function."""
        qr = build_query({"SearchableText": "hallo", "Language": "de"})
        lang_params = [v for v in qr["params"].values() if v == "de"]
        assert lang_params

    def test_searchable_text_with_language_dict(self):
        """Language as a dict query spec."""
        qr = build_query({"SearchableText": "bonjour", "Language": {"query": "fr"}})
        lang_params = [v for v in qr["params"].values() if v == "fr"]
        assert lang_params

    def test_text_field_with_idx_key(self):
        """Title/Description with idx_key → tsvector expression match."""
        qr = build_query({"Title": "Hello"})
        assert "to_tsvector('simple'::regconfig" in qr["where"]
        assert "plainto_tsquery('simple'::regconfig" in qr["where"]
        assert "idx->>'Title'" in qr["where"]
        # Should NOT use JSONB containment for text indexes
        assert "idx @>" not in qr["where"]

    def test_description_text_search(self):
        """Description index also uses tsvector expression match."""
        qr = build_query({"Description": "overview"})
        assert "to_tsvector('simple'::regconfig" in qr["where"]
        assert "idx->>'Description'" in qr["where"]

    def test_addon_text_index_uses_tsvector(self):
        """A dynamically registered TEXT index uses tsvector, not containment."""
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        registry = get_registry()
        registry.register("my_text_field", IndexType.TEXT, "my_text_field")
        try:
            qr = build_query({"my_text_field": "search term"})
            assert "to_tsvector('simple'::regconfig" in qr["where"]
            assert "idx->>'my_text_field'" in qr["where"]
            assert "plainto_tsquery" in qr["where"]
        finally:
            registry._indexes.pop("my_text_field", None)

    def test_auto_relevance_order_by(self):
        """SearchableText query should auto-add ts_rank_cd ORDER BY."""
        qr = build_query({"SearchableText": "hello"})
        assert qr["order_by"] is not None
        assert "ts_rank_cd" in qr["order_by"]
        assert "DESC" in qr["order_by"]

    def test_sort_on_overrides_relevance(self):
        """Explicit sort_on should override auto-relevance ranking."""
        qr = build_query(
            {
                "SearchableText": "hello",
                "sort_on": "modified",
                "sort_order": "descending",
            }
        )
        assert "ts_rank_cd" not in qr["order_by"]
        assert "modified" in qr["order_by"]

    def test_no_relevance_without_searchable_text(self):
        """Without SearchableText, no auto-relevance ORDER BY."""
        qr = build_query({"portal_type": "Document"})
        assert qr["order_by"] is None


# ---------------------------------------------------------------------------
# PathIndex
# ---------------------------------------------------------------------------


class TestPathIndex:
    def test_subtree_default(self):
        qr = build_query({"path": "/plone/folder"})
        # Built-in "path" dispatches to typed columns (#132)
        assert "path =" in qr["where"]
        assert "path LIKE" in qr["where"]
        assert "idx->>'path'" not in qr["where"]
        # LIKE pattern should end with /%
        like_val = [
            v for v in qr["params"].values() if isinstance(v, str) and v.endswith("/%")
        ]
        assert like_val

    def test_exact_depth_0(self):
        qr = build_query({"path": {"query": "/plone/folder", "depth": 0}})
        assert "path = " in qr["where"]
        assert "idx->>'path'" not in qr["where"]
        assert "LIKE" not in qr["where"]

    def test_children_depth_1(self):
        qr = build_query({"path": {"query": "/plone/folder", "depth": 1}})
        assert "parent_path =" in qr["where"]
        assert "idx->>'path_parent'" not in qr["where"]

    def test_limited_depth(self):
        qr = build_query({"path": {"query": "/plone/folder", "depth": 2}})
        assert "path LIKE" in qr["where"]
        assert "path_depth <=" in qr["where"]
        assert "idx->>'path'" not in qr["where"]
        assert "idx->>'path_depth'" not in qr["where"]

    def test_navtree_depth_1(self):
        qr = build_query(
            {"path": {"query": "/plone/folder/doc", "navtree": True, "depth": 1}}
        )
        assert "parent_path = ANY(" in qr["where"]
        assert "idx->>'path_parent'" not in qr["where"]

    def test_navtree_depth_0_breadcrumbs(self):
        qr = build_query(
            {"path": {"query": "/plone/folder/doc", "navtree": True, "depth": 0}}
        )
        assert "path = ANY(" in qr["where"]
        assert "idx->>'path'" not in qr["where"]

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
        qr = build_query({"path": {"query": ["/plone/folder1", "/plone/folder2"]}})
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
# Multi-column sort (sort_on as list)
# ---------------------------------------------------------------------------


class TestMultiSort:
    def test_sort_on_list_two_fields(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": ["portal_type", "sortable_title"],
            }
        )
        assert qr["order_by"] == "idx->>'portal_type' ASC, idx->>'sortable_title' ASC"

    def test_sort_on_list_with_mixed_orders(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": ["sortable_title", "modified"],
                "sort_order": ["ascending", "descending"],
            }
        )
        assert qr["order_by"] is not None
        parts = qr["order_by"].split(", ")
        assert len(parts) == 2
        assert parts[0] == "idx->>'sortable_title' ASC"
        assert "pgcatalog_to_timestamptz" in parts[1]
        assert "DESC" in parts[1]

    def test_sort_on_list_single_order_applies_to_all(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": ["portal_type", "sortable_title"],
                "sort_order": "descending",
            }
        )
        assert qr["order_by"] == "idx->>'portal_type' DESC, idx->>'sortable_title' DESC"

    def test_sort_on_list_with_unknown_index_skipped(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": ["nonexistent", "sortable_title"],
            }
        )
        assert qr["order_by"] == "idx->>'sortable_title' ASC"

    def test_sort_on_list_all_unknown(self):
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": ["nonexistent", "also_nonexistent"],
            }
        )
        assert qr["order_by"] is None

    def test_sort_on_single_string_still_works(self):
        """Backward compat: single string sort_on unchanged."""
        qr = build_query({"portal_type": "Document", "sort_on": "sortable_title"})
        assert qr["order_by"] == "idx->>'sortable_title' ASC"


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
        qr = build_query({"portal_type": "Document", "sort_limit": 50, "b_size": 30})
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

    def test_unknown_index_falls_back_to_jsonb_field(self):
        qr = build_query({"nonexistent_index": "value"})
        # Unregistered indexes fall back to JSONB field queries
        assert "nonexistent_index" in qr["where"]
        assert len(qr["params"]) > 0

    def test_unregistered_boolean_field_uses_json_format(self):
        """Boolean values in unregistered fields should use JSON format ('true'/'false')."""
        # Test True value
        qr = build_query({"show_in_sidecalendar": True})
        assert "show_in_sidecalendar" in qr["where"]
        param_val = _find_str_param(qr["params"])
        assert param_val == "true"  # JSON format, not Python "True"

        # Test False value
        qr = build_query({"show_in_sidecalendar": False})
        param_val = _find_str_param(qr["params"])
        assert param_val == "false"  # JSON format, not Python "False"

        # Test boolean in list
        qr = build_query({"some_boolean_field": [True, False]})
        param_vals = _find_list_param(qr["params"])
        assert set(param_vals) == {"true", "false"}

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
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": "is_folderish",
            }
        )
        assert "::boolean" in qr["order_by"]

    def test_sort_composite_index_ignored(self):
        """effectiveRange has idx_key=None — can't sort on it."""
        qr = build_query(
            {
                "portal_type": "Document",
                "sort_on": "effectiveRange",
            }
        )
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


# ---------------------------------------------------------------------------
# Additional PathIndex (tgpath — idx JSONB)
# ---------------------------------------------------------------------------


class TestAdditionalPathIndex:
    """PATH-type indexes with idx_key != None query against idx JSONB keys."""

    def test_tgpath_subtree(self):
        qr = build_query({"tgpath": "/uuid1/uuid2"})
        assert "idx->>'tgpath' =" in qr["where"]
        assert "idx->>'tgpath' LIKE" in qr["where"]
        like_val = [
            v for v in qr["params"].values() if isinstance(v, str) and v.endswith("/%")
        ]
        assert like_val

    def test_tgpath_exact(self):
        qr = build_query({"tgpath": {"query": "/uuid1/uuid2", "depth": 0}})
        assert "idx->>'tgpath' =" in qr["where"]
        assert "LIKE" not in qr["where"]

    def test_tgpath_children(self):
        qr = build_query({"tgpath": {"query": "/uuid1/uuid2", "depth": 1}})
        assert "idx->>'tgpath_parent' =" in qr["where"]

    def test_tgpath_limited_depth(self):
        qr = build_query({"tgpath": {"query": "/uuid1/uuid2", "depth": 2}})
        assert "idx->>'tgpath' LIKE" in qr["where"]
        assert "(idx->>'tgpath_depth')::integer <=" in qr["where"]

    def test_tgpath_navtree(self):
        qr = build_query(
            {"tgpath": {"query": "/uuid1/uuid2/uuid3", "navtree": True, "depth": 1}}
        )
        assert "idx->>'tgpath_parent' = ANY(" in qr["where"]

    def test_tgpath_breadcrumbs(self):
        qr = build_query(
            {"tgpath": {"query": "/uuid1/uuid2/uuid3", "navtree": True, "depth": 0}}
        )
        assert "idx->>'tgpath' = ANY(" in qr["where"]

    def test_tgpath_multiple_paths_subtree(self):
        qr = build_query({"tgpath": {"query": ["/uuid1/uuid2", "/uuid3/uuid4"]}})
        assert "OR" in qr["where"]
        assert "idx->>'tgpath'" in qr["where"]

    def test_tgpath_sort(self):
        qr = build_query({"tgpath": "/uuid1", "sort_on": "tgpath"})
        assert qr["order_by"] == "idx->>'tgpath' ASC"

    def test_tgpath_sort_descending(self):
        qr = build_query(
            {"tgpath": "/uuid1", "sort_on": "tgpath", "sort_order": "descending"}
        )
        assert qr["order_by"] == "idx->>'tgpath' DESC"

    def test_builtin_path_uses_typed_columns(self):
        """Built-in 'path' index dispatches to typed columns, not idx JSONB (#132)."""
        qr = build_query({"path": "/plone/folder"})
        assert "path =" in qr["where"]
        assert "path LIKE" in qr["where"]
        assert "idx->>'path'" not in qr["where"]

    def test_builtin_path_sort(self):
        """Built-in 'path' sort uses the typed `path` column, not idx JSONB (#132)."""
        qr = build_query({"path": "/plone", "sort_on": "path"})
        assert qr["order_by"] == "path ASC"
        assert "idx->>'path'" not in qr["order_by"]

    def test_builtin_path_sort_descending(self):
        """Built-in 'path' sort honors descending order on typed column (#132)."""
        qr = build_query(
            {"path": "/plone", "sort_on": "path", "sort_order": "descending"}
        )
        assert qr["order_by"] == "path DESC"
        assert "idx->>'path'" not in qr["order_by"]

    def test_combined_path_and_tgpath(self):
        """Built-in path uses typed columns; tgpath (custom) still uses idx JSONB (#132)."""
        qr = build_query(
            {
                "path": "/plone/folder",
                "tgpath": {"query": "/uuid1/uuid2", "depth": 1},
            }
        )
        where = qr["where"]
        # Built-in path dispatches to typed columns
        assert "path = " in where or "path LIKE" in where
        assert "idx->>'path'" not in where
        # Custom tgpath still uses idx JSONB
        assert "idx->>'tgpath_parent' =" in where


class TestPathValidation:
    def test_invalid_path_type_raises(self):
        from plone.pgcatalog.query import _validate_path

        import pytest

        with pytest.raises(ValueError, match="must be a string"):
            _validate_path(123)

    def test_too_many_paths_raises(self):
        """Path queries with more than _MAX_PATHS paths are rejected."""
        from plone.pgcatalog.query import _MAX_PATHS

        import pytest

        paths = [f"/plone/folder{i}" for i in range(_MAX_PATHS + 1)]
        with pytest.raises(ValueError, match="Too many paths"):
            build_query({"path": {"query": paths}})

    def test_exactly_max_paths_accepted(self):
        from plone.pgcatalog.query import _MAX_PATHS

        paths = [f"/plone/folder{i}" for i in range(_MAX_PATHS)]
        qr = build_query({"path": {"query": paths}})
        assert "idx" in qr["where"]


class TestNavtreeEdgeCases:
    def test_navtree_breadcrumbs_empty(self):
        """navtree_start beyond path length produces FALSE clause."""
        qr = build_query(
            {
                "path": {
                    "query": "/a",
                    "navtree": True,
                    "depth": 0,
                    "navtree_start": 10,
                },
            }
        )
        assert "FALSE" in qr["where"]

    def test_navtree_parents_empty(self):
        """navtree_start beyond path length with depth=1 produces FALSE clause."""
        qr = build_query(
            {
                "path": {
                    "query": "/a",
                    "navtree": True,
                    "depth": 1,
                    "navtree_start": 10,
                },
            }
        )
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


def _find_str_param(params):
    """Find the first plain string parameter value."""
    for v in params.values():
        if isinstance(v, str):
            return v
    return None


def _find_bool_param(params):
    """Find the first bool parameter value."""
    for v in params.values():
        if isinstance(v, bool):
            return v
    return None


# ---------------------------------------------------------------------------
# Dynamic index tests — indexes registered at runtime via IndexRegistry
# ---------------------------------------------------------------------------


class TestDynamicFieldIndex:
    """FieldIndex dynamically registered via registry."""

    def test_dynamic_field_exact(self, populated_registry):
        """Query a dynamically registered FieldIndex."""
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("my_addon_field", IndexType.FIELD, "my_addon_field")

        qr = build_query({"my_addon_field": "some_value"})
        assert "idx->>'my_addon_field' =" in qr["where"]
        param = _find_str_param(qr["params"])
        assert param == "some_value"

    def test_dynamic_field_range(self, populated_registry):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("my_addon_field", IndexType.FIELD, "my_addon_field")

        qr = build_query({"my_addon_field": {"query": "b", "range": "min"}})
        assert "idx->>'my_addon_field' >=" in qr["where"]

    def test_dynamic_field_multi_value(self, populated_registry):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("my_addon_field", IndexType.FIELD, "my_addon_field")

        qr = build_query({"my_addon_field": {"query": ["a", "b"]}})
        assert "= ANY(" in qr["where"]


class TestFieldRangeNumeric:
    """Regression for #150 — FieldIndex range on numeric fields.

    Two stacked bugs the old ``_field_range`` had:

    1. No min/max normalization: caller-supplied ``[max, min]`` produced
       always-false SQL.  ``plone.app.querystring`` / ``collective.collectionfilter``
       pass values in URL order and do not sort.
    2. Lexicographic string comparison on ``idx->>'key'`` — wrong for
       numeric fields (``'46.1' <= '5.0' <= '49.0'`` returns true).

    Both shipped together — the map-widget bbox filter on aaf-6 silently
    returned zero rows.
    """

    def _register(self, name):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register(name, IndexType.FIELD, name)

    def test_numeric_field_range_casts_to_numeric(self, populated_registry):
        self._register("latitude")
        qr = build_query({"latitude": {"query": [46.1, 49.0], "range": "minmax"}})
        assert "(idx->>'latitude')::numeric" in qr["where"]
        # min param gets the lower value, max param the upper — regardless
        # of the caller-supplied order.
        params = qr["params"]
        min_key = next(k for k in params if k.endswith("_min_1"))
        max_key = next(k for k in params if k.endswith("_max_2"))
        assert params[min_key] == 46.1
        assert params[max_key] == 49.0

    def test_numeric_field_range_normalizes_reversed_order(self, populated_registry):
        self._register("latitude")
        # Caller sends [max, min] — collective.collectionfilter's map
        # widget does this on every pan/zoom.
        qr = build_query({"latitude": {"query": [49.0, 46.1], "range": "minmax"}})
        params = qr["params"]
        min_key = next(k for k in params if k.endswith("_min_1"))
        max_key = next(k for k in params if k.endswith("_max_2"))
        assert params[min_key] == 46.1
        assert params[max_key] == 49.0

    def test_numeric_field_range_int(self, populated_registry):
        self._register("priority")
        qr = build_query({"priority": {"query": [5, 1], "range": "minmax"}})
        assert "(idx->>'priority')::numeric" in qr["where"]
        params = qr["params"]
        min_key = next(k for k in params if k.endswith("_min_1"))
        max_key = next(k for k in params if k.endswith("_max_2"))
        assert params[min_key] == 1
        assert params[max_key] == 5

    def test_numeric_field_range_min_only(self, populated_registry):
        self._register("latitude")
        qr = build_query({"latitude": {"query": 46.1, "range": "min"}})
        assert "(idx->>'latitude')::numeric >=" in qr["where"]

    def test_numeric_field_range_max_only(self, populated_registry):
        self._register("latitude")
        qr = build_query({"latitude": {"query": 49.0, "range": "max"}})
        assert "(idx->>'latitude')::numeric <=" in qr["where"]

    def test_string_field_range_keeps_text_comparison(self, populated_registry):
        """Non-numeric values continue to use plain ``idx->>'key'``
        comparison (correct for ISO-format dates and similar
        lexicographically-orderable strings).
        """
        self._register("getId")
        qr = build_query({"getId": {"query": ["a", "m"], "range": "minmax"}})
        assert "::numeric" not in qr["where"]
        assert "idx->>'getId' >=" in qr["where"]
        assert "idx->>'getId' <=" in qr["where"]

    def test_string_field_range_normalizes_order(self, populated_registry):
        """Normalization applies regardless of type — ``_field_range`` sorts
        caller-supplied values even for strings.
        """
        self._register("getId")
        qr = build_query({"getId": {"query": ["m", "a"], "range": "minmax"}})
        params = qr["params"]
        min_key = next(k for k in params if k.endswith("_min_1"))
        max_key = next(k for k in params if k.endswith("_max_2"))
        assert params[min_key] == "a"
        assert params[max_key] == "m"


class TestDynamicKeywordIndex:
    """KeywordIndex dynamically registered via registry."""

    def test_dynamic_keyword_or(self, populated_registry):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("my_tags", IndexType.KEYWORD, "my_tags")

        qr = build_query({"my_tags": {"query": ["tag1", "tag2"], "operator": "or"}})
        assert "?|" in qr["where"]

    def test_dynamic_keyword_and(self, populated_registry):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("my_tags", IndexType.KEYWORD, "my_tags")

        qr = build_query({"my_tags": {"query": ["tag1", "tag2"], "operator": "and"}})
        assert "idx @>" in qr["where"]


class TestDynamicDateIndex:
    """DateIndex dynamically registered via registry."""

    def test_dynamic_date_exact(self, populated_registry):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("event_start", IndexType.DATE, "event_start")

        dt = datetime(2025, 6, 15, tzinfo=UTC)
        qr = build_query({"event_start": dt})
        assert "pgcatalog_to_timestamptz(idx->>'event_start')" in qr["where"]


class TestDynamicSort:
    """Sort on dynamically registered indexes."""

    def test_sort_dynamic_field(self, populated_registry):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("my_sort_field", IndexType.FIELD, "my_sort_field")

        qr = build_query({"sort_on": "my_sort_field"})
        assert qr["order_by"] == "idx->>'my_sort_field' ASC"

    def test_sort_dynamic_date(self, populated_registry):
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        get_registry().register("event_start", IndexType.DATE, "event_start")

        qr = build_query({"sort_on": "event_start", "sort_order": "descending"})
        assert "pgcatalog_to_timestamptz" in qr["order_by"]
        assert "DESC" in qr["order_by"]


class TestIPGIndexTranslatorQuery:
    """IPGIndexTranslator fallback for unknown index types in query builder."""

    def test_translator_query_called(self, populated_registry):
        """If an index is not in the registry, query looks up IPGIndexTranslator."""
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import getSiteManager

        translator = mock.Mock()
        translator.query.return_value = (
            "idx->>'custom_range' BETWEEN %(cr_lo)s AND %(cr_hi)s",
            {"cr_lo": "2025-01-01", "cr_hi": "2025-12-31"},
        )

        sm = getSiteManager()
        sm.registerUtility(translator, IPGIndexTranslator, name="custom_range")
        try:
            qr = build_query({"custom_range": {"lo": "2025-01-01", "hi": "2025-12-31"}})
            translator.query.assert_called_once_with(
                "custom_range",
                {"lo": "2025-01-01", "hi": "2025-12-31"},
                mock.ANY,
            )
            assert "custom_range" in qr["where"]
        finally:
            sm.unregisterUtility(provided=IPGIndexTranslator, name="custom_range")

    def test_translator_sort_called(self, populated_registry):
        """IPGIndexTranslator.sort() is used when index not in registry."""
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import getSiteManager

        translator = mock.Mock()
        translator.sort.return_value = "idx->>'custom_range'"

        sm = getSiteManager()
        sm.registerUtility(translator, IPGIndexTranslator, name="custom_range")
        try:
            qr = build_query({"sort_on": "custom_range"})
            translator.sort.assert_called_once_with("custom_range")
            assert "custom_range" in qr["order_by"]
        finally:
            sm.unregisterUtility(provided=IPGIndexTranslator, name="custom_range")

    def test_no_translator_falls_back_to_field(self, populated_registry):
        """Unknown index without translator falls back to JSONB field query."""
        qr = build_query({"totally_unknown_index": "val"})
        assert "totally_unknown_index" in qr["where"]

    def test_translator_sort_returns_none(self, populated_registry):
        """If translator.sort() returns None, no ORDER BY is added."""
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import getSiteManager

        translator = mock.Mock()
        translator.sort.return_value = None

        sm = getSiteManager()
        sm.registerUtility(translator, IPGIndexTranslator, name="unsortable")
        try:
            qr = build_query({"sort_on": "unsortable"})
            assert qr["order_by"] is None
        finally:
            sm.unregisterUtility(provided=IPGIndexTranslator, name="unsortable")


# ---------------------------------------------------------------------------
# Helper Function Tests
# ---------------------------------------------------------------------------


class TestBoolToLowerStr:
    """Test the _bool_to_lower_str helper function."""

    def test_boolean_true(self):
        """Test True converts to 'true'."""
        assert _bool_to_lower_str(True) == "true"

    def test_boolean_false(self):
        """Test False converts to 'false'."""
        assert _bool_to_lower_str(False) == "false"

    def test_string_passthrough(self):
        """Test strings are passed through unchanged."""
        assert _bool_to_lower_str("hello") == "hello"
        assert _bool_to_lower_str("True") == "True"  # String "True" stays as-is
        assert _bool_to_lower_str("false") == "false"

    def test_number_conversion(self):
        """Test numbers are converted to strings."""
        assert _bool_to_lower_str(42) == "42"
        assert _bool_to_lower_str(3.14) == "3.14"

    def test_none_conversion(self):
        """Test None converts to 'None'."""
        assert _bool_to_lower_str(None) == "None"
