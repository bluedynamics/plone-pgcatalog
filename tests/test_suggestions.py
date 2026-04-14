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
        result = suggest_indexes(["portal_type"], None, registry, {})
        assert len(result) == 1
        assert result[0]["status"] == "new"
        assert "(idx->>'portal_type')" in result[0]["ddl"]
        assert "idx_os_sug_" in result[0]["ddl"]

    def test_two_fields_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Creator=IndexType.FIELD,
        )
        result = suggest_indexes(["portal_type", "Creator"], None, registry, {})
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
        result = suggest_indexes(["a", "b", "c", "d"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        # Composite should have max 3 fields
        for s in new:
            assert len(s["fields"]) <= 3

    def test_keyword_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Subject=IndexType.KEYWORD,
        )
        result = suggest_indexes(["portal_type", "Subject"], None, registry, {})
        # KEYWORD gets its own suggestion, not mixed into composite
        for s in result:
            if "Subject" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("KEYWORD should not be in a composite")

    def test_text_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Title=IndexType.TEXT,
        )
        result = suggest_indexes(["portal_type", "Title"], None, registry, {})
        for s in result:
            if "Title" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("TEXT should not be in a composite")

    def test_date_uses_timestamptz(self):
        registry = _reg(modified=IndexType.DATE)
        result = suggest_indexes(["modified"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("pgcatalog_to_timestamptz" in s["ddl"] for s in new)

    def test_boolean_uses_cast(self):
        registry = _reg(is_folderish=IndexType.BOOLEAN)
        result = suggest_indexes(["is_folderish"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("((idx->>'is_folderish')::boolean)" in s["ddl"] for s in new)

    def test_boolean_in_composite_has_outer_parens(self):
        """Boolean cast in composite index must be wrapped in expression parens.

        Without outer parens, PG parser sees (idx->>'field')::boolean and
        interprets ')' as closing the index expression, leaving ::boolean
        as an invalid token.  Regression test for #104.
        """
        registry = _reg(
            portal_type=IndexType.FIELD,
            exclude_from_nav=IndexType.BOOLEAN,
        )
        result = suggest_indexes(
            ["portal_type", "exclude_from_nav"], None, registry, {}
        )
        composites = [
            s for s in result if s["status"] == "new" and len(s["fields"]) > 1
        ]
        assert len(composites) == 1
        ddl = composites[0]["ddl"]
        # The boolean expression must have double parens so ::boolean is
        # inside the CREATE INDEX expression grouping:
        #   ... ((idx->>'exclude_from_nav')::boolean) ...
        # NOT:
        #   ... (idx->>'exclude_from_nav')::boolean ...
        assert "((idx->>'exclude_from_nav')::boolean)" in ddl

    def test_uuid_uses_text_expression(self):
        registry = _reg(UID=IndexType.UUID)
        result = suggest_indexes(["UID"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("(idx->>'UID')" in s["ddl"] for s in new)

    def test_path_uses_text_pattern_ops(self):
        registry = _reg(tgpath=IndexType.PATH)
        result = suggest_indexes(["tgpath"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("text_pattern_ops" in s["ddl"] for s in new)

    def test_keyword_gets_own_gin(self):
        """Unknown KEYWORD fields (not in _DEDICATED_FIELDS) get a GIN suggestion."""
        registry = _reg(custom_tags=IndexType.KEYWORD)
        result = suggest_indexes(["custom_tags"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("GIN" in s["ddl"].upper() or "gin" in s["ddl"] for s in new)

    def test_non_idx_fields_filtered(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(
            ["portal_type", "sort_on", "b_size"], None, registry, {}
        )
        for s in result:
            assert "sort_on" not in s["fields"]
            assert "b_size" not in s["fields"]

    def test_pagination_meta_dropped(self):
        """b_size / b_start are pagination-meta — never appear in suggestions."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(
            ["portal_type", "b_size", "b_start"], None, registry, {}
        )
        for s in result:
            assert "b_size" not in s["fields"]
            assert "b_start" not in s["fields"]

    def test_sort_meta_keys_dropped(self):
        """sort_on / sort_order keys (as raw keys) are dropped from the filter list."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(
            ["portal_type", "sort_on", "sort_order"], None, registry, {}
        )
        for s in result:
            assert "sort_on" not in s["fields"]
            assert "sort_order" not in s["fields"]

    def test_unknown_field_skipped(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type", "unknown_field"], None, registry, {})
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
        result = suggest_indexes(["portal_type"], None, registry, existing)
        assert all(s["status"] == "already_covered" for s in result)

    def test_already_covered_by_sug_name(self):
        """Re-applying the same suggestion detects by idx_os_sug_ name."""
        registry = _reg(portal_type=IndexType.FIELD)
        existing = {
            "idx_os_sug_portal_type": (
                "CREATE INDEX idx_os_sug_portal_type ON public.object_state "
                "USING btree (((idx ->> 'portal_type'::text))) "
                "WHERE (idx IS NOT NULL)"
            )
        }
        result = suggest_indexes(["portal_type"], None, registry, existing)
        assert all(s["status"] == "already_covered" for s in result)

    def test_dedicated_field_already_covered(self):
        registry = _reg(
            allowedRolesAndUsers=IndexType.KEYWORD,
        )
        result = suggest_indexes(["allowedRolesAndUsers"], None, registry, {})
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1
        assert "dedicated" in covered[0]["reason"].lower()

    def test_object_provides_already_covered(self):
        """object_provides has a dedicated GIN index — always covered."""
        registry = _reg(object_provides=IndexType.KEYWORD)
        result = suggest_indexes(["object_provides"], None, registry, {})
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1

    def test_subject_already_covered(self):
        """Subject has a dedicated GIN index — always covered."""
        registry = _reg(Subject=IndexType.KEYWORD)
        result = suggest_indexes(["Subject"], None, registry, {})
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1

    def test_empty_keys_returns_empty(self):
        registry = _reg()
        result = suggest_indexes([], None, registry, {})
        assert result == []

    def test_all_filtered_keys_returns_empty(self):
        registry = _reg()
        result = suggest_indexes(["sort_on", "b_size"], None, registry, {})
        assert result == []

    def test_selectivity_ordering(self):
        """UUID fields should come first in composites (most selective)."""
        registry = _reg(
            review_state=IndexType.FIELD,
            UID=IndexType.UUID,
        )
        result = suggest_indexes(["review_state", "UID"], None, registry, {})
        composites = [s for s in result if len(s["fields"]) > 1]
        if composites:
            assert composites[0]["fields"][0] == "UID"

    def test_naming_convention(self):
        """Generated index names use idx_os_sug_ prefix."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        for s in new:
            assert "idx_os_sug_" in s["ddl"]

    def test_date_range_excluded(self):
        """The effectiveRange virtual key never appears verbatim in outputs."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type", "effectiveRange"], None, registry, {})
        for s in result:
            assert "effectiveRange" not in s["fields"]

    def test_effective_range_expands_to_effective(self):
        """effectiveRange in query keys yields a composite mentioning effective."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type", "effectiveRange"], None, registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert "portal_type" in new[0]["fields"]
        assert "effective" in new[0]["fields"]
        assert "pgcatalog_to_timestamptz(idx->>'effective')" in new[0]["ddl"]

    def test_effective_range_narrow_no_expires(self):
        """Narrow expansion — expires is NOT added to the composite."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type", "effectiveRange"], None, registry, {})
        for s in result:
            assert "expires" not in s["fields"]
            assert "'expires'" not in s["ddl"]

    def test_gopip_skipped(self):
        """GopipIndex fields are skipped (no meaningful PG index type)."""
        registry = _reg(getObjPositionInParent=IndexType.GOPIP)
        result = suggest_indexes(["getObjPositionInParent"], None, registry, {})
        assert result == []

    def test_mixed_case_name_already_covered(self):
        """Mixed-case field name should match existing lowercased PG index.

        Regression for #119: `_check_covered` Check 1 was case-sensitive
        but PostgreSQL folds unquoted identifiers to lowercase in
        `pg_indexes.indexname`.
        """
        registry = _reg(Language=IndexType.FIELD)
        # PG stores unquoted identifiers lowercased in pg_indexes.
        existing = {
            "idx_os_sug_language": (
                "CREATE INDEX idx_os_sug_language ON public.object_state "
                "USING btree (((idx ->> 'Language'::text))) "
                "WHERE (idx IS NOT NULL)"
            )
        }
        result = suggest_indexes(["Language"], None, registry, existing)
        assert all(s["status"] == "already_covered" for s in result)

    def test_composite_already_covered_by_pg_normalized_indexdef(self):
        """Composite suggestion detects equivalent PG-stored indexdef.

        Regression for #119: `_normalize_idx_expr` did not normalize
        whitespace around `->>`, so the generated form and the
        PG-stored form didn't compare as equal even after the existing
        normalization passes.
        """
        registry = _reg(
            Language=IndexType.FIELD,
            portal_type=IndexType.FIELD,
            end=IndexType.DATE,
        )
        # Real indexdef text captured from pg_indexes after a successful
        # apply of this exact composite suggestion.
        existing = {
            "idx_os_sug_language_portal_type_end": (
                "CREATE INDEX idx_os_sug_language_portal_type_end "
                "ON public.object_state USING btree ("
                "((idx ->> 'Language'::text)), "
                "((idx ->> 'portal_type'::text)), "
                "pgcatalog_to_timestamptz((idx ->> 'end'::text))"
                ") WHERE (idx IS NOT NULL)"
            )
        }
        result = suggest_indexes(
            ["Language", "portal_type", "end"], None, registry, existing
        )
        assert all(s["status"] == "already_covered" for s in result)


class TestNormalizeIdxExpr:
    """Unit tests for _normalize_idx_expr — comparison canonicalization."""

    def test_generated_and_pg_stored_composite_equal(self):
        """Same index in generated and PG-stored form normalize equal."""
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        generated = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_Language_portal_type_end "
            "ON object_state ("
            "(idx->>'Language'), (idx->>'portal_type'), "
            "pgcatalog_to_timestamptz(idx->>'end')"
            ") WHERE idx IS NOT NULL"
        )
        stored = (
            "CREATE INDEX idx_os_sug_language_portal_type_end "
            "ON public.object_state USING btree ("
            "((idx ->> 'Language'::text)), "
            "((idx ->> 'portal_type'::text)), "
            "pgcatalog_to_timestamptz((idx ->> 'end'::text))"
            ") WHERE (idx IS NOT NULL)"
        )
        assert _normalize_idx_expr(generated) == _normalize_idx_expr(stored)

    def test_arrow_whitespace_normalized(self):
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        with_spaces = "CREATE INDEX i ON t ((idx ->> 'x')) WHERE idx IS NOT NULL"
        without_spaces = "CREATE INDEX i ON t ((idx->>'x')) WHERE idx IS NOT NULL"
        assert _normalize_idx_expr(with_spaces) == _normalize_idx_expr(without_spaces)

    def test_text_cast_stripped(self):
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        with_cast = "CREATE INDEX i ON t ((idx->>'x'::text)) WHERE idx IS NOT NULL"
        without_cast = "CREATE INDEX i ON t ((idx->>'x')) WHERE idx IS NOT NULL"
        assert _normalize_idx_expr(with_cast) == _normalize_idx_expr(without_cast)

    def test_nested_paren_collapse(self):
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        triple = "CREATE INDEX i ON t (((x))) WHERE idx IS NOT NULL"
        single = "CREATE INDEX i ON t ((x)) WHERE idx IS NOT NULL"
        # After normalization both collapse to the same canonical form.
        assert _normalize_idx_expr(triple) == _normalize_idx_expr(single)

    def test_no_where_clause(self):
        """DDL without WHERE still normalizes — pattern uses `|$` fallback."""
        from plone.pgcatalog.suggestions import _normalize_idx_expr

        # Graceful: regex falls through to $ anchor
        result = _normalize_idx_expr("CREATE INDEX i ON t ((idx->>'x'))")
        assert "idx->>'x'" in result


class TestApplyIndexPreflight:
    """Unit tests for apply_index idempotency (#119)."""

    def _make_conn(self, preflight_row):
        """Return a mock conn whose pg_index pre-flight returns the
        given row (tuple or None).
        """
        from unittest import mock

        conn = mock.MagicMock()
        conn.autocommit = False
        cur = mock.MagicMock()
        cur.fetchone.return_value = preflight_row
        # Support context-manager protocol on conn.cursor()
        ctx = mock.MagicMock()
        ctx.__enter__.return_value = cur
        ctx.__exit__.return_value = None
        conn.cursor.return_value = ctx
        return conn, cur

    def test_valid_pre_existing_index_is_noop(self):
        from plone.pgcatalog.suggestions import apply_index

        # pg_index returns indisvalid=True
        conn, _cur = self._make_conn(preflight_row=(True,))
        ddl = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_foo "
            "ON object_state ((idx->>'foo')) WHERE idx IS NOT NULL"
        )
        success, msg, duration = apply_index(conn, ddl)

        assert success is True
        assert "already exists" in msg
        assert duration == 0.0
        # No CIC issued — conn.execute is only called for the pre-flight
        # SELECT (via cur.execute), never for the CREATE INDEX.
        executed_sqls = [c.args[0] for c in conn.execute.call_args_list]
        assert not any("CREATE INDEX" in s for s in executed_sqls)

    def test_invalid_pre_existing_index_is_dropped_then_built(self):
        from plone.pgcatalog.suggestions import apply_index

        # pg_index returns indisvalid=False
        conn, _cur = self._make_conn(preflight_row=(False,))
        ddl = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_foo "
            "ON object_state ((idx->>'foo')) WHERE idx IS NOT NULL"
        )
        success, _msg, _duration = apply_index(conn, ddl)

        assert success is True
        executed_sqls = [c.args[0] for c in conn.execute.call_args_list]
        # Both DROP INDEX CONCURRENTLY and CREATE INDEX CONCURRENTLY
        assert any("DROP INDEX CONCURRENTLY IF EXISTS" in s for s in executed_sqls)
        assert any("CREATE INDEX CONCURRENTLY" in s for s in executed_sqls)

    def test_no_pre_existing_index_proceeds_to_create(self):
        from plone.pgcatalog.suggestions import apply_index

        conn, _cur = self._make_conn(preflight_row=None)
        ddl = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_foo "
            "ON object_state ((idx->>'foo')) WHERE idx IS NOT NULL"
        )
        success, _msg, _duration = apply_index(conn, ddl)

        assert success is True
        executed_sqls = [c.args[0] for c in conn.execute.call_args_list]
        # No DROP, just CREATE
        assert not any("DROP INDEX CONCURRENTLY IF EXISTS" in s for s in executed_sqls)
        assert any("CREATE INDEX CONCURRENTLY" in s for s in executed_sqls)


class TestExtractSortField:
    """Unit tests for _extract_sort_field helper."""

    def test_returns_none_when_params_is_none(self):
        from plone.pgcatalog.suggestions import _extract_sort_field

        assert _extract_sort_field(None, _reg()) is None

    def test_returns_none_when_params_empty(self):
        from plone.pgcatalog.suggestions import _extract_sort_field

        assert _extract_sort_field({}, _reg()) is None

    def test_plain_sort_on_extracted(self):
        from plone.pgcatalog.suggestions import _extract_sort_field

        registry = _reg(effective=IndexType.DATE)
        result = _extract_sort_field({"sort_on": "effective"}, registry)
        assert result == ("effective", IndexType.DATE)

    def test_plone_aliased_sort_on_extracted(self):
        """Plone generates p_sort_on_1 etc. — substring match wins."""
        from plone.pgcatalog.suggestions import _extract_sort_field

        registry = _reg(effective=IndexType.DATE)
        result = _extract_sort_field({"p_sort_on_1": "effective"}, registry)
        assert result == ("effective", IndexType.DATE)

    def test_returns_none_for_unknown_field(self):
        """Sort value not in registry → None (no crash)."""
        from plone.pgcatalog.suggestions import _extract_sort_field

        registry = _reg(effective=IndexType.DATE)
        assert _extract_sort_field({"sort_on": "bogus"}, registry) is None

    def test_returns_none_for_non_composite_type(self):
        """Sort on a KEYWORD/TEXT field → None (cannot be a trailing btree column)."""
        from plone.pgcatalog.suggestions import _extract_sort_field

        registry = _reg(Subject=IndexType.KEYWORD)
        assert _extract_sort_field({"sort_on": "Subject"}, registry) is None

    def test_returns_none_for_skip_type(self):
        """GOPIP / DATE_RANGE cannot be a trailing btree column either."""
        from plone.pgcatalog.suggestions import _extract_sort_field

        registry = _reg(getObjPositionInParent=IndexType.GOPIP)
        assert (
            _extract_sort_field({"sort_on": "getObjPositionInParent"}, registry) is None
        )
