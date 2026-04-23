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


def _flat(result):
    """Flatten a list[Bundle] into list[dict] matching the pre-α shape.

    Existing tests assert on dict keys (fields, field_types, ddl,
    status, reason).  This helper keeps them readable without
    duplicating the unwrap in every test.
    """
    from dataclasses import asdict

    out = []
    for bundle in result:
        for member in bundle.members:
            d = asdict(member)
            out.append(d)
    return out


class TestSuggestIndexes:
    """Test the pure suggestion engine."""

    def test_single_field_returns_single_btree(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(suggest_indexes(["portal_type"], None, registry, {}))
        assert len(result) == 1
        assert result[0]["status"] == "new"
        assert "(idx->>'portal_type')" in result[0]["ddl"]
        assert "idx_os_sug_" in result[0]["ddl"]

    def test_two_fields_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Creator=IndexType.FIELD,
        )
        result = _flat(suggest_indexes(["portal_type", "Creator"], None, registry, {}))
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
        result = _flat(suggest_indexes(["a", "b", "c", "d"], None, registry, {}))
        new = [s for s in result if s["status"] == "new"]
        # Composite should have max 3 fields
        for s in new:
            assert len(s["fields"]) <= 3

    def test_keyword_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Subject=IndexType.KEYWORD,
        )
        result = _flat(suggest_indexes(["portal_type", "Subject"], None, registry, {}))
        # KEYWORD gets its own suggestion, not mixed into composite
        for s in result:
            if "Subject" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("KEYWORD should not be in a composite")

    def test_text_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Title=IndexType.TEXT,
        )
        result = _flat(suggest_indexes(["portal_type", "Title"], None, registry, {}))
        for s in result:
            if "Title" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("TEXT should not be in a composite")

    def test_date_uses_timestamptz(self):
        registry = _reg(modified=IndexType.DATE)
        result = _flat(suggest_indexes(["modified"], None, registry, {}))
        new = [s for s in result if s["status"] == "new"]
        assert any("pgcatalog_to_timestamptz" in s["ddl"] for s in new)

    def test_boolean_uses_cast(self):
        registry = _reg(is_folderish=IndexType.BOOLEAN)
        result = _flat(suggest_indexes(["is_folderish"], None, registry, {}))
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
        result = _flat(
            suggest_indexes(["portal_type", "exclude_from_nav"], None, registry, {})
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
        result = _flat(suggest_indexes(["UID"], None, registry, {}))
        new = [s for s in result if s["status"] == "new"]
        assert any("(idx->>'UID')" in s["ddl"] for s in new)

    def test_path_uses_text_pattern_ops(self):
        registry = _reg(tgpath=IndexType.PATH)
        result = _flat(suggest_indexes(["tgpath"], None, registry, {}))
        new = [s for s in result if s["status"] == "new"]
        assert any("text_pattern_ops" in s["ddl"] for s in new)

    def test_keyword_gets_own_gin(self):
        """Unknown KEYWORD fields (not in _DEDICATED_FIELDS) get a GIN suggestion."""
        registry = _reg(custom_tags=IndexType.KEYWORD)
        result = _flat(suggest_indexes(["custom_tags"], None, registry, {}))
        new = [s for s in result if s["status"] == "new"]
        assert any("GIN" in s["ddl"].upper() or "gin" in s["ddl"] for s in new)

    def test_non_idx_fields_filtered(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(["portal_type", "sort_on", "b_size"], None, registry, {})
        )
        for s in result:
            assert "sort_on" not in s["fields"]
            assert "b_size" not in s["fields"]

    def test_pagination_meta_dropped(self):
        """b_size / b_start are pagination-meta — never appear in suggestions."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(["portal_type", "b_size", "b_start"], None, registry, {})
        )
        for s in result:
            assert "b_size" not in s["fields"]
            assert "b_start" not in s["fields"]

    def test_sort_meta_keys_dropped(self):
        """sort_on / sort_order keys (as raw keys) are dropped from the filter list."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(
                ["portal_type", "sort_on", "sort_order"], None, registry, {}
            )
        )
        for s in result:
            assert "sort_on" not in s["fields"]
            assert "sort_order" not in s["fields"]

    def test_unknown_field_skipped(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(["portal_type", "unknown_field"], None, registry, {})
        )
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
        result = _flat(suggest_indexes(["portal_type"], None, registry, existing))
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
        result = _flat(suggest_indexes(["portal_type"], None, registry, existing))
        assert all(s["status"] == "already_covered" for s in result)

    def test_dedicated_field_already_covered(self):
        registry = _reg(
            allowedRolesAndUsers=IndexType.KEYWORD,
        )
        result = _flat(suggest_indexes(["allowedRolesAndUsers"], None, registry, {}))
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1
        assert "dedicated" in covered[0]["reason"].lower()

    def test_object_provides_already_covered(self):
        """object_provides has a dedicated GIN index — always covered."""
        registry = _reg(object_provides=IndexType.KEYWORD)
        result = _flat(suggest_indexes(["object_provides"], None, registry, {}))
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1

    def test_subject_already_covered(self):
        """Subject has a dedicated GIN index — always covered."""
        registry = _reg(Subject=IndexType.KEYWORD)
        result = _flat(suggest_indexes(["Subject"], None, registry, {}))
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1

    def test_empty_keys_returns_empty(self):
        registry = _reg()
        result = _flat(suggest_indexes([], None, registry, {}))
        assert result == []

    def test_all_filtered_keys_returns_empty(self):
        registry = _reg()
        result = _flat(suggest_indexes(["sort_on", "b_size"], None, registry, {}))
        assert result == []

    def test_selectivity_ordering(self):
        """UUID fields should come first in composites (most selective)."""
        registry = _reg(
            review_state=IndexType.FIELD,
            UID=IndexType.UUID,
        )
        result = _flat(suggest_indexes(["review_state", "UID"], None, registry, {}))
        composites = [s for s in result if len(s["fields"]) > 1]
        if composites:
            assert composites[0]["fields"][0] == "UID"

    def test_naming_convention(self):
        """Generated index names use idx_os_sug_ prefix."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(suggest_indexes(["portal_type"], None, registry, {}))
        new = [s for s in result if s["status"] == "new"]
        for s in new:
            assert "idx_os_sug_" in s["ddl"]

    def test_date_range_excluded(self):
        """The effectiveRange virtual key never appears verbatim in outputs."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(["portal_type", "effectiveRange"], None, registry, {})
        )
        for s in result:
            assert "effectiveRange" not in s["fields"]

    def test_effective_range_expands_to_effective(self):
        """effectiveRange in query keys yields a composite mentioning effective."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(["portal_type", "effectiveRange"], None, registry, {})
        )
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert "portal_type" in new[0]["fields"]
        assert "effective" in new[0]["fields"]
        assert "pgcatalog_to_timestamptz(idx->>'effective')" in new[0]["ddl"]

    def test_effective_range_narrow_no_expires(self):
        """Narrow expansion — expires is NOT added to the composite."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(["portal_type", "effectiveRange"], None, registry, {})
        )
        for s in result:
            assert "expires" not in s["fields"]
            assert "'expires'" not in s["ddl"]

    def test_gopip_skipped(self):
        """GopipIndex fields are skipped (no meaningful PG index type)."""
        registry = _reg(getObjPositionInParent=IndexType.GOPIP)
        result = _flat(suggest_indexes(["getObjPositionInParent"], None, registry, {}))
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
        result = _flat(suggest_indexes(["Language"], None, registry, existing))
        assert all(s["status"] == "already_covered" for s in result)

    def test_sort_on_appends_trailing_column(self):
        """sort_on in params appends the sort field as the last composite column."""
        registry = _reg(
            portal_type=IndexType.FIELD,
            effective=IndexType.DATE,
        )
        result = _flat(
            suggest_indexes(
                ["portal_type"],
                {"sort_on": "effective"},
                registry,
                {},
            )
        )
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert new[0]["fields"] == ["portal_type", "effective"]
        ddl = new[0]["ddl"]
        assert ddl.index("pgcatalog_to_timestamptz(idx->>'effective')") > ddl.index(
            "(idx->>'portal_type')"
        )
        assert "ORDER BY effective" in new[0]["reason"]

    def test_sort_on_deduped_when_already_leading(self):
        """Sort field already present as filter column — not appended twice."""
        registry = _reg(
            portal_type=IndexType.FIELD,
            effective=IndexType.DATE,
        )
        result = _flat(
            suggest_indexes(
                ["portal_type", "effectiveRange"],
                {"sort_on": "effective"},
                registry,
                {},
            )
        )
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert new[0]["fields"].count("effective") == 1

    def test_sort_on_ignored_for_non_composite_type(self):
        """Sort on a TEXT field does not add a trailing column."""
        registry = _reg(
            portal_type=IndexType.FIELD,
            Title=IndexType.TEXT,
        )
        result = _flat(
            suggest_indexes(
                ["portal_type"],
                {"sort_on": "Title"},
                registry,
                {},
            )
        )
        new = [
            s for s in result if s["status"] == "new" and "portal_type" in s["fields"]
        ]
        assert all("Title" not in s["fields"] for s in new)

    def test_sort_on_unknown_field_ignored(self):
        """Sort on an unregistered field produces no covering column, no crash."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = _flat(
            suggest_indexes(
                ["portal_type"],
                {"sort_on": "not_in_registry"},
                registry,
                {},
            )
        )
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert new[0]["fields"] == ["portal_type"]

    def test_composite_cap_includes_sort(self):
        """Three filter fields + sort → filter list truncated to 2, sort appended."""
        registry = _reg(
            a=IndexType.FIELD,
            b=IndexType.FIELD,
            c=IndexType.FIELD,
            effective=IndexType.DATE,
        )
        result = _flat(
            suggest_indexes(
                ["a", "b", "c"],
                {"sort_on": "effective"},
                registry,
                {},
            )
        )
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert len(new[0]["fields"]) == 3
        assert new[0]["fields"][-1] == "effective"

    def test_issue_122_pattern(self):
        """Regression for #122: portal_type + effectiveRange + sort_on=effective."""
        registry = _reg(
            portal_type=IndexType.FIELD,
            effective=IndexType.DATE,
        )
        result = _flat(
            suggest_indexes(
                ["portal_type", "effectiveRange"],
                {"sort_on": "effective"},
                registry,
                {},
            )
        )
        new = [s for s in result if s["status"] == "new"]
        assert len(new) == 1
        assert new[0]["fields"] == ["portal_type", "effective"]
        ddl = new[0]["ddl"]
        assert "(idx->>'portal_type')" in ddl
        assert "pgcatalog_to_timestamptz(idx->>'effective')" in ddl

    def test_params_none_behaves_as_before(self):
        """Passing params=None is equivalent to the pre-PR-2 behavior."""
        registry = _reg(portal_type=IndexType.FIELD)
        r_none = _flat(suggest_indexes(["portal_type"], None, registry, {}))
        r_empty = _flat(suggest_indexes(["portal_type"], {}, registry, {}))
        assert [s["ddl"] for s in r_none] == [s["ddl"] for s in r_empty]

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
        result = _flat(
            suggest_indexes(
                ["Language", "portal_type", "end"], None, registry, existing
            )
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


class TestBundleTypes:
    """Bundle / BundleMember dataclass construction and semantics."""

    def test_bundle_member_is_frozen(self):
        from dataclasses import FrozenInstanceError
        from plone.pgcatalog.suggestions import BundleMember

        m = BundleMember(
            ddl="CREATE INDEX i ON t (x) WHERE idx IS NOT NULL",
            fields=["x"],
            field_types=["FIELD"],
            status="new",
            role="btree_composite",
            reason="test",
        )
        with pytest.raises(FrozenInstanceError):
            m.status = "already_covered"

    def test_bundle_is_frozen(self):
        from dataclasses import FrozenInstanceError
        from plone.pgcatalog.suggestions import Bundle
        from plone.pgcatalog.suggestions import BundleMember

        m = BundleMember(
            ddl="d",
            fields=["x"],
            field_types=["FIELD"],
            status="new",
            role="btree_composite",
            reason="r",
        )
        b = Bundle(
            name="test-bundle",
            rationale="unit test",
            shape_classification="BTREE_ONLY",
            members=[m],
        )
        with pytest.raises(FrozenInstanceError):
            b.name = "other"

    def test_asdict_roundtrip(self):
        from dataclasses import asdict
        from plone.pgcatalog.suggestions import Bundle
        from plone.pgcatalog.suggestions import BundleMember

        m = BundleMember(
            ddl="d",
            fields=["x"],
            field_types=["FIELD"],
            status="new",
            role="btree_composite",
            reason="r",
        )
        b = Bundle(
            name="n",
            rationale="why",
            shape_classification="BTREE_ONLY",
            members=[m],
        )
        d = asdict(b)
        assert d["name"] == "n"
        assert d["members"][0]["ddl"] == "d"
        assert d["members"][0]["role"] == "btree_composite"


class TestExtractFilterFields:
    """_extract_filter_fields turns query_keys + params into structured
    (name, IndexType, operator, value) tuples."""

    def test_scalar_equality(self):
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg(portal_type=IndexType.FIELD)
        out = _extract_filter_fields(
            ["portal_type"], {"portal_type": "Event"}, registry
        )
        assert out == [("portal_type", IndexType.FIELD, "equality", "Event")]

    def test_range_operator(self):
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg(effective=IndexType.DATE)
        out = _extract_filter_fields(
            ["effective"],
            {"effective": {"query": [1, 2], "range": "min:max"}},
            registry,
        )
        assert out == [("effective", IndexType.DATE, "range", None)]

    def test_multi_value_equality(self):
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg(portal_type=IndexType.FIELD)
        out = _extract_filter_fields(
            ["portal_type"], {"portal_type": ["Event", "News"]}, registry
        )
        assert out == [("portal_type", IndexType.FIELD, "equality_multi", None)]

    def test_virtual_field_expands(self):
        """effectiveRange expands to ('effective', DATE) via _FILTER_VIRTUAL."""
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg()
        out = _extract_filter_fields(["effectiveRange"], {}, registry)
        # Virtual expansion carries no value or operator; mark as range
        # since effectiveRange inherently denotes a date window.
        assert out == [("effective", IndexType.DATE, "range", None)]

    def test_pagination_and_sort_dropped(self):
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg(portal_type=IndexType.FIELD)
        out = _extract_filter_fields(
            ["portal_type", "b_size", "sort_on"],
            {"portal_type": "Event", "b_size": 20, "sort_on": "effective"},
            registry,
        )
        names = [o[0] for o in out]
        assert "b_size" not in names
        assert "sort_on" not in names

    def test_unknown_field_skipped(self):
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg(portal_type=IndexType.FIELD)
        out = _extract_filter_fields(
            ["portal_type", "custom_field_not_in_registry"],
            {},
            registry,
        )
        names = [o[0] for o in out]
        assert "custom_field_not_in_registry" not in names

    def test_skip_fields_dropped(self):
        """path / SearchableText deferred — drop from filter list here too."""
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg(portal_type=IndexType.FIELD)
        out = _extract_filter_fields(["portal_type", "path"], {}, registry)
        names = [o[0] for o in out]
        assert "path" not in names

    def test_no_params_yields_unknown_operator(self):
        """When params is None, operator is 'unknown' (still usable for shape)."""
        from plone.pgcatalog.suggestions import _extract_filter_fields

        registry = _reg(portal_type=IndexType.FIELD)
        out = _extract_filter_fields(["portal_type"], None, registry)
        assert out == [("portal_type", IndexType.FIELD, "unknown", None)]


class TestClassifyFilterShape:
    """_classify_filter_shape routes filter lists to one of five shapes."""

    def _ff(self, *pairs):
        """Build a filter_fields list from (name, type) pairs."""
        return [(n, t, "equality", "v") for (n, t) in pairs]

    def test_empty_is_unknown(self):
        from plone.pgcatalog.suggestions import _classify_filter_shape

        assert _classify_filter_shape([]) == "UNKNOWN"

    def test_btree_only(self):
        from plone.pgcatalog.suggestions import _classify_filter_shape

        out = _classify_filter_shape(
            self._ff(("portal_type", IndexType.FIELD), ("effective", IndexType.DATE))
        )
        assert out == "BTREE_ONLY"

    def test_keyword_only(self):
        from plone.pgcatalog.suggestions import _classify_filter_shape

        out = _classify_filter_shape(
            self._ff(("Subject", IndexType.KEYWORD), ("tags", IndexType.KEYWORD))
        )
        assert out == "KEYWORD_ONLY"

    def test_mixed(self):
        from plone.pgcatalog.suggestions import _classify_filter_shape

        out = _classify_filter_shape(
            self._ff(("portal_type", IndexType.FIELD), ("Subject", IndexType.KEYWORD))
        )
        assert out == "MIXED"

    def test_text_dominates(self):
        """Any TEXT filter → TEXT_ONLY, even if others are present."""
        from plone.pgcatalog.suggestions import _classify_filter_shape

        out = _classify_filter_shape(
            self._ff(("Title", IndexType.TEXT), ("portal_type", IndexType.FIELD))
        )
        assert out == "TEXT_ONLY"


class TestBuildBtreeBundle:
    """_build_btree_bundle produces a single-member BTREE_ONLY Bundle."""

    def test_single_field_bundle(self):
        from plone.pgcatalog.suggestions import _build_btree_bundle

        filter_fields = [("portal_type", IndexType.FIELD, "equality", "Event")]
        bundle = _build_btree_bundle(filter_fields, None, {})
        assert bundle is not None
        assert bundle.shape_classification == "BTREE_ONLY"
        assert len(bundle.members) == 1
        m = bundle.members[0]
        assert m.role == "btree_composite"
        assert "(idx->>'portal_type')" in m.ddl
        assert m.fields == ["portal_type"]
        assert m.field_types == ["FIELD"]
        assert m.status == "new"

    def test_composite_with_sort_covering(self):
        from plone.pgcatalog.suggestions import _build_btree_bundle

        filter_fields = [("portal_type", IndexType.FIELD, "equality", "Event")]
        sort_field = ("effective", IndexType.DATE)
        bundle = _build_btree_bundle(filter_fields, sort_field, {})
        m = bundle.members[0]
        assert m.fields == ["portal_type", "effective"]
        assert "pgcatalog_to_timestamptz(idx->>'effective')" in m.ddl
        assert "ORDER BY effective" in m.reason

    def test_already_covered_propagates(self):
        from plone.pgcatalog.suggestions import _build_btree_bundle

        filter_fields = [("portal_type", IndexType.FIELD, "equality", "Event")]
        existing = {
            "idx_os_cat_portal_type": (
                "CREATE INDEX idx_os_cat_portal_type ON object_state "
                "((idx->>'portal_type')) WHERE idx IS NOT NULL"
            )
        }
        bundle = _build_btree_bundle(filter_fields, None, existing)
        assert bundle.members[0].status == "already_covered"

    def test_empty_filter_returns_none(self):
        from plone.pgcatalog.suggestions import _build_btree_bundle

        assert _build_btree_bundle([], None, {}) is None


class TestBuildKeywordGinBundle:
    """_build_keyword_gin_bundle produces a plain GIN bundle (T3)."""

    def test_single_keyword_plain_gin(self):
        """Without a partial predicate, emits T3 plain GIN."""
        from plone.pgcatalog.suggestions import _build_keyword_gin_bundle

        filter_fields = [("custom_tags", IndexType.KEYWORD, "equality", "alpha")]
        bundle = _build_keyword_gin_bundle(filter_fields, [], {})
        assert bundle is not None
        assert bundle.shape_classification == "KEYWORD_ONLY"
        assert len(bundle.members) == 1
        m = bundle.members[0]
        assert m.role == "plain_gin"
        assert "USING gin ((idx->'custom_tags'))" in m.ddl
        assert "WHERE" in m.ddl
        assert "idx->>'" not in m.ddl
        assert m.fields == ["custom_tags"]
        assert m.field_types == ["KEYWORD"]

    def test_partial_predicate_emits_t4(self):
        """With partial_where_terms provided, emits T4 partial GIN."""
        from plone.pgcatalog.suggestions import _build_keyword_gin_bundle

        filter_fields = [("Subject", IndexType.KEYWORD, "equality", "AT26")]
        where_terms = ["idx->>'portal_type' = 'Event'"]
        bundle = _build_keyword_gin_bundle(filter_fields, where_terms, {})
        m = bundle.members[0]
        assert m.role == "partial_gin"
        assert "USING gin ((idx->'Subject'))" in m.ddl
        assert "idx->>'portal_type' = 'Event'" in m.ddl

    def test_multiple_keywords_yield_separate_members(self):
        """Two KEYWORD filters → one bundle with two members (each GIN)."""
        from plone.pgcatalog.suggestions import _build_keyword_gin_bundle

        filter_fields = [
            ("Subject", IndexType.KEYWORD, "equality", "AT26"),
            ("tags", IndexType.KEYWORD, "equality_multi", None),
        ]
        bundle = _build_keyword_gin_bundle(filter_fields, [], {})
        assert len(bundle.members) == 2
        roles = {m.role for m in bundle.members}
        assert roles == {"plain_gin"}
        fields_covered = {tuple(m.fields) for m in bundle.members}
        assert fields_covered == {("Subject",), ("tags",)}

    def test_empty_filter_returns_none(self):
        from plone.pgcatalog.suggestions import _build_keyword_gin_bundle

        assert _build_keyword_gin_bundle([], [], {}) is None


class TestProbeSelectivity:
    """_probe_selectivity uses pg_stats MCV first, falls back to COUNT."""

    def _mock_conn(self, mcv_row=None, count_row=None, total_row=None):
        """Build a mock psycopg connection whose cursor returns the
        given rows in sequence.  mcv_row / count_row / total_row are
        dicts (or None).
        """
        from unittest import mock

        conn = mock.MagicMock()
        cur = mock.MagicMock()
        ctx = mock.MagicMock()
        ctx.__enter__.return_value = cur
        ctx.__exit__.return_value = None
        conn.cursor.return_value = ctx

        # Always include a slot for the pg_stats fetchone — None when
        # no MCV data is expected (simulates "no row in pg_stats").
        responses = [mcv_row]  # may be None → pg_stats returns no row
        if count_row is not None:
            responses.append(count_row)
        if total_row is not None:
            responses.append(total_row)
        cur.fetchone.side_effect = responses
        return conn, cur

    def test_mcv_hit_returns_frequency(self):
        from plone.pgcatalog.suggestions import _probe_selectivity
        from plone.pgcatalog.suggestions import _reset_probe_cache

        _reset_probe_cache()
        mcv_row = {
            "most_common_vals": ["Event", "News", "Page"],
            "most_common_freqs": [0.05, 0.20, 0.50],
        }
        conn, cur = self._mock_conn(mcv_row=mcv_row)
        sel = _probe_selectivity(conn, "portal_type", "Event")
        assert sel == 0.05

    def test_mcv_miss_falls_through_to_count(self):
        from plone.pgcatalog.suggestions import _probe_selectivity
        from plone.pgcatalog.suggestions import _reset_probe_cache

        _reset_probe_cache()
        mcv_row = {
            "most_common_vals": ["Event", "News"],
            "most_common_freqs": [0.05, 0.20],
        }
        count_row = {"c": 115}
        total_row = {"t": 3900000}
        conn, cur = self._mock_conn(
            mcv_row=mcv_row, count_row=count_row, total_row=total_row
        )
        sel = _probe_selectivity(conn, "portal_type", "RareValue")
        assert sel == 115 / 3900000

    def test_no_mcv_row_falls_through_to_count(self):
        """pg_stats returns None (e.g. attname unknown) → pure COUNT path."""
        from plone.pgcatalog.suggestions import _probe_selectivity
        from plone.pgcatalog.suggestions import _reset_probe_cache

        _reset_probe_cache()
        count_row = {"c": 42}
        total_row = {"t": 10000}
        conn, cur = self._mock_conn(
            mcv_row=None, count_row=count_row, total_row=total_row
        )
        sel = _probe_selectivity(conn, "custom_field", "value")
        assert sel == 42 / 10000

    def test_request_cache_avoids_duplicate_probe(self):
        from plone.pgcatalog.suggestions import _probe_selectivity
        from plone.pgcatalog.suggestions import _reset_probe_cache

        _reset_probe_cache()
        mcv_row = {
            "most_common_vals": ["Event"],
            "most_common_freqs": [0.05],
        }
        conn, cur = self._mock_conn(mcv_row=mcv_row)
        _probe_selectivity(conn, "portal_type", "Event")
        call_count_before = cur.execute.call_count
        _probe_selectivity(conn, "portal_type", "Event")
        assert cur.execute.call_count == call_count_before

    def test_conn_none_returns_one(self):
        """When conn is None (no probe possible), return 1.0 — safe default."""
        from plone.pgcatalog.suggestions import _probe_selectivity
        from plone.pgcatalog.suggestions import _reset_probe_cache

        _reset_probe_cache()
        assert _probe_selectivity(None, "portal_type", "Event") == 1.0


class TestPartialWhereTerms:
    """_partial_where_terms applies threshold, escapes values, filters ops."""

    def test_below_threshold_baked(self):
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [("portal_type", IndexType.FIELD, "equality", "Event")]
        probes = {("portal_type", "Event"): 0.05}
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == ["idx->>'portal_type' = 'Event'"]

    def test_above_threshold_excluded(self):
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [("portal_type", IndexType.FIELD, "equality", "Page")]
        probes = {("portal_type", "Page"): 0.50}
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == []

    def test_multiple_filters_anded(self):
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [
            ("portal_type", IndexType.FIELD, "equality", "Event"),
            ("review_state", IndexType.FIELD, "equality", "published"),
        ]
        probes = {
            ("portal_type", "Event"): 0.05,
            ("review_state", "published"): 0.03,
        }
        terms = _partial_where_terms(filter_fields, probes)
        assert set(terms) == {
            "idx->>'portal_type' = 'Event'",
            "idx->>'review_state' = 'published'",
        }

    def test_partial_selection_mixed_thresholds(self):
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [
            ("portal_type", IndexType.FIELD, "equality", "Event"),
            ("review_state", IndexType.FIELD, "equality", "published"),
        ]
        probes = {
            ("portal_type", "Event"): 0.05,
            ("review_state", "published"): 0.25,
        }
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == ["idx->>'portal_type' = 'Event'"]

    def test_single_quote_escaping(self):
        """Values containing single quotes are SQL-escaped (doubled)."""
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [("Creator", IndexType.FIELD, "equality", "o'hara")]
        probes = {("Creator", "o'hara"): 0.01}
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == ["idx->>'Creator' = 'o''hara'"]

    def test_range_operator_excluded(self):
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [("effective", IndexType.DATE, "range", None)]
        probes = {}
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == []

    def test_multi_value_equality_excluded(self):
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [("portal_type", IndexType.FIELD, "equality_multi", None)]
        probes = {}
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == []

    def test_date_type_excluded_even_if_equality(self):
        """DATE values are timestamps — rarely in MCV, partial scoping
        is usually an anti-pattern.  Not baked."""
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [("effective", IndexType.DATE, "equality", "2026-04-15")]
        probes = {("effective", "2026-04-15"): 0.001}
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == []


class TestBuildHybridBundle:
    """_build_hybrid_bundle for MIXED shapes yields btree + N GIN members."""

    def test_single_btree_plus_single_keyword(self):
        from plone.pgcatalog.suggestions import _build_hybrid_bundle

        filter_fields = [
            ("portal_type", IndexType.FIELD, "equality", "Event"),
            ("Subject", IndexType.KEYWORD, "equality", "AT26"),
        ]
        bundle = _build_hybrid_bundle(filter_fields, None, [], {})
        assert bundle is not None
        assert bundle.shape_classification == "MIXED"
        assert len(bundle.members) == 2
        roles = {m.role for m in bundle.members}
        assert roles == {"btree_composite", "plain_gin"}

    def test_partial_scoping_wraps_gin_not_btree(self):
        """Partial WHERE baked into GIN member, NOT into btree member."""
        from plone.pgcatalog.suggestions import _build_hybrid_bundle

        filter_fields = [
            ("portal_type", IndexType.FIELD, "equality", "Event"),
            ("Subject", IndexType.KEYWORD, "equality", "AT26"),
        ]
        where_terms = ["idx->>'portal_type' = 'Event'"]
        bundle = _build_hybrid_bundle(filter_fields, None, where_terms, {})
        btree_member = next(m for m in bundle.members if m.role == "btree_composite")
        gin_member = next(m for m in bundle.members if m.role == "partial_gin")
        assert "idx->>'portal_type' = 'Event'" not in btree_member.ddl
        assert "idx->>'portal_type' = 'Event'" in gin_member.ddl

    def test_multiple_keywords_produce_multiple_members(self):
        from plone.pgcatalog.suggestions import _build_hybrid_bundle

        filter_fields = [
            ("portal_type", IndexType.FIELD, "equality", "Event"),
            ("Subject", IndexType.KEYWORD, "equality", "A"),
            ("tags", IndexType.KEYWORD, "equality", "B"),
        ]
        bundle = _build_hybrid_bundle(filter_fields, None, [], {})
        assert len(bundle.members) == 3
        gin_fields = [m.fields[0] for m in bundle.members if m.role.endswith("gin")]
        assert set(gin_fields) == {"Subject", "tags"}

    def test_sort_covering_on_btree_member(self):
        from plone.pgcatalog.suggestions import _build_hybrid_bundle

        filter_fields = [
            ("portal_type", IndexType.FIELD, "equality", "Event"),
            ("Subject", IndexType.KEYWORD, "equality", "AT26"),
        ]
        sort_field = ("effective", IndexType.DATE)
        bundle = _build_hybrid_bundle(filter_fields, sort_field, [], {})
        btree_member = next(m for m in bundle.members if m.role == "btree_composite")
        assert "effective" in btree_member.fields
        assert "ORDER BY effective" in btree_member.reason


class TestIssue122AT26Regression:
    """Canonical slow query from #122 gap-analysis comment:
        portal_type=Event + review_state=published + Subject=AT26
        + effective<=now + expires>=now + sort_on=effective
    Expected: ONE MIXED bundle with btree composite + partial GIN.
    """

    def _reg_at26(self):
        return _reg(
            portal_type=IndexType.FIELD,
            review_state=IndexType.FIELD,
            Subject=IndexType.KEYWORD,
            effective=IndexType.DATE,
            expires=IndexType.DATE,
        )

    def test_at26_both_filters_low_selectivity(self):
        from plone.pgcatalog.suggestions import _probe_cache
        from plone.pgcatalog.suggestions import _reset_probe_cache
        from plone.pgcatalog.suggestions import suggest_indexes

        _reset_probe_cache()
        _probe_cache[("portal_type", "Event")] = 0.05
        _probe_cache[("review_state", "published")] = 0.03

        registry = self._reg_at26()
        bundles = suggest_indexes(
            [
                "portal_type",
                "review_state",
                "Subject",
                "effective",
                "expires",
            ],
            {
                "portal_type": "Event",
                "review_state": "published",
                "Subject": "AT26",
                "effective": {"query": "now", "range": "max"},
                "expires": {"query": "now", "range": "min"},
                "sort_on": "effective",
            },
            registry,
            {},
            conn=object(),
        )
        mixed = [b for b in bundles if b.shape_classification == "MIXED"]
        assert len(mixed) == 1
        b = mixed[0]
        roles = {m.role for m in b.members}
        assert roles == {"btree_composite", "partial_gin"}
        gin = next(m for m in b.members if m.role == "partial_gin")
        assert "idx->'Subject'" in gin.ddl
        assert "idx->>'portal_type' = 'Event'" in gin.ddl
        assert "idx->>'review_state' = 'published'" in gin.ddl

    def test_at26_only_portal_type_below_threshold(self):
        from plone.pgcatalog.suggestions import _probe_cache
        from plone.pgcatalog.suggestions import _reset_probe_cache
        from plone.pgcatalog.suggestions import suggest_indexes

        _reset_probe_cache()
        _probe_cache[("portal_type", "Event")] = 0.05
        _probe_cache[("review_state", "published")] = 0.25

        registry = self._reg_at26()
        bundles = suggest_indexes(
            [
                "portal_type",
                "review_state",
                "Subject",
                "effective",
                "expires",
            ],
            {
                "portal_type": "Event",
                "review_state": "published",
                "Subject": "AT26",
                "effective": {"query": "now", "range": "max"},
                "expires": {"query": "now", "range": "min"},
                "sort_on": "effective",
            },
            registry,
            {},
            conn=object(),
        )
        mixed = [b for b in bundles if b.shape_classification == "MIXED"]
        assert len(mixed) == 1
        gin = next(m for m in mixed[0].members if m.role == "partial_gin")
        assert "idx->>'portal_type' = 'Event'" in gin.ddl
        assert "review_state" not in gin.ddl

    def test_at26_no_probes_degrades_to_plain_gin(self):
        """conn=None -> probes return 1.0 -> no partial scoping -> plain GIN."""
        from plone.pgcatalog.suggestions import _reset_probe_cache
        from plone.pgcatalog.suggestions import suggest_indexes

        _reset_probe_cache()
        registry = self._reg_at26()
        bundles = suggest_indexes(
            [
                "portal_type",
                "review_state",
                "Subject",
                "effective",
                "expires",
            ],
            {"portal_type": "Event", "review_state": "published", "Subject": "AT26"},
            registry,
            {},
            conn=None,
        )
        mixed = [b for b in bundles if b.shape_classification == "MIXED"]
        assert len(mixed) == 1
        gin = next(m for m in mixed[0].members if m.role.endswith("gin"))
        assert gin.role == "plain_gin"
        assert "idx->>'portal_type'" not in gin.ddl
