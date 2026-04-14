# Suggestions Engine PR 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the suggestion engine's field-categorization constants into purpose-specific sets, expand `effectiveRange` to a real date column, and append the query's `sort_on` field as a trailing covering column in composite suggestions — so Slow Queries that previously produced no suggestion (issue #122) get an actionable, planner-covering index.

**Architecture:** Pure-function edits to `src/plone/pgcatalog/suggestions.py` (signature change + new helpers + constant split), one SQL change in `src/plone/pgcatalog/catalog.py` to fetch a representative params blob per slow-query group via LATERAL, and test updates/additions in `tests/test_suggestions.py`. No DDL changes, no schema migration, no ZMI template changes.

**Tech Stack:** Python 3.11+, psycopg 3 (dict_row), PostgreSQL 17 extended statistics already in place from PR 1, pytest, ZCatalog IndexRegistry.

**Spec:** `docs/superpowers/specs/2026-04-13-suggestions-engine-pr2-design.md` (committed as `cb8ab7b`).

---

## File Structure

**Modified:**
- `src/plone/pgcatalog/suggestions.py` — constant split, signature change, sort extraction, covering-composite logic
- `src/plone/pgcatalog/catalog.py:1175-1218` — SQL gains LATERAL subquery; params is passed through to `suggest_indexes`
- `tests/test_suggestions.py` — existing call sites migrate to new 4-arg signature; 10 new tests added
- `CHANGES.md` — changelog entry under unreleased

**Not touched:**
- `schema.py`, `startup.py`, `pending.py`, `processor.py`, `pool.py` — unaffected
- DDL templates (`_btree_expr`, `_gin_expr`)
- `_check_covered` / `_normalize_idx_expr`
- `pgcatalog_slow_queries` table schema
- ZMI DTML templates

---

## Task 1: Add `params` parameter to `suggest_indexes()` (no behavioral change)

**Why first:** A pure signature change with default-None behavior is low-risk and unblocks all subsequent tasks. Keeps each subsequent test-driven task focused on one behavioral change.

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (signature + docstring)
- Modify: `src/plone/pgcatalog/catalog.py:1175-1218` (pass `None` at the new position)
- Modify: `tests/test_suggestions.py` (update all call sites; all existing tests still pass)

- [ ] **Step 1: Update `suggest_indexes` signature and docstring**

Edit `src/plone/pgcatalog/suggestions.py` at the `def suggest_indexes(` signature. Change from:

```python
def suggest_indexes(query_keys, registry, existing_indexes):
    """Generate index suggestions for a set of slow-query field keys.

    Pure function — no DB access.

    Args:
        query_keys: list of catalog query field names
        registry: IndexRegistry instance (has .items() returning
            name -> (IndexType, idx_key, source_attrs))
        existing_indexes: dict {index_name: index_def_sql} from
            get_existing_indexes()
```

to:

```python
def suggest_indexes(query_keys, params, registry, existing_indexes):
    """Generate index suggestions for a set of slow-query field keys.

    Pure function — no DB access.

    Args:
        query_keys: list of catalog query field names
        params: dict of representative query params (value of the
            slowest observed invocation of this query-key group), or
            None when no representative is available.  Used to extract
            a sort_on value for covering-composite suggestions.
        registry: IndexRegistry instance (has .items() returning
            name -> (IndexType, idx_key, source_attrs))
        existing_indexes: dict {index_name: index_def_sql} from
            get_existing_indexes()
```

The function body does not yet read `params` — just accept it.

- [ ] **Step 2: Update the single production call site in catalog.py**

Edit `src/plone/pgcatalog/catalog.py` around line 1215. Change:

```python
"suggestions": suggest_indexes(keys, registry, existing),
```

to:

```python
"suggestions": suggest_indexes(keys, None, registry, existing),
```

(Task 6 will replace the `None` with the real representative params from a LATERAL subquery. This step only migrates the signature.)

- [ ] **Step 3: Update all existing test call sites**

Edit `tests/test_suggestions.py`. Every call of the form `suggest_indexes(<keys>, <registry>, <existing>)` becomes `suggest_indexes(<keys>, None, <registry>, <existing>)`.

There are 23 occurrences in `TestSuggestIndexes`. Use a targeted `replace_all` substitution on each pattern. The simplest approach: in each test, locate `suggest_indexes(` and insert `None, ` after the first argument's closing structure.

Verified occurrences (line numbers from the read):
- Line 32: `suggest_indexes(["portal_type"], registry, {})`
- Line 43: `suggest_indexes(["portal_type", "Creator"], registry, {})`
- Line 56: `suggest_indexes(["a", "b", "c", "d"], registry, {})`
- Line 67: `suggest_indexes(["portal_type", "Subject"], registry, {})`
- Line 78: `suggest_indexes(["portal_type", "Title"], registry, {})`
- Line 85: `suggest_indexes(["modified"], registry, {})`
- Line 91: `suggest_indexes(["is_folderish"], registry, {})`
- Line 106: `suggest_indexes(["portal_type", "exclude_from_nav"], registry, {})`
- Line 121: `suggest_indexes(["UID"], registry, {})`
- Line 127: `suggest_indexes(["tgpath"], registry, {})`
- Line 134: `suggest_indexes(["custom_tags"], registry, {})`
- Line 140: `suggest_indexes(["portal_type", "sort_on", "b_size"], registry, {})`
- Line 147: `suggest_indexes(["portal_type", "unknown_field"], registry, {})`
- Line 159: `suggest_indexes(["portal_type"], registry, existing)`
- Line 172: `suggest_indexes(["portal_type"], registry, existing)`
- Line 179: `suggest_indexes(["allowedRolesAndUsers"], registry, {})`
- Line 187: `suggest_indexes(["object_provides"], registry, {})`
- Line 194: `suggest_indexes(["Subject"], registry, {})`
- Line 200: `suggest_indexes([], registry, {})`
- Line 205: `suggest_indexes(["sort_on", "b_size"], registry, {})`
- Line 214: `suggest_indexes(["review_state", "UID"], registry, {})`
- Line 222: `suggest_indexes(["portal_type"], registry, {})`
- Line 230: `suggest_indexes(["portal_type", "effectiveRange"], registry, {})`
- Line 237: `suggest_indexes(["getObjPositionInParent"], registry, {})`
- Line 256: `suggest_indexes(["Language"], registry, existing)`
- Line 284: `suggest_indexes(["Language", "portal_type", "end"], registry, existing)`

For each: rewrite the call so the second arg is `None`. Existing registry becomes arg 3, existing existing becomes arg 4. Example transform:

```python
# Before
result = suggest_indexes(["portal_type"], registry, {})
# After
result = suggest_indexes(["portal_type"], None, registry, {})
```

- [ ] **Step 4: Run the full suggestions test suite**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py -v`
Expected: all 31 tests pass unchanged (28 `TestSuggestIndexes` + 5 `TestNormalizeIdxExpr` + 3 `TestApplyIndexPreflight`, accounting for: the 23 listed above plus 5 that do not call `suggest_indexes`).

If any fail with `TypeError: suggest_indexes() missing 1 required positional argument: 'existing_indexes'`, a call site was missed — grep `suggest_indexes(` and fix.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py src/plone/pgcatalog/catalog.py tests/test_suggestions.py
git commit -m "refactor: add params arg to suggest_indexes() (no-op pass-through)

Preparation for #122 PR 2. Plumbs the representative query params
dict through the public signature without reading it yet."
```

---

## Task 2: Replace `_NON_IDX_FIELDS` with three named constants

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (constants block + `suggest_indexes` classification loop)
- Modify: `tests/test_suggestions.py` (rename the legacy-specific test name for clarity; behavior unchanged)

- [ ] **Step 1: Write the failing test for the new categorization — pagination keys stay dropped**

In `tests/test_suggestions.py`, **add** (don't replace) this test inside `class TestSuggestIndexes`:

```python
def test_pagination_meta_dropped(self):
    """b_size / b_start are pagination-meta — never appear in suggestions."""
    registry = _reg(portal_type=IndexType.FIELD)
    result = suggest_indexes(
        ["portal_type", "b_size", "b_start"], None, registry, {}
    )
    for s in result:
        assert "b_size" not in s["fields"]
        assert "b_start" not in s["fields"]
```

Also add:

```python
def test_sort_meta_keys_dropped(self):
    """sort_on / sort_order keys (as raw keys) are dropped from the filter list."""
    registry = _reg(portal_type=IndexType.FIELD)
    result = suggest_indexes(
        ["portal_type", "sort_on", "sort_order"], None, registry, {}
    )
    for s in result:
        assert "sort_on" not in s["fields"]
        assert "sort_order" not in s["fields"]
```

- [ ] **Step 2: Run to confirm they pass against the current `_NON_IDX_FIELDS`**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py::TestSuggestIndexes::test_pagination_meta_dropped tests/test_suggestions.py::TestSuggestIndexes::test_sort_meta_keys_dropped -v`
Expected: PASS (the old constant still covers these). They are green now — they will stay green across the refactor.

- [ ] **Step 3: Replace the constant in `suggestions.py`**

Edit `src/plone/pgcatalog/suggestions.py`. Replace the `_NON_IDX_FIELDS` block (current lines 29-40):

```python
# Query-meta keys that are not index fields
_NON_IDX_FIELDS = frozenset(
    {
        "SearchableText",
        "path",
        "effectiveRange",
        "sort_on",
        "sort_order",
        "b_size",
        "b_start",
    }
)
```

with:

```python
# Pagination meta — ignored everywhere in the suggestion engine.
_PAGINATION_META = frozenset({"b_size", "b_start"})

# Sort meta keys — not a filter.  The VALUE of sort_on drives
# covering-composite construction (see _extract_sort_field).
_SORT_META = frozenset({"sort_on", "sort_order"})

# Virtual filter keys that expand to real idx columns for composite
# suggestions.  Each entry maps virtual_key -> list of (real_field,
# IndexType) tuples.  The real fields then participate in
# _add_btree_suggestions as if the query had named them directly.
_FILTER_VIRTUAL = {
    "effectiveRange": [("effective", IndexType.DATE)],
}

# Fields we deliberately skip in PR 2 — path suggestions are deferred
# to PR 3 (EXPLAIN-driven coverage); SearchableText already has a
# dedicated tsvector column and is additionally handled via
# _DEDICATED_FIELDS for the reason-string.
_SKIP_FIELDS = frozenset({"path", "SearchableText"})
```

- [ ] **Step 4: Update the classification loop in `suggest_indexes`**

In the same file, replace the body of the `for key in query_keys:` loop (current lines 117-147):

```python
for key in query_keys:
    if key in _NON_IDX_FIELDS:
        continue

    # Dedicated column check
    if key in _DEDICATED_FIELDS:
        suggestions.append(
            {
                "fields": [key],
                "field_types": [
                    reg_lookup[key].name if key in reg_lookup else "KEYWORD"
                ],
                "ddl": "",
                "status": "already_covered",
                "reason": f"Dedicated column: {_DEDICATED_FIELDS[key]}",
            }
        )
        continue

    idx_type = reg_lookup.get(key)
    if idx_type is None:
        continue  # unknown field — skip

    if idx_type in _SKIP_TYPES:
        continue

    if idx_type in _NON_COMPOSITE_TYPES:
        # KEYWORD / TEXT get their own suggestion
        _add_standalone_suggestion(key, idx_type, existing_indexes, suggestions)
    else:
        btree_fields.append((key, idx_type))
```

with:

```python
for key in query_keys:
    # Pagination/sort meta keys are never filter columns.
    if key in _PAGINATION_META or key in _SORT_META:
        continue

    # Virtual filter fields (e.g. effectiveRange) expand to their
    # real date/text contributors.  The expansion participates in the
    # btree composite the same way a direct key would.
    if key in _FILTER_VIRTUAL:
        for real_field, real_type in _FILTER_VIRTUAL[key]:
            btree_fields.append((real_field, real_type))
        continue

    # Dedicated column check comes BEFORE the skip set so SearchableText
    # emits its "dedicated column" reason rather than silently vanishing.
    if key in _DEDICATED_FIELDS:
        suggestions.append(
            {
                "fields": [key],
                "field_types": [
                    reg_lookup[key].name if key in reg_lookup else "KEYWORD"
                ],
                "ddl": "",
                "status": "already_covered",
                "reason": f"Dedicated column: {_DEDICATED_FIELDS[key]}",
            }
        )
        continue

    # Explicitly skipped fields (path — deferred to PR 3).
    if key in _SKIP_FIELDS:
        continue

    idx_type = reg_lookup.get(key)
    if idx_type is None:
        continue  # unknown field — skip

    if idx_type in _SKIP_TYPES:
        continue

    if idx_type in _NON_COMPOSITE_TYPES:
        # KEYWORD / TEXT get their own suggestion
        _add_standalone_suggestion(key, idx_type, existing_indexes, suggestions)
    else:
        btree_fields.append((key, idx_type))
```

- [ ] **Step 5: Run the full test suite**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py -v`
Expected: all 33 tests pass (31 previous + 2 new from Step 1).

The existing test `test_date_range_excluded` (line 227) now exercises the new `_FILTER_VIRTUAL` expansion: when `effectiveRange` is in keys alongside `portal_type`, the result no longer contains `"effectiveRange"` in any `s["fields"]` (it expanded to `"effective"`). The assertion `assert "effectiveRange" not in s["fields"]` still holds — `"effectiveRange"` itself is never returned.

The existing test `test_non_idx_fields_filtered` (line 138) still passes — `sort_on` and `b_size` are dropped by the new `_PAGINATION_META`/`_SORT_META` paths.

If `test_date_range_excluded` fails with an assertion like `"effective" in s["fields"]`, that's expected — but the existing test asserts only on `"effectiveRange"`, not `"effective"`, so it passes.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "refactor: split _NON_IDX_FIELDS into purpose-specific constants

Replaces the vague _NON_IDX_FIELDS frozenset with _PAGINATION_META,
_SORT_META, _FILTER_VIRTUAL, and _SKIP_FIELDS.  _FILTER_VIRTUAL now
expands effectiveRange to ('effective', DATE) so composite btree
suggestions can incorporate published-content date predicates.

Refs #122."
```

---

## Task 3: effectiveRange expansion — assert the new positive behavior

**Why separate:** Task 2 made expansion structurally possible; this task pins it down with a positive test and proves a composite is produced.

**Files:**
- Modify: `tests/test_suggestions.py` (two new tests)

- [ ] **Step 1: Write the failing tests**

Add to `class TestSuggestIndexes`:

```python
def test_effective_range_expands_to_effective(self):
    """effectiveRange in query keys yields a composite mentioning effective."""
    registry = _reg(portal_type=IndexType.FIELD)
    result = suggest_indexes(
        ["portal_type", "effectiveRange"], None, registry, {}
    )
    new = [s for s in result if s["status"] == "new"]
    assert len(new) == 1
    # The composite should include the 'effective' DATE contributor
    # via pgcatalog_to_timestamptz, alongside portal_type.
    assert "portal_type" in new[0]["fields"]
    assert "effective" in new[0]["fields"]
    assert "pgcatalog_to_timestamptz(idx->>'effective')" in new[0]["ddl"]

def test_effective_range_narrow_no_expires(self):
    """Narrow expansion — expires is NOT added to the composite."""
    registry = _reg(portal_type=IndexType.FIELD)
    result = suggest_indexes(
        ["portal_type", "effectiveRange"], None, registry, {}
    )
    for s in result:
        assert "expires" not in s["fields"]
        assert "'expires'" not in s["ddl"]
```

- [ ] **Step 2: Run — both should pass after Task 2**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py::TestSuggestIndexes::test_effective_range_expands_to_effective tests/test_suggestions.py::TestSuggestIndexes::test_effective_range_narrow_no_expires -v`
Expected: PASS (Task 2 already wired `_FILTER_VIRTUAL` with `("effective", DATE)` only).

- [ ] **Step 3: Commit**

```bash
git add tests/test_suggestions.py
git commit -m "test: pin effectiveRange expansion behavior

Asserts effectiveRange expands to effective (and not expires) in
composite suggestions.  Refs #122."
```

---

## Task 4: Extract sort field from params (`_extract_sort_field` helper)

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (new private helper, no callers yet)
- Modify: `tests/test_suggestions.py` (new unit tests for the helper)

- [ ] **Step 1: Write the failing tests for the helper**

Add a new test class at the bottom of `tests/test_suggestions.py`, after `class TestApplyIndexPreflight`:

```python
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
        assert _extract_sort_field(
            {"sort_on": "getObjPositionInParent"}, registry
        ) is None
```

- [ ] **Step 2: Run to verify the tests fail**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py::TestExtractSortField -v`
Expected: FAIL with `ImportError: cannot import name '_extract_sort_field'`.

- [ ] **Step 3: Implement `_extract_sort_field` in `suggestions.py`**

In `src/plone/pgcatalog/suggestions.py`, add this function just above `def _add_standalone_suggestion(`:

```python
def _extract_sort_field(params, registry):
    """Return ``(field_name, IndexType)`` for a composite-eligible sort
    column, or ``None``.

    Plone emits the sort key under various param names — plain
    ``sort_on`` for direct catalog searches, ``p_sort_on_1`` for
    restapi-generated queries, etc.  Substring matching on
    ``"sort_on"`` is the pragmatic fit.

    Only btree-composite-eligible types are returned — KEYWORD, TEXT,
    GOPIP, and DATE_RANGE cannot be trailing columns of a btree index.
    """
    if not params:
        return None

    sort_value = None
    for param_name, value in params.items():
        if "sort_on" in param_name:
            sort_value = value
            break
    if not sort_value:
        return None

    # Registry lookup.  items() returns name -> (IndexType, idx_key, source_attrs).
    for name, (idx_type, _idx_key, _source_attrs) in registry.items():
        if name == sort_value:
            if idx_type in _NON_COMPOSITE_TYPES:
                return None
            if idx_type in _SKIP_TYPES:
                return None
            return (name, idx_type)
    return None
```

- [ ] **Step 4: Run the helper tests**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py::TestExtractSortField -v`
Expected: all 7 PASS.

- [ ] **Step 5: Run the full suite to confirm no regressions**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py -v`
Expected: all prior tests still pass (no caller yet).

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: add _extract_sort_field helper for covering composites

Pure function — looks up the sort_on value (under its various
Plone-emitted param names) in the registry and returns a
(field_name, IndexType) tuple when it can serve as a trailing
btree composite column.  Not yet wired into suggest_indexes.

Refs #122."
```

---

## Task 5: Wire sort field into `_add_btree_suggestions` (covering trailing column)

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (`_add_btree_suggestions` signature + body; `suggest_indexes` call site)
- Modify: `tests/test_suggestions.py` (new covering-composite tests + issue-122 regression)

- [ ] **Step 1: Write the failing test — sort appends as trailing column**

Add to `class TestSuggestIndexes`:

```python
def test_sort_on_appends_trailing_column(self):
    """sort_on in params appends the sort field as the last composite column."""
    registry = _reg(
        portal_type=IndexType.FIELD,
        effective=IndexType.DATE,
    )
    result = suggest_indexes(
        ["portal_type"],
        {"sort_on": "effective"},
        registry,
        {},
    )
    new = [s for s in result if s["status"] == "new"]
    assert len(new) == 1
    # portal_type is the filter column; effective is appended covering.
    assert new[0]["fields"] == ["portal_type", "effective"]
    ddl = new[0]["ddl"]
    # The last expression in the composite must be the DATE cover column.
    assert ddl.index("pgcatalog_to_timestamptz(idx->>'effective')") > \
        ddl.index("(idx->>'portal_type')")
    assert "ORDER BY effective" in new[0]["reason"]

def test_sort_on_deduped_when_already_leading(self):
    """Sort field already present as filter column — not appended twice."""
    registry = _reg(
        portal_type=IndexType.FIELD,
        effective=IndexType.DATE,
    )
    result = suggest_indexes(
        ["portal_type", "effectiveRange"],  # effectiveRange expands to effective
        {"sort_on": "effective"},
        registry,
        {},
    )
    new = [s for s in result if s["status"] == "new"]
    assert len(new) == 1
    # effective appears exactly once in the fields list
    assert new[0]["fields"].count("effective") == 1

def test_sort_on_ignored_for_non_composite_type(self):
    """Sort on a TEXT field does not add a trailing column."""
    registry = _reg(
        portal_type=IndexType.FIELD,
        Title=IndexType.TEXT,
    )
    result = suggest_indexes(
        ["portal_type"],
        {"sort_on": "Title"},
        registry,
        {},
    )
    new = [s for s in result if s["status"] == "new" and "portal_type" in s["fields"]]
    # Only a single-column btree suggestion for portal_type — no Title.
    assert all("Title" not in s["fields"] for s in new)

def test_sort_on_unknown_field_ignored(self):
    """Sort on an unregistered field produces no covering column, no crash."""
    registry = _reg(portal_type=IndexType.FIELD)
    result = suggest_indexes(
        ["portal_type"],
        {"sort_on": "not_in_registry"},
        registry,
        {},
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
    result = suggest_indexes(
        ["a", "b", "c"],
        {"sort_on": "effective"},
        registry,
        {},
    )
    new = [s for s in result if s["status"] == "new"]
    assert len(new) == 1
    assert len(new[0]["fields"]) == 3
    # effective must be the trailing element.
    assert new[0]["fields"][-1] == "effective"

def test_issue_122_pattern(self):
    """Regression for #122: portal_type + effectiveRange + sort_on=effective.

    Prior behavior: no suggestion emitted because the effectiveRange
    and sort_on keys were both in _NON_IDX_FIELDS.  Expected now: one
    composite (portal_type, effective) with effective as covering column.
    """
    registry = _reg(
        portal_type=IndexType.FIELD,
        effective=IndexType.DATE,
    )
    result = suggest_indexes(
        ["portal_type", "effectiveRange"],
        {"sort_on": "effective"},
        registry,
        {},
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
    r_none = suggest_indexes(["portal_type"], None, registry, {})
    r_empty = suggest_indexes(["portal_type"], {}, registry, {})
    # Same DDL, same fields, same status for both.
    assert [s["ddl"] for s in r_none] == [s["ddl"] for s in r_empty]
```

- [ ] **Step 2: Run to confirm they fail**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py::TestSuggestIndexes::test_sort_on_appends_trailing_column tests/test_suggestions.py::TestSuggestIndexes::test_issue_122_pattern -v`
Expected: FAIL — sort field is not yet read by `_add_btree_suggestions`.

- [ ] **Step 3: Add a module-level constant for the cap**

In `src/plone/pgcatalog/suggestions.py`, just below the `_SELECTIVITY_ORDER` definition, add:

```python
# Hard cap on columns in a composite btree suggestion — beyond three
# the write-amplification cost outweighs read savings in real Plone
# catalogs.  Sort covering column counts against this cap.
_MAX_COMPOSITE_COLUMNS = 3
```

- [ ] **Step 4: Rewrite `_add_btree_suggestions` to accept a sort field**

Replace the current `_add_btree_suggestions` (lines 189-227) with:

```python
def _add_btree_suggestions(btree_fields, sort_field, existing_indexes, suggestions):
    """Add a btree suggestion — single column or composite.

    Args:
        btree_fields: list of ``(field_name, IndexType)`` tuples for
            filter columns discovered in the query.  Order is the
            traversal order of query_keys — this function sorts by
            selectivity.
        sort_field: ``(field_name, IndexType)`` for a trailing covering
            column, or None.  Makes the planner skip the ORDER BY sort
            step when the leading filter columns have equality predicates.
        existing_indexes: dict {name: indexdef} from get_existing_indexes.
        suggestions: output list to append the resulting dict to.
    """
    # Sort filters by selectivity (most selective first).
    btree_fields = sorted(btree_fields, key=lambda ft: _SELECTIVITY_ORDER.get(ft[1], 99))

    # Reserve one slot for sort_field if present.  Cap stays 3 total.
    if sort_field is not None:
        filter_cap = _MAX_COMPOSITE_COLUMNS - 1
    else:
        filter_cap = _MAX_COMPOSITE_COLUMNS
    fields_limited = btree_fields[:filter_cap]

    # Build the ordered column list: filters first, sort trailing.
    # Dedupe: if sort_field's name is already a filter column, don't
    # repeat it — the leading position already satisfies ORDER BY when
    # the remaining columns have equality predicates.
    ordered = list(fields_limited)
    sort_covering = False
    if sort_field is not None:
        existing_names = {f for f, _t in ordered}
        if sort_field[0] not in existing_names:
            ordered.append(sort_field)
            sort_covering = True

    # Empty after dedupe (shouldn't happen in practice — caller gates
    # on `btree_fields` truthy — but guard anyway).
    if not ordered:
        return

    field_names = [f for f, _t in ordered]
    if len(ordered) == 1:
        field, idx_type = ordered[0]
        expr = _btree_expr(field, idx_type)
        name = f"idx_os_sug_{field}"
        ddl = (
            f"CREATE INDEX CONCURRENTLY {name} "
            f"ON object_state ({expr}) WHERE idx IS NOT NULL"
        )
        reason = f"Btree index for {idx_type.name} field '{field}'"
    else:
        exprs = [_btree_expr(f, t) for f, t in ordered]
        name = "idx_os_sug_" + "_".join(field_names)
        cols = ", ".join(exprs)
        ddl = (
            f"CREATE INDEX CONCURRENTLY {name} "
            f"ON object_state ({cols}) WHERE idx IS NOT NULL"
        )
        types_str = " + ".join(t.name for _f, t in ordered)
        reason = f"Composite btree ({types_str}) for {len(ordered)} fields"
    if sort_covering:
        reason += f"; last column covers ORDER BY {sort_field[0]}"

    status = _check_covered(ddl, existing_indexes)
    suggestions.append(
        {
            "fields": field_names,
            "field_types": [t.name for _f, t in ordered],
            "ddl": ddl,
            "status": status,
            "reason": reason if status == "new" else f"Already covered: {reason}",
        }
    )
```

- [ ] **Step 5: Update the call in `suggest_indexes`**

In `suggest_indexes`, replace the final block (currently:

```python
    # Build composite from btree-eligible fields
    if btree_fields:
        _add_btree_suggestions(btree_fields, existing_indexes, suggestions)

    return suggestions
```

) with:

```python
    # Extract sort field for covering trailing column (if any).
    sort_field = _extract_sort_field(params, registry)

    # Build composite from btree-eligible fields plus optional sort cover.
    if btree_fields or sort_field is not None:
        # When there are no filter columns but there IS a sort field,
        # we do not emit a sort-only suggestion — a btree on sort alone
        # does not accelerate a filter-less query in a meaningful way.
        # Require at least one filter column.
        if btree_fields:
            _add_btree_suggestions(
                btree_fields, sort_field, existing_indexes, suggestions
            )

    return suggestions
```

- [ ] **Step 6: Run the new and regression tests**

Run: `cd sources/plone-pgcatalog && uv run pytest tests/test_suggestions.py -v`
Expected: all tests pass — the 7 new ones from Step 1 plus all prior tests.

Pay attention to `test_selectivity_ordering` (line 208): the btree field sort is now done on a copy (`btree_fields = sorted(...)`), not in-place. The test only checks the final output ordering, which is unchanged, so it still passes.

Pay attention to `test_max_three_fields_in_composite` (line 49): the cap is now `_MAX_COMPOSITE_COLUMNS = 3` and the assertion `len(s["fields"]) <= 3` still holds.

- [ ] **Step 7: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: append sort_on field as trailing covering composite column

When the representative slow-query params carry a sort_on value
whose target is btree-composite-eligible, the suggestion engine
now appends it as the last column of the composite so the planner
can skip the ORDER BY sort step.

Closes the issue-#122 slow-query suggestion gap for the canonical
portal_type + effectiveRange + sort_on=effective pattern.

Refs #122."
```

---

## Task 6: Fetch representative params via LATERAL subquery in `manage_get_slow_query_stats`

**Files:**
- Modify: `src/plone/pgcatalog/catalog.py:1175-1218`

- [ ] **Step 1: Replace the SQL with a LATERAL-joined form**

Edit `src/plone/pgcatalog/catalog.py`. Replace the cursor execute block and the result loop (lines 1187-1218):

```python
                with pg_conn.cursor() as cur:
                    cur.execute(
                        "SELECT query_keys, "
                        "  COUNT(*) AS cnt, "
                        "  ROUND(AVG(duration_ms)::numeric, 1) AS avg_ms, "
                        "  ROUND(MAX(duration_ms)::numeric, 1) AS max_ms, "
                        "  MAX(created_at) AS last_seen "
                        "FROM pgcatalog_slow_queries "
                        "GROUP BY query_keys "
                        "ORDER BY max_ms DESC "
                        "LIMIT 50"
                    )
                    rows = cur.fetchall()
            finally:
                pool.putconn(pg_conn)
        except Exception:
            return []

        result = []
        for row in rows:
            keys = row["query_keys"]
            result.append(
                {
                    "query_keys": ", ".join(keys),
                    "count": row["cnt"],
                    "avg_ms": float(row["avg_ms"]),
                    "max_ms": float(row["max_ms"]),
                    "last_seen": str(row["last_seen"])[:19],
                    "suggestions": suggest_indexes(keys, None, registry, existing),
                }
            )
        return result
```

with:

```python
                with pg_conn.cursor() as cur:
                    cur.execute(
                        "SELECT grp.query_keys, "
                        "  grp.cnt, "
                        "  grp.avg_ms, "
                        "  grp.max_ms, "
                        "  grp.last_seen, "
                        "  slowest.params AS representative_params "
                        "FROM ( "
                        "  SELECT query_keys, "
                        "    COUNT(*) AS cnt, "
                        "    ROUND(AVG(duration_ms)::numeric, 1) AS avg_ms, "
                        "    ROUND(MAX(duration_ms)::numeric, 1) AS max_ms, "
                        "    MAX(created_at) AS last_seen "
                        "  FROM pgcatalog_slow_queries "
                        "  GROUP BY query_keys "
                        "  ORDER BY max_ms DESC "
                        "  LIMIT 50 "
                        ") grp "
                        "LEFT JOIN LATERAL ( "
                        "  SELECT params "
                        "  FROM pgcatalog_slow_queries s "
                        "  WHERE s.query_keys = grp.query_keys "
                        "  ORDER BY s.duration_ms DESC "
                        "  LIMIT 1 "
                        ") slowest ON TRUE "
                        "ORDER BY grp.max_ms DESC"
                    )
                    rows = cur.fetchall()
            finally:
                pool.putconn(pg_conn)
        except Exception:
            return []

        result = []
        for row in rows:
            keys = row["query_keys"]
            params = row["representative_params"]
            result.append(
                {
                    "query_keys": ", ".join(keys),
                    "count": row["cnt"],
                    "avg_ms": float(row["avg_ms"]),
                    "max_ms": float(row["max_ms"]),
                    "last_seen": str(row["last_seen"])[:19],
                    "suggestions": suggest_indexes(
                        keys, params, registry, existing
                    ),
                }
            )
        return result
```

- [ ] **Step 2: Run the affected tests**

This method is integration-tested by `tests/test_slow_queries.py`. Run:

```bash
cd sources/plone-pgcatalog && uv run pytest tests/test_slow_queries.py -v
```

Expected: tests pass. The outer SELECT list preserves the column names `query_keys`, `cnt`, `avg_ms`, `max_ms`, `last_seen` — only the subquery structure changed plus one new `representative_params` column. If any test asserts on the returned shape and fails because of the new column, update the test to tolerate the extra key (do not remove it).

- [ ] **Step 3: Run the full project test suite**

Run: `cd sources/plone-pgcatalog && uv run pytest -v`
Expected: all tests pass (688+ from the project memory baseline, plus the new 9-10 in `test_suggestions.py`).

- [ ] **Step 4: Commit**

```bash
git add src/plone/pgcatalog/catalog.py
git commit -m "feat: fetch representative params for slow-query suggestions via LATERAL

Each query-key group now pulls the params blob of its slowest row
and passes it to suggest_indexes, enabling sort_on-driven covering
composite suggestions for issue #122 patterns.

Refs #122."
```

---

## Task 7: CHANGES.md entry

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Read the current top of CHANGES.md to match style**

Run: `head -40 sources/plone-pgcatalog/CHANGES.md`

The most recent release header gives the style — typically `## 1.0.0b51 (YYYY-MM-DD)` followed by a bullet list. The current unreleased entries (if any) sit above the last released header.

- [ ] **Step 2: Check the real next version on GitHub releases**

Run: `gh release list --repo bluedynamics/plone-pgcatalog --limit 5`

This avoids inferring from stale CHANGES headers. The next version for PR 2 is the next beta after the latest released tag.

- [ ] **Step 3: Add the changelog bullet**

Edit `CHANGES.md`. Under the current unreleased-changes block (or create one if none exists), add:

```markdown
- Slow-query suggestions now produce covering composite indexes for the
  common `portal_type + effectiveRange + sort_on=effective` pattern
  (issue #122).  The suggestion engine splits the legacy `_NON_IDX_FIELDS`
  into purpose-specific constants, expands `effectiveRange` to its
  `effective` date contributor, and appends the query's `sort_on` field
  as a trailing btree composite column so the planner can skip the
  ORDER BY sort step.
```

- [ ] **Step 4: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog for PR 2 suggestions engine (#122)"
```

---

## Task 8: End-to-end verification

- [ ] **Step 1: Run the full test suite one final time**

Run: `cd sources/plone-pgcatalog && uv run pytest -v`
Expected: green. No unexpected skips, no warnings about the new code paths.

- [ ] **Step 2: Lint / ruff**

Run: `cd sources/plone-pgcatalog && uv run ruff check src/plone/pgcatalog/suggestions.py src/plone/pgcatalog/catalog.py tests/test_suggestions.py`
Expected: no warnings. Fix any flagged issues.

- [ ] **Step 3: Exercise the suggestion flow in a live instance (manual smoke test)**

1. Start the Zope instance with `.venv/bin/runwsgi instance/etc/zope.ini`.
2. Log in as admin.
3. Generate a slow query matching the issue-#122 pattern: via `unrestrictedSearchResults({"portal_type": "Document", "effectiveRange": DateTime(), "sort_on": "effective"})` from a Script (Python) or `pdbpp` console.
4. Open the Slow Queries ZMI tab (`@@manage_slow_queries` or the registered view path).
5. Verify: the row for that query now shows a non-empty "Suggestions" section whose DDL reads
   `CREATE INDEX CONCURRENTLY idx_os_sug_portal_type_effective ON object_state ((idx->>'portal_type'), pgcatalog_to_timestamptz(idx->>'effective')) WHERE idx IS NOT NULL`
   and whose reason mentions `covers ORDER BY effective`.

If the Suggestions section is still empty, inspect `registry` (the Plone catalog must have an `effective` date index registered — it's standard in all Plone sites).

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin <branch>
gh pr create --title "Suggestions engine PR 2: field split + covering composites (#122)" --body "$(cat <<'EOF'
## Summary
- Replaces `_NON_IDX_FIELDS` with `_PAGINATION_META`, `_SORT_META`, `_FILTER_VIRTUAL`, `_SKIP_FIELDS`.
- Expands `effectiveRange` to `('effective', DATE)` so published-content queries participate in composite suggestions.
- Appends the query's `sort_on` field as a trailing covering column when btree-composite-eligible (cap stays 3 total columns).
- Plumbs the representative params blob through `manage_get_slow_query_stats` via LATERAL.

Closes the suggestion-engine gap for the canonical Plone slow-query pattern from issue #122.

## Test plan
- [ ] Full pytest suite green
- [ ] `ruff check` clean on modified files
- [ ] Manual smoke: trigger the issue-#122 query pattern in a live instance and confirm the Slow Queries tab now shows a covering composite suggestion
- [ ] Multi-pod safety: no DDL changes, no schema migration, signature change is internal to the package (single call site in `catalog.py`)

Spec: `docs/superpowers/specs/2026-04-13-suggestions-engine-pr2-design.md`

Refs #122.
EOF
)"
```

---

## Post-merge: release

Per project convention (MEMORY.md: *Check GitHub releases before CHANGES.md edits*, *Always write changelog before tagging*):

1. Confirm `CHANGES.md` header bumped to the next beta (e.g. `1.0.0b52 (YYYY-MM-DD)`) on `main` **before** the tag.
2. Tag & release via the usual flow.
