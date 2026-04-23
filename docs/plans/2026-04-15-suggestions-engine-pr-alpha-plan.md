# Suggestions Engine PR α Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize the suggestion engine from flat btree-composite suggestions to bundles of indexes with partial GIN, hybrid btree+GIN, and data-driven partial-predicate scoping via live DB selectivity probes — closes the AT26-style suggestion gap from [issue #122](https://github.com/bluedynamics/plone-pgcatalog/issues/122).

**Architecture:** Dataclass-based `Bundle`/`BundleMember` output. Shape classifier routes to per-shape builders (BTREE_ONLY / KEYWORD_ONLY / MIXED / TEXT_ONLY / UNKNOWN). Selectivity probe uses `pg_stats.most_common_vals` first, falls back to live `COUNT(*)`, cached per request. Partial-predicate scoping bakes all qualifying equality filters (selectivity < 10 %) into the index WHERE clause.

**Tech Stack:** Python 3.11+, psycopg 3 (dict_row), PostgreSQL 17, pytest.

**Spec:** [`docs/superpowers/specs/2026-04-15-suggestions-engine-pr-alpha-design.md`](../superpowers/specs/2026-04-15-suggestions-engine-pr-alpha-design.md).
**PR β carryover:** [`docs/superpowers/specs/2026-04-15-suggestions-engine-pr-beta-notes.md`](../superpowers/specs/2026-04-15-suggestions-engine-pr-beta-notes.md).

---

## File Structure

**Modified:**
- `src/plone/pgcatalog/suggestions.py` — dataclasses, shape classifier, dispatcher, probe, partial-where builder, bundle builders (btree / GIN / hybrid), GIN existing-index detection, `suggest_indexes` signature.
- `src/plone/pgcatalog/catalog.py:~1187` — `manage_get_slow_query_stats` passes `pg_conn` to `suggest_indexes`, exposes both `suggestions` (flat, back-compat) and `suggestions_bundles` (full structure for PR β).
- `tests/test_suggestions.py` — existing tests migrate to unwrap bundles; 6 new test classes added.
- `CHANGES.md` — 1.0.0b54 unreleased entry.

**Not touched:**
- `schema.py`, `startup.py`, `pending.py`, `processor.py`, `pool.py`, ZMI templates, `pgcatalog_slow_queries` schema.
- DDL templates (`_btree_expr`, `_gin_expr`) — stay unchanged.
- `apply_index` / `drop_index` / `_DEFAULT_INDEX_TIMEOUT` / `_SAFE_NAME_RE`.

---

## Pre-work: Worktree setup

This is a multi-task implementation; run it in an isolated worktree per project convention.

- [ ] **Step 1: Create worktree**

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/plone-pgcatalog
git checkout main && git pull --ff-only
git worktree add .worktrees/feat-suggestions-pr-alpha -b feat/suggestions-pr-alpha-122
cd .worktrees/feat-suggestions-pr-alpha
```

- [ ] **Step 2: Prepare venv**

The worktree needs its own `.venv` with Plone test deps — `uv sync` alone isn't enough (it strips dev deps).

```bash
uv sync
.venv/bin/pip install pytest pytest-cov plone.app.testing zope.pytestlayer 'psycopg[binary]' httpx
```

- [ ] **Step 3: Confirm baseline**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: `52 passed` (PR 2 baseline).

**Test runner contract:** for the rest of this plan, all test commands use `.venv/bin/pytest`. Do NOT use `uv run pytest` — it re-syncs and strips dev deps.

---

## Task 1: `Bundle` and `BundleMember` dataclasses

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (add dataclasses at top)
- Modify: `tests/test_suggestions.py` (new `TestBundleTypes` class)

- [ ] **Step 1: Write the failing test**

Add at the bottom of `tests/test_suggestions.py` (after existing classes):

```python
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
        from plone.pgcatalog.suggestions import Bundle, BundleMember

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
        from plone.pgcatalog.suggestions import Bundle, BundleMember

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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBundleTypes -v
```

Expected: `ImportError` — `Bundle`/`BundleMember` don't exist yet.

- [ ] **Step 3: Add dataclasses to suggestions.py**

In `src/plone/pgcatalog/suggestions.py`, add these imports at the top of the module (just below the existing `import time`):

```python
from dataclasses import dataclass, field
from dataclasses import asdict as dc_asdict
```

Then, immediately below the `_SAFE_NAME_RE = re.compile(...)` line and before the `# ── DDL expression builders` comment, add:

```python
# ── Bundle output types ─────────────────────────────────────────────────


@dataclass(frozen=True)
class BundleMember:
    """One index in a bundle — carries its own DDL and coverage status."""

    ddl: str
    fields: list
    field_types: list
    status: str  # "new" | "already_covered"
    role: str  # "btree_composite" | "plain_gin" | "partial_gin"
    reason: str


@dataclass(frozen=True)
class Bundle:
    """One or more indexes that together address a slow-query shape.

    Single-member bundles (status-quo btree composites) back-compat
    with the existing UI via a per-row flatten step in catalog.py.
    """

    name: str
    rationale: str
    shape_classification: str  # BTREE_ONLY | KEYWORD_ONLY | MIXED | TEXT_ONLY | UNKNOWN
    members: list = field(default_factory=list)
```

Also update `__all__` to expose the new types:

```python
__all__ = [
    "Bundle",
    "BundleMember",
    "apply_index",
    "drop_index",
    "explain_query",
    "get_existing_indexes",
    "suggest_indexes",
]
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBundleTypes -v
```

Expected: `3 passed`.

- [ ] **Step 5: Run full suite to ensure no regressions**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: `55 passed` (52 baseline + 3 new).

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: add Bundle and BundleMember dataclasses

Foundational types for PR α's multi-index suggestion output.
Frozen dataclasses so the engine output is safe to pass around
and serialize. No callers yet — introduced as a separate commit
to keep the refactor surface reviewable.

Refs #122."
```

If pre-commit reformats, `git add` and re-commit with the same message.

---

## Task 2: `_extract_filter_fields` with operator classification

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (new helper function, not yet called)
- Modify: `tests/test_suggestions.py` (new `TestExtractFilterFields` class)

**Rationale:** the current `suggest_indexes` loop conflates "classify the key" with "add to output". The dispatcher needs a structured `list[(name, IndexType, operator, value)]` input. This task extracts that as a pure helper. No behavior change yet; existing path still builds its own `btree_fields` list.

- [ ] **Step 1: Write the failing tests**

Add this class to `tests/test_suggestions.py` (after `TestBundleTypes`):

```python
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

        registry = _reg()  # registry doesn't need the real effective name
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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestExtractFilterFields -v
```

Expected: `ImportError: cannot import name '_extract_filter_fields'`.

- [ ] **Step 3: Implement the helper**

In `src/plone/pgcatalog/suggestions.py`, add this function just above `def _extract_sort_field(` (the existing PR 2 helper):

```python
def _classify_operator(value):
    """Infer filter operator from a representative params value.

    Returns one of: 'equality' (scalar), 'equality_multi' (list),
    'range' (dict with 'range'), 'unknown' (anything else / None).
    """
    if isinstance(value, dict) and "range" in value:
        return "range"
    if isinstance(value, list):
        if len(value) == 1:
            # Single-element list is effectively equality.
            return "equality"
        return "equality_multi"
    if isinstance(value, (str, int, float, bool)) and value is not None:
        return "equality"
    return "unknown"


def _extract_filter_fields(query_keys, params, registry):
    """Build a structured filter-field list for shape classification.

    Returns a list of tuples ``(name, IndexType, operator, value)``.
    - ``name`` is the real index field (virtual fields like
      ``effectiveRange`` are expanded via ``_FILTER_VIRTUAL``).
    - ``operator`` is one of 'equality' | 'equality_multi' | 'range'
      | 'unknown' (when params is None or missing the key).
    - ``value`` is the scalar equality value when operator='equality',
      else ``None``.

    Pagination meta, sort meta, dedicated fields, explicitly skipped
    fields, unknown fields, and SKIP_TYPES fields are filtered out.
    Virtual field expansions carry operator='range' and value=None
    (since effectiveRange inherently denotes a date window, never an
    equality on the virtual key itself).
    """
    reg_lookup = {}
    for name, (idx_type, _idx_key, _source_attrs) in registry.items():
        reg_lookup[name] = idx_type

    out = []
    for key in query_keys:
        # Pagination / sort meta keys are never filter columns.
        if key in _PAGINATION_META or key in _SORT_META:
            continue

        # Virtual filter fields expand to their real contributors.
        if key in _FILTER_VIRTUAL:
            for real_field, real_type in _FILTER_VIRTUAL[key]:
                out.append((real_field, real_type, "range", None))
            continue

        # Dedicated columns are handled elsewhere (they emit a
        # already_covered reason but do NOT participate in the filter
        # shape).  Drop here.
        if key in _DEDICATED_FIELDS:
            continue

        # Explicitly skipped fields.
        if key in _SKIP_FIELDS:
            continue

        idx_type = reg_lookup.get(key)
        if idx_type is None:
            continue  # unknown field
        if idx_type in _SKIP_TYPES:
            continue

        value = None if params is None else params.get(key)
        op = _classify_operator(value)
        equality_value = value if op == "equality" else None
        # Unpack single-element list so AT26-like patterns with
        # ``Subject: ['AT26']`` behave like scalar equality.
        if isinstance(value, list) and len(value) == 1 and op == "equality":
            equality_value = value[0]

        out.append((key, idx_type, op, equality_value))

    return out
```

- [ ] **Step 4: Run helper tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestExtractFilterFields -v
```

Expected: `8 passed`.

- [ ] **Step 5: Run full suite**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: `63 passed` (55 + 8 new).

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: add _extract_filter_fields with operator classification

Pure helper that turns (query_keys, params, registry) into a
structured list of (name, IndexType, operator, value) tuples.
Handles virtual field expansion (effectiveRange -> effective),
equality/range/equality_multi operator inference, and all the
drop rules (pagination / sort / dedicated / skip / unknown).

Not yet wired into suggest_indexes — subsequent tasks will use it
via the new shape classifier.

Refs #122."
```

---

## Task 3: `_classify_filter_shape`

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (new helper)
- Modify: `tests/test_suggestions.py` (new `TestClassifyFilterShape` class)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_suggestions.py` (after `TestExtractFilterFields`):

```python
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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestClassifyFilterShape -v
```

Expected: `ImportError: cannot import name '_classify_filter_shape'`.

- [ ] **Step 3: Implement**

In `src/plone/pgcatalog/suggestions.py`, add just above `def _extract_sort_field(`:

```python
def _classify_filter_shape(filter_fields):
    """Route a filter-field list to one of five shape classifications.

    - BTREE_ONLY: all types ∈ {FIELD, DATE, BOOL, UUID, PATH}
    - KEYWORD_ONLY: all types == KEYWORD
    - MIXED: at least one btree-eligible + at least one KEYWORD
    - TEXT_ONLY: any TEXT filter (dominates — TEXT means tsvector)
    - UNKNOWN: empty list, or any type outside the five plus TEXT
    """
    if not filter_fields:
        return "UNKNOWN"

    types = {ft[1] for ft in filter_fields}
    if IndexType.TEXT in types:
        return "TEXT_ONLY"

    btree_eligible = {
        IndexType.FIELD,
        IndexType.DATE,
        IndexType.BOOLEAN,
        IndexType.UUID,
        IndexType.PATH,
    }
    has_btree = bool(types & btree_eligible)
    has_keyword = IndexType.KEYWORD in types

    if has_btree and has_keyword:
        return "MIXED"
    if has_btree and not has_keyword:
        # If there are types outside btree_eligible ∪ {KEYWORD,TEXT}
        # we fall through to UNKNOWN.
        remainder = types - btree_eligible
        if remainder:
            return "UNKNOWN"
        return "BTREE_ONLY"
    if has_keyword and not has_btree:
        remainder = types - {IndexType.KEYWORD}
        if remainder:
            return "UNKNOWN"
        return "KEYWORD_ONLY"
    return "UNKNOWN"
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestClassifyFilterShape -v
```

Expected: `5 passed`.

- [ ] **Step 5: Run full suite**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: `68 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: add _classify_filter_shape

Pure helper that maps a filter-field list to one of five shape
classifications (BTREE_ONLY, KEYWORD_ONLY, MIXED, TEXT_ONLY,
UNKNOWN) — the routing input for the bundle dispatcher.

Refs #122."
```

---

## Task 4: `_build_btree_bundle` + dispatcher BTREE_ONLY path

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (extract existing btree logic into `_build_btree_bundle`, add `_dispatch_templates` with BTREE_ONLY path)
- Modify: `tests/test_suggestions.py` (new `TestBuildBtreeBundle` class)

**Rationale:** wrap the existing `_add_btree_suggestions` logic in a `Bundle`-returning form. No behavior change to `suggest_indexes` yet — it still returns list[dict] by flattening the bundle. The migration of return type happens in Task 6.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_suggestions.py`:

```python
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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBuildBtreeBundle -v
```

Expected: `ImportError: cannot import name '_build_btree_bundle'`.

- [ ] **Step 3: Implement `_build_btree_bundle`**

In `src/plone/pgcatalog/suggestions.py`, add just above the existing `def _add_btree_suggestions(`:

```python
def _build_btree_bundle(filter_fields, sort_field, existing_indexes):
    """Build a single-member Bundle holding one btree composite index.

    Wraps the PR 2 btree-composite logic (selectivity ordering, 3-column
    cap including sort covering, dedupe when sort field is already a
    filter column) into the Bundle output shape.

    Returns None when filter_fields is empty.
    """
    if not filter_fields:
        return None

    # Convert filter_fields to the (name, IndexType) pairs the legacy
    # ordering logic expects — it doesn't consume operator/value here.
    btree_pairs = [(name, idx_type) for (name, idx_type, _op, _val) in filter_fields]

    # Selectivity ordering (most selective first).
    btree_pairs = sorted(
        btree_pairs, key=lambda ft: _SELECTIVITY_ORDER.get(ft[1], 99)
    )

    filter_cap = (
        _MAX_COMPOSITE_COLUMNS - 1 if sort_field is not None else _MAX_COMPOSITE_COLUMNS
    )
    fields_limited = btree_pairs[:filter_cap]

    ordered = list(fields_limited)
    sort_covering = False
    if sort_field is not None:
        existing_names = {f for f, _t in ordered}
        if sort_field[0] not in existing_names:
            ordered.append(sort_field)
            sort_covering = True

    if not ordered:
        return None

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
    member = BundleMember(
        ddl=ddl,
        fields=field_names,
        field_types=[t.name for _f, t in ordered],
        status=status,
        role="btree_composite",
        reason=reason if status == "new" else f"Already covered: {reason}",
    )

    bundle_name = "btree-" + "-".join(field_names)
    rationale = f"Btree composite for filter shape BTREE_ONLY on {', '.join(field_names)}"
    return Bundle(
        name=bundle_name,
        rationale=rationale,
        shape_classification="BTREE_ONLY",
        members=[member],
    )
```

- [ ] **Step 4: Run the bundle tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBuildBtreeBundle -v
```

Expected: `4 passed`.

- [ ] **Step 5: Run full suite — no regressions yet**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: `72 passed`. The legacy `_add_btree_suggestions` is still called by `suggest_indexes`; new helper stands alongside.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: extract _build_btree_bundle — Bundle-returning btree path

Wraps PR 2's btree-composite + sort-covering logic into a
Bundle output shape.  Not yet consumed by suggest_indexes —
that migration happens in a later commit.

Refs #122."
```

---

## Task 5: `_build_keyword_gin_bundle` + dispatcher KEYWORD_ONLY path

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (extract KEYWORD path)
- Modify: `tests/test_suggestions.py` (new `TestBuildKeywordGinBundle` class)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_suggestions.py`:

```python
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
        assert "WHERE" in m.ddl  # the base WHERE idx IS NOT NULL clause
        assert "idx->>'" not in m.ddl  # no partial predicate
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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBuildKeywordGinBundle -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement `_build_keyword_gin_bundle`**

In `src/plone/pgcatalog/suggestions.py`, add just above `def _add_standalone_suggestion(`:

```python
def _build_keyword_gin_bundle(filter_fields, partial_where_terms, existing_indexes):
    """Build a KEYWORD_ONLY Bundle — one plain or partial GIN per KEYWORD.

    Args:
        filter_fields: list of ``(name, IndexType, operator, value)`` — all
            expected to be IndexType.KEYWORD entries (caller guarantees).
        partial_where_terms: list of SQL predicate strings to AND into
            the index WHERE clause for T4 partial GIN.  Empty list → T3.
        existing_indexes: dict for coverage detection.

    Returns:
        Bundle with one BundleMember per KEYWORD filter, or None when
        filter_fields is empty.
    """
    if not filter_fields:
        return None

    keyword_fields = [
        (name, idx_type) for (name, idx_type, _op, _val) in filter_fields
        if idx_type == IndexType.KEYWORD
    ]
    if not keyword_fields:
        return None

    members = []
    for name, _idx_type in keyword_fields:
        expr = _gin_expr(name)
        base_where = f"idx IS NOT NULL AND idx ? '{name}'"
        if partial_where_terms:
            where_clause = base_where + " AND " + " AND ".join(partial_where_terms)
            role = "partial_gin"
            # Derive a distinguishing name suffix from the partial predicate keys
            scope_suffix = "_" + "_".join(
                _extract_where_key(t) for t in partial_where_terms
            )
            idx_name = f"idx_os_sug_{name}_partial{scope_suffix}"
        else:
            where_clause = base_where
            role = "plain_gin"
            idx_name = f"idx_os_sug_{name}"

        ddl = (
            f"CREATE INDEX CONCURRENTLY {idx_name} "
            f"ON object_state USING gin ({expr}) "
            f"WHERE {where_clause}"
        )
        reason_detail = (
            f"partial GIN scoped by {len(partial_where_terms)} predicate(s)"
            if partial_where_terms else "plain GIN"
        )
        reason = f"{reason_detail.capitalize()} for KEYWORD field '{name}'"
        status = _check_covered(ddl, existing_indexes)
        members.append(
            BundleMember(
                ddl=ddl,
                fields=[name],
                field_types=[IndexType.KEYWORD.name],
                status=status,
                role=role,
                reason=reason if status == "new" else f"Already covered: {reason}",
            )
        )

    bundle_name = "kwgin-" + "-".join(f for f, _ in keyword_fields)
    rationale = (
        f"GIN indexes for KEYWORD filter shape on "
        f"{', '.join(f for f, _ in keyword_fields)}"
    )
    if partial_where_terms:
        rationale += f" (partial: scoped by {len(partial_where_terms)} predicate(s))"
    return Bundle(
        name=bundle_name,
        rationale=rationale,
        shape_classification="KEYWORD_ONLY",
        members=members,
    )


def _extract_where_key(term):
    """Pull the JSONB key name out of a WHERE predicate.

    Input: ``idx->>'portal_type' = 'Event'``
    Output: ``portal_type``  (used only for deterministic index names)
    Returns 'x' on parse failure — the index name still needs to
    satisfy ``_SAFE_NAME_RE`` so we keep it alphanumeric-only.
    """
    m = re.search(r"idx->>'([A-Za-z_][A-Za-z0-9_]*)'", term)
    return m.group(1) if m else "x"
```

- [ ] **Step 4: Run the bundle tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBuildKeywordGinBundle -v
```

Expected: `4 passed`.

- [ ] **Step 5: Run full suite**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: `76 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: add _build_keyword_gin_bundle — plain and partial GIN

Handles both T3 (plain GIN) when partial_where_terms is empty and
T4 (partial GIN) when provided.  Multiple KEYWORD filters produce
multiple members in one bundle so the planner can BitmapAnd them.

Refs #122."
```

---

## Task 6: Migrate `suggest_indexes` return type to `list[Bundle]` + update all existing tests + catalog.py flatten

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (`suggest_indexes` body rewritten; old `_add_btree_suggestions` / `_add_standalone_suggestion` removed)
- Modify: `src/plone/pgcatalog/catalog.py` (`manage_get_slow_query_stats` consumes bundles, emits both `suggestions` flat + `suggestions_bundles`)
- Modify: `tests/test_suggestions.py` (all existing `suggest_indexes(...)` assertions migrated through a test helper)

This is the biggest commit. Carefully read the whole task before starting.

- [ ] **Step 1: Add a test helper to unwrap bundles**

Add at the top of `tests/test_suggestions.py`, just below the existing `_reg` helper (around line 24):

```python
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
            # The pre-α shape did not carry 'role'; tests ignore it.
            out.append(d)
    return out
```

- [ ] **Step 2: Add a new `_dispatch_templates` + rewrite `suggest_indexes` body**

In `src/plone/pgcatalog/suggestions.py`:

Replace the entire `def suggest_indexes(...)` function (and the two legacy helpers `_add_btree_suggestions` and `_add_standalone_suggestion`) with:

```python
def _dispatch_templates(
    filter_fields,
    sort_field,
    existing_indexes,
    conn=None,
):
    """Route a classified filter-field list to a list of Bundles.

    This is the core PR α dispatcher.  Shape classification determines
    which builder runs; the builders decide partial-scoping, sort
    covering, etc.

    conn is None-safe: when absent, partial-scoping probes skip (all
    selectivities treated as 1.0 — nothing qualifies for partial
    WHERE), and the dispatcher degrades to plain T1/T3 output.
    """
    shape = _classify_filter_shape(filter_fields)

    # Probes + partial-predicate terms are computed lazily only when
    # the dispatch branch needs them — saves DB round-trips.
    partial_where_terms = []  # computed later per-branch

    if shape == "BTREE_ONLY":
        bundle = _build_btree_bundle(filter_fields, sort_field, existing_indexes)
        return [bundle] if bundle is not None else []

    if shape == "KEYWORD_ONLY":
        bundle = _build_keyword_gin_bundle(
            filter_fields, partial_where_terms, existing_indexes
        )
        return [bundle] if bundle is not None else []

    if shape == "MIXED":
        bundle = _build_hybrid_bundle(
            filter_fields, sort_field, partial_where_terms, existing_indexes
        )
        return [bundle] if bundle is not None else []

    # TEXT_ONLY → dedicated tsvector already serves.  UNKNOWN → silent skip.
    return []


def _dedicated_bundles(query_keys, reg_lookup):
    """Emit one-member 'already_covered' Bundle per dedicated-column key
    the query references.  Makes the UI aware the engine saw the
    coverage — important context alongside the main bundles.
    """
    bundles = []
    for key in query_keys:
        if key not in _DEDICATED_FIELDS:
            continue
        field_type = reg_lookup[key].name if key in reg_lookup else "KEYWORD"
        member = BundleMember(
            ddl="",
            fields=[key],
            field_types=[field_type],
            status="already_covered",
            role="dedicated",
            reason=f"Dedicated column: {_DEDICATED_FIELDS[key]}",
        )
        bundles.append(
            Bundle(
                name=f"dedicated-{key}",
                rationale=f"{key} has a dedicated column / index",
                shape_classification="DEDICATED",
                members=[member],
            )
        )
    return bundles


def suggest_indexes(query_keys, params, registry, existing_indexes, conn=None):
    """Generate index-suggestion bundles for a slow-query shape.

    Args:
        query_keys: list of catalog query field names from the slow log.
        params: dict of representative query params, or None.
        registry: IndexRegistry (name -> (IndexType, idx_key, source_attrs)).
        existing_indexes: dict {index_name: index_def_sql} from get_existing_indexes.
        conn: optional psycopg connection for selectivity probes.
            When None, no partial-predicate scoping occurs; bundles
            degrade to plain T1/T3 shapes — identical to pre-α output.

    Returns:
        list[Bundle].  Each Bundle groups one or more BundleMembers
        (indexes) that together address the slow-query shape.  Empty
        list when no suggestion is meaningful (TEXT_ONLY, UNKNOWN,
        etc.).
    """
    reg_lookup = {name: idx_type for name, (idx_type, _k, _a) in registry.items()}

    # Emit dedicated-field 'already_covered' notices first (context
    # for the main bundles).
    bundles = _dedicated_bundles(query_keys, reg_lookup)

    # Build the structured filter-field list and extract sort field.
    filter_fields = _extract_filter_fields(query_keys, params, registry)
    sort_field = _extract_sort_field(params, registry)

    # Route to the shape-appropriate builder(s).
    bundles.extend(
        _dispatch_templates(filter_fields, sort_field, existing_indexes, conn=conn)
    )

    return bundles
```

Note — the new body:
- Threads `conn` through (defaults to `None` for back-compat callers and tests).
- Produces a list of `Bundle` objects, **including** a dedicated-field "already_covered" bundle when relevant. The UI flatten in `catalog.py` will present those the same way the old `_DEDICATED_FIELDS` dict path did.
- Removes the legacy `_add_btree_suggestions` and `_add_standalone_suggestion` — their logic moved to the `_build_*_bundle` helpers.

- [ ] **Step 3: Remove the legacy helpers**

Search for and delete:
- `def _add_standalone_suggestion(field, idx_type, existing_indexes, suggestions):` and its body (the whole function, ~30 lines).
- `def _add_btree_suggestions(btree_fields, sort_field, existing_indexes, suggestions):` and its body (~75 lines).

Both were replaced by `_build_btree_bundle` / `_build_keyword_gin_bundle`. Verify with grep:

```bash
grep -n "_add_standalone_suggestion\|_add_btree_suggestions" src/plone/pgcatalog/suggestions.py
```

Expected: no matches.

- [ ] **Step 4: Migrate every existing `suggest_indexes(...)` assertion to use `_flat(...)`**

In `tests/test_suggestions.py`, every test currently doing:

```python
result = suggest_indexes([...], None, registry, {})
```

where `result` is treated as a list of dicts — wrap it:

```python
result = _flat(suggest_indexes([...], None, registry, {}))
```

Concrete transformations for each existing test in `TestSuggestIndexes` (lines ~30 onwards in PR 2's file). Find each call site and wrap the result:

Example transform:

```python
# Before
def test_single_field_returns_single_btree(self):
    registry = _reg(portal_type=IndexType.FIELD)
    result = suggest_indexes(["portal_type"], None, registry, {})
    assert len(result) == 1
    ...

# After
def test_single_field_returns_single_btree(self):
    registry = _reg(portal_type=IndexType.FIELD)
    result = _flat(suggest_indexes(["portal_type"], None, registry, {}))
    assert len(result) == 1
    ...
```

**Every** test in `TestSuggestIndexes` gets the same `result = _flat(suggest_indexes(...))` wrapping. There are roughly 30 such tests after PR 2.

Note: tests that previously asserted `len(result) == 0` on TEXT_ONLY or dedicated-field cases need adjustment — the dedicated-field bundle is now *in* the result, just with `status="already_covered"`. Check each `len(result) == 0` assertion: if the test exercises a dedicated field (`allowedRolesAndUsers`, `Subject`, `object_provides`, `SearchableText`), the flat result still has an entry. Those tests already check `status == "already_covered"`, so they continue to pass. Tests like `test_empty_keys_returns_empty` and `test_all_filtered_keys_returns_empty` still hold: no query keys → no bundles at all.

- [ ] **Step 5: Update `src/plone/pgcatalog/catalog.py`**

Find `manage_get_slow_query_stats` (around line 1175). The current body builds a `result` list where each row has a `"suggestions"` key populated by `suggest_indexes(keys, params, registry, existing)`.

Replace the result-building loop with this shape (the SQL block stays unchanged):

```python
        result = []
        for row in rows:
            keys = row["query_keys"]
            params = row["representative_params"]
            bundles = suggest_indexes(
                keys, params, registry, existing, conn=pg_conn
            )
            # Back-compat flat list for the existing DTML.
            flat_suggestions = []
            for bundle in bundles:
                for member in bundle.members:
                    flat_suggestions.append({
                        "fields": member.fields,
                        "field_types": member.field_types,
                        "ddl": member.ddl,
                        "status": member.status,
                        "reason": member.reason,
                    })
            result.append(
                {
                    "query_keys": ", ".join(keys),
                    "count": row["cnt"],
                    "avg_ms": float(row["avg_ms"]),
                    "max_ms": float(row["max_ms"]),
                    "last_seen": str(row["last_seen"])[:19],
                    "suggestions": flat_suggestions,
                    "suggestions_bundles": [
                        {
                            "name": b.name,
                            "rationale": b.rationale,
                            "shape_classification": b.shape_classification,
                            "members": [
                                {
                                    "ddl": m.ddl,
                                    "fields": m.fields,
                                    "field_types": m.field_types,
                                    "status": m.status,
                                    "role": m.role,
                                    "reason": m.reason,
                                }
                                for m in b.members
                            ],
                        }
                        for b in bundles
                    ],
                }
            )
        return result
```

**Important:** `suggest_indexes(..., conn=pg_conn)` must be called inside the `try/with pool.getconn()` block *before* `pool.putconn(pg_conn)` — i.e. inside the existing `try:` block, not after. Structurally: the cursor and fetch happen, rows are read, the conn is still live when we call `suggest_indexes(..., conn=pg_conn)` → bundles come back → then the `finally: pool.putconn(pg_conn)` returns the conn. The above loop must therefore live **before** `pool.putconn(pg_conn)`.

Concretely — read the existing code's shape around lines 1187-1218. The current layout is:

```python
                with pg_conn.cursor() as cur:
                    cur.execute(...)
                    rows = cur.fetchall()
            finally:
                pool.putconn(pg_conn)
        except Exception:
            return []

        result = []
        for row in rows:
            ...  # <- currently builds result WITHOUT conn
```

The loop moves into the `try` block so it has conn access:

```python
                with pg_conn.cursor() as cur:
                    cur.execute(...)
                    rows = cur.fetchall()

                result = []
                for row in rows:
                    keys = row["query_keys"]
                    params = row["representative_params"]
                    bundles = suggest_indexes(
                        keys, params, registry, existing, conn=pg_conn
                    )
                    flat_suggestions = []
                    for bundle in bundles:
                        for member in bundle.members:
                            flat_suggestions.append({
                                "fields": member.fields,
                                "field_types": member.field_types,
                                "ddl": member.ddl,
                                "status": member.status,
                                "reason": member.reason,
                            })
                    result.append({
                        "query_keys": ", ".join(keys),
                        "count": row["cnt"],
                        "avg_ms": float(row["avg_ms"]),
                        "max_ms": float(row["max_ms"]),
                        "last_seen": str(row["last_seen"])[:19],
                        "suggestions": flat_suggestions,
                        "suggestions_bundles": [
                            {
                                "name": b.name,
                                "rationale": b.rationale,
                                "shape_classification": b.shape_classification,
                                "members": [
                                    {
                                        "ddl": m.ddl,
                                        "fields": m.fields,
                                        "field_types": m.field_types,
                                        "status": m.status,
                                        "role": m.role,
                                        "reason": m.reason,
                                    }
                                    for m in b.members
                                ],
                            }
                            for b in bundles
                        ],
                    })
            finally:
                pool.putconn(pg_conn)
        except Exception:
            return []

        return result
```

- [ ] **Step 6: Run the migrated suite**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: all pre-existing + new tests green. The TEXT tests from PR 2 expected flat-list output — with the new engine, TEXT keys now produce no bundle (TEXT_ONLY → dedicated tsvector handles it → silent skip). Two tests may need per-test adjustment:

- `test_text_excluded_from_composite`: previously asserted TEXT not mixed with FIELD composite; now the MIXED path doesn't fire (shape becomes TEXT_ONLY which returns empty). Verify the assertion still holds: `for s in result: if "Title" in s["fields"] and len(s["fields"]) > 1: fail`. With empty result the loop runs 0 times → still passes.
- Existing tests asserting on ``s["reason"]`` phrasing ("Btree index for FIELD field 'x'", "GIN index for KEYWORD field 'x'") need to match the new strings. The btree phrasing is unchanged (copied verbatim into `_build_btree_bundle`). The GIN phrasing is slightly different — `"Plain GIN for KEYWORD field 'custom_tags'"` vs the legacy `"GIN index for KEYWORD field 'custom_tags'"`.

Search for tests asserting on that string and update:

```bash
grep -n "GIN index for KEYWORD" tests/test_suggestions.py
```

Any matches → update the expected substring to `"Plain GIN for KEYWORD"` (the new phrasing). Similarly for the tsvector tests, which are **not** produced any longer by PR α's engine (TEXT_ONLY returns no bundle). Those tests exercised `_add_standalone_suggestion` for TEXT; they must be updated or removed:

- `test_keyword_gets_own_gin` — still passes (KEYWORD_ONLY branch emits plain GIN).
- Any test exercising TEXT through `suggest_indexes` — the new engine returns no bundle for TEXT-only shapes. Remove or rewrite those tests to pass a mixed shape if they need to exercise TEXT exclusion in composites (that's still correct — TEXT isn't composable).

Walk through each failure and make the minimal adjustment. When you're done, running `.venv/bin/pytest tests/test_suggestions.py -q` should yield all green. Count should be roughly `76 + 0 minus legacy-TEXT tests`.

- [ ] **Step 7: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py src/plone/pgcatalog/catalog.py tests/test_suggestions.py
git commit -m "feat: suggest_indexes returns list[Bundle]

Signature gains conn=None.  Return type migrates from
list[dict] to list[Bundle] with BundleMember.  catalog.py
flattens bundles for back-compat DTML rendering and
additionally exposes the full bundle structure under a new
'suggestions_bundles' key for PR β's JSON/JS consumer.

Legacy _add_btree_suggestions and _add_standalone_suggestion
helpers removed — their logic lives in _build_btree_bundle and
_build_keyword_gin_bundle.

Refs #122."
```

---

## Task 7: `_probe_selectivity` helper + request-scoped cache

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (new helper + request cache module-level dict)
- Modify: `tests/test_suggestions.py` (new `TestProbeSelectivity` class with mocked cursor)

- [ ] **Step 1: Write failing tests**

Add to `tests/test_suggestions.py`:

```python
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

        # Track execute calls so tests can assert order
        responses = []
        if mcv_row is not None:
            responses.append(mcv_row)
        if count_row is not None:
            responses.append(count_row)
        if total_row is not None:
            responses.append(total_row)
        cur.fetchone.side_effect = responses
        return conn, cur

    def test_mcv_hit_returns_frequency(self):
        from plone.pgcatalog.suggestions import _probe_selectivity, _reset_probe_cache

        _reset_probe_cache()
        mcv_row = {
            "most_common_vals": ["Event", "News", "Page"],
            "most_common_freqs": [0.05, 0.20, 0.50],
        }
        conn, cur = self._mock_conn(mcv_row=mcv_row)
        sel = _probe_selectivity(conn, "portal_type", "Event")
        assert sel == 0.05

    def test_mcv_miss_falls_through_to_count(self):
        from plone.pgcatalog.suggestions import _probe_selectivity, _reset_probe_cache

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
        from plone.pgcatalog.suggestions import _probe_selectivity, _reset_probe_cache

        _reset_probe_cache()
        count_row = {"c": 42}
        total_row = {"t": 10000}
        conn, cur = self._mock_conn(
            mcv_row=None, count_row=count_row, total_row=total_row
        )
        sel = _probe_selectivity(conn, "custom_field", "value")
        assert sel == 42 / 10000

    def test_request_cache_avoids_duplicate_probe(self):
        from plone.pgcatalog.suggestions import _probe_selectivity, _reset_probe_cache

        _reset_probe_cache()
        mcv_row = {
            "most_common_vals": ["Event"],
            "most_common_freqs": [0.05],
        }
        conn, cur = self._mock_conn(mcv_row=mcv_row)
        _probe_selectivity(conn, "portal_type", "Event")
        # Second call on same (key, value) must NOT issue a new execute.
        call_count_before = cur.execute.call_count
        _probe_selectivity(conn, "portal_type", "Event")
        assert cur.execute.call_count == call_count_before

    def test_conn_none_returns_one(self):
        """When conn is None (no probe possible), return 1.0 — safe default."""
        from plone.pgcatalog.suggestions import _probe_selectivity

        assert _probe_selectivity(None, "portal_type", "Event") == 1.0
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestProbeSelectivity -v
```

Expected: `ImportError` — `_probe_selectivity`, `_reset_probe_cache` don't exist.

- [ ] **Step 3: Implement the probe**

In `src/plone/pgcatalog/suggestions.py`, add at module level (just below the `_MAX_COMPOSITE_COLUMNS = 3` constant):

```python
# ── Selectivity probing (request-scoped cache) ─────────────────────────
#
# The cache is module-level — deliberately.  catalog.py resets it at
# the start of each manage_get_slow_query_stats call via
# _reset_probe_cache(), so the scope is effectively "one ZMI tab load".
# A per-call dict threaded through the call chain would be cleaner but
# requires plumbing through every builder; the reset-at-entry pattern
# is simpler and fits the single-threaded WSGI request model.

_probe_cache: dict = {}
_pg_stats_cache: dict = {}


def _reset_probe_cache():
    """Clear probe caches at the start of a ZMI page load."""
    _probe_cache.clear()
    _pg_stats_cache.clear()


def _probe_selectivity(conn, key, value):
    """Return the selectivity of ``idx->>'key' = 'value'`` in object_state.

    Range: [0.0, 1.0].  Lower values mean more selective (fewer rows).

    Probing strategy:
    1. Check request-scoped cache.
    2. If conn is None, return 1.0 (no probe possible — safe default
       that disables partial-scoping for tests / cold-start callers).
    3. Look up ``pg_stats.most_common_vals`` for the key's expression
       attname.  MCV hit → return the corresponding frequency.
    4. MCV miss → live ``SELECT COUNT(*)`` divided by ``reltuples``.

    Caches both the pg_stats row (per attname) and the final
    selectivity (per (key, value)) to bound DB round-trips.
    """
    cache_key = (key, value)
    if cache_key in _probe_cache:
        return _probe_cache[cache_key]

    if conn is None:
        return 1.0

    # Step 1: pg_stats MCV — fetched once per attname per request.
    # pg_stats.attname for an expression index is typically something
    # opaque; we approximate by asking PG for stats on the JSON arrow
    # expression's derived column if it exists.  Fallback: no MCV row.
    attname = f"(idx ->> '{key}'::text)"  # expression-column stat name PG assigns
    if attname not in _pg_stats_cache:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT most_common_vals, most_common_freqs FROM pg_stats "
                "WHERE schemaname = 'public' AND tablename = 'object_state' "
                "AND attname = %s",
                (attname,),
            )
            _pg_stats_cache[attname] = cur.fetchone()

    stats = _pg_stats_cache[attname]
    if stats and stats.get("most_common_vals"):
        vals = stats["most_common_vals"]
        freqs = stats["most_common_freqs"] or []
        if value in vals:
            idx = vals.index(value)
            sel = float(freqs[idx])
            _probe_cache[cache_key] = sel
            return sel

    # Step 2: fall back to live COUNT.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM object_state "
            "WHERE idx IS NOT NULL AND idx->>%s = %s",
            (key, value),
        )
        count = cur.fetchone()["c"]
        cur.execute(
            "SELECT reltuples::bigint AS t FROM pg_class "
            "WHERE relname = 'object_state'"
        )
        total = max(cur.fetchone()["t"], 1)
    sel = count / total
    _probe_cache[cache_key] = sel
    return sel
```

- [ ] **Step 4: Run probe tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestProbeSelectivity -v
```

Expected: `5 passed`.

- [ ] **Step 5: Run full suite**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: _probe_selectivity with MCV-first / COUNT-fallback cache

Two-stage selectivity probe for partial-predicate scoping.
pg_stats.most_common_vals gives zero-round-trip answers for
common values; unknown values fall back to live COUNT.  All
results cached in a module-level dict reset per ZMI page load.

Refs #122."
```

---

## Task 8: `_partial_where_terms` + threshold constant + env var

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (new helper + threshold constant)
- Modify: `tests/test_suggestions.py` (new `TestPartialWhereTerms` class)

- [ ] **Step 1: Write failing tests**

Add to `tests/test_suggestions.py`:

```python
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
            ("review_state", "published"): 0.25,  # above threshold
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
        probes = {}  # range never probed
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == []

    def test_multi_value_equality_excluded(self):
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [
            ("portal_type", IndexType.FIELD, "equality_multi", None),
        ]
        probes = {}
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == []

    def test_date_type_excluded_even_if_equality(self):
        """DATE values are timestamps — rarely in MCV, partial scoping
        is usually an anti-pattern.  Not baked."""
        from plone.pgcatalog.suggestions import _partial_where_terms

        filter_fields = [("effective", IndexType.DATE, "equality", "2026-04-15")]
        probes = {("effective", "2026-04-15"): 0.001}  # very selective, but DATE
        terms = _partial_where_terms(filter_fields, probes)
        assert terms == []
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestPartialWhereTerms -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement**

In `src/plone/pgcatalog/suggestions.py`, add at module level (just below `_MAX_COMPOSITE_COLUMNS` and **above** `_probe_cache`):

```python
import os as _os

# Selectivity threshold below which an equality filter gets baked into
# a partial index's WHERE clause.  Configurable via env var for
# production tuning without a code change.
_PARTIAL_PREDICATE_SELECTIVITY_THRESHOLD = float(
    _os.environ.get("PGCATALOG_PARTIAL_SELECTIVITY_THRESHOLD", "0.1")
)

# IndexTypes whose equality values are eligible for partial-predicate
# scoping.  DATE excluded — values are timestamps, rarely appear in
# MCV, partial-DATE scoping is typically an anti-pattern.
_PARTIAL_SCOPING_ELIGIBLE_TYPES = frozenset(
    {IndexType.FIELD, IndexType.BOOLEAN, IndexType.PATH, IndexType.UUID}
)
```

Then add the helper itself, immediately above `_build_btree_bundle`:

```python
def _partial_where_terms(filter_fields, probes):
    """Build the list of WHERE predicates for a partial index.

    For each filter field with operator='equality' and a scalar value,
    check the probed selectivity against the threshold; if below,
    include the predicate.  Values are SQL-escaped (single quote
    doubled).  DATE / KEYWORD / range / multi-value filters are
    excluded — only btree-eligible text-ish types qualify.

    Args:
        filter_fields: list of ``(name, IndexType, operator, value)``.
        probes: dict ``{(name, value): selectivity_float}``.  Missing
            entries are treated as 1.0 (never qualify).

    Returns:
        list[str] of SQL predicate fragments ready for AND-joining.
    """
    terms = []
    for name, idx_type, op, value in filter_fields:
        if op != "equality":
            continue
        if idx_type not in _PARTIAL_SCOPING_ELIGIBLE_TYPES:
            continue
        if value is None:
            continue
        sel = probes.get((name, value), 1.0)
        if sel >= _PARTIAL_PREDICATE_SELECTIVITY_THRESHOLD:
            continue
        safe_value = str(value).replace("'", "''")
        terms.append(f"idx->>'{name}' = '{safe_value}'")
    return terms
```

- [ ] **Step 4: Run partial-where tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestPartialWhereTerms -v
```

Expected: `8 passed`.

- [ ] **Step 5: Run full suite**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: _partial_where_terms with selectivity threshold

Pure helper that builds the list of AND-joined WHERE predicates
for a partial index, applying the 10%-selectivity threshold
(configurable via PGCATALOG_PARTIAL_SELECTIVITY_THRESHOLD env
var).  Excludes DATE and KEYWORD values, multi-value equality,
and range operators.  SQL-escapes single quotes.

Refs #122."
```

---

## Task 9: `_build_hybrid_bundle` + MIXED dispatch + AT26 regression + probe wiring

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (hybrid builder, wire probes into dispatcher, reset cache on entry)
- Modify: `tests/test_suggestions.py` (new `TestBuildHybridBundle` class + `TestIssue122AT26Regression`)

- [ ] **Step 1: Write failing tests**

Add to `tests/test_suggestions.py`:

```python
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
        assert len(bundle.members) == 3  # 1 btree + 2 GIN
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
    """The canonical slow query from #122 gap-analysis comment:

        portal_type=Event + review_state=published + Subject=AT26
        + effective<=now + expires>=now + sort_on=effective

    Expected: ONE MIXED bundle with a btree composite member
    (portal_type, review_state, effective) + a partial GIN on Subject
    scoped by the low-selectivity equality filters.
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
        """When BOTH portal_type and review_state probe below threshold,
        GIN is scoped by both.  Needs conn-backed probes; mocked here
        by pre-populating the module probe cache."""
        from plone.pgcatalog.suggestions import (
            _probe_cache,
            _reset_probe_cache,
            suggest_indexes,
        )

        _reset_probe_cache()
        # Pre-populate cache so the probe short-circuits without a DB.
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
            conn=object(),  # sentinel; probes all pre-cached
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
        from plone.pgcatalog.suggestions import (
            _probe_cache,
            _reset_probe_cache,
            suggest_indexes,
        )

        _reset_probe_cache()
        _probe_cache[("portal_type", "Event")] = 0.05
        _probe_cache[("review_state", "published")] = 0.25  # above threshold

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
        """conn=None → probes return 1.0 → no partial scoping → plain GIN."""
        from plone.pgcatalog.suggestions import _reset_probe_cache, suggest_indexes

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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBuildHybridBundle tests/test_suggestions.py::TestIssue122AT26Regression -v
```

Expected: `ImportError: cannot import name '_build_hybrid_bundle'` (and/or failing AT26 tests because MIXED dispatch doesn't yet exist).

- [ ] **Step 3: Implement `_build_hybrid_bundle`**

In `src/plone/pgcatalog/suggestions.py`, add just below `_build_keyword_gin_bundle`:

```python
def _build_hybrid_bundle(
    filter_fields, sort_field, partial_where_terms, existing_indexes
):
    """Build a MIXED-shape Bundle — one btree composite + N partial GINs.

    The btree member handles the btree-eligible filter axes plus any
    sort covering (reuses T1 logic).  The GIN members handle the
    KEYWORD filters, each scoped by the same ``partial_where_terms``
    so the planner can BitmapAnd them.

    Note the btree member does NOT gain a partial WHERE — PR α skips
    T2 (partial btree) as YAGNI.

    Returns None when filter_fields contains neither a btree-eligible
    nor a KEYWORD entry (shouldn't happen given the dispatcher gates
    on shape == MIXED, but guard anyway).
    """
    btree_candidates = [
        (n, t, op, v) for (n, t, op, v) in filter_fields
        if t != IndexType.KEYWORD and t != IndexType.TEXT
    ]
    keyword_candidates = [
        (n, t, op, v) for (n, t, op, v) in filter_fields
        if t == IndexType.KEYWORD
    ]

    if not btree_candidates and not keyword_candidates:
        return None

    members = []

    btree_bundle = _build_btree_bundle(btree_candidates, sort_field, existing_indexes)
    if btree_bundle is not None:
        members.extend(btree_bundle.members)  # always exactly one

    gin_bundle = _build_keyword_gin_bundle(
        keyword_candidates, partial_where_terms, existing_indexes
    )
    if gin_bundle is not None:
        members.extend(gin_bundle.members)

    if not members:
        return None

    all_field_names = [m.fields[0] for m in members]  # representative
    bundle_name = "hybrid-" + "-".join(all_field_names)
    rationale = (
        f"Hybrid bundle for MIXED filter shape: "
        f"btree covers {', '.join(f for (f, _, _, _) in btree_candidates)} "
        f"+ GIN covers {', '.join(f for (f, _, _, _) in keyword_candidates)}"
    )
    if partial_where_terms:
        rationale += (
            f"; partial predicate scopes GIN by {len(partial_where_terms)} equality filter(s)"
        )
    return Bundle(
        name=bundle_name,
        rationale=rationale,
        shape_classification="MIXED",
        members=members,
    )
```

- [ ] **Step 4: Wire probe resolution + partial-where into the dispatcher**

Replace the existing `_dispatch_templates` body (from Task 6) with:

```python
def _dispatch_templates(
    filter_fields,
    sort_field,
    existing_indexes,
    conn=None,
):
    """Route a classified filter-field list to a list of Bundles."""
    shape = _classify_filter_shape(filter_fields)

    # Resolve probes for every scalar-equality filter up front —
    # cheap (all cached / deduped) and avoids branching probe logic
    # between the builders.
    probes = {}
    for name, idx_type, op, value in filter_fields:
        if op != "equality" or value is None:
            continue
        if idx_type not in _PARTIAL_SCOPING_ELIGIBLE_TYPES:
            continue
        probes[(name, value)] = _probe_selectivity(conn, name, value)

    partial_where_terms = _partial_where_terms(filter_fields, probes)

    if shape == "BTREE_ONLY":
        bundle = _build_btree_bundle(filter_fields, sort_field, existing_indexes)
        return [bundle] if bundle is not None else []

    if shape == "KEYWORD_ONLY":
        bundle = _build_keyword_gin_bundle(
            filter_fields, partial_where_terms, existing_indexes
        )
        return [bundle] if bundle is not None else []

    if shape == "MIXED":
        bundle = _build_hybrid_bundle(
            filter_fields, sort_field, partial_where_terms, existing_indexes
        )
        return [bundle] if bundle is not None else []

    # TEXT_ONLY / UNKNOWN → silent skip.
    return []
```

- [ ] **Step 5: Reset the probe cache at the entry of `suggest_indexes`**

Find `suggest_indexes`. Add `_reset_probe_cache()` as the first line of the function body, right below the docstring:

```python
def suggest_indexes(query_keys, params, registry, existing_indexes, conn=None):
    """...(unchanged docstring)..."""
    _reset_probe_cache()
    reg_lookup = {name: idx_type for name, (idx_type, _k, _a) in registry.items()}
    ...
```

This ensures unit tests that pre-populate the cache (like `TestIssue122AT26Regression`) clobber the reset. Those tests have to call `_reset_probe_cache()` *before* pre-populating — which they already do. Then when `suggest_indexes` runs, it resets again (wiping the pre-populated values!).

The workaround: tests pre-populate AFTER calling `suggest_indexes`-by-way-of-reset is tricky. Instead, make `_reset_probe_cache()` conditional — skip if tests have explicitly seeded it. Cleaner: remove the reset from `suggest_indexes`, do the reset in `catalog.py` at the top of `manage_get_slow_query_stats` instead. Tests bypass catalog.py, so the cache stays whatever the test left it.

Adjust Step 5: **do not** add `_reset_probe_cache()` to `suggest_indexes`. Instead, add it to `manage_get_slow_query_stats` (Task 10 will wire catalog.py thoroughly; for now, place the reset call at the top of the existing manage_get_slow_query_stats method just after the function signature — there's no code between the docstring and the try block for the existing implementation. Insert `_reset_probe_cache()` as the first line of the try block so it runs exactly once per tab load).

Find the existing block in `src/plone/pgcatalog/catalog.py`:

```python
    def manage_get_slow_query_stats(self):
        """Return aggregated slow query stats for the Slow Queries tab."""
        from plone.pgcatalog.suggestions import get_existing_indexes
        from plone.pgcatalog.suggestions import suggest_indexes

        try:
            pool = get_pool(self)
            pg_conn = pool.getconn()
```

Immediately after the import block, add the import + call:

```python
    def manage_get_slow_query_stats(self):
        """Return aggregated slow query stats for the Slow Queries tab."""
        from plone.pgcatalog.suggestions import _reset_probe_cache
        from plone.pgcatalog.suggestions import get_existing_indexes
        from plone.pgcatalog.suggestions import suggest_indexes

        _reset_probe_cache()
        try:
            pool = get_pool(self)
            pg_conn = pool.getconn()
```

- [ ] **Step 6: Run hybrid + AT26 tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestBuildHybridBundle tests/test_suggestions.py::TestIssue122AT26Regression -v
```

Expected: all 7 pass.

- [ ] **Step 7: Run full suite**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py src/plone/pgcatalog/catalog.py tests/test_suggestions.py
git commit -m "feat: hybrid bundle + MIXED dispatch + probe wiring

MIXED shape (btree + KEYWORD filters together) now produces a
single Bundle containing a btree composite member plus one
partial-GIN member per KEYWORD, all scoped by the same
partial WHERE predicates (the planner BitmapAnds them).

Closes the issue-#122 AT26 gap:
  portal_type=Event + review_state=published + Subject=AT26
  + effective+expires range + sort_on=effective
now yields
  btree (portal_type, review_state, effective)  AND
  partial GIN on Subject
    WHERE portal_type='Event' AND review_state='published'

Probe cache is reset in manage_get_slow_query_stats so unit
tests can seed it independently.

Refs #122."
```

---

## Task 10: GIN existing-index detection via `_normalize_gin_expr`

**Files:**
- Modify: `src/plone/pgcatalog/suggestions.py` (extend `_check_covered` or add GIN-specific normalization)
- Modify: `tests/test_suggestions.py` (new `TestGinCoveredDetection` class)

- [ ] **Step 1: Write failing tests**

Add to `tests/test_suggestions.py`:

```python
class TestGinCoveredDetection:
    """GIN expression indexes get detected as 'already_covered'."""

    def test_existing_plain_gin_detected(self):
        """A plain GIN on (idx->'Subject') makes a new plain GIN 'already_covered'."""
        from plone.pgcatalog.suggestions import _build_keyword_gin_bundle

        filter_fields = [("Subject", IndexType.KEYWORD, "equality", "x")]
        existing = {
            "idx_os_cat_subject_gin": (
                "CREATE INDEX idx_os_cat_subject_gin ON public.object_state "
                "USING gin ((idx -> 'Subject'::text)) "
                "WHERE (idx IS NOT NULL AND idx ? 'Subject')"
            )
        }
        bundle = _build_keyword_gin_bundle(filter_fields, [], existing)
        assert bundle.members[0].status == "already_covered"

    def test_whitespace_normalization(self):
        from plone.pgcatalog.suggestions import _build_keyword_gin_bundle

        filter_fields = [("tags", IndexType.KEYWORD, "equality", "x")]
        # PG-stored form with spaces around -> and ::text casts.
        existing = {
            "idx_os_sug_tags": (
                "CREATE INDEX idx_os_sug_tags ON public.object_state "
                "USING gin ((idx -> 'tags'::text)) "
                "WHERE (idx IS NOT NULL AND idx ? 'tags')"
            )
        }
        bundle = _build_keyword_gin_bundle(filter_fields, [], existing)
        assert bundle.members[0].status == "already_covered"

    def test_partial_gin_narrower_where_is_new(self):
        """Suggested partial GIN has STRICTER WHERE than existing plain
        GIN — treat as NEW (different index)."""
        from plone.pgcatalog.suggestions import _build_keyword_gin_bundle

        filter_fields = [("Subject", IndexType.KEYWORD, "equality", "x")]
        where_terms = ["idx->>'portal_type' = 'Event'"]
        existing = {
            "idx_os_cat_subject_gin": (
                "CREATE INDEX idx_os_cat_subject_gin ON public.object_state "
                "USING gin ((idx -> 'Subject'::text)) "
                "WHERE (idx IS NOT NULL AND idx ? 'Subject')"
            )
        }
        bundle = _build_keyword_gin_bundle(filter_fields, where_terms, existing)
        assert bundle.members[0].status == "new"
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestGinCoveredDetection -v
```

Expected: the plain-GIN coverage tests fail — `_check_covered`'s expression normalization matches btree's `(idx->>'k')` form but not GIN's `(idx->'k')`.

- [ ] **Step 3: Extend `_normalize_idx_expr` to handle GIN expressions**

The existing `_normalize_idx_expr` strips `::text` casts and collapses whitespace. It already handles both `->>` and `->` via the regex `r"\s*(->>?|#>>?|#>)\s*"`. The remaining issue is the **WHERE clause**: GIN indexes include `AND idx ? 'k'` in their WHERE, which the regex captures as part of the expression region. The existing function extracts the expression up to `WHERE`, so the `idx ? 'k'` clause is not captured — good.

Verify by running the test manually and inspecting. If it still fails, the issue is likely the **paren-collapse** step: the generated form is `((idx->'tags'))` but PG stores `((idx -> 'tags'::text))`. After collapse both become `idx->'tags'`. Actually that should match.

The actual issue is likely in the DDL-matching substring check:

```python
for _name, idx_def in existing_indexes.items():
    if norm in _normalize_idx_expr(idx_def):
        return "already_covered"
```

Re-examine: generated form is `CREATE INDEX CONCURRENTLY idx_os_sug_Subject ON object_state USING gin ((idx->'Subject')) WHERE idx IS NOT NULL AND idx ? 'Subject'`. `_normalize_idx_expr` extracts `((idx->'Subject'))` (paren-collapsed → `idx->'Subject'`).

PG-stored form: `CREATE INDEX ... USING gin ((idx -> 'Subject'::text)) WHERE (idx IS NOT NULL AND idx ? 'Subject')`. Extract `((idx -> 'Subject'::text))` → strip `::text` → `((idx -> 'Subject'))` → collapse whitespace around `->` → `((idx->'Subject'))` → paren-collapse → `idx->'Subject'`. Matches!

So the plain-GIN coverage test should pass already. If it does, skip the no-op Step 3 and move on. If not, the specific debugging needed is to print `_normalize_idx_expr(generated_ddl)` and `_normalize_idx_expr(stored_ddl)` and compare.

**Action:** run the test, see which assertion fails. If `test_existing_plain_gin_detected` fails, instrument briefly:

```python
print(_normalize_idx_expr(generated_ddl))
print(_normalize_idx_expr(existing["idx_os_cat_subject_gin"]))
```

Adjust the regex or the expression-extraction logic as needed. Likely no change required — PR 2's `_normalize_idx_expr` is robust.

For `test_partial_gin_narrower_where_is_new`: this requires comparing WHERE clauses. The *current* `_check_covered` compares only the expression substring — so a partial WHERE is ignored. Add a WHERE-aware check:

Replace `_check_covered` body with:

```python
def _check_covered(ddl, existing_indexes):
    """Check if the suggested index already exists.

    Three checks:
    1. Exact name match (case-insensitive).
    2. Normalized expression match (substring) — AND matching /
       more-permissive WHERE clause.  A broader existing WHERE
       covers a narrower suggested one; a stricter existing WHERE
       does NOT cover a broader one — but more importantly, a
       narrower *suggested* WHERE is a distinct partial index
       (different plan-usability), so we do NOT mark it covered.
    """
    m = re.search(r"(?:CREATE INDEX\s+(?:CONCURRENTLY\s+)?)(\S+)", ddl, re.I)
    if m and m.group(1).lower() in existing_indexes:
        return "already_covered"

    norm = _normalize_idx_expr(ddl)
    suggested_where = _extract_where_clause(ddl)
    if norm:
        for _name, idx_def in existing_indexes.items():
            existing_norm = _normalize_idx_expr(idx_def)
            if norm not in existing_norm:
                continue
            existing_where = _extract_where_clause(idx_def)
            # Broader or equal existing WHERE covers the suggested.
            if _where_covers(existing_where, suggested_where):
                return "already_covered"
    return "new"


def _extract_where_clause(ddl):
    """Extract the normalized WHERE-clause predicate set from an index DDL.

    Returns a frozenset of predicate strings (AND-separated), each
    stripped of surrounding parens and whitespace.
    """
    m = re.search(r"\bWHERE\b\s+(.+)$", ddl, re.I | re.S)
    if not m:
        return frozenset()
    raw = m.group(1).strip().rstrip(")")
    # Strip one level of outer parens if present.
    raw = re.sub(r"^\s*\(\s*", "", raw)
    raw = re.sub(r"\s*\)\s*$", "", raw)
    # Split on AND (case-insensitive, word-boundary)
    parts = re.split(r"\s+and\s+", raw, flags=re.I)
    return frozenset(_normalize_predicate(p) for p in parts if p.strip())


def _normalize_predicate(p):
    """Canonicalize a single WHERE predicate for equality comparison."""
    # Strip ::text casts, collapse whitespace around arrows and whitespace,
    # then strip outer whitespace.
    p = re.sub(r"::text\b", "", p)
    p = re.sub(r"\s*(->>?|#>>?|#>)\s*", r"\1", p)
    p = re.sub(r"\s+", " ", p).strip()
    # Strip outermost parens on individual predicate.
    p = re.sub(r"^\((.+)\)$", r"\1", p)
    return p.strip()


def _where_covers(existing_where, suggested_where):
    """An existing WHERE predicate set 'covers' a suggested one iff
    every existing predicate is also in the suggested set (existing
    is at most as strict as suggested).

    Equivalently: existing ⊆ suggested.  An existing index with
    WHERE idx IS NOT NULL covers a suggested index with
    WHERE idx IS NOT NULL AND idx->>'k'='v' (existing is less strict).
    """
    return existing_where.issubset(suggested_where)
```

- [ ] **Step 4: Run GIN coverage tests**

```bash
.venv/bin/pytest tests/test_suggestions.py::TestGinCoveredDetection -v
```

Expected: all 3 pass.

- [ ] **Step 5: Run full suite (including PR 2's `TestNormalizeIdxExpr`)**

```bash
.venv/bin/pytest tests/test_suggestions.py -q
```

Expected: all green. PR 2's normalization tests don't touch `_extract_where_clause` — they test `_normalize_idx_expr` directly. No regressions expected.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: GIN existing-index detection + WHERE-aware coverage

Existing plain-GIN indexes on a KEYWORD field now match as
'already_covered' for plain-GIN suggestions on the same field.
Partial-GIN suggestions with STRICTER WHERE than the existing
index remain 'new' — they are genuinely distinct indexes with
different plan usability.

_check_covered now compares WHERE clauses as predicate sets:
existing ⊆ suggested = covered; else new.

Refs #122."
```

---

## Task 11: CHANGES.md + full verification

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Confirm next version**

```bash
gh release list --repo bluedynamics/plone-pgcatalog --limit 3
```

Expected: `1.0.0b53` is the latest; the next is `1.0.0b54`.

- [ ] **Step 2: Add the changelog entry**

Read current `CHANGES.md`, find the top-most `## 1.0.0b53` heading, and insert above it:

```markdown
## 1.0.0b54 (unreleased)

### Added

- Suggestions engine now emits **bundles** of indexes.  The new `Bundle`
  data shape groups one or more related indexes together so the UI can
  present "apply these together" semantics for hybrid patterns.  Single
  btree-composite suggestions render unchanged in the existing ZMI via a
  back-compat flatten in `manage_get_slow_query_stats`; the full bundle
  structure is additionally exposed under `suggestions_bundles` for the
  upcoming PR β JSON/JS UI.

- New templates: **partial GIN** (T4 — GIN with an AND-joined equality
  predicate baked into WHERE) and **hybrid bundles** (T5 — one btree
  composite plus one or more partial GINs working together).  Triggered
  by the MIXED shape classification when a slow query has both
  btree-eligible and KEYWORD filters.

- Partial-predicate scoping is data-driven.  A selectivity probe uses
  `pg_stats.most_common_vals` for common values (zero DB round-trip)
  and falls back to live `SELECT COUNT(*)` for long-tail values.
  Equality filters whose live selectivity is below 10% (configurable
  via `PGCATALOG_PARTIAL_SELECTIVITY_THRESHOLD` env var) are baked
  into the partial index's WHERE clause.

- Closes the canonical Plone slow-query gap from
  [#122](https://github.com/bluedynamics/plone-pgcatalog/issues/122):
  `portal_type + review_state + Subject + effective range + sort_on`
  now produces a hybrid bundle — btree composite `(portal_type,
  review_state, effective)` + partial GIN on Subject scoped by
  `portal_type='Event' AND review_state='published'`.

### Changed

- `suggest_indexes()` now returns `list[Bundle]` (was `list[dict]`).
  Signature gains an optional `conn` argument for selectivity probes;
  `conn=None` disables partial scoping and keeps bundle output identical
  to pre-α behavior.  No call-site changes required in existing code
  that imports this function from outside the package.
```

- [ ] **Step 3: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog for PR α suggestions engine (#122)"
```

- [ ] **Step 4: Final full test run**

```bash
.venv/bin/pytest -q
```

Expected: green across the entire project suite. Baseline was 1328 passing + 39 skipped after PR 2. PR α adds ~35 new tests. Target: `~1363 passing, 39 skipped`.

- [ ] **Step 5: Ruff check**

```bash
uvx ruff check src/plone/pgcatalog/suggestions.py src/plone/pgcatalog/catalog.py tests/test_suggestions.py
```

Expected: `All checks passed!`. Fix any flagged issues.

- [ ] **Step 6: Smoke test in a live instance** (optional — same as PR 2's Task 8 Step 3)

Start Zope, navigate to the Slow Queries ZMI tab, verify bundles render through the back-compat flatten. The UI shouldn't look different from PR 2's output — that's the point. PR β will surface the new bundle structure.

---

## Post-merge: release

Per project convention (MEMORY.md: *Check GitHub releases before CHANGES.md edits*, *Always write changelog before tagging*):

1. Confirm `CHANGES.md` `## 1.0.0b54 (unreleased)` is on `main` after merge.
2. Tag & release via the usual flow.

## Self-review

**Spec coverage:**

- Q3 hybrid approach → Task 3 (classifier) + Task 4–5 (builders) + Task 7–8 (probes + partial WHERE) ✓
- Q4 bundle output model → Task 1 (dataclasses) + Task 6 (signature change + catalog.py) ✓
- Q5 two-stage rules+EXPLAIN → PR α ships the rules; Q5's EXPLAIN is deferred to PR β per spec ✓
- Q6 template vocabulary T1/T3/T4/T5/T6 → Task 4 (T1) + Task 5 (T3) + Task 5 (T4 via partial_where_terms arg) + Task 9 (T5 hybrid) + T6 untouched (still dedicated tsvector path) ✓
- Q7 EXPLAIN trigger → deferred to PR β ✓
- Q8 partial-predicate scoping (b + iii) → Task 7 (probe: MCV+COUNT) + Task 8 (bake-all-qualifying) ✓
- Q9 two-PR decomposition → PR α ships here ✓
- Section 1 architecture → Tasks 1–10 ✓
- Section 2 bundle data model + dispatcher → Task 1 + Task 3 + Task 6 ✓
- Section 3 classification inputs + probe algorithm + partial-where → Tasks 2, 7, 8 ✓
- Section 4 testing → 7 new test classes across Tasks 1–10 ✓

**Placeholder scan:** No TBD/TODO. One documented-not-blocking unknown: `_derived_attname_for_key` in the spec — handled in Task 7's probe implementation by using the expression-attname form PG typically assigns; if PG returns no stats row, probe falls through to COUNT (never incorrect, just less fast). Not a placeholder — a documented graceful fallback.

**Type consistency:**

- `Bundle` / `BundleMember` field names (`ddl`, `fields`, `field_types`, `status`, `role`, `reason`, `name`, `rationale`, `shape_classification`, `members`) — consistent across Tasks 1, 4, 5, 9, 10.
- Shape strings (`BTREE_ONLY`, `KEYWORD_ONLY`, `MIXED`, `TEXT_ONLY`, `UNKNOWN`, `DEDICATED`) — consistent across Tasks 3, 4, 5, 6, 9.
- Role strings (`btree_composite`, `plain_gin`, `partial_gin`, `dedicated`) — consistent across Tasks 4, 5, 6, 9.
- `filter_fields` tuple shape `(name, IndexType, operator, value)` — consistent across Tasks 2, 3, 4, 5, 9.
- Function names (`_extract_filter_fields`, `_classify_filter_shape`, `_build_btree_bundle`, `_build_keyword_gin_bundle`, `_build_hybrid_bundle`, `_dispatch_templates`, `_probe_selectivity`, `_reset_probe_cache`, `_partial_where_terms`) — all used consistently across tasks.

Plan complete.
