# PGIndex `_apply_index` + robust catalog resolution — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the silent raw-index fallback in `_CatalogCompat`, implement `PGIndex._apply_index` via `_QueryBuilder` reuse, complete the `_PGIndexMapping` BTree-shaped read surface, and self-heal missing `__parent__` on first access so aaf-prod recovers with zero touch.

**Architecture:** One helper `_resolve_catalog(compat)` in `maintenance.py` becomes the single lookup path (explicit parent → Acquisition → `getSite`). `_CatalogCompat.indexes` property extends its b60 self-heal to also persist `__parent__`. `PGIndex._apply_index` instantiates `_QueryBuilder`, reuses its `_process_index` dispatch, wraps the resulting `WHERE`-fragment in `SELECT zoid FROM object_state`. `_PGIndexMapping` gains `__getitem__`/`__len__` for Plone-core compat and raises `NotImplementedError` with guidance on `items()`/`values()`. All emit-once `DeprecationWarning`s.

**Tech Stack:** Python 3.13, psycopg 3, `Acquisition.Implicit`, `zope.component.hooks.getSite`, `BTrees.IIBTree.IITreeSet`, `plone.pgcatalog.query._QueryBuilder`, pytest, `plone.app.testing`.

**Spec:** `docs/superpowers/specs/2026-04-20-pgindex-apply-index-design.md`

**Branch:** `fix/pgindex-apply-index-and-parent-resolution` (already exists; spec committed on it).

**Target release:** 1.0.0b61.

---

## File Map

- **Create:** none — no new modules.
- **Modify:**
  - `src/plone/pgcatalog/maintenance.py` — add `_resolve_catalog`; extend `_CatalogCompat.indexes` self-heal; rewrite `_CatalogCompat.getIndex`; rewrite `_CatalogIndexesView.__getitem__`.
  - `src/plone/pgcatalog/pgindex.py` — add `_apply_index` on `PGIndex`; add `__getitem__`, `__len__`, `items`, `values` on `_PGIndexMapping`; wrap `_index` property in a `DeprecationWarning`.
  - `CHANGES.md` — `## 1.0.0b61` entry (last task).
- **Test (modify):**
  - `tests/test_catalog_indexes_view.py` — add `TestResolveCatalog`, `TestParentSelfHeal`, `TestGetIndexWithoutParent`; rewrite `TestViewWithoutCatalog` to match the new no-silent-fallback contract.
  - `tests/test_pgindex.py` — add `TestPGIndexMappingNewMethods`, `TestPGIndexIndexDeprecation`, `TestPGIndexApplyIndex`.
  - `tests/test_plone_integration.py` — add `TestKeywordsVocabulary` (integration layer).

**No changes to:** `src/plone/pgcatalog/query.py` (`_QueryBuilder` reused as-is).

---

## Task 1: `_resolve_catalog` helper

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py` — add helper right above `class _CatalogIndexesView`
- Test: `tests/test_catalog_indexes_view.py` — new class `TestResolveCatalog` at the end of the file

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_catalog_indexes_view.py`:

```python
# ── _resolve_catalog: three-step lookup, no silent None ──────────────────


class TestResolveCatalog:
    def test_returns_explicit_parent_first(self):
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        compat = _CatalogCompat()
        tool = mock.Mock(name="tool-via-parent")
        compat.__dict__["__parent__"] = tool

        assert _resolve_catalog(compat) is tool

    def test_falls_through_to_acquisition_parent(self):
        from Acquisition import Implicit
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        class _ParentHolder(Implicit):
            pass

        parent = _ParentHolder()
        compat = _CatalogCompat()
        wrapped = compat.__of__(parent)  # Acquisition wrapper

        assert _resolve_catalog(wrapped) is parent

    def test_falls_through_to_get_site_portal_catalog(self):
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        compat = _CatalogCompat()
        tool = mock.Mock(name="tool-via-get-site")
        site = mock.Mock(portal_catalog=tool)

        with mock.patch(
            "plone.pgcatalog.maintenance.getSite", return_value=site
        ):
            assert _resolve_catalog(compat) is tool

    def test_raises_runtimeerror_when_all_three_fail(self):
        import pytest
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        compat = _CatalogCompat()
        with (
            mock.patch("plone.pgcatalog.maintenance.getSite", return_value=None),
            pytest.raises(RuntimeError, match="cannot find portal_catalog"),
        ):
            _resolve_catalog(compat)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py::TestResolveCatalog -v
```

Expected: four FAILs with `ImportError: cannot import name '_resolve_catalog' from 'plone.pgcatalog.maintenance'`.

- [ ] **Step 3: Add module-level import and helper**

Add to the imports block at the top of `src/plone/pgcatalog/maintenance.py` (merge alphabetically with existing `Acquisition` imports):

```python
from zope.component.hooks import getSite
```

Insert the helper just before `class _CatalogIndexesView:` (≈ line 94):

```python
def _resolve_catalog(compat):
    """Find the PlonePGCatalogTool that owns this _CatalogCompat.

    Tries three resolution paths in order:

    1. ``__parent__`` set on the bare instance (by the v1->v2 migration
       step or by the ``indexes`` property's self-heal).
    2. Acquisition chain — ``aq_parent(aq_inner(compat))`` — when the
       compat is reached via an Acquisition wrapper and some parent in
       the chain is the catalog tool.
    3. ``zope.component.hooks.getSite().portal_catalog`` — works during
       request handling and in any code path that sets up the local
       site hook.

    Raises ``RuntimeError`` if all three fail.  **Never returns
    ``None``** — a silent ``None`` triggered the raw-index fallback
    bug that masked #143 / #146 for weeks.
    """
    parent = compat.__dict__.get("__parent__")
    if parent is not None:
        return parent
    via_aq = aq_parent(aq_inner(compat))
    if via_aq is not None:
        return via_aq
    site = getSite()
    if site is not None:
        tool = getattr(site, "portal_catalog", None)
        if tool is not None:
            return tool
    raise RuntimeError(
        "plone.pgcatalog._CatalogCompat: cannot find portal_catalog "
        "(no __parent__, no acquisition context, no getSite). "
        "_CatalogCompat is not usable outside a Plone site."
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py::TestResolveCatalog -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/maintenance.py tests/test_catalog_indexes_view.py
git commit -m "feat(maintenance): _resolve_catalog helper with three-step lookup (#146)"
```

---

## Task 2: `_CatalogCompat.indexes` property — self-heal `__parent__`

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py` — `_CatalogCompat.indexes` property body
- Test: `tests/test_catalog_indexes_view.py` — new class `TestParentSelfHeal`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_catalog_indexes_view.py`:

```python
# ── Self-heal: __parent__ gets persisted on first indexes access ─────────


class TestParentSelfHeal:
    def test_property_heals_missing_parent_via_get_site(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["_raw_indexes"] = PersistentMapping()
        compat.__dict__["schema"] = PersistentMapping()
        # no __parent__
        compat._p_changed = False

        tool = mock.Mock(name="portal_catalog")
        site = mock.Mock(portal_catalog=tool)

        # Need a fake jar so ``_p_changed = True`` sticks under Persistent.
        from plone.pgcatalog.upgrades.profile_2 import _NoOpJar

        compat._p_jar = _NoOpJar()

        with mock.patch(
            "plone.pgcatalog.maintenance.getSite", return_value=site
        ):
            _view = compat.indexes  # trigger property

        assert compat.__dict__.get("__parent__") is tool
        assert compat._p_changed is True

    def test_property_tolerates_get_site_returning_none(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["_raw_indexes"] = PersistentMapping()
        # no __parent__, no site hook

        with mock.patch(
            "plone.pgcatalog.maintenance.getSite", return_value=None
        ):
            _view = compat.indexes  # must not raise

        assert "__parent__" not in compat.__dict__

    def test_property_leaves_existing_parent_untouched(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        explicit = mock.Mock(name="explicit-parent")
        other = mock.Mock(name="get-site-tool")
        site = mock.Mock(portal_catalog=other)

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["_raw_indexes"] = PersistentMapping()
        compat.__dict__["__parent__"] = explicit

        with mock.patch(
            "plone.pgcatalog.maintenance.getSite", return_value=site
        ):
            _view = compat.indexes

        # Still the explicit parent — self-heal must only set when missing.
        assert compat.__dict__["__parent__"] is explicit
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py::TestParentSelfHeal -v
```

Expected: `test_property_heals_missing_parent_via_get_site` FAILs (`__parent__` missing after access). The other two may pass accidentally.

- [ ] **Step 3: Extend `_CatalogCompat.indexes` property**

In `src/plone/pgcatalog/maintenance.py`, locate the `indexes` property on `_CatalogCompat` (≈ line 198 in b60) and replace the body:

```python
    @property
    def indexes(self):
        """..."""  # keep existing docstring — extend with "and __parent__"
        state = self.__dict__
        raw = state.get("_raw_indexes")
        if raw is None:
            raw = state.pop("indexes", None)
            if raw is None:
                raw = PersistentMapping()
            state["_raw_indexes"] = raw
            self._p_changed = True
        if state.get("__parent__") is None:
            site = getSite()
            tool = getattr(site, "portal_catalog", None) if site else None
            if tool is not None:
                state["__parent__"] = tool
                self._p_changed = True
        return _CatalogIndexesView(self, raw)
```

Extend the docstring to mention that `__parent__` is also self-healed when `getSite` yields a tool — the b60 docstring already describes the raw-indexes rename; add one paragraph.

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py::TestParentSelfHeal -v
```

Expected: 3 passed.

- [ ] **Step 5: Regression — full existing view/compat suite**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py -q
```

Expected: all pre-existing tests still pass (one failure is acceptable at this point only if it's `TestViewWithoutCatalog::test_view_returns_raw_on_getitem_when_no_catalog` — we rewrite that in Task 4).

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/maintenance.py tests/test_catalog_indexes_view.py
git commit -m "feat(maintenance): self-heal __parent__ on indexes property access (#146)"
```

---

## Task 3: `_CatalogCompat.getIndex` uses `_resolve_catalog`

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py` — `_CatalogCompat.getIndex`
- Test: `tests/test_catalog_indexes_view.py` — new class `TestGetIndexWithoutParent`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_catalog_indexes_view.py`:

```python
# ── getIndex no longer falls back to the raw index silently ─────────────


class TestGetIndexWithoutParent:
    def test_finds_catalog_via_get_site_returns_wrapped(
        self, pg_conn_with_catalog
    ):
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.pgindex import PGIndex

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        compat = _CatalogCompat()
        tool._catalog = compat
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        # no __parent__, no acquisition — only getSite works
        site = mock.Mock(portal_catalog=tool)
        with mock.patch(
            "plone.pgcatalog.maintenance.getSite", return_value=site
        ):
            result = compat.getIndex("portal_type")

        assert isinstance(result, PGIndex)
        assert result._wrapped is raw

    def test_raises_runtimeerror_when_all_three_fail(self):
        import pytest
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat()
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        with (
            mock.patch("plone.pgcatalog.maintenance.getSite", return_value=None),
            pytest.raises(RuntimeError, match="cannot find portal_catalog"),
        ):
            compat.getIndex("portal_type")

    def test_explicit_parent_still_works(self, pg_conn_with_catalog):
        """Regression guard — the happy path remains unchanged."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.pgindex import PGIndex

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        compat = _CatalogCompat(parent=tool)
        tool._catalog = compat
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        result = compat.getIndex("portal_type")
        assert isinstance(result, PGIndex)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py::TestGetIndexWithoutParent -v
```

Expected:
- `test_finds_catalog_via_get_site_returns_wrapped` FAILs (getIndex returns raw mock, not PGIndex — currently falls back to raw).
- `test_raises_runtimeerror_when_all_three_fail` FAILs (currently no raise).
- `test_explicit_parent_still_works` PASSES (unchanged path).

- [ ] **Step 3: Rewrite `_CatalogCompat.getIndex`**

Replace the body of `_CatalogCompat.getIndex` in `src/plone/pgcatalog/maintenance.py` (≈ line 211 in b60):

```python
    def getIndex(self, name):
        """Return a PG-backed index wrapper for *name*.

        Mirrors ``self.indexes[name]`` but implemented directly on the
        method so legacy callers (eea.facetednavigation,
        plone.app.vocabularies.Keywords) keep working through the
        Acquisition wrapper.

        Unlike the pre-#146 implementation, this does **not** fall back
        to the raw ZCatalog index when the catalog tool is
        unreachable — a raw index has empty BTrees in pgcatalog and
        silently returns empty result sets, which masked #143/#146 for
        weeks.  Instead, ``_resolve_catalog`` raises ``RuntimeError``
        if none of its three lookup strategies finds the catalog.
        """
        raw_index = self._raw_indexes[name]  # raises KeyError if missing
        catalog = _resolve_catalog(self)
        return _maybe_wrap_index(catalog, name, raw_index)
```

Note: the existing import of `_maybe_wrap_index` at the top of `maintenance.py` stays.

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py::TestGetIndexWithoutParent -v
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/maintenance.py tests/test_catalog_indexes_view.py
git commit -m "feat(maintenance): getIndex uses _resolve_catalog, no raw fallback (#146)"
```

---

## Task 4: `_CatalogIndexesView.__getitem__` uses `_resolve_catalog` + update legacy tests

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py` — `_CatalogIndexesView.__getitem__` + `.get`
- Test: `tests/test_catalog_indexes_view.py` — rewrite `TestViewWithoutCatalog`

- [ ] **Step 1: Rewrite `TestViewWithoutCatalog` to match the new contract**

Replace the existing class in `tests/test_catalog_indexes_view.py`:

```python
# ── View with no reachable catalog: must raise, not return raw ───────────


class TestViewWithoutCatalog:
    """Pre-#146 behavior: getitem returned the raw ZCatalog index when
    aq_parent returned None.  That masked silent "empty results" bugs.
    New contract: the view raises RuntimeError when no catalog is
    reachable.  Callers that intentionally work without a catalog now
    need to set __parent__ explicitly.
    """

    def test_getitem_raises_when_no_catalog_reachable(self):
        import pytest
        compat = _fresh_compat()
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        with (
            mock.patch(
                "plone.pgcatalog.maintenance.getSite", return_value=None
            ),
            pytest.raises(RuntimeError, match="cannot find portal_catalog"),
        ):
            _ = compat.indexes["portal_type"]

    def test_view_keys_are_unwrapped_strings(self):
        compat = _fresh_compat()
        compat._raw_indexes["a"] = mock.Mock()
        compat._raw_indexes["b"] = mock.Mock()
        assert set(compat.indexes.keys()) == {"a", "b"}

    def test_view_contains_and_len(self):
        compat = _fresh_compat()
        compat._raw_indexes["a"] = mock.Mock()
        assert "a" in compat.indexes
        assert "missing" not in compat.indexes
        assert len(compat.indexes) == 1

    def test_view_iteration_yields_keys(self):
        compat = _fresh_compat()
        compat._raw_indexes["a"] = mock.Mock()
        compat._raw_indexes["b"] = mock.Mock()
        assert sorted(iter(compat.indexes)) == ["a", "b"]
```

Note: `keys`, `__contains__`, `__len__`, `__iter__` don't need the catalog at all — they operate on `_raw` directly. Those tests stay green without change.

- [ ] **Step 2: Run tests — only `test_getitem_raises_when_no_catalog_reachable` fails**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py::TestViewWithoutCatalog -v
```

Expected: 3 pass, 1 FAIL (`test_getitem_raises_when_no_catalog_reachable`).

- [ ] **Step 3: Rewrite `_CatalogIndexesView.__getitem__` and `.get`**

In `src/plone/pgcatalog/maintenance.py`, locate `_CatalogIndexesView.__getitem__` (≈ line 116) and replace:

```python
    # read-through access → wrapped
    def __getitem__(self, key):
        raw_index = self._raw[key]  # raises KeyError
        catalog = _resolve_catalog(self._compat)
        return _maybe_wrap_index(catalog, key, raw_index)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
```

Note the `get` method's behavior: it catches only `KeyError` (missing key), **not** `RuntimeError`. An unreachable catalog is a configuration bug, not a missing key — the `RuntimeError` must propagate to the caller.

- [ ] **Step 4: Run tests to verify all pass**

```bash
.venv/bin/python -m pytest tests/test_catalog_indexes_view.py -q
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/maintenance.py tests/test_catalog_indexes_view.py
git commit -m "feat(maintenance): view __getitem__ uses _resolve_catalog (#146)"
```

---

## Task 5: `_PGIndexMapping.__getitem__`

**Files:**
- Modify: `src/plone/pgcatalog/pgindex.py` — `_PGIndexMapping`
- Test: `tests/test_pgindex.py` — new class `TestPGIndexMappingNewMethods`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pgindex.py`:

```python
# ---------------------------------------------------------------------------
# _PGIndexMapping: __getitem__, __len__, items/values NotImplementedError
# ---------------------------------------------------------------------------


class TestPGIndexMappingNewMethods:
    def test_getitem_returns_zoid_for_existing_value(
        self, pg_conn_with_catalog
    ):
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        assert mapping["uid-aaa-100"] == 100

    def test_getitem_raises_keyerror_on_miss(self, pg_conn_with_catalog):
        import pytest
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        with pytest.raises(KeyError, match="nonexistent-uid"):
            _ = mapping["nonexistent-uid"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexMappingNewMethods -v
```

Expected: both FAIL with `TypeError: '_PGIndexMapping' object is not subscriptable`.

- [ ] **Step 3: Add `__getitem__`**

Insert into `_PGIndexMapping` in `src/plone/pgcatalog/pgindex.py` right after `__contains__`:

```python
    def __getitem__(self, value):
        """Dict-style lookup, raising ``KeyError`` on miss.

        For KeywordIndex, returns the zoid of *some* object whose array
        contains *value* (matches the ``get()`` semantics — not the
        full IITreeSet that ZCatalog's OOBTree[value] would return).
        Callers wanting the IITreeSet should use
        ``PGIndex._apply_index({name: value})``.
        """
        zoid = self.get(value)
        if zoid is None:
            raise KeyError(value)
        return zoid
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexMappingNewMethods -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/pgindex.py tests/test_pgindex.py
git commit -m "feat(pgindex): _PGIndexMapping.__getitem__ raises KeyError on miss (#146)"
```

---

## Task 6: `_PGIndexMapping.__len__`

**Files:**
- Modify: `src/plone/pgcatalog/pgindex.py` — `_PGIndexMapping`
- Test: `tests/test_pgindex.py` — extend `TestPGIndexMappingNewMethods`

- [ ] **Step 1: Write the failing tests**

Append to `TestPGIndexMappingNewMethods`:

```python
    def test_len_scalar_index(self, pg_conn_with_catalog):
        _catalog_objects(pg_conn_with_catalog)  # 2 Documents, 1 Folder
        mapping = _PGIndexMapping(
            "portal_type", lambda: pg_conn_with_catalog
        )
        assert len(mapping) == 2  # distinct values

    def test_len_keyword_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType
        _catalog_keyword_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping(
            "Subject",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.KEYWORD,
        )
        # distinct keywords across the three fixture docs
        assert len(mapping) == 4

    def test_len_keyword_with_legacy_scalar_row(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType
        _catalog_keyword_objects(pg_conn_with_catalog)
        insert_object(pg_conn_with_catalog, 299)
        catalog_object(
            pg_conn_with_catalog,
            zoid=299,
            path="/plone/legacy-scalar",
            idx={"portal_type": "Document", "Subject": "Legacy"},
        )
        pg_conn_with_catalog.commit()
        mapping = _PGIndexMapping(
            "Subject",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.KEYWORD,
        )
        assert len(mapping) == 5  # 4 array keywords + "Legacy" scalar
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest "tests/test_pgindex.py::TestPGIndexMappingNewMethods::test_len_scalar_index" "tests/test_pgindex.py::TestPGIndexMappingNewMethods::test_len_keyword_index" "tests/test_pgindex.py::TestPGIndexMappingNewMethods::test_len_keyword_with_legacy_scalar_row" -v
```

Expected: all FAIL with `TypeError: object of type '_PGIndexMapping' has no len()`.

- [ ] **Step 3: Add `__len__`**

Insert into `_PGIndexMapping`, after `__iter__`:

```python
    def __len__(self):
        """Count distinct index values.

        For scalar indexes: ``SELECT COUNT(DISTINCT idx->>key)``.
        For KEYWORD: same ``UNION ALL`` pattern as ``keys()``, wrapped
        in ``COUNT(DISTINCT val)``.
        """
        try:
            conn = self._get_conn()
        except Exception:
            return 0
        if self._index_type == IndexType.KEYWORD:
            sql = (
                "SELECT COUNT(DISTINCT val) AS n FROM ("
                "  SELECT jsonb_array_elements_text(idx->%(key)s) AS val "
                "    FROM object_state "
                "    WHERE idx ? %(key)s "
                "      AND jsonb_typeof(idx->%(key)s) = 'array' "
                "  UNION ALL "
                "  SELECT idx->>%(key)s AS val "
                "    FROM object_state "
                "    WHERE idx ? %(key)s "
                "      AND jsonb_typeof(idx->%(key)s) NOT IN ('array', 'null') "
                ") u WHERE val IS NOT NULL"
            )
        else:
            sql = (
                "SELECT COUNT(DISTINCT idx->>%(key)s) AS n "
                "FROM object_state "
                "WHERE idx ? %(key)s AND idx->>%(key)s IS NOT NULL"
            )
        with conn.cursor() as cur:
            cur.execute(sql, {"key": self._idx_key})
            row = cur.fetchone()
        return int(row["n"]) if row and row["n"] is not None else 0
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexMappingNewMethods -v
```

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/pgindex.py tests/test_pgindex.py
git commit -m "feat(pgindex): _PGIndexMapping.__len__ via COUNT DISTINCT (#146)"
```

---

## Task 7: `_PGIndexMapping.items` / `values` raise `NotImplementedError`

**Files:**
- Modify: `src/plone/pgcatalog/pgindex.py` — `_PGIndexMapping`
- Test: `tests/test_pgindex.py` — extend `TestPGIndexMappingNewMethods`

- [ ] **Step 1: Write the failing tests**

Append to `TestPGIndexMappingNewMethods`:

```python
    def test_items_raises_notimplemented_with_guidance(
        self, pg_conn_with_catalog
    ):
        import pytest
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        with pytest.raises(NotImplementedError) as excinfo:
            mapping.items()
        msg = str(excinfo.value)
        assert "items()" in msg
        assert "uniqueValues" in msg
        assert "_apply_index" in msg
        assert "catalog(**query)" in msg
        assert "https://github.com/bluedynamics/plone-pgcatalog/issues" in msg

    def test_values_raises_notimplemented_with_guidance(
        self, pg_conn_with_catalog
    ):
        import pytest
        _catalog_objects(pg_conn_with_catalog)
        mapping = _PGIndexMapping("UID", lambda: pg_conn_with_catalog)
        with pytest.raises(NotImplementedError) as excinfo:
            mapping.values()
        assert "values()" in str(excinfo.value)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest "tests/test_pgindex.py::TestPGIndexMappingNewMethods::test_items_raises_notimplemented_with_guidance" "tests/test_pgindex.py::TestPGIndexMappingNewMethods::test_values_raises_notimplemented_with_guidance" -v
```

Expected: both FAIL with `AttributeError: '_PGIndexMapping' object has no attribute 'items'`.

- [ ] **Step 3: Add message template and methods**

Above the `_PGIndexMapping` class in `src/plone/pgcatalog/pgindex.py`, add:

```python
_ITEMS_VALUES_NOT_IMPLEMENTED = (
    "PGIndex._index.{method}() is not implemented in plone.pgcatalog: "
    "the ZCatalog BTree shape [(value, IITreeSet(rids)), ...] materializes "
    "all (value, objects)-pairs of the index, which would be prohibitively "
    "expensive against the PG-backed catalog.  Alternatives:\n"
    "  * catalog.Indexes[name].uniqueValues()                 — distinct values\n"
    "  * catalog.Indexes[name]._apply_index({{name: value}})  — zoids per value\n"
    "  * catalog(**query)                                      — full secured search\n"
    "If you have a legitimate usecase, please file an issue at "
    "https://github.com/bluedynamics/plone-pgcatalog/issues with the "
    "caller and the expected result shape."
)
```

Insert into `_PGIndexMapping` after `__len__`:

```python
    def items(self):
        raise NotImplementedError(
            _ITEMS_VALUES_NOT_IMPLEMENTED.format(method="items")
        )

    def values(self):
        raise NotImplementedError(
            _ITEMS_VALUES_NOT_IMPLEMENTED.format(method="values")
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexMappingNewMethods -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/pgindex.py tests/test_pgindex.py
git commit -m "feat(pgindex): items/values raise NotImplementedError with guidance (#146)"
```

---

## Task 8: `PGIndex._index` property emits `DeprecationWarning`

**Files:**
- Modify: `src/plone/pgcatalog/pgindex.py` — `PGIndex._index` property
- Test: `tests/test_pgindex.py` — new class `TestPGIndexIndexDeprecation`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pgindex.py`:

```python
# ---------------------------------------------------------------------------
# PGIndex._index deprecation warning
# ---------------------------------------------------------------------------


class TestPGIndexIndexDeprecation:
    def test_index_property_emits_deprecation_warning(self):
        import pytest
        from plone.pgcatalog.pgindex import PGIndex

        wrapped = mock.Mock()
        wrapped.id = "portal_type"

        def no_conn():
            raise RuntimeError("no conn")

        idx = PGIndex(wrapped, "portal_type", no_conn)
        with pytest.warns(DeprecationWarning, match="PGIndex._index accessed"):
            _ = idx._index
```

- [ ] **Step 2: Run test to verify it fails**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexIndexDeprecation -v
```

Expected: FAIL — no warning emitted.

- [ ] **Step 3: Add `warnings` import and wrap `_index` property**

At the top of `src/plone/pgcatalog/pgindex.py` — alongside the existing `import logging`:

```python
import warnings
```

Replace the `PGIndex._index` property (currently a one-line `return self._pg_index`):

```python
    @property
    def _index(self):
        """PG-backed mapping that emulates ZCatalog's ``Index._index``
        OOBTree.

        Emitting a ``DeprecationWarning`` signals callers that they are
        on an emulation path; the preferred APIs are
        ``catalog.Indexes[name].uniqueValues()`` for distinct values
        and ``catalog(**query)`` for full searches.  Python's default
        warning filter shows each unique ``(module, lineno, message)``
        once per process, so this amounts to one log line per caller
        site per deploy — no log flood.
        """
        warnings.warn(
            f"PGIndex._index accessed for {self._idx_key!r}: ZCatalog "
            f"BTree-shaped API is emulated against PostgreSQL; prefer "
            f"catalog.Indexes[name].uniqueValues() or catalog(**query).",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._pg_index
```

- [ ] **Step 4: Run test to verify it passes**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexIndexDeprecation -v
```

Expected: PASS.

- [ ] **Step 5: Regression — existing pgindex tests still green**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py -q
```

Expected: all green.

Note: some existing tests access `idx._index` without expecting a warning. `pytest.warns` is only needed when the test *asserts* on a warning; passive accesses are fine — Python's default filter downgrades `DeprecationWarning` to a displayed warning but does not error unless `pytest.ini` or command-line flags turn them into errors. `pyproject.toml` in this repo currently does not have `filterwarnings = error`.

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/pgindex.py tests/test_pgindex.py
git commit -m "feat(pgindex): emit DeprecationWarning on PGIndex._index access (#146)"
```

---

## Task 9: `PGIndex._apply_index` via `_QueryBuilder` reuse

**Files:**
- Modify: `src/plone/pgcatalog/pgindex.py` — new method on `PGIndex`
- Test: `tests/test_pgindex.py` — new class `TestPGIndexApplyIndex`

This is the largest task. Split into a series of TDD rounds — one failing test per IndexType.

### 9a. Scaffolding + FIELD

- [ ] **Step 1: Write the failing scaffolding + field tests**

Append to `tests/test_pgindex.py`:

```python
# ---------------------------------------------------------------------------
# PGIndex._apply_index (issue #146)
# ---------------------------------------------------------------------------


def _apply_pg_index(idx_key, index_type, pg_conn, request):
    """Test helper: build a PGIndex and call _apply_index."""
    from plone.pgcatalog.pgindex import PGIndex

    wrapped = mock.Mock()
    wrapped.id = idx_key
    pg_index = PGIndex(
        wrapped, idx_key, lambda: pg_conn, index_type=index_type
    )
    return pg_index._apply_index(request)


class TestPGIndexApplyIndex:
    def test_returns_empty_set_when_index_not_in_request(
        self, pg_conn_with_catalog
    ):
        from BTrees.IIBTree import IITreeSet
        from plone.pgcatalog.columns import IndexType

        result, info = _apply_pg_index(
            "portal_type",
            IndexType.FIELD,
            pg_conn_with_catalog,
            {"other_index": "whatever"},
        )
        assert isinstance(result, IITreeSet)
        assert list(result) == []
        assert info == ("portal_type",)

    def test_field_index_single_value(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_objects(pg_conn_with_catalog)
        result, info = _apply_pg_index(
            "portal_type",
            IndexType.FIELD,
            pg_conn_with_catalog,
            {"portal_type": "Document"},
        )
        assert set(result) == {100, 101}
        assert info == ("portal_type",)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexApplyIndex -v
```

Expected: both FAIL with `AttributeError` (PGIndex has no `_apply_index`) or similar.

- [ ] **Step 3: Add scaffolding + field dispatch**

At the top of `src/plone/pgcatalog/pgindex.py`:

```python
from BTrees.IIBTree import IITreeSet
```

Insert into `PGIndex` (below `uniqueValues`, above `__getattr__`):

```python
    def _apply_index(self, request, resultset=None):
        """ZCatalog-compatible low-level query entry point.

        Returns ``(IITreeSet(zoids), (index_name,))``.  Matches
        ZCatalog semantics:

        * No implicit security filtering.  Callers that need secured
          results must use ``catalog(**query)``.
        * Empty set if this index isn't in ``request``.
        * ``resultset`` is accepted for signature compatibility but
          currently ignored (see #146 non-goals — index-chaining can
          land in a follow-up issue if a caller needs SQL-side
          intersection).

        Implementation reuses ``plone.pgcatalog.query._QueryBuilder``
        so every registered IndexType and every
        ``IPGIndexTranslator`` utility works for free — the dispatch
        table stays in one place.
        """
        from plone.pgcatalog.query import _QueryBuilder

        index_id = getattr(self._wrapped, "id", self._idx_key)
        if index_id not in request:
            return IITreeSet(), (index_id,)

        warnings.warn(
            f"PGIndex._apply_index({index_id!r}) called: this is a "
            f"ZCatalog-compatibility shim; prefer catalog(**query) for "
            f"the full pgcatalog query pipeline.",
            DeprecationWarning,
            stacklevel=2,
        )

        try:
            conn = self._pg_index._get_conn()
        except Exception:
            return IITreeSet(), (index_id,)

        builder = _QueryBuilder()
        builder._query = request  # for cross-index Language lookup
        builder.clauses.append("idx IS NOT NULL")
        builder._process_index(index_id, request[index_id])
        result = builder.result()
        sql = f"SELECT zoid FROM object_state WHERE {result['where']}"

        with conn.cursor() as cur:
            cur.execute(sql, result["params"])
            zoids = IITreeSet(row["zoid"] for row in cur.fetchall())
        return zoids, (index_id,)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexApplyIndex -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/pgindex.py tests/test_pgindex.py
git commit -m "feat(pgindex): _apply_index scaffolding + FIELD dispatch (#146)"
```

### 9b. KEYWORD + PATH

- [ ] **Step 1: Write the failing KEYWORD and PATH tests**

Append to `TestPGIndexApplyIndex`:

```python
    def test_keyword_index_single_value(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_keyword_objects(pg_conn_with_catalog)
        # Fixture: 200={Werkvortrag, Tirol, Aktuelles}, 201={Tirol,
        # AUSSCHREIBUNG}, 202={Aktuelles}
        result, info = _apply_pg_index(
            "Subject",
            IndexType.KEYWORD,
            pg_conn_with_catalog,
            {"Subject": "Tirol"},
        )
        assert set(result) == {200, 201}

    def test_keyword_index_or_operator(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_keyword_objects(pg_conn_with_catalog)
        result, info = _apply_pg_index(
            "Subject",
            IndexType.KEYWORD,
            pg_conn_with_catalog,
            {"Subject": {"query": ["Aktuelles", "AUSSCHREIBUNG"],
                         "operator": "or"}},
        )
        assert set(result) == {200, 201, 202}

    def test_path_index_subtree(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_objects(pg_conn_with_catalog)  # paths /plone/doc1, etc.
        result, info = _apply_pg_index(
            "path",
            IndexType.PATH,
            pg_conn_with_catalog,
            {"path": {"query": "/plone", "depth": -1}},
        )
        assert 100 in result
        assert 101 in result
        assert 102 in result
```

Note: PATH index uses idx-key=None in the real registry; for unit tests we bypass the `_maybe_wrap_index` logic and instantiate `PGIndex` directly.  The builder's `_handle_path` uses the registered idx_key via the registry; make sure the `path` index is registered in the test's populated registry (check `conftest.py::populated_registry` — `path` is added as `(IndexType.PATH, None)`).

- [ ] **Step 2: Run tests to verify they pass or fail**

```bash
.venv/bin/python -m pytest "tests/test_pgindex.py::TestPGIndexApplyIndex::test_keyword_index_single_value" "tests/test_pgindex.py::TestPGIndexApplyIndex::test_keyword_index_or_operator" "tests/test_pgindex.py::TestPGIndexApplyIndex::test_path_index_subtree" -v
```

Expected: all 3 PASS immediately — the `_QueryBuilder` already handles KEYWORD/PATH. This is the reuse-dividend. If any of them fails, the bug is either in the fixture setup or the builder (file a sub-issue; do not inline-fix in this task).

- [ ] **Step 3: Commit**

```bash
git add tests/test_pgindex.py
git commit -m "test(pgindex): regression tests for _apply_index KEYWORD/PATH (#146)"
```

### 9c. DATE / DATE_RANGE / TEXT / UUID / BOOLEAN — smoke coverage

- [ ] **Step 1: Write one smoke test per remaining IndexType**

Append to `TestPGIndexApplyIndex`:

```python
    def test_date_index_range_query(self, pg_conn_with_catalog):
        from datetime import datetime, UTC
        from plone.pgcatalog.columns import IndexType

        insert_object(pg_conn_with_catalog, 300)
        catalog_object(
            pg_conn_with_catalog,
            zoid=300,
            path="/plone/event-2025",
            idx={"portal_type": "Event", "start": "2025-06-15T10:00:00+00:00"},
        )
        insert_object(pg_conn_with_catalog, 301)
        catalog_object(
            pg_conn_with_catalog,
            zoid=301,
            path="/plone/event-2026",
            idx={"portal_type": "Event", "start": "2026-03-15T10:00:00+00:00"},
        )
        pg_conn_with_catalog.commit()

        result, info = _apply_pg_index(
            "start",
            IndexType.DATE,
            pg_conn_with_catalog,
            {"start": {"query": datetime(2026, 1, 1, tzinfo=UTC),
                       "range": "min"}},
        )
        assert 301 in result
        assert 300 not in result

    def test_boolean_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        insert_object(pg_conn_with_catalog, 400)
        catalog_object(
            pg_conn_with_catalog,
            zoid=400,
            path="/plone/default",
            idx={"portal_type": "Document", "is_default_page": True},
        )
        insert_object(pg_conn_with_catalog, 401)
        catalog_object(
            pg_conn_with_catalog,
            zoid=401,
            path="/plone/non-default",
            idx={"portal_type": "Document", "is_default_page": False},
        )
        pg_conn_with_catalog.commit()

        result, info = _apply_pg_index(
            "is_default_page",
            IndexType.BOOLEAN,
            pg_conn_with_catalog,
            {"is_default_page": True},
        )
        assert 400 in result
        assert 401 not in result

    def test_uuid_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.columns import IndexType

        _catalog_objects(pg_conn_with_catalog)
        result, info = _apply_pg_index(
            "UID",
            IndexType.UUID,
            pg_conn_with_catalog,
            {"UID": "uid-aaa-100"},
        )
        assert set(result) == {100}
```

- [ ] **Step 2: Run tests — expect all PASS (builder reuse dividend)**

```bash
.venv/bin/python -m pytest "tests/test_pgindex.py::TestPGIndexApplyIndex::test_date_index_range_query" "tests/test_pgindex.py::TestPGIndexApplyIndex::test_boolean_index" "tests/test_pgindex.py::TestPGIndexApplyIndex::test_uuid_index" -v
```

Expected: all 3 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_pgindex.py
git commit -m "test(pgindex): smoke _apply_index for DATE/BOOLEAN/UUID (#146)"
```

### 9d. Semantics: no security, resultset ignored, deprecation warning

- [ ] **Step 1: Write the remaining semantic tests**

Append to `TestPGIndexApplyIndex`:

```python
    def test_no_implicit_security_filter(self, pg_conn_with_catalog):
        """_apply_index must NOT auto-inject allowed_roles — matches
        ZCatalog semantics.  Object restricted to Managers still shows
        up in the result set.
        """
        from plone.pgcatalog.columns import IndexType

        insert_object(pg_conn_with_catalog, 500)
        catalog_object(
            pg_conn_with_catalog,
            zoid=500,
            path="/plone/private-event",
            idx={
                "portal_type": "Event",
                "allowedRolesAndUsers": ["Manager"],
            },
        )
        pg_conn_with_catalog.commit()

        result, info = _apply_pg_index(
            "portal_type",
            IndexType.FIELD,
            pg_conn_with_catalog,
            {"portal_type": "Event"},
        )
        assert 500 in result

    def test_resultset_parameter_ignored(self, pg_conn_with_catalog):
        """The resultset kwarg is accepted but not yet wired to SQL."""
        from BTrees.IIBTree import IITreeSet
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.pgindex import PGIndex

        _catalog_objects(pg_conn_with_catalog)
        wrapped = mock.Mock()
        wrapped.id = "portal_type"
        idx = PGIndex(
            wrapped,
            "portal_type",
            lambda: pg_conn_with_catalog,
            index_type=IndexType.FIELD,
        )
        base, _ = idx._apply_index({"portal_type": "Document"})
        with_rs, _ = idx._apply_index(
            {"portal_type": "Document"},
            resultset=IITreeSet([100]),  # intentionally wrong
        )
        assert set(base) == set(with_rs)  # resultset ignored

    def test_emits_deprecation_warning(self, pg_conn_with_catalog):
        import pytest
        from plone.pgcatalog.columns import IndexType

        _catalog_objects(pg_conn_with_catalog)
        with pytest.warns(DeprecationWarning, match="_apply_index"):
            _apply_pg_index(
                "portal_type",
                IndexType.FIELD,
                pg_conn_with_catalog,
                {"portal_type": "Document"},
            )

    def test_handles_connection_error_returns_empty_set(self):
        from BTrees.IIBTree import IITreeSet
        from plone.pgcatalog.columns import IndexType
        from plone.pgcatalog.pgindex import PGIndex

        def bad_conn():
            raise RuntimeError("no conn")

        wrapped = mock.Mock()
        wrapped.id = "portal_type"
        idx = PGIndex(
            wrapped, "portal_type", bad_conn, index_type=IndexType.FIELD
        )
        result, info = idx._apply_index({"portal_type": "Document"})
        assert isinstance(result, IITreeSet)
        assert list(result) == []
```

- [ ] **Step 2: Run tests — all should PASS**

```bash
.venv/bin/python -m pytest tests/test_pgindex.py::TestPGIndexApplyIndex -v
```

Expected: all green.

- [ ] **Step 3: Commit**

```bash
git add tests/test_pgindex.py
git commit -m "test(pgindex): _apply_index semantics (no security, deprecation, errors) (#146)"
```

---

## Task 10: Integration test — `KeywordsVocabulary` end-to-end

**Files:**
- Test: `tests/test_plone_integration.py` — new class `TestKeywordsVocabulary`

- [ ] **Step 1: Write the integration test**

Append to `tests/test_plone_integration.py`:

```python
# ===========================================================================
# KeywordsVocabulary — end-to-end tag autocomplete via PGIndex (#146)
# ===========================================================================


class TestKeywordsVocabulary:
    """Regression for #146: typing into the tag/Schlagwort autocomplete
    offers individual keywords, not serialized JSON arrays or nothing."""

    def test_keywords_vocabulary_returns_individual_keywords(
        self, pgcatalog_layer
    ):
        from plone.app.testing import setRoles
        from plone.app.testing import TEST_USER_ID
        from plone.app.vocabularies.catalog import KeywordsVocabularyFactory

        portal = pgcatalog_layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        # Create two Documents with distinct Subject keywords.
        doc1 = portal.invokeFactory(
            "Document", "doc1", title="D1", subject=("alpha", "beta"),
        )
        doc2 = portal.invokeFactory(
            "Document", "doc2", title="D2", subject=("beta", "gamma"),
        )
        portal[doc1].reindexObject()
        portal[doc2].reindexObject()

        import transaction
        transaction.commit()

        vocab = KeywordsVocabularyFactory(portal)
        tokens = {term.value for term in vocab}
        assert tokens == {"alpha", "beta", "gamma"}

    def test_keywords_vocabulary_survives_missing_parent(
        self, pgcatalog_layer
    ):
        """Regression guard for the __parent__-missing production case:
        clear __parent__ on the compat, fetch vocabulary — ``getIndex``
        still finds the tool via ``_resolve_catalog``'s ``getSite``
        branch and the vocabulary returns correct tokens.

        Persistent self-heal of ``__parent__`` is tested separately
        against the ``.indexes`` property (Task 2).  The ``getIndex``
        code path used by the vocabulary doesn't persist via self-heal
        — it relies on the lookup fallback each time.  Both behaviors
        are correct and complementary.
        """
        from plone.app.testing import setRoles
        from plone.app.testing import TEST_USER_ID
        from plone.app.vocabularies.catalog import KeywordsVocabularyFactory
        import transaction

        portal = pgcatalog_layer["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])

        doc_id = portal.invokeFactory(
            "Document", "doc-x", title="X", subject=("AT26",),
        )
        portal[doc_id].reindexObject()
        transaction.commit()

        # Simulate production state: wipe __parent__ from the compat.
        compat = portal.portal_catalog._catalog
        compat.__dict__.pop("__parent__", None)

        vocab = KeywordsVocabularyFactory(portal)
        tokens = {term.value for term in vocab}
        assert "AT26" in tokens
```

- [ ] **Step 2: Run tests**

```bash
.venv/bin/python -m pytest tests/test_plone_integration.py::TestKeywordsVocabulary -v
```

Expected: both PASS. If `test_keywords_vocabulary_returns_individual_keywords` fails, the wiring between `_resolve_catalog` / self-heal / `_PGIndexMapping.__iter__` has a gap — debug before moving on.

- [ ] **Step 3: Commit**

```bash
git add tests/test_plone_integration.py
git commit -m "test(integration): KeywordsVocabulary end-to-end via PGIndex (#146)"
```

---

## Task 11: Full regression suite

- [ ] **Step 1: Run the full suite**

```bash
.venv/bin/python -m pytest tests/ --no-header -q
```

Expected: `~1420 passed, 23 skipped, 0 failed` (roughly 1403 existing + ~17 new tests).

- [ ] **Step 2: If anything red, fix or pause**

If the only failures are pre-existing flakes (check against `main`), note them but don't block. If any failure is in a test we wrote or in code we changed, fix inline before moving to CHANGES.md.

---

## Task 12: CHANGES.md + release notes

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Add entry for 1.0.0b61**

Prepend above `## 1.0.0b60` in `CHANGES.md`:

```markdown
## 1.0.0b61

### Fixed

- `_CatalogCompat.getIndex` and `_CatalogIndexesView.__getitem__` no
  longer silently fall back to the raw ZCatalog index when they
  cannot find the catalog tool — that fallback returned empty BTrees
  and masked #143 / #146 for weeks.  Instead a new private helper
  ``_resolve_catalog`` tries three paths in order (``__parent__`` →
  acquisition chain → ``zope.component.hooks.getSite().portal_catalog``)
  and raises ``RuntimeError`` if all three fail.

- ``_CatalogCompat.indexes`` property now self-heals a missing
  ``__parent__`` on first access, using the same
  ``getSite().portal_catalog`` lookup.  The first page render after
  deploy persists ``__parent__``; no second upgrade-step click is
  required on sites where the #139 upgrade ran before that fix
  landed (prod-recovery for aaf-prod).

### Added

- ``PGIndex._apply_index(request, resultset=None)`` — ZCatalog-
  compatible low-level query entry point.  Returns
  ``(IITreeSet(zoids), (index_name,))``.  Reuses
  ``_QueryBuilder._process_index`` so every registered IndexType
  plus every ``IPGIndexTranslator`` utility works for free.  No
  implicit security filtering (matches ZCatalog semantics; use
  ``catalog(**query)`` for secured results).  Emits a
  ``DeprecationWarning`` once per caller site.

- ``_PGIndexMapping.__getitem__`` / ``__len__`` — round out the
  PG-backed mapping so Plone core callers
  (``plone.app.uuid.utils``, ``plone.app.vocabularies.Keywords``)
  work against ``catalog._catalog.getIndex(name)._index`` without
  needing ``catalog.Indexes[name]`` acquisition.

- ``_PGIndexMapping.items()`` / ``values()`` raise
  ``NotImplementedError`` with guidance pointing at ``uniqueValues``,
  ``_apply_index``, and ``catalog(**query)`` as alternatives.  No
  Plone-core caller uses them on a wrapped index; a concrete usecase
  can land in a future issue with server-side-cursor streaming.

- ``PGIndex._index`` property emits a ``DeprecationWarning`` on
  access — signals callers that the BTree-shaped API is an
  emulation and suggests the preferred pgcatalog-native
  alternatives.

Closes #146.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGES.md
git commit -m "docs: CHANGES entry for 1.0.0b61 (#146)"
```

---

## Task 13: Push, PR, merge, release

- [ ] **Step 1: Push branch**

```bash
git push -u origin fix/pgindex-apply-index-and-parent-resolution
```

- [ ] **Step 2: Open PR via `gh pr create`**

PR title: `fix(pgindex): _apply_index + robust catalog resolution (#146)`

PR body template (use `gh pr create --body "$(cat <<'EOF' ... EOF)"`):

```markdown
## Summary

Closes #146. Compound fix for the Schlagwort-autocomplete production bug on aaf-prod:

- **Root cause 1** — `_CatalogCompat` on prod had no `__parent__`; `getIndex` silently fell back to the raw ZCatalog index (empty BTree). Fixed by `_resolve_catalog` three-step lookup that never returns `None`.
- **Root cause 2** — even with the fallback removed, `PGIndex` delegated `_apply_index` to the raw ZCatalog index for every caller that used the ZCatalog low-level API (`plone.app.vocabularies.KeywordsVocabulary.keywords_of_section` etc.). Fixed by implementing `_apply_index` via `_QueryBuilder._process_index` reuse.
- **Round-out** — `_PGIndexMapping` gains `__getitem__`, `__len__`, and explicit `NotImplementedError` for `items()`/`values()`. `PGIndex._index` emits a `DeprecationWarning`.

Spec: `docs/superpowers/specs/2026-04-20-pgindex-apply-index-design.md`.

## Test plan

- [x] Unit: ~17 new tests covering `_resolve_catalog`, parent self-heal, getIndex without parent, `_PGIndexMapping.__getitem__`/`__len__`/`items`/`values`, `PGIndex._index` deprecation, `PGIndex._apply_index` for FIELD/KEYWORD/PATH/DATE/BOOLEAN/UUID + semantics (no security, resultset ignored, deprecation, conn errors).
- [x] Updated legacy `TestViewWithoutCatalog` to match the new no-silent-fallback contract.
- [x] Integration: `TestKeywordsVocabulary` end-to-end on the Plone layer — verifies tag autocomplete returns individual keywords and survives missing `__parent__` via `getSite` fallback.
- [x] Full suite: ~1420 passed / 23 skipped / 0 failed.
- [ ] After deploy on aaf-prod: typing "AT" in the Collection Schlagwort widget offers "AT26"; `SELECT state ? '__parent__' FROM object_state WHERE zoid = <compat_zoid>` flips to `t` after the first request.
```

- [ ] **Step 3: Wait for review & merge**

Johannes reviews. Address comments inline, re-push, re-request review. Once approved and merged, continue.

- [ ] **Step 4: Tag & release**

```bash
git switch main
git pull --ff-only origin main
git tag -a v1.0.0b61 -m "Release 1.0.0b61"
git push origin v1.0.0b61
gh release create v1.0.0b61 --prerelease --title "v1.0.0b61" \
  --notes "$(sed -n '/^## 1.0.0b61/,/^## 1.0.0b60/p' CHANGES.md | sed '1d;$d')"
```

- [ ] **Step 5: Deploy verification on aaf-prod**

After the aaf deployment picks up v1.0.0b61:

```bash
# 1) Hit any page that triggers vocabulary access (e.g. Collection edit view).

# 2) Verify __parent__ self-heal persisted on the compat.
kubectl -n aaf-prod exec aaf-deployment-db-cluster-c897d15b-1 -c postgres -- \
  psql -U postgres -d plone -c \
  "SELECT state ? '__parent__' AS parent_set FROM object_state WHERE zoid=66615937;"
# expected: t

# 3) Manual check: type "AT" into the Collection Schlagwort field on /Plone/at26/programm/edit — "AT26" must appear as a suggestion.

# 4) Verify no TypeError / Unauthorized / empty-vocab errors in recent logs.
kubectl -n aaf-prod logs --tail=500 --since=10m \
  deployment/aaf-deployment-plone-backend-deployment-c8f5cad7 \
  | grep -cE "keywords must be strings|Unauthorized: You are not allowed to access"
# expected: 0
```

- [ ] **Step 6: Close #146**

If prod verification passes, close the GitHub issue referencing the merged PR and the release.

---

## Out-of-scope — open separate follow-up issues (do not implement here)

These surfaced during design but are not part of this plan:

- `items()` / `values()` real implementation with server-side-cursor lazy streaming — only if a concrete caller appears and files an issue.
- `_apply_index(resultset=...)` SQL-side index-chaining — only if a caller needs it.
- Security filter on `_apply_index` — explicitly rejected (matches ZCatalog semantics).
- `object_state`-as-catalog-source architectural concerns (security on `uniqueValues`, scan cost on huge catalogs, separate `catalog_state` view) — own tracking issue.
