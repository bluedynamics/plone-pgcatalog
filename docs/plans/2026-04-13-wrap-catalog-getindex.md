# Wrap `_catalog.getIndex()` with PGIndex — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `catalog._catalog.getIndex(name)` return a PG-backed `PGIndex` wrapper so Plone code that bypasses `catalog.Indexes[name]` (e.g. `plone.app.vocabularies.KeywordsVocabulary`, `Products.CMFPlone.browser.search`) reads real data instead of empty ZCatalog BTrees.

**Architecture:** The existing `PGCatalogIndexes._getOb()` wraps indexes accessed via `catalog.Indexes[name]` with `PGIndex`. This plan extends the same wrapping to `_CatalogCompat.getIndex()`. Since the shim has no direct reference to the catalog tool, we resolve it via Acquisition (`aq_parent(self)`) and delegate to the catalog's `Indexes._getOb()`.

**Tech Stack:** Python 3.13, psycopg3, Zope Acquisition, ZODB, pytest.

---

## Background

Two orthogonal APIs access ZCatalog indexes:

| API | Current behavior in plone-pgcatalog | Status |
|---|---|---|
| `catalog.Indexes[name]` | Returns `PGIndex` wrapper via `PGCatalogIndexes._getOb` | ✅ Works |
| `catalog._catalog.getIndex(name)` | Returns raw ZCatalog index (empty BTrees) via `_CatalogCompat.getIndex` | ❌ Broken |

Real-world callers using the broken path (verified in a typical Plone 6 install):

- `plone.app.vocabularies.catalog.KeywordsVocabulary.all_keywords()` —
  used by all Subject/Keywords widgets and search facets.
  Reads `index._index` (the BTree).
- `Products.CMFPlone.browser.search.Search.types_list()` —
  populates the search form's "Item type" dropdown.
  Reads `index.uniqueValues()`.
- `plone.app.event.setuphandlers` — DateIndex migration check.
  Reads `index.meta_type`.
- `plone.app.multilingual.browser.migrator` — Language index access.
  Reads `index._index`.

### Symptom

- Keyword/Subject widgets show empty vocabularies.
- The `@@search` form's "Item type" filter is empty.
- Language migration fails to detect existing indexes.

### Related work

`PGIndex` already exists ([pgindex.py](../../src/plone/pgcatalog/pgindex.py)) and is fully tested in
[test_pgindex.py](../../tests/test_pgindex.py). It provides:

- `._index` property returning a `_PGIndexMapping` that implements
  `.get(value, default)`, `.__contains__`, and `.keys()` against PG.
- `.uniqueValues(name=None, withLengths=False)` against PG.
- `.__getattr__` that delegates unknown attrs (e.g. `meta_type`, `id`)
  to the wrapped ZCatalog index.

So the infrastructure is in place; this plan only wires `_CatalogCompat.getIndex()` into it.

---

## File Structure

| File | Role | Changes |
|---|---|---|
| `src/plone/pgcatalog/maintenance.py` | `_CatalogCompat` shim | Modify `getIndex()` to delegate to catalog.Indexes._getOb via Acquisition |
| `src/plone/pgcatalog/pgindex.py` | `PGIndex`, `PGCatalogIndexes` | Extract wrapping logic into a reusable helper function (currently inside `PGCatalogIndexes._getOb`) |
| `tests/test_clean_break.py` | `_CatalogCompat` tests | Update existing test `test_get_index` (it asserts raw-index behavior) + add new tests |
| `tests/test_pgindex.py` | PGIndex tests | Add integration-style test for the `_catalog.getIndex(name)` path |
| `CHANGES.md` | Changelog | Add entry under 1.0.0b50 |

---

## Tasks

### Task 1: Extract PGIndex wrapping into reusable helper

**Rationale:** `PGCatalogIndexes._getOb()` contains the logic for "given a catalog + index name, return a PGIndex if appropriate". We need to call the same logic from `_CatalogCompat.getIndex()`. Extracting it avoids duplication.

**Files:**
- Modify: `src/plone/pgcatalog/pgindex.py`
- Test: `tests/test_pgindex.py`

- [ ] **Step 1: Read the current `PGCatalogIndexes._getOb` implementation**

Read [pgindex.py:133-169](../../src/plone/pgcatalog/pgindex.py) to understand the existing logic.

The current code:

```python
class PGCatalogIndexes(ZCatalogIndexes):
    def _getOb(self, id, default=_marker):  # noqa: A002
        index = super()._getOb(id, default)
        if index is None or (default is not _marker and index is default):
            return index

        catalog = aq_parent(aq_inner(self))
        if catalog is None:
            return index

        from plone.pgcatalog.interfaces import IPGCatalogTool
        if not IPGCatalogTool.providedBy(catalog):
            return index

        from plone.pgcatalog.columns import get_registry
        registry = get_registry()
        entry = registry.get(id)
        if entry is not None:
            idx_key = entry[1]
            if idx_key is None:
                return index  # Special index — no wrapping
        else:
            idx_key = id

        return PGIndex(index, idx_key, catalog._get_pg_read_connection)
```

- [ ] **Step 2: Write failing unit test for the new helper**

Add to `tests/test_pgindex.py` at the end of the file:

```python
class TestMaybeWrapIndex:
    """Test the _maybe_wrap_index() helper."""

    def test_wraps_field_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import _maybe_wrap_index
        from unittest import mock

        catalog = mock.Mock()
        catalog._get_pg_read_connection = lambda: pg_conn_with_catalog
        raw_index = mock.Mock()
        raw_index.id = "portal_type"
        raw_index.meta_type = "FieldIndex"

        wrapped = _maybe_wrap_index(catalog, "portal_type", raw_index)
        assert isinstance(wrapped, PGIndex)

    def test_returns_raw_for_non_pg_catalog(self):
        """Non-IPGCatalogTool catalogs get the raw index back."""
        from plone.pgcatalog.pgindex import _maybe_wrap_index
        from unittest import mock

        # A catalog that does NOT implement IPGCatalogTool
        from zope.interface import Interface

        class IOtherCatalog(Interface):
            pass

        catalog = mock.Mock()
        from zope.interface import directlyProvides

        directlyProvides(catalog, IOtherCatalog)
        raw_index = mock.Mock()

        wrapped = _maybe_wrap_index(catalog, "portal_type", raw_index)
        assert wrapped is raw_index

    def test_returns_raw_for_none_index(self):
        """None stays None."""
        from plone.pgcatalog.pgindex import _maybe_wrap_index

        assert _maybe_wrap_index(object(), "x", None) is None

    def test_special_index_not_wrapped(self, pg_conn_with_catalog):
        """Indexes registered with idx_key=None (SearchableText,
        path, effectiveRange) return the raw index unchanged.
        """
        from plone.pgcatalog.pgindex import _maybe_wrap_index
        from plone.pgcatalog.columns import IndexType, get_registry
        from unittest import mock

        # Register SearchableText with idx_key=None
        registry = get_registry()
        registry.register(
            name="SearchableText",
            idx_type=IndexType.TEXT,
            idx_key=None,
            source_attrs=[],
        )

        from plone.pgcatalog.interfaces import IPGCatalogTool
        from zope.interface import directlyProvides

        catalog = mock.Mock()
        directlyProvides(catalog, IPGCatalogTool)
        catalog._get_pg_read_connection = lambda: pg_conn_with_catalog
        raw_index = mock.Mock()

        wrapped = _maybe_wrap_index(catalog, "SearchableText", raw_index)
        assert wrapped is raw_index
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_pgindex.py::TestMaybeWrapIndex -xvs`
Expected: FAIL with `ImportError: cannot import name '_maybe_wrap_index'`

- [ ] **Step 4: Extract helper and refactor `PGCatalogIndexes._getOb`**

In `src/plone/pgcatalog/pgindex.py`, add the helper function above `class PGCatalogIndexes`:

```python
def _maybe_wrap_index(catalog, name, raw_index):
    """Wrap *raw_index* with ``PGIndex`` if *catalog* is a PG catalog.

    Returns the raw index unchanged when:

    - The raw index is ``None``.
    - The catalog is not an ``IPGCatalogTool``.
    - The index is registered with ``idx_key=None`` (special indexes
      like SearchableText, path, effectiveRange — they have dedicated
      columns and don't need PG-backed JSONB wrapping).
    """
    if raw_index is None:
        return None

    from plone.pgcatalog.interfaces import IPGCatalogTool

    if not IPGCatalogTool.providedBy(catalog):
        return raw_index

    from plone.pgcatalog.columns import get_registry

    registry = get_registry()
    entry = registry.get(name)
    if entry is not None:
        idx_key = entry[1]
        if idx_key is None:
            return raw_index  # Special index — no wrapping needed
    else:
        idx_key = name  # Fallback: use index name as JSONB key

    return PGIndex(raw_index, idx_key, catalog._get_pg_read_connection)
```

Then simplify `PGCatalogIndexes._getOb` to use it:

```python
class PGCatalogIndexes(ZCatalogIndexes):
    """ZCatalogIndexes replacement that wraps indexes with PGIndex.

    When code accesses ``catalog.Indexes[name]``, this returns a
    ``PGIndex`` proxy instead of the raw ZCatalog index object.
    Special indexes (SearchableText, path, effectiveRange) with
    ``idx_key=None`` are returned unwrapped.
    """

    def _getOb(self, id, default=_marker):  # noqa: A002
        index = super()._getOb(id, default)
        if index is None or (default is not _marker and index is default):
            return index

        catalog = aq_parent(aq_inner(self))
        if catalog is None:
            return index

        return _maybe_wrap_index(catalog, id, index)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_pgindex.py -q`
Expected: all pass (existing 28+ tests + 4 new TestMaybeWrapIndex tests).

- [ ] **Step 6: Commit**

```bash
git add src/plone/pgcatalog/pgindex.py tests/test_pgindex.py
git commit -m "refactor: extract _maybe_wrap_index helper from PGCatalogIndexes._getOb"
```

---

### Task 2: Update `_CatalogCompat.getIndex` to return wrapped indexes

**Rationale:** This is the actual fix. `_CatalogCompat.getIndex()` needs to return a `PGIndex` wrapper, not the raw index.

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py`
- Test: `tests/test_clean_break.py`

- [ ] **Step 1: Write a failing test for wrapped getIndex**

Add to `tests/test_clean_break.py` (after the existing `TestCatalogCompat` class body but before the next class). First locate the existing `test_get_index` method (around line 119) — we're replacing its expectation.

Replace:

```python
    def test_get_index(self, tool):
        # Add an index object, verify getIndex works
        tool._catalog.indexes["test_idx"] = object()
        result = tool._catalog.getIndex("test_idx")
        assert result is tool._catalog.indexes["test_idx"]

    def test_get_index_missing_raises(self, tool):
        with pytest.raises(KeyError):
            tool._catalog.getIndex("nonexistent")
```

with:

```python
    def test_get_index_missing_raises(self, tool):
        with pytest.raises(KeyError):
            tool._catalog.getIndex("nonexistent")

    def test_get_index_returns_pgindex_for_field(self, tool):
        """getIndex wraps field indexes with PGIndex.

        Plone code such as ``plone.app.vocabularies.KeywordsVocabulary``
        and ``Products.CMFPlone.browser.search`` accesses indexes via
        ``catalog._catalog.getIndex(name)`` — this path must return a
        PG-backed wrapper, otherwise ``index._index`` and
        ``index.uniqueValues()`` read empty ZCatalog BTrees.
        """
        from unittest import mock

        raw = mock.Mock()
        raw.id = "portal_type"
        raw.meta_type = "FieldIndex"
        tool._catalog.indexes["portal_type"] = raw
        # Register portal_type so _maybe_wrap_index finds an idx_key.
        from plone.pgcatalog.columns import IndexType, get_registry

        get_registry().register(
            name="portal_type",
            idx_type=IndexType.FIELD,
            idx_key="portal_type",
            source_attrs=["portal_type"],
        )

        from plone.pgcatalog.pgindex import PGIndex

        result = tool._catalog.getIndex("portal_type")
        assert isinstance(result, PGIndex)

    def test_get_index_delegates_meta_type(self, tool):
        """Wrapped index delegates attribute access to the raw index.

        plone.app.event's setuphandlers reads ``index.meta_type`` to
        detect outdated DateIndex definitions — this must still work
        through the wrapper.
        """
        from unittest import mock

        raw = mock.Mock()
        raw.id = "start"
        raw.meta_type = "DateRecurringIndex"
        tool._catalog.indexes["start"] = raw
        from plone.pgcatalog.columns import IndexType, get_registry

        get_registry().register(
            name="start",
            idx_type=IndexType.DATE,
            idx_key="start",
            source_attrs=["start"],
        )

        result = tool._catalog.getIndex("start")
        assert result.meta_type == "DateRecurringIndex"

    def test_get_index_special_index_unwrapped(self, tool):
        """Special indexes (SearchableText, effectiveRange, path) with
        ``idx_key=None`` return the raw index unchanged — they use
        dedicated PG columns, not JSONB ->> access.
        """
        from unittest import mock

        raw = mock.Mock()
        raw.id = "SearchableText"
        raw.meta_type = "ZCTextIndex"
        tool._catalog.indexes["SearchableText"] = raw
        from plone.pgcatalog.columns import IndexType, get_registry

        get_registry().register(
            name="SearchableText",
            idx_type=IndexType.TEXT,
            idx_key=None,
            source_attrs=[],
        )

        result = tool._catalog.getIndex("SearchableText")
        # Special index → not wrapped
        assert result is raw
```

Note: the original `test_get_index` test is removed because its expectation was incorrect — it codified the bug behavior ("return raw index"). The `test_get_index_missing_raises` is kept as-is.

- [ ] **Step 2: Run tests — expect the new ones to fail**

Run: `.venv/bin/pytest tests/test_clean_break.py::TestCatalogCompat -xvs`
Expected: FAIL on `test_get_index_returns_pgindex_for_field` because `_CatalogCompat.getIndex` still returns the raw index.

- [ ] **Step 3: Update `_CatalogCompat.getIndex` to wrap**

In `src/plone/pgcatalog/maintenance.py` find the current implementation (around line 106):

```python
    def getIndex(self, name):
        return self.indexes[name]
```

Replace with:

```python
    def getIndex(self, name):
        """Return a PG-backed index wrapper for *name*.

        Plone code that bypasses ``catalog.Indexes[name]`` (notably
        ``plone.app.vocabularies.KeywordsVocabulary``,
        ``Products.CMFPlone.browser.search.Search.types_list``, and
        ``plone.app.event.setuphandlers``) accesses indexes via
        ``catalog._catalog.getIndex(name)``.  Returning the raw
        ZCatalog index would give those callers empty BTrees, so we
        wrap with ``PGIndex`` — same as ``catalog.Indexes[name]``.

        Raises ``KeyError`` if *name* is not a known index.
        """
        raw_index = self.indexes[name]  # raises KeyError if missing

        from Acquisition import aq_parent
        from plone.pgcatalog.pgindex import _maybe_wrap_index

        catalog = aq_parent(self)
        if catalog is None:
            return raw_index
        return _maybe_wrap_index(catalog, name, raw_index)
```

- [ ] **Step 4: Run the failing tests to verify they now pass**

Run: `.venv/bin/pytest tests/test_clean_break.py::TestCatalogCompat -q`
Expected: all pass (including the 3 new tests).

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/maintenance.py tests/test_clean_break.py
git commit -m "fix: _CatalogCompat.getIndex returns PGIndex wrapper (fixes empty KeywordsVocabulary)"
```

---

### Task 3: Integration test for a realistic Plone code path

**Rationale:** The unit tests use mocks. Add one integration-style test that goes through the actual call pattern used by `plone.app.vocabularies.KeywordsVocabulary.all_keywords()`:

```python
index = catalog._catalog.getIndex("Subject")
list(index._index.keys())   # or index.uniqueValues()
```

**Files:**
- Test: `tests/test_pgindex.py`

- [ ] **Step 1: Locate the existing `_catalog_objects` helper**

Grep in `tests/test_pgindex.py` for `_catalog_objects` to see how fixtures populate PG. Use the same fixture for the new test.

- [ ] **Step 2: Write integration test**

Add to `tests/test_pgindex.py` at the end of `TestCatalogIndexesWrapper`:

```python
    def test_catalog_getindex_keywords_vocabulary_flow(
        self, pg_conn_with_catalog
    ):
        """End-to-end: simulate the KeywordsVocabulary code path.

        plone.app.vocabularies.catalog.KeywordsVocabulary.all_keywords()::

            index = self.catalog._catalog.getIndex(self.keyword_index)
            return safe_simplevocabulary_from_values(index._index, ...)

        The ``index._index`` lookup must return PG-backed data, not the
        empty ZCatalog BTree.  Regression test for empty Subjects/Tags
        dropdowns.
        """
        _catalog_objects(pg_conn_with_catalog)
        catalog = self._make_catalog_with_indexes(pg_conn_with_catalog)

        # Register portal_type in the IndexRegistry so the wrapper knows
        # which JSONB key to query.
        from plone.pgcatalog.columns import IndexType, get_registry

        get_registry().register(
            name="portal_type",
            idx_type=IndexType.FIELD,
            idx_key="portal_type",
            source_attrs=["portal_type"],
        )

        # This is the exact line KeywordsVocabulary runs
        index = catalog._catalog.getIndex("portal_type")

        # Access as KeywordsVocabulary does: index._index
        assert index._index is not None
        # And uniqueValues() as CMFPlone.browser.search does
        values = list(index.uniqueValues())
        # _catalog_objects creates Document and Folder rows
        assert "Document" in values
        assert "Folder" in values
```

- [ ] **Step 3: Run the test**

Run: `.venv/bin/pytest tests/test_pgindex.py::TestCatalogIndexesWrapper::test_catalog_getindex_keywords_vocabulary_flow -xvs`
Expected: PASS.

- [ ] **Step 4: Run the full test suite**

Run: `.venv/bin/pytest tests/ -q`
Expected: all tests pass (no regressions).

- [ ] **Step 5: Commit**

```bash
git add tests/test_pgindex.py
git commit -m "test: integration test for _catalog.getIndex path (KeywordsVocabulary)"
```

---

### Task 4: Changelog + PR

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Check the actual last release**

Run: `gh release list --repo bluedynamics/plone-pgcatalog --limit 3`

Identify the latest tag.  As of 2026-04-12 it is `v1.0.0b49`; the next release is therefore **`1.0.0b50`**.  If a release newer than `v1.0.0b49` has happened in the meantime, adjust the version header below.

- [ ] **Step 2: Add changelog entry**

Open `CHANGES.md`.  Find the top-most unreleased version header (likely `## 1.0.0b50`).  If a `1.0.0b50` header already exists from an earlier merged PR (for example the uuid-list-query fix), add a new bullet under `### Fixed` of that same header.  If no `1.0.0b50` header exists yet, insert one above the last released version:

```markdown
## 1.0.0b50

### Fixed

- ``catalog._catalog.getIndex(name)`` now returns a ``PGIndex`` wrapper
  with PG-backed ``_index`` and ``uniqueValues()``, same as
  ``catalog.Indexes[name]``.  Previously it returned the raw ZCatalog
  index with empty BTrees, which broke:

  - ``plone.app.vocabularies.KeywordsVocabulary`` (empty Subject/Tags
    dropdowns).
  - ``Products.CMFPlone.browser.search.Search.types_list()`` (empty
    "Item type" filter in ``@@search``).
  - ``plone.app.event.setuphandlers`` (DateIndex detection).
  - Other Plone code paths that bypass ``catalog.Indexes[name]``.

  Special indexes registered with ``idx_key=None`` (SearchableText,
  path, effectiveRange) are returned unwrapped so dedicated columns
  are used for them.
```

- [ ] **Step 3: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog for _catalog.getIndex PG wrapping"
```

- [ ] **Step 4: Push branch and create PR**

```bash
git push -u origin fix/catalog-getindex-pg-wrap
gh pr create --repo bluedynamics/plone-pgcatalog --base main \
  --head fix/catalog-getindex-pg-wrap \
  --title "fix: _catalog.getIndex returns PGIndex (fixes empty KeywordsVocabulary)" \
  --body "Second half of the 'empty vocabulary' investigation (first: #113).

## Problem

Plone code that reads catalog indexes via \`catalog._catalog.getIndex(name)\` (as opposed to \`catalog.Indexes[name]\`) receives the raw ZCatalog index with empty BTrees, because plone-pgcatalog never populates ZCatalog's BTree storage.

Affected Plone callers (verified in a typical Plone 6 install):

- \`plone.app.vocabularies.KeywordsVocabulary.all_keywords\` — empty Subject/Tags dropdowns.
- \`Products.CMFPlone.browser.search.Search.types_list\` — empty 'Item type' filter in \`@@search\`.
- \`plone.app.event.setuphandlers\` — DateIndex detection.
- \`plone.app.multilingual.browser.migrator\` — Language index migration.

## Fix

\`_CatalogCompat.getIndex()\` now delegates to the same PGIndex-wrapping logic that \`PGCatalogIndexes._getOb()\` uses. The wrapping logic is extracted into a reusable helper \`_maybe_wrap_index()\` to avoid duplication.

Special indexes (SearchableText, effectiveRange, path) with \`idx_key=None\` keep their raw behavior — they use dedicated PG columns, not JSONB \`->>\`.

## Test plan

- Unit tests for the extracted \`_maybe_wrap_index()\` helper: non-PG catalog, None index, special index unwrapping.
- Unit tests for \`_CatalogCompat.getIndex()\`: wraps field index, delegates \`meta_type\`, special indexes unwrapped.
- Integration test replaying the exact \`plone.app.vocabularies.KeywordsVocabulary\` code path (\`catalog._catalog.getIndex(name)._index\` + \`uniqueValues()\`).
- Existing test \`test_get_index\` removed — it asserted the bug as the expected behavior.

## Related

First half of the investigation was #113 (\`_handle_uuid\` with list queries).
"
```

---

## Self-Review

**Spec coverage**

- `_CatalogCompat.getIndex` now wraps → Task 2 ✓
- Extracted helper avoids duplication with `PGCatalogIndexes._getOb` → Task 1 ✓
- Integration test mirroring the real Plone call path → Task 3 ✓
- Changelog entry → Task 4 ✓
- Old test `test_get_index` that codified the bug is replaced → Task 2 Step 1 ✓
- Special-index handling preserved → Task 1 Step 4, Task 2 Step 1 ✓

**Placeholder scan**

No TODOs, TBDs, or "similar to Task N" references.  Every code block is complete.  The PR body placeholder for `gh pr create` contains the actual body text inline.

**Type consistency**

- `_maybe_wrap_index(catalog, name, raw_index)` — defined in Task 1, used in Task 1 and Task 2.
- `PGIndex(raw_index, idx_key, catalog._get_pg_read_connection)` — constructor signature consistent with existing code in `pgindex.py`.
- Test helper `_catalog_objects(conn)` and fixture `pg_conn_with_catalog` — both already exist in `test_pgindex.py`, confirmed via grep in plan preamble.
- Existing `_make_catalog_with_indexes` method on `TestCatalogIndexesWrapper` — used in Task 3, exists at [test_pgindex.py:236](../../tests/test_pgindex.py).
