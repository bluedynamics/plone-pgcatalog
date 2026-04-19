# Wrap `_catalog.indexes` access — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `portal_catalog._catalog.indexes[name]`, `.get(name)`, `.items()`, `.values()` return `PGIndex`-wrapped objects instead of the raw ZCatalog indexes with empty BTrees, so that the many Plone / addon call-sites that use this private-but-common API pattern get correct results.

**Architecture:** `_CatalogCompat.indexes` becomes a **property** that returns a transient `_CatalogIndexesView` wrapping the underlying `PersistentMapping` and holding a reference to the acquisition-wrapped compat object (from which `_maybe_wrap_index(catalog, name, raw)` reaches the tool via `aq_parent`). The persistent storage is renamed to `_raw_indexes` so the wrapper is never pickled and mutations go to the raw mapping unchanged. A GenericSetup upgrade step from profile version 1 → 2 migrates existing ZODB state (`self.indexes` → `self._raw_indexes`).

**Tech Stack:** Python 3.13, `persistent.PersistentMapping`, `Acquisition.Implicit`, ZCatalog compatibility layer, Plone GenericSetup upgrade steps, pytest.

**Tracking issue:** [bluedynamics/plone-pgcatalog#137](https://github.com/bluedynamics/plone-pgcatalog/issues/137)

---

## Background: why the prior attempt failed

`origin/thet/indexes-wrapper` (commit `6ec453f`) took the obvious approach — subclass `PersistentMapping` as `_CatalogIndexMapping` and override `__getitem__` / `get` / `items` / `values`. That branch is useful as a reference but has three issues:

1. **ZODB migration wasn't run.** `_CatalogCompat.__init__` creating a `_CatalogIndexMapping` only affects **new** sites. Existing sites have a plain `PersistentMapping` already persisted in ZODB; you cannot change a persisted object's class by updating Python. The branch added an upgrade step but the reporter never ran it, hence the `<KeywordIndex at tags>` in their debug session.
2. **Uses `api.portal.get_tool("portal_catalog")` inside the mapping to find the catalog.** Works at runtime but is fragile during bootstrap/tests/scripts and adds an unnecessary traversal on every index access.
3. **Couples ZODB persistence to the wrapper class.** If we ever change the wrapper shape, we need another migration.

This plan fixes all three by making the wrapper **transient** (built from a property) and finding the catalog via **Acquisition from the compat object** (which is already `Implicit`).

---

## File structure

| File | Responsibility | Action |
|---|---|---|
| `src/plone/pgcatalog/maintenance.py` | `_CatalogCompat` shim + one-off DB maintenance | **Modify**: rename persisted `indexes` → `_raw_indexes`; add `indexes` property returning `_CatalogIndexesView`; add the view class. |
| `src/plone/pgcatalog/upgrades/__init__.py` | Package marker for upgrade steps | **Create**: empty file (or docstring). |
| `src/plone/pgcatalog/upgrades/configure.zcml` | ZCML include for upgrade step bundle | **Create**: include `profile-2.zcml`. |
| `src/plone/pgcatalog/upgrades/profile_2.zcml` | GenericSetup upgrade step registration (v1→v2) | **Create**: one `<gs:upgradeStep>` calling `.profile_2.migrate_catalog_indexes`. |
| `src/plone/pgcatalog/upgrades/profile_2.py` | The migration function | **Create**: rename `compat.indexes` attr → `compat._raw_indexes`; mark persistent; idempotent. |
| `src/plone/pgcatalog/configure.zcml` | Package-level ZCML | **Modify**: `<include package=".upgrades" />`. |
| `src/plone/pgcatalog/profiles/default/metadata.xml` | GenericSetup profile version | **Modify**: bump `<version>` from `1` to `2`. |
| `tests/test_catalog_indexes_view.py` | Unit + integration tests for the view | **Create**: tests for `[]`/`.get`/`.items`/`.values`/`.keys`/iteration/mutation, plus the upgrade migration. |
| `CHANGES.md` | Changelog | **Modify**: entry under `## 1.0.0b55`. |

**Internal callers that already use `catalog._catalog.indexes`** (reviewed for impact — all forward correctly via `PGIndex.__getattr__`):

- `src/plone/pgcatalog/columns.py:125` — `sync_from_catalog` iterates `.items()` reading `meta_type`, `getIndexSourceNames`, etc.
- `src/plone/pgcatalog/catalog.py:285,330,352,353` — `indexes()` list, `addIndex`, `delIndex`. Mutations go to raw; `.keys()` returns strings.
- `src/plone/pgcatalog/startup.py:608,636` — DRI/DRIRI translator registration, reads `meta_type`, `attr_recurdef`, `attr_until`, `startindex`, `endindex`.
- `src/plone/pgcatalog/setuphandlers.py:200` — catalog snapshot.

All of these call `getattr(index_obj, "meta_type", None)` / `hasattr` / `getattr(…, default)`, which all pass through `PGIndex.__getattr__`. No internal code needs to change.

**Out of scope (separate follow-ups):**

- Adapting the 13 external packages listed in #137. They all use `cat._catalog.indexes[...]` which this plan fixes; if any of them do something more exotic (e.g. direct `_index.keys()` on an index with a dedicated typed column), that's a separate per-package investigation.
- Caching the `_CatalogIndexesView` across calls. It's tiny; measure first.

---

## Task 1: Failing test scaffold

**Files:**
- Create: `tests/test_catalog_indexes_view.py`

- [ ] **Step 1.1** — Create the test file with failing-first contract tests.

```python
"""Tests for the wrapping behavior of _CatalogCompat.indexes.

See: docs/plans/2026-04-15-wrap-catalog-indexes-attr.md
Issue: bluedynamics/plone-pgcatalog#137
"""

from unittest import mock

import pytest

from persistent.mapping import PersistentMapping


def _fresh_compat():
    """Build a _CatalogCompat with no catalog parent (tests defensive path)."""
    from plone.pgcatalog.maintenance import _CatalogCompat
    return _CatalogCompat()


def _bind_to_catalog(compat, catalog):
    """Bind compat to a PG catalog via Acquisition so aq_parent works."""
    # The compat is Implicit; set it as attribute of the catalog to build
    # the acquisition chain.  Use __of__ via Acquisition: catalog.compat = compat
    catalog._catalog = compat
    return catalog._catalog  # returns the acquisition-wrapped view


# ── Wrapper contract when no catalog context is reachable ─────────────────

class TestViewWithoutCatalog:
    def test_view_returns_raw_on_getitem_when_no_catalog(self):
        """Without an acquisition parent, accessing a key returns the raw index."""
        compat = _fresh_compat()
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw
        # No acquisition parent — should fall back to raw
        assert compat.indexes["portal_type"] is raw

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


# ── Wrapper contract with a PG catalog parent — indexes get wrapped ──────

class TestViewWithPgCatalog:
    def _setup(self, pg_conn_with_catalog):
        """Build a PlonePGCatalogTool + _CatalogCompat chained by Acquisition."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        tool._catalog = _CatalogCompat()
        return tool

    def _register_field_index(self, tool, name="portal_type"):
        raw = mock.Mock(id=name, meta_type="FieldIndex")
        tool._catalog._raw_indexes[name] = raw
        return raw

    def test_getitem_returns_pg_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex
        tool = self._setup(pg_conn_with_catalog)
        raw = self._register_field_index(tool, "portal_type")

        result = tool._catalog.indexes["portal_type"]

        assert isinstance(result, PGIndex)
        assert result._wrapped is raw

    def test_get_returns_pg_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex
        tool = self._setup(pg_conn_with_catalog)
        self._register_field_index(tool, "portal_type")

        result = tool._catalog.indexes.get("portal_type")
        assert isinstance(result, PGIndex)

    def test_get_missing_returns_default(self, pg_conn_with_catalog):
        tool = self._setup(pg_conn_with_catalog)
        assert tool._catalog.indexes.get("missing") is None
        assert tool._catalog.indexes.get("missing", "sentinel") == "sentinel"

    def test_items_yields_wrapped(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex
        tool = self._setup(pg_conn_with_catalog)
        self._register_field_index(tool, "a")
        self._register_field_index(tool, "b")

        pairs = dict(tool._catalog.indexes.items())
        assert set(pairs.keys()) == {"a", "b"}
        for v in pairs.values():
            assert isinstance(v, PGIndex)

    def test_values_yields_wrapped(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex
        tool = self._setup(pg_conn_with_catalog)
        self._register_field_index(tool, "a")

        vs = list(tool._catalog.indexes.values())
        assert len(vs) == 1
        assert isinstance(vs[0], PGIndex)

    def test_special_index_not_wrapped(self, pg_conn_with_catalog):
        """`idx_key=None` indexes (path, SearchableText, effectiveRange) return raw."""
        from plone.pgcatalog.columns import get_registry, IndexType
        registry = get_registry()
        registry.register("path", IndexType.PATH, None)

        tool = self._setup(pg_conn_with_catalog)
        raw = mock.Mock(id="path", meta_type="ExtendedPathIndex")
        tool._catalog._raw_indexes["path"] = raw

        # Special indexes skip wrapping — receive raw back
        result = tool._catalog.indexes["path"]
        assert result is raw


# ── Mutations go to raw mapping, no wrapping ────────────────────────────

class TestViewMutations:
    def test_setitem_writes_to_raw(self):
        compat = _fresh_compat()
        idx = mock.Mock(id="x", meta_type="FieldIndex")
        compat.indexes["x"] = idx
        assert compat._raw_indexes["x"] is idx

    def test_delitem_removes_from_raw(self):
        compat = _fresh_compat()
        compat._raw_indexes["x"] = mock.Mock()
        del compat.indexes["x"]
        assert "x" not in compat._raw_indexes

    def test_update_writes_to_raw(self):
        compat = _fresh_compat()
        compat.indexes.update({"a": mock.Mock(), "b": mock.Mock()})
        assert set(compat._raw_indexes.keys()) == {"a", "b"}


# ── Upgrade step migrates legacy persisted state ────────────────────────

class TestProfileUpgradeV1ToV2:
    def test_migrate_moves_indexes_attr_to_raw_indexes(self):
        """Legacy compat has compat.indexes: PersistentMapping — migrate it."""
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.upgrades.profile_2 import migrate_catalog_indexes

        compat = _CatalogCompat.__new__(_CatalogCompat)
        # Simulate legacy persisted state: `indexes` as plain PersistentMapping.
        legacy = PersistentMapping()
        legacy["portal_type"] = mock.Mock()
        compat.__dict__["indexes"] = legacy
        # And no `_raw_indexes` yet
        assert "_raw_indexes" not in compat.__dict__

        migrate_catalog_indexes(compat)

        assert "_raw_indexes" in compat.__dict__
        assert compat._raw_indexes is legacy
        assert "indexes" not in compat.__dict__

    def test_migrate_is_idempotent(self):
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.upgrades.profile_2 import migrate_catalog_indexes

        compat = _CatalogCompat()  # already has _raw_indexes
        assert "_raw_indexes" in compat.__dict__

        migrate_catalog_indexes(compat)  # no-op
        migrate_catalog_indexes(compat)  # no-op again
        assert "_raw_indexes" in compat.__dict__

    def test_migrate_marks_persistent_dirty(self):
        """After migration the Persistent instance must be marked changed so ZODB
        commits the rename."""
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.upgrades.profile_2 import migrate_catalog_indexes

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["indexes"] = PersistentMapping()
        compat._p_changed = False  # simulate freshly loaded

        migrate_catalog_indexes(compat)
        assert compat._p_changed is True


# ── getIndex still works (existing API) ────────────────────────────────

class TestGetIndexMethod:
    def test_get_index_via_method(self, pg_conn_with_catalog):
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.pgindex import PGIndex

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        tool._catalog = _CatalogCompat()
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        tool._catalog._raw_indexes["portal_type"] = raw

        result = tool._catalog.getIndex("portal_type")
        assert isinstance(result, PGIndex)
        assert result._wrapped is raw
```

- [ ] **Step 1.2** — Run the tests; confirm they fail the right way:

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/plone-pgcatalog
.venv/bin/pytest tests/test_catalog_indexes_view.py -v --tb=short 2>&1 | tail -40
```

Expected: every test in `TestViewWithPgCatalog`, `TestViewMutations`, `TestProfileUpgradeV1ToV2` fails with `AttributeError` (for `_raw_indexes`) or `ImportError` (for `plone.pgcatalog.upgrades.profile_2`). `TestViewWithoutCatalog` fails for similar reasons. `TestGetIndexMethod` is preserved by the existing code path once `_raw_indexes` exists.

- [ ] **Step 1.3** — Commit:

```bash
git add tests/test_catalog_indexes_view.py
git commit -m "test: failing scaffold for wrapping _catalog.indexes (#137)"
```

---

## Task 2: Rename persisted attribute + add `indexes` property + wrapper view

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py`

- [ ] **Step 2.1** — Confirm baseline failures:

```bash
.venv/bin/pytest tests/test_catalog_indexes_view.py::TestViewWithoutCatalog -v
```

Expected: all 4 fail with `AttributeError: '_CatalogCompat' object has no attribute '_raw_indexes'`.

- [ ] **Step 2.2** — Replace the current `_CatalogCompat` block in `src/plone/pgcatalog/maintenance.py` with the property-based design. Look for the block starting `class _CatalogCompat(Implicit, Persistent):` (around line 94). Replace it with:

```python
class _CatalogIndexesView:
    """Transient dict-like view over ``_CatalogCompat._raw_indexes``
    that wraps each index with ``PGIndex`` on read-through access.

    Built fresh from ``_CatalogCompat.indexes`` on every attribute access
    and NEVER persisted.  Mutations pass through to the raw mapping
    unchanged (they write raw ZCatalog index objects, as the upstream
    catalog.py / setuphandlers.py code expects).

    Finds the catalog via acquisition from the _CatalogCompat instance
    that built the view — no ``api.portal.get_tool`` needed.  When no
    catalog is reachable (e.g. during tests or bootstrap), falls back
    to returning the raw index.
    """

    __slots__ = ("_compat", "_raw")

    def __init__(self, compat, raw):
        self._compat = compat
        self._raw = raw

    # read-through access → wrapped
    def __getitem__(self, key):
        raw_index = self._raw[key]  # raises KeyError
        catalog = aq_parent(aq_inner(self._compat))
        if catalog is None:
            return raw_index
        return _maybe_wrap_index(catalog, key, raw_index)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        return key in self._raw

    def __iter__(self):
        return iter(self._raw)

    def __len__(self):
        return len(self._raw)

    def keys(self):
        return self._raw.keys()

    def values(self):
        for key in self._raw:
            yield self[key]

    def items(self):
        for key in self._raw:
            yield (key, self[key])

    # mutations → bypass wrapping, go to raw
    def __setitem__(self, key, value):
        self._raw[key] = value

    def __delitem__(self, key):
        del self._raw[key]

    def update(self, *args, **kwargs):
        self._raw.update(*args, **kwargs)

    def clear(self):
        self._raw.clear()

    def pop(self, key, *args):
        return self._raw.pop(key, *args)


class _CatalogCompat(Implicit, Persistent):
    """Minimal _catalog providing index object storage.

    ZCatalogIndexes._getOb() reads aq_parent(self)._catalog.indexes.
    eea.facetednavigation and many Plone internals read
    `catalog._catalog.indexes[name]` (and `.get(name)`, `.items()` …) directly.
    This shim provides just enough API for both — and crucially, the
    ``indexes`` attribute is a *view* that wraps each raw ZCatalog
    index with ``PGIndex``, so that direct dictionary access returns
    PG-backed results.

    Persisted state:
      _raw_indexes: PersistentMapping[str, ZCatalogIndex]   -- the real storage
      schema:       PersistentMapping[str, int]             -- metadata columns

    For existing ZODB instances the old attribute was ``indexes`` (a plain
    PersistentMapping); an upgrade step in
    ``plone.pgcatalog.upgrades.profile_2`` renames it to ``_raw_indexes``.
    """

    def __init__(self):
        self._raw_indexes = PersistentMapping()
        self.schema = PersistentMapping()

    @property
    def indexes(self):
        """Return a view that auto-wraps raw ZCatalog indexes with ``PGIndex``.

        The view is transient — built fresh on every access so it never
        gets pickled and never caches a stale catalog reference.
        """
        return _CatalogIndexesView(self, self._raw_indexes)

    def getIndex(self, name):
        """Return a PG-backed index wrapper for *name*.

        Equivalent to ``self.indexes[name]`` — kept for callers (notably
        ``eea.facetednavigation``) that specifically call this method.

        Raises ``KeyError`` if *name* is not a known index.
        """
        return self.indexes[name]
```

The changes from the original:

- `self.indexes = PersistentMapping()` → `self._raw_indexes = PersistentMapping()`.
- Added `indexes` property that builds a `_CatalogIndexesView`.
- `getIndex` now just delegates to `self.indexes[name]` (same behavior, smaller body).
- New `_CatalogIndexesView` class above `_CatalogCompat`.

Imports at the top of the file remain as-is (already imports `aq_inner`, `aq_parent`, `_maybe_wrap_index`, `PersistentMapping`).

- [ ] **Step 2.3** — Run the view tests:

```bash
.venv/bin/pytest tests/test_catalog_indexes_view.py::TestViewWithoutCatalog tests/test_catalog_indexes_view.py::TestViewWithPgCatalog tests/test_catalog_indexes_view.py::TestViewMutations tests/test_catalog_indexes_view.py::TestGetIndexMethod -v 2>&1 | tail -30
```

Expected: all pass. Upgrade tests still fail with `ImportError: plone.pgcatalog.upgrades.profile_2` — that's Task 3.

- [ ] **Step 2.4** — Regression sweep on existing tests:

```bash
.venv/bin/pytest tests/test_pgindex.py tests/test_catalog_plone.py tests/test_indexing.py tests/test_schema.py tests/test_query.py --tb=line -q 2>&1 | tail -10
```

Expected: all green. If `test_pgindex.py::TestCatalogIndexesWrapper` has any test that asserted on `compat.indexes["name"]` returning raw, it was testing the bug — update it.

- [ ] **Step 2.5** — Full suite sanity (skip the slow plone integration layers for speed):

```bash
.venv/bin/pytest tests/ -q --ignore=tests/test_plone_integration.py --ignore=tests/test_move_integration.py --ignore=tests/test_pg_integration.py --tb=line 2>&1 | tail -5
```

Expected: green.

- [ ] **Step 2.6** — Commit:

```bash
git add src/plone/pgcatalog/maintenance.py tests/
git commit -m "feat(maintenance): wrap _catalog.indexes via transient view (#137)"
```

---

## Task 3: Upgrade step v1 → v2 (migrate persisted state)

**Files:**
- Create: `src/plone/pgcatalog/upgrades/__init__.py`
- Create: `src/plone/pgcatalog/upgrades/profile_2.py`
- Create: `src/plone/pgcatalog/upgrades/configure.zcml`
- Create: `src/plone/pgcatalog/upgrades/profile_2.zcml`
- Modify: `src/plone/pgcatalog/configure.zcml` (add `<include package=".upgrades" />`)
- Modify: `src/plone/pgcatalog/profiles/default/metadata.xml` (bump version to `2`)

- [ ] **Step 3.1** — Confirm upgrade tests currently fail:

```bash
.venv/bin/pytest tests/test_catalog_indexes_view.py::TestProfileUpgradeV1ToV2 -v
```

Expected: all 3 fail with `ModuleNotFoundError: No module named 'plone.pgcatalog.upgrades.profile_2'`.

- [ ] **Step 3.2** — Create the upgrades package:

`src/plone/pgcatalog/upgrades/__init__.py` — empty (or a one-line docstring):

```python
"""GenericSetup upgrade steps for the plone.pgcatalog:default profile."""
```

- [ ] **Step 3.3** — Create `src/plone/pgcatalog/upgrades/profile_2.py`:

```python
"""Profile v1 → v2 migration.

Moves ``_CatalogCompat.indexes`` (a plain ``PersistentMapping``) to
``_CatalogCompat._raw_indexes`` so the new ``indexes`` property can
wrap reads via ``_CatalogIndexesView``.  See
``docs/plans/2026-04-15-wrap-catalog-indexes-attr.md`` (#137).
"""

import logging


log = logging.getLogger(__name__)


def migrate_catalog_indexes(context):
    """Rename ``indexes`` → ``_raw_indexes`` on the catalog's ``_catalog``.

    Accepts either a ``_CatalogCompat`` instance or a GenericSetup
    environment (in which case the Plone catalog is resolved to its
    ``_catalog`` shim).

    Idempotent.  Marks the instance persistent-dirty so the ZODB commit
    captures the rename.
    """
    compat = _resolve_compat(context)
    if compat is None:
        log.warning(
            "migrate_catalog_indexes: no _CatalogCompat found; skipping"
        )
        return

    state = compat.__dict__
    if "_raw_indexes" in state:
        # Already migrated.  Clean up stray legacy attribute if present.
        if "indexes" in state:
            # Both present — shouldn't happen, but be conservative:
            # keep _raw_indexes, drop the stale shadow.
            del state["indexes"]
            compat._p_changed = True
            log.info(
                "migrate_catalog_indexes: removed stale 'indexes' attr "
                "(already had '_raw_indexes')"
            )
        return

    if "indexes" not in state:
        log.info(
            "migrate_catalog_indexes: no 'indexes' attribute to migrate; "
            "creating empty _raw_indexes"
        )
        from persistent.mapping import PersistentMapping
        state["_raw_indexes"] = PersistentMapping()
        compat._p_changed = True
        return

    legacy = state.pop("indexes")
    state["_raw_indexes"] = legacy
    compat._p_changed = True
    log.info(
        "migrate_catalog_indexes: renamed 'indexes' → '_raw_indexes' "
        "(%d entries)",
        len(legacy),
    )


def _resolve_compat(context):
    """Return the ``_CatalogCompat`` from either a compat or a setup context."""
    from plone.pgcatalog.maintenance import _CatalogCompat

    if isinstance(context, _CatalogCompat):
        return context

    # GenericSetup: context is an ImportContext; its site is the Plone root.
    getSite = getattr(context, "getSite", None)
    if getSite is None:
        return None
    site = getSite()
    catalog = getattr(site, "portal_catalog", None)
    if catalog is None:
        return None
    return getattr(catalog, "_catalog", None)
```

- [ ] **Step 3.4** — Create `src/plone/pgcatalog/upgrades/configure.zcml`:

```xml
<configure xmlns="http://namespaces.zope.org/zope">
  <include file="profile_2.zcml" />
</configure>
```

- [ ] **Step 3.5** — Create `src/plone/pgcatalog/upgrades/profile_2.zcml`:

```xml
<configure
    xmlns="http://namespaces.zope.org/zope"
    xmlns:gs="http://namespaces.zope.org/genericsetup"
    i18n_domain="plone.pgcatalog"
    >

  <gs:upgradeStep
      profile="plone.pgcatalog:default"
      source="1"
      destination="2"
      title="Wrap _catalog.indexes"
      description="Rename _catalog.indexes → _raw_indexes so the new view-backed
                   property can wrap reads with PGIndex.  See #137."
      handler=".profile_2.migrate_catalog_indexes"
      />

</configure>
```

- [ ] **Step 3.6** — Modify `src/plone/pgcatalog/configure.zcml` to include the upgrades package. Locate the existing `<configure>` block and add a line near the other `<include>`s:

```xml
  <include package=".upgrades" />
```

- [ ] **Step 3.7** — Bump the profile metadata. Edit `src/plone/pgcatalog/profiles/default/metadata.xml`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<metadata>
  <version>2</version>
</metadata>
```

- [ ] **Step 3.8** — Run the upgrade tests:

```bash
.venv/bin/pytest tests/test_catalog_indexes_view.py::TestProfileUpgradeV1ToV2 -v
```

Expected: all 3 pass.

- [ ] **Step 3.9** — Full scaffold must now be green:

```bash
.venv/bin/pytest tests/test_catalog_indexes_view.py -v
```

Expected: every test passes.

- [ ] **Step 3.10** — Full suite (including integration tests — the ZCML include is part of the loaded package):

```bash
.venv/bin/pytest tests/ --tb=line -q 2>&1 | tail -5
```

Expected: green. Pay attention to `test_plone_integration.py` — ZCML parsing happens there.

- [ ] **Step 3.11** — Commit:

```bash
git add src/plone/pgcatalog/upgrades/ src/plone/pgcatalog/configure.zcml \
        src/plone/pgcatalog/profiles/default/metadata.xml
git commit -m "feat(upgrades): profile v1→v2 renames indexes attr (#137)"
```

---

## Task 4: Document the fix in CHANGES.md

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 4.1** — Confirm the latest release on GitHub (per project preference):

```bash
gh release list -L 3 --repo bluedynamics/plone-pgcatalog
```

If `v1.0.0b54` is the latest, the new entry is `## 1.0.0b55`. If something newer has shipped in the meantime, adjust.

- [ ] **Step 4.2** — Edit the top of `CHANGES.md`. Insert after `# Changelog` and before the existing latest version:

```markdown
## 1.0.0b55

### Fixed

- Plone and addon code commonly reaches into the catalog via the non-API-
  conform pattern ``catalog._catalog.indexes[name]`` / ``.get(name)`` /
  ``.items()``.  Previously this returned the raw ZCatalog index objects
  with empty BTrees, so queries against them silently returned no results.
  ``_CatalogCompat.indexes`` is now a property returning a transient view
  that wraps each index with ``PGIndex`` (same behavior as
  ``catalog.Indexes[name]``).  Custom ``PATH``-type indexes and other
  special indexes (``idx_key=None``) continue to be returned raw, since
  they have dedicated typed columns and don't need PG-backed wrapping.

  **Migration:** GenericSetup profile bumped from v1 to v2.  The upgrade
  step renames the persisted ``indexes`` attribute to ``_raw_indexes``
  so the new property can wrap reads.  Run
  *Plone Site Setup → Add-ons → plone.pgcatalog → Upgrade* on existing
  sites, or let the next ``runAllImportSteps`` on the default profile
  pick it up.

  Known callers fixed by this change:

  - ``plone.base.utils.check_id`` — reserved-name check against catalog indexes.
  - ``plone.restapi.search.query.Query.get_index`` —  hypermedia index-specific handling.
  - ``Products.CMFPlone.tests.testCheckId`` and several ``ZCatalog`` tests.
  - ``plone.app.discussion``, ``plone.app.referenceablebehavior``, ``plone.volto``,
    ``collective.collectionfilter``, ``collective.exportimport`` and others.

  Closes #137.
```

- [ ] **Step 4.3** — Commit:

```bash
git add CHANGES.md
git commit -m "docs: changelog entry for #137 _catalog.indexes wrap"
```

---

## Task 5: Final full-suite run + lint + push

**Files:** none new.

- [ ] **Step 5.1** — Full suite:

```bash
.venv/bin/pytest tests/ -q --tb=line 2>&1 | tail -10
```

Expected: all green.

- [ ] **Step 5.2** — Lint:

```bash
uvx ruff check src/plone/pgcatalog/
```

Expected: clean.

- [ ] **Step 5.3** — Push the branch:

```bash
git push -u origin feat/wrap-catalog-indexes
```

- [ ] **Step 5.4** — Open PR with `gh pr create`, referencing #137 and the prior attempt branch:

```bash
gh pr create --title "Wrap _catalog.indexes access" --body "$(cat <<'EOF'
## Summary

Closes #137.

`portal_catalog._catalog.indexes[name]` / `.get(name)` / `.items()` / `.values()`
now return `PGIndex`-wrapped objects instead of raw ZCatalog indexes with empty
BTrees.  Affects all the Plone / addon places listed in #137 that use this
private-but-common API.

### Design

- `_CatalogCompat.indexes` becomes a property returning a transient
  `_CatalogIndexesView` wrapping the underlying `PersistentMapping`.
  The view is never persisted.
- The view finds the catalog tool via Acquisition from the compat
  instance (which is `Implicit`) — no `api.portal.get_tool` calls,
  no per-access traversals.
- Mutations (`__setitem__`, `__delitem__`, `.update()`) pass through
  to the raw mapping unchanged; internal catalog code that adds or
  removes indexes (`catalog.addIndex`, `sync_from_catalog`) keeps
  storing raw ZCatalog index objects.
- A GenericSetup v1→v2 upgrade step renames the persisted
  `_CatalogCompat.indexes` attribute to `_raw_indexes`.

### Why not the prior attempt (`thet/indexes-wrapper`)

That branch subclassed `PersistentMapping` as `_CatalogIndexMapping`.  The
reporter correctly observed no change in a debug session because the
ZODB-persisted `PersistentMapping` stays the same after a class change —
only new instances get the subclass.  The branch did include an upgrade
step, but:
1. it wasn't run on the reporter's site,
2. it used `api.portal.get_tool("portal_catalog")` inside the mapping
   to find the catalog, which is fragile during bootstrap/tests, and
3. it couples ZODB state to the wrapper class shape.

This PR makes the wrapper transient (property + view), which avoids all
three issues.

## Test plan

- [x] 17 new unit/integration tests in `tests/test_catalog_indexes_view.py`
      cover: view behavior with/without catalog, special indexes skipped,
      mutations passthrough, `getIndex()`, upgrade-step migration idempotency.
- [x] Full suite green.
- [x] `ruff check` clean.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

Checked plan against issue requirements:

1. **Spec coverage:** #137 asks for `_catalog.indexes[name]` to return wrapped indexes. Task 2 implements the wrapper view; Task 3 migrates existing sites so the fix applies to them. Every problematic access pattern listed in #137 (`[]`, `.get`, `.items`, `.values`, iteration) is covered by the view's methods and by tests. ✅

2. **Placeholder scan:** No "TBD" / "handle edge cases" / "similar to Task N". Every code step shows the code. ✅

3. **Type consistency:**
   - `_CatalogIndexesView(compat, raw)` constructor signature matches all call sites (property + tests). ✅
   - `_CatalogCompat._raw_indexes` attribute name matches everywhere (tests, view, upgrade step, migration function). ✅
   - `migrate_catalog_indexes(context)` signature is the same in the function definition, ZCML handler attribute, and all tests. ✅

4. **Coverage of the "no change" bug:** `TestProfileUpgradeV1ToV2::test_migrate_moves_indexes_attr_to_raw_indexes` specifically simulates the reporter's scenario (a legacy compat with `indexes` set to a plain `PersistentMapping`) and asserts the rename. ✅

5. **Internal-caller safety:** Reviewed all 7 production sites that use `_catalog.indexes` (`columns.py`, `catalog.py`, `startup.py`, `setuphandlers.py`). All of them either mutate (pass-through) or iterate `.items()` reading passthrough attributes like `meta_type`/`attr_recurdef` — which `PGIndex.__getattr__` forwards correctly. No changes required. Documented in the File Structure section. ✅

6. **Profile version bump:** `metadata.xml` `<version>1` → `<version>2` plus registered upgrade step from `source="1"` to `destination="2"` — consistent. ✅

---

## Future Work (not in this plan)

- **Verify against real Plone addons:** #137 lists 13 external packages. After this PR, smoke-test at least `plone.restapi` (search endpoint) and `collective.collectionfilter` against a PG-backed site to confirm they now work.
- **Private-API audit:** there may be other `_catalog.X` accesses (`_catalog.schema`, `_catalog.getpath`, etc.) that the same Plone callers use. Grep each caller for `_catalog\.` and verify.
- **Caching the view:** if profiling shows hot-loop allocations of `_CatalogIndexesView` are measurable (unlikely at ~50-byte instances), add an `_indexes_view` cached-attribute to `_CatalogCompat` — guarded by invalidation on `_raw_indexes` writes.
