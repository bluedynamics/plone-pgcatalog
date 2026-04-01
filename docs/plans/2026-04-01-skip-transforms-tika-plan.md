# Skip portal_transforms for IFile (Tika) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When `PGCATALOG_TIKA_URL` is set, skip the expensive `portal_transforms` pipeline for `IFile` SearchableText extraction — Tika handles blob text async.

**Architecture:** Override the `SearchableText` indexer for `IFile` via `overrides.zcml`. The override checks `PGCATALOG_TIKA_URL` at call time: if set, returns Title+Description only (from `plone.app.contenttypes.indexers.SearchableText`); if not set, delegates to the original `SearchableText_file` indexer.

**Tech Stack:** Python, plone.indexer, ZCML, pytest

**Spec:** `docs/plans/2026-04-01-skip-transforms-tika-design.md`
**Issue:** https://github.com/bluedynamics/plone-pgcatalog/issues/41

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/plone/pgcatalog/indexers.py` | Create | Conditional `SearchableText` indexer for `IFile` |
| `src/plone/pgcatalog/overrides.zcml` | Modify | Add adapter registration |
| `tests/test_indexers.py` | Create | Unit tests for the conditional indexer |
| `CHANGES.md` | Modify | Changelog entry |

---

### Task 1: Write tests for the conditional indexer

**Files:**
- Create: `tests/test_indexers.py`

- [ ] **Step 1: Create test file with both test cases**

```python
"""Tests for SearchableText IFile indexer override."""

from plone.pgcatalog.indexers import SearchableText_file_override
from unittest import mock

import os
import pytest


class TestSearchableTextFileOverride:
    """Test conditional SearchableText indexer for IFile."""

    def _make_obj(self, title="Test File", description="A test", subject=()):
        """Create a mock IFile object with the minimum needed attributes."""
        obj = mock.Mock()
        obj.id = "test-file"
        obj.title = title
        obj.description = description
        obj.Subject.return_value = subject
        # IRichTextBehavior adaptation returns None (File has no rich text)
        return obj

    def test_with_tika_returns_title_description_only(self):
        """When PGCATALOG_TIKA_URL is set, skip transforms entirely."""
        obj = self._make_obj(title="My Report", description="Quarterly results")
        with mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}):
            result = SearchableText_file_override(obj)
        # Should contain title and description but NOT blob content
        assert "My Report" in result
        assert "Quarterly results" in result

    def test_without_tika_delegates_to_original(self):
        """When PGCATALOG_TIKA_URL is not set, delegate to original indexer."""
        obj = self._make_obj()
        with (
            mock.patch.dict(os.environ, {}, clear=False),
            mock.patch(
                "plone.pgcatalog.indexers._original_searchable_text_file",
                return_value="original result",
            ) as original_mock,
        ):
            # Ensure PGCATALOG_TIKA_URL is not set
            os.environ.pop("PGCATALOG_TIKA_URL", None)
            result = SearchableText_file_override(obj)
        original_mock.assert_called_once_with(obj)
        assert result == "original result"

    def test_with_empty_tika_url_delegates_to_original(self):
        """Empty PGCATALOG_TIKA_URL is treated as unset."""
        obj = self._make_obj()
        with (
            mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "  "}),
            mock.patch(
                "plone.pgcatalog.indexers._original_searchable_text_file",
                return_value="original result",
            ) as original_mock,
        ):
            result = SearchableText_file_override(obj)
        original_mock.assert_called_once_with(obj)
        assert result == "original result"

    def test_tika_mode_includes_subject(self):
        """Tika mode should include Subject keywords (same as base SearchableText)."""
        obj = self._make_obj(subject=("python", "plone"))
        with mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}):
            result = SearchableText_file_override(obj)
        assert "python" in result
        assert "plone" in result

    def test_tika_mode_does_not_call_transforms(self):
        """Verify portal_transforms is never accessed in Tika mode."""
        obj = self._make_obj()
        with mock.patch.dict(os.environ, {"PGCATALOG_TIKA_URL": "http://tika:9998"}):
            result = SearchableText_file_override(obj)
        # getToolByName should not have been called for portal_transforms
        # (the mock obj would raise if unexpected attrs were accessed with spec)
        assert result is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_indexers.py -v`
Expected: FAIL with `ImportError: cannot import name 'SearchableText_file_override'`

- [ ] **Step 3: Commit test file**

```bash
git add tests/test_indexers.py
git commit -m "test: add tests for conditional SearchableText IFile indexer (#41)"
```

---

### Task 2: Implement the conditional indexer

**Files:**
- Create: `src/plone/pgcatalog/indexers.py`

- [ ] **Step 1: Create the indexer module**

```python
"""Conditional SearchableText indexer for IFile.

When ``PGCATALOG_TIKA_URL`` is configured, skips the expensive
``portal_transforms`` pipeline (pdftotext, wv, BFS graph traversal)
and returns only Title + Description.  The Tika async worker
extracts blob text and merges it into ``searchable_text`` as
tsvector weight 'C'.

When ``PGCATALOG_TIKA_URL`` is NOT set, delegates to the original
``plone.app.contenttypes.indexers.SearchableText_file`` so the
full transform pipeline runs as before.
"""

from plone.app.contenttypes.indexers import SearchableText
from plone.app.contenttypes.indexers import SearchableText_file as _original_searchable_text_file
from plone.app.contenttypes.interfaces import IFile
from plone.indexer import indexer

import os


@indexer(IFile)
def SearchableText_file_override(obj):
    """SearchableText for IFile — skips transforms when Tika is active."""
    tika_url = os.environ.get("PGCATALOG_TIKA_URL", "").strip()
    if tika_url:
        # Tika handles blob text extraction async — just return
        # Title + Description + Subject (no blob I/O, no transforms).
        return SearchableText(obj)
    return _original_searchable_text_file(obj)
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_indexers.py -v`
Expected: all 5 tests PASS

- [ ] **Step 3: Commit implementation**

```bash
git add src/plone/pgcatalog/indexers.py
git commit -m "feat: conditional SearchableText indexer for IFile — skip transforms with Tika (#41)"
```

---

### Task 3: Register the override in ZCML

**Files:**
- Modify: `src/plone/pgcatalog/overrides.zcml`

- [ ] **Step 1: Add the adapter registration**

Add before the closing `</configure>` tag in `overrides.zcml`:

```xml
  <!-- Skip portal_transforms for IFile when Tika is active (#41).
       The indexer checks PGCATALOG_TIKA_URL at call time and delegates
       to the original plone.app.contenttypes indexer when unset. -->
  <adapter
      factory=".indexers.SearchableText_file_override"
      name="SearchableText"
      />
```

The full file should be:

```xml
<configure
    xmlns="http://namespaces.zope.org/zope"
    xmlns:genericsetup="http://namespaces.zope.org/genericsetup"
    xmlns:zcml="http://namespaces.zope.org/zcml"
    >

  <!-- Override toolset step to protect PlonePGCatalogTool from
       being replaced by CMFPlone's CatalogTool during profile imports -->
  <genericsetup:importStep
      name="toolset"
      title="Required Tools"
      description="Create required tools, skip portal_catalog when PlonePGCatalogTool."
      handler="plone.pgcatalog.setuphandlers.importToolset"
      />

  <!-- eea.facetednavigation: PG-backed IFacetedCatalog adapter -->
  <include
      zcml:condition="installed eea.facetednavigation"
      package=".addons_compat"
      file="eeafacetednavigation_overrides.zcml"
      />

  <!-- Skip portal_transforms for IFile when Tika is active (#41).
       The indexer checks PGCATALOG_TIKA_URL at call time and delegates
       to the original plone.app.contenttypes indexer when unset. -->
  <adapter
      factory=".indexers.SearchableText_file_override"
      name="SearchableText"
      />

</configure>
```

- [ ] **Step 2: Run full unit test suite to verify no regressions**

Run: `uv run pytest tests/test_catalog_plone.py tests/test_indexers.py -v`
Expected: all tests PASS

- [ ] **Step 3: Commit ZCML change**

```bash
git add src/plone/pgcatalog/overrides.zcml
git commit -m "feat: register SearchableText IFile override in overrides.zcml (#41)"
```

---

### Task 4: Add changelog entry

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Add entry at the top of the changelog**

Add a new version section above the current top entry:

```markdown
## 1.0.0b25

### Added

- Skip `portal_transforms` text extraction for `IFile` when
  `PGCATALOG_TIKA_URL` is set.  The async Tika worker handles blob
  text extraction — no more synchronous pdftotext/wv calls or BFS
  graph traversal of the transform registry during indexing.
  Custom types with blob fields need their own override (see docs).
  Fixes #41.
```

- [ ] **Step 2: Commit changelog**

```bash
git add CHANGES.md
git commit -m "docs: changelog for skip-transforms-tika (#41)"
```

---

### Task 5: Add documentation for custom blob types

**Files:**
- Create: `docs/sources/how-to/custom-blob-searchabletext.md`

- [ ] **Step 1: Write the how-to doc**

```markdown
# Custom types with blob fields and Tika

When `PGCATALOG_TIKA_URL` is configured, `plone.pgcatalog` overrides
the `SearchableText` indexer for `IFile` to skip the synchronous
`portal_transforms` pipeline.  The Tika async worker extracts text
from blobs instead.

## What's covered automatically

- **File** content type (`IFile`) — the override handles this.

## What's NOT covered

Custom Dexterity content types with `NamedBlobFile` primary fields
that do NOT provide `IFile`.  If such a type has a custom
`SearchableText` indexer that calls `portal_transforms`, the
transforms will still run synchronously.

## How to add Tika support for custom types

Register a conditional indexer similar to the built-in override:

```python
from plone.app.contenttypes.indexers import SearchableText
from plone.indexer import indexer
from my.package.interfaces import IMyCustomType

import os

# Import your original indexer
from my.package.indexers import SearchableText_mycustomtype as _original


@indexer(IMyCustomType)
def SearchableText_mycustomtype_tika(obj):
    tika_url = os.environ.get("PGCATALOG_TIKA_URL", "").strip()
    if tika_url:
        return SearchableText(obj)
    return _original(obj)
```

Register it in your package's `overrides.zcml`:

```xml
<adapter
    factory=".indexers.SearchableText_mycustomtype_tika"
    name="SearchableText"
    />
```

This ensures:

- With Tika: only Title + Description are indexed synchronously;
  Tika extracts blob text async.
- Without Tika: the original transform-based indexer runs as before.
```

- [ ] **Step 2: Commit documentation**

```bash
git add docs/sources/how-to/custom-blob-searchabletext.md
git commit -m "docs: how-to for custom blob types with Tika (#41)"
```
