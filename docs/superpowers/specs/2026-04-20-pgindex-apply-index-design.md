# PGIndex `_apply_index` + robust catalog resolution

- **Issue**: [bluedynamics/plone-pgcatalog#146](https://github.com/bluedynamics/plone-pgcatalog/issues/146)
- **Author**: Jens W. Klein (spec drafted with Claude Opus 4.7 via superpowers)
- **Status**: Draft
- **Date**: 2026-04-20
- **Target release**: 1.0.0b61

## Problem

`PGIndex` wraps raw ZCatalog indexes and overrides `uniqueValues()` (#143 / b59). Several widely-used downstream callers reach for **private ZCatalog attributes** that `PGIndex` does not translate:

1. `index._apply_index(query)` — ZCatalog's low-level query entry point returning `(IISet(rids), info)`. `PGIndex.__getattr__` delegates to the wrapped raw index, whose BTree is empty in pgcatalog. Called by `plone.app.vocabularies.KeywordsVocabulary.keywords_of_section`, `plone.app.vocabularies.CatalogSearchSource`, and addon dashboards / filters.
2. `index._index` — the internal `OOBTree` mapping value → set-of-rids. Partially fixed in b60 via `_PGIndexMapping` (get, `__iter__`, `__contains__`, `keys`), but still missing `__getitem__`, `__len__` and the deprecation-warning envelope that signals "you are on an emulation path".

A second, related problem surfaced during the deep investigation for aaf-prod:

3. **Silent `__parent__`-missing failure**. `_CatalogCompat.getIndex(name)` and `_CatalogIndexesView.__getitem__(name)` both do `aq_parent(aq_inner(self))` to find the catalog tool needed for `_maybe_wrap_index`. On aaf-prod the persisted `_CatalogCompat` has no `__parent__` (the v1→v2 migration ran before the #139 fix, so the `__parent__` half of the migration never executed). Without a reachable catalog, both call sites fall back to returning the **raw** ZCatalog index — whose BTrees are empty, so every caller gets correct-looking-but-zero results. This is not a graceful fallback; it is a silent failure mode that masked other bugs for weeks.

## User-visible breakage

aaf-prod Plone 6 post-b60: typing in the Collection `Schlagwort`/tag autocomplete produces **zero suggestions** even though `uniqueValues()` on the same index returns ~1900 entries and `catalog(Subject="AT26")` returns 131 hits. The vocabulary uses `catalog._catalog.getIndex("Subject")._index`, which hits the raw-index path because `__parent__` is missing; the iterator over the empty OOBTree yields nothing.

Other known-affected callers in a standard Plone 6 stack:
- `plone.app.vocabularies.KeywordsVocabulary` (both `all_keywords` and `keywords_of_section` branches)
- `plone.app.vocabularies.CatalogSearchSource`
- `collective.collectionfilter`, `plone.app.referenceablebehavior`, various dashboards/portlets using `cat._catalog.indexes[x]._apply_index(...)`

## Goals

- Every access to `catalog._catalog.getIndex(name)` / `catalog._catalog.indexes[name]` returns a `PGIndex` wrapper when the catalog is a `PlonePGCatalogTool`. No silent raw-index fallback.
- `PGIndex._apply_index(query)` returns the same set of zoids as `catalog(**{name: query})` for the same index, minus security filtering. Invariant: "`PGIndex._apply_index(q)` ⇔ `catalog(q)` without `allowed_roles`."
- `PGIndex._index` continues to return a PG-backed mapping with the methods Plone core actually uses (`get`, `__iter__`, `__contains__`, `keys`, `__getitem__`, `__len__`), plus explicit `NotImplementedError` with a helpful message for `items()` / `values()`.
- Deprecation warnings on `_apply_index` and `_index` access, each emitted once per caller site via `warnings.warn(..., DeprecationWarning, stacklevel=2)`.
- Self-heal on aaf-prod without a second upgrade-step click: the compat's `__parent__` gets persisted on first access after deploy.
- Zero schema changes, zero new DDL, zero data migration. Pure Python / Acquisition plumbing.

## Non-goals

- **`items()` / `values()` implementation**. No Plone-core caller uses them on a wrapped `PGIndex`. A caller that does hit `NotImplementedError` with a helpful message pointing to `uniqueValues()`, `_apply_index`, and `catalog(**query)` as alternatives. If a real usecase surfaces, a future issue can implement it with server-side-cursor lazy streaming.
- **`_apply_index(resultset=...)`** index-chaining semantics. The `resultset` parameter is accepted for ZCatalog signature compatibility but ignored. Callers that do Python-side `intersection()` (e.g. `KeywordsVocabulary.keywords_of_section`) work unchanged.
- **Implicit security filtering on `_apply_index`**. Matches ZCatalog semantics: `_apply_index` is the unsecured low-level API. Callers that need secured results must use `catalog(**query)` which injects `allowed_roles` via `apply_security_filters`.
- **`object_state`-as-catalog-source** architectural discussion. The three tracked concerns (security on `uniqueValues`, scan cost on huge catalogs, separate `catalog_state` view) are independent and get their own tracking issue.

## Design

### 1. Catalog resolution — no more silent raw-index fallback

New module-private helper in `src/plone/pgcatalog/maintenance.py`:

```python
def _resolve_catalog(compat):
    """Find the PlonePGCatalogTool that owns this _CatalogCompat.

    Tries three resolution paths in order.  Raises RuntimeError if all
    three fail — never returns None (that would trigger the silent
    raw-index-fallback bug that masked #143 / #146 for weeks).
    """
    # 1) __parent__ on the bare instance (set by migration / self-heal)
    parent = compat.__dict__.get("__parent__")
    if parent is not None:
        return parent
    # 2) Acquisition chain (compat reached via AcqWrapper)
    via_aq = aq_parent(aq_inner(compat))
    if via_aq is not None:
        return via_aq
    # 3) zope.component.hooks.getSite() — works during request handling
    from zope.component.hooks import getSite
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

**Call-site changes**:
- `_CatalogCompat.getIndex(name)` — replace `catalog = aq_parent(aq_inner(self))` with `catalog = _resolve_catalog(self)`. Drop the `if catalog is None: return raw_index` branch.
- `_CatalogIndexesView.__getitem__(key)` — same replacement. Drop the raw-index fallback.

After these changes, an unreachable catalog raises `RuntimeError` at the call site. A test-only code path that previously relied on the `return raw_index` fallback (one test in `test_catalog_indexes_view.py::TestViewWithoutCatalog`) will need to be updated to match the new contract — that test currently documents the old buggy behavior and needs to either assert `pytest.raises(RuntimeError)` or pass an explicit `__parent__`.

### 2. Self-heal extension — persist `__parent__` on first access

`_CatalogCompat.indexes` property (already self-heals `_raw_indexes` per b60) gains a `__parent__` fixup block:

```python
@property
def indexes(self):
    state = self.__dict__
    raw = state.get("_raw_indexes")
    if raw is None:
        raw = state.pop("indexes", None) or PersistentMapping()
        state["_raw_indexes"] = raw
        self._p_changed = True
    # NEW: also heal missing __parent__ when we can find the tool.
    if state.get("__parent__") is None:
        from zope.component.hooks import getSite
        site = getSite()
        tool = getattr(site, "portal_catalog", None) if site else None
        if tool is not None:
            state["__parent__"] = tool
            self._p_changed = True
    return _CatalogIndexesView(self, raw)
```

Tolerant of `getSite() is None` (Zope startup, unit-test contexts without a site hook). In that case `__parent__` stays unset and later lookups fall back to the `_resolve_catalog` chain.

### 3. `PGIndex._apply_index(request, resultset=None)`

Exact ZCatalog signature. Reuses `QueryBuilder._process_index` from `query.py` — no duplicated dispatch logic.

```python
from BTrees.IIBTree import IITreeSet
import warnings

def _apply_index(self, request, resultset=None):
    """Low-level ZCatalog-compatible query entry point.

    Returns (IITreeSet(zoids), (index_name,)).  Applies no security —
    matches ZCatalog semantics.  Caller is responsible for injecting
    allowed_roles into the request if secured results are required.

    The ``resultset`` parameter is accepted for signature compatibility
    but currently ignored (see non-goals).
    """
    from plone.pgcatalog.query import QueryBuilder

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

    builder = QueryBuilder()
    builder._query = request  # for cross-index Language lookup in _handle_text
    builder.clauses.append("idx IS NOT NULL")
    builder._process_index(index_id, request[index_id])
    result = builder.result()
    sql = f"SELECT zoid FROM object_state WHERE {result['where']}"

    with conn.cursor() as cur:
        cur.execute(sql, result["params"])
        zoids = IITreeSet(row["zoid"] for row in cur.fetchall())
    return zoids, (index_id,)
```

Dispatch is inherited from `QueryBuilder._process_index`, which already handles FIELD, KEYWORD, PATH, DATE, DATE_RANGE, UUID, TEXT, BOOLEAN, GOPIP, plus `IPGIndexTranslator`-utility fallback for custom types (DateRecurringIndex with rrule etc.). Full IndexType coverage comes for free.

### 4. `_PGIndexMapping` — complete the BTree-shaped read surface

| Method | Status | Notes |
|---|---|---|
| `get(value, default)` | b60 — present | KEYWORD branch via `idx->key @> to_jsonb(value::text)` |
| `__contains__(value)` | b60 — present | Delegates to `get()` |
| `keys()` | b60 — present | UNION ALL for KEYWORD; `SELECT DISTINCT` for scalar |
| `__iter__()` | b60 — present | `iter(self.keys())` |
| `__getitem__(value)` | **new** | `v = self.get(value); if v is None: raise KeyError(value); return v` |
| `__len__()` | **new** | `SELECT COUNT(DISTINCT …)` using same UNION ALL pattern as `keys()` |
| `items()` | **new — raises** | `NotImplementedError` with guidance |
| `values()` | **new — raises** | `NotImplementedError` with guidance |

Unified `NotImplementedError` message template (literal braces in the query-dict examples must be doubled so `.format(method=...)` does not try to substitute them):

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

### 5. Deprecation warning on `PGIndex._index`

```python
@property
def _index(self):
    warnings.warn(
        f"PGIndex._index accessed for {self._idx_key!r}: ZCatalog "
        f"BTree-shaped API is emulated against PostgreSQL; prefer "
        f"catalog.Indexes[name].uniqueValues() or catalog(**query).",
        DeprecationWarning,
        stacklevel=2,
    )
    return self._pg_index
```

Python's default warning filter shows each unique `(module, lineno, message)` once per process — effectively once per caller site, no log flood, consistent with the Plone deprecation guide.

## Module structure

All changes stay in existing files:
- `src/plone/pgcatalog/maintenance.py` — add `_resolve_catalog` helper; extend `_CatalogCompat.indexes` property with `__parent__` self-heal; update `_CatalogCompat.getIndex` and `_CatalogIndexesView.__getitem__` to use `_resolve_catalog` instead of inline `aq_parent(aq_inner(...))` with raw-index fallback.
- `src/plone/pgcatalog/pgindex.py` — add `_apply_index` on `PGIndex`; add `__getitem__` / `__len__` / `items` / `values` on `_PGIndexMapping`; wrap `_index` property in deprecation warning.
- `src/plone/pgcatalog/query.py` — no changes. `QueryBuilder._process_index` is reused as-is.

## Tests

### Unit (`tests/test_pgindex.py`, `tests/test_catalog_indexes_view.py`)

**`_resolve_catalog`**
- `test_resolve_catalog_via_explicit_parent`
- `test_resolve_catalog_via_acquisition`
- `test_resolve_catalog_via_get_site`
- `test_resolve_catalog_raises_when_all_fail`

**Self-heal `__parent__`**
- `test_property_sets_parent_from_get_site` — unmigrated compat + `getSite()` returns tool → `__parent__` set, `_p_changed=True`
- `test_property_skips_parent_heal_when_no_site` — no site hook → property still works, `__parent__` stays unset

**`PGIndex._apply_index`**
- `test_apply_index_field_single_value` → correct zoid set
- `test_apply_index_keyword_single_value` (`{"Subject": "AT26"}`)
- `test_apply_index_keyword_or` (`{"Subject": {"query": ["a","b"], "operator": "or"}}`)
- `test_apply_index_path_subtree`
- `test_apply_index_date_range` — sanity for Date dispatch
- `test_apply_index_returns_empty_when_index_not_in_request`
- `test_apply_index_ignores_resultset_kwarg`
- `test_apply_index_no_implicit_security` — object with `allowed_roles=['Manager']` still returned
- `test_apply_index_emits_deprecation_warning` — `pytest.warns(DeprecationWarning)`

**`_PGIndexMapping` new methods**
- `test_getitem_returns_zoid` / `test_getitem_raises_keyerror_on_miss`
- `test_len_scalar` / `test_len_keyword` (incl. legacy-scalar branch)
- `test_items_raises_notimplemented_with_guidance`
- `test_values_raises_notimplemented_with_guidance`

**`PGIndex._index` deprecation**
- `test_index_property_warns`

**`_CatalogCompat.getIndex` / `_CatalogIndexesView.__getitem__` without `__parent__`**
- `test_get_index_finds_catalog_via_get_site`
- `test_get_index_raises_runtimeerror_when_all_fail`
- `test_get_index_with_parent_still_works` (regression guard)

### Integration (PG-backed layer)

- `test_keywords_vocabulary_returns_individual_keywords` — full `KeywordsVocabulary()(context)` round-trip, assert tag strings
- `test_keywords_vocabulary_survives_missing_parent` — clear `__parent__` on compat, fetch vocabulary, still works via `getSite` fallback

### Regression

All existing `uniqueValues` / `_PGIndexMapping` / `PGCatalogIndexes` / `_CatalogIndexesView` tests must stay green. Specifically:
- `TestPGIndexMappingKeyword::test_mapping_iterates_individual_keywords` (b60 guarantee)
- `TestPGIndexKeyword::test_keyword_unique_values_returns_individual_tags` (b59 guarantee)
- `TestViewWithoutCatalog::*` — needs updating to match the new `RuntimeError`-or-explicit-parent contract.

**Target**: existing 1403 tests + ~17 new, all green. The one test class `TestViewWithoutCatalog` moves from "passing with buggy raw fallback" to "updated to match new contract" — net-zero count.

## Prod recovery / deployment story

On aaf-prod after deploy of v1.0.0b61:

1. Any page that fetches a vocabulary, renders a tile, or runs a search triggers `_CatalogCompat.indexes` property access.
2. The b60-extended self-heal renames legacy state (no-op on prod — already done) **and** detects missing `__parent__`, finds the tool via `getSite().portal_catalog`, persists it, marks dirty.
3. First ZODB commit of that request writes `__parent__` to PG.
4. Subsequent requests hit the fast path (`__parent__` set, no `getSite` lookup needed).
5. `KeywordsVocabulary` now finds a real `PGIndex` via `_resolve_catalog`, iterates the `_PGIndexMapping`, and returns individual keywords.

**No manual intervention required**: no re-run of the upgrade step, no SQL patch, no pod restart.

**Verification SQL after deploy + one request**:
```sql
SELECT state ? '__parent__' FROM object_state WHERE zoid = <compat_zoid>;
-- expected: t
```

**Rollback story**: pure Python code change, no DDL, no data migration. `kubectl rollout undo` returns to b60; the persisted `__parent__` attribute is ignored by b60 (harmless).

## Risks

| Risk | Mitigation |
|---|---|
| `QueryBuilder._process_index` expects `self._query` context for `_handle_text` cross-index Language lookup | Set `builder._query = request` before the call |
| `getSite()` returns `None` during Zope startup before site initialization | Self-heal is tolerant (`if site is not None`); lookup chain falls back to other mechanisms; `RuntimeError` only when **all three** resolution paths fail |
| `_p_changed = True` on a compat without a Jar (unit tests) | Already solved via `_test_inject_jar` in b59; new self-heal path respects the same escape hatch (tests can construct compat with explicit `__parent__` to bypass the heal) |
| Deprecation warnings could be noisy in production logs | Python default filter shows each `(module, lineno, message)` once per process; in practice one log line per caller site per deploy |
| Breaking change on `TestViewWithoutCatalog` | One test file updated to reflect new contract; documented in PR |

## Out of scope — tracking

Independent follow-ups that won't be tackled here:

- `items()` / `values()` real implementation (only if a concrete caller appears)
- `_apply_index(resultset=...)` index-chaining (only if a caller needs SQL-side intersection)
- Security filter on `_apply_index` (explicitly rejected — matches ZCatalog semantics)
- `object_state`-as-source concerns (three separate points: security on `uniqueValues`, scan cost on huge catalogs, `catalog_state` denormalization) — own tracking issue

## Release

- PR on branch `fix/pgindex-apply-index-and-parent-resolution`
- Branch closes #146
- Target version: `1.0.0b61`
- CHANGES.md entry written when the PR lands
