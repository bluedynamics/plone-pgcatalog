# Plan: Level 1 — Weighted Relevance Ranking for plone-pgcatalog

## Summary

Implements **Write-Path Option A** from [bm25-integration-analysis.md, Section 7](bm25-integration-analysis.md): prepend weighted Title (A) + Description (B) into the stored `searchable_text` tsvector alongside the full SearchableText body (D). This creates intentional duplicate tokens where A/B weights dominate D-weight in `ts_rank_cd()` scoring.

Add field-weighted `ts_rank_cd()` relevance ranking to plone-pgcatalog using vanilla PostgreSQL (no extensions). Title matches rank ~10x higher than body-only matches. Auto-applied when SearchableText is queried without explicit `sort_on`.

## Files to Modify

| File | Change |
|---|---|
| `src/plone/pgcatalog/indexing.py` | Weighted tsvector in `catalog_object()` and `reindex_object()` |
| `src/plone/pgcatalog/config.py` | Weighted `ExtraColumn.value_expr` in `CatalogStateProcessor.get_extra_columns()` |
| `src/plone/pgcatalog/query.py` | Auto-relevance ORDER BY in `_QueryBuilder` |
| `tests/test_fulltext.py` | New ranking tests (weighted fields, auto-ranking) |
| `tests/test_query.py` | Unit tests for relevance ORDER BY generation |
| `tests/test_indexing.py` | Update searchable_text write verification |
| `CHANGES.md` | Changelog entry |

Working directory: `sources/plone-pgcatalog/`

## Step 1: Worktree + Branch

```bash
cd /home/jensens/ws/cdev/z3blobs
git worktree add .worktrees/level1-ranking -b feature/weighted-relevance-ranking
```

Then work in `.worktrees/level1-ranking/sources/plone-pgcatalog/`.

## Step 2: Write Path — indexing.py

Two write paths exist (both must be updated):

### catalog_object() (line 17)

Change searchable_text SQL from:
```sql
searchable_text = to_tsvector(%(lang)s::regconfig, %(text)s)
```
To:
```sql
searchable_text =
  setweight(to_tsvector('simple'::regconfig,
    COALESCE(%(idx)s::jsonb->>'Title', '')), 'A') ||
  setweight(to_tsvector('simple'::regconfig,
    COALESCE(%(idx)s::jsonb->>'Description', '')), 'B') ||
  setweight(to_tsvector(%(lang)s::regconfig, %(text)s), 'D')
```

The `%(idx)s` param is already `Json(idx)` — PG casts it to jsonb and extracts Title/Description. If Title/Description are missing, COALESCE→'' gives an empty tsvector that merges harmlessly.

### reindex_object() (line 101)

When searchable_text is provided (line 128), change to use merged idx:
```sql
searchable_text =
  setweight(to_tsvector('simple'::regconfig,
    COALESCE((COALESCE(idx, '{}'::jsonb) || %(updates)s::jsonb)->>'Title', '')), 'A') ||
  setweight(to_tsvector('simple'::regconfig,
    COALESCE((COALESCE(idx, '{}'::jsonb) || %(updates)s::jsonb)->>'Description', '')), 'B') ||
  setweight(to_tsvector(%(lang)s::regconfig, %(text)s), 'D')
```

This reads Title/Description from the merged idx (existing || updates).

### Helper: Extract SQL to avoid repetition

Create a module-level constant for the weighted tsvector expression used in both functions to keep DRY:

```python
_WEIGHTED_TSVECTOR_EXPR = (
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE({idx_expr}->>'Title', '')), 'A') || "
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE({idx_expr}->>'Description', '')), 'B') || "
    "setweight(to_tsvector({lang_expr}::regconfig, {text_expr}), 'D')"
)
```

Use `.format()` with different `idx_expr` for catalog_object vs reindex_object.

## Step 3: Write Path — config.py

Change `CatalogStateProcessor.get_extra_columns()`:

From:
```python
ExtraColumn(
    "searchable_text",
    "to_tsvector("
    "pgcatalog_lang_to_regconfig(%(idx)s::jsonb->>'Language')"
    "::regconfig, %(searchable_text)s)",
),
```

To:
```python
ExtraColumn(
    "searchable_text",
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE(%(idx)s::jsonb->>'Title', '')), 'A') || "
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE(%(idx)s::jsonb->>'Description', '')), 'B') || "
    "setweight(to_tsvector("
    "pgcatalog_lang_to_regconfig(%(idx)s::jsonb->>'Language')"
    "::regconfig, %(searchable_text)s), 'D')",
),
```

This is the production write path (ZODB → CatalogStateProcessor → _batch_write_objects).

## Step 4: Query Path — query.py

### 4a: Store ranking expression in _handle_text()

After generating the WHERE clause for SearchableText (idx_key is None), store a ranking expression:

```python
# Store relevance expression for auto-ranking
self._text_rank_expr = (
    f"ts_rank_cd("
    f"'{{0.1, 0.2, 0.4, 1.0}}'::float4[], "
    f"searchable_text, "
    f"plainto_tsquery("
    f"pgcatalog_lang_to_regconfig(%({p_lang})s)::regconfig, "
    f"%({p_text})s))"
)
```

No new params needed — reuses `p_text` and `p_lang` already in `self.params`.

### 4b: Auto-apply ranking in process()

After sort processing, add:

```python
# Auto-rank by relevance when SearchableText is queried without explicit sort
if self.order_by is None and hasattr(self, "_text_rank_expr"):
    self.order_by = f"{self._text_rank_expr} DESC"
```

### Behavior:
- `{"SearchableText": "term"}` → auto ORDER BY relevance DESC
- `{"SearchableText": "term", "sort_on": "modified"}` → ORDER BY modified (explicit sort wins)
- `{"portal_type": "Document"}` → no ORDER BY (no text search, no sort_on)

## Step 5: Tests

### test_fulltext.py — New tests

- **test_title_match_ranks_higher_than_body_only**: Title="Python Guide" ranks above body-only "Python" mention
- **test_auto_relevance_ordering**: Results ordered by relevance, not insertion order
- **test_sort_on_overrides_relevance**: Explicit sort_on wins over auto-relevance
- **test_weighted_tsvector_has_weights**: Stored tsvector has A/B/D weight labels

### test_query.py — Unit tests

- **test_auto_relevance_order_by**: SearchableText query → order_by contains `ts_rank_cd`
- **test_sort_on_overrides_relevance**: sort_on overrides ts_rank_cd
- **test_no_relevance_without_searchable_text**: No SearchableText → order_by is None

### test_indexing.py — Update existing

Update `test_writes_searchable_text` to verify tsvector contains weight labels.

## Step 6: CHANGES.md

Add entry under new version.

## Step 7: Run Tests

All 571+ existing tests must still pass. New tests must pass.

## Step 8: Commit + PR

## Schema Impact

- **No DDL changes.** Same `searchable_text TSVECTOR` column, same GIN index.
- GIN indexes work on any tsvector regardless of weight labels.
- **Requires full reindex** on upgrade: existing tsvector values lack weight labels.

## Risk Assessment

| Risk | Mitigation |
|---|---|
| ts_rank_cd adds CPU per query | Only applied when SearchableText is queried; tsvector is indexed |
| Duplicate tokens from Title in SearchableText | Intentional — A-weight dominates D-weight in scoring |
| Breaking existing tests | SQL output changes (ORDER BY added) — update assertions |
| reindex_object edge case | Merged idx expression tested explicitly |
