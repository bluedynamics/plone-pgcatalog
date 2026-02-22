# Plan: Phase 2 — BM25 via VectorChord-BM25 Backend

## Summary

Introduce a **SearchBackend abstraction** and add optional BM25 ranking via VectorChord-BM25. Refactors the current Level 1 tsvector logic into `TsvectorBackend` (zero behavior change) and adds `BM25Backend` that activates automatically when `vchord_bm25` + `pg_tokenizer` extensions are detected at startup.

- **Tsvector path unchanged** — all 571+ existing tests pass without modification
- **BM25 is additive** — keeps tsvector for GIN-indexed boolean pre-filtering, adds `bm25vector` column for ranking
- **Field boosting** via title repetition in combined text (3x title, 1x description, 1x body)
- **Auto-fallback** — removing extensions reverts to Level 1 ranking seamlessly

Working directory: `sources/plone-pgcatalog/`

## Files to Create/Modify

| File | Change |
|---|---|
| **NEW** `src/plone/pgcatalog/backends.py` | SearchBackend ABC, TsvectorBackend, BM25Backend, singleton |
| `src/plone/pgcatalog/config.py` | Delegate to backend in CatalogStateProcessor + detect at startup |
| `src/plone/pgcatalog/query.py` | Delegate SearchableText WHERE/ranking to backend |
| `src/plone/pgcatalog/indexing.py` | Optional BM25 column in test helpers |
| `src/plone/pgcatalog/catalog.py` | BM25-aware `clear_catalog_data()` |
| **NEW** `tests/test_backends.py` | Unit tests for backend interface (no PG needed) |
| **NEW** `tests/test_bm25_integration.py` | Integration tests (skipped without extensions) |
| `tests/conftest.py` | BM25 fixtures |
| `CHANGES.md` | Changelog entry |

## Step 1: Worktree + Branch

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/plone-pgcatalog
git worktree add .worktrees/phase2-bm25 -b feature/bm25-backend
```

## Step 2: Create `backends.py`

New module with thin abstraction (6 methods + 1 property):

### SearchBackend (ABC)

```python
class SearchBackend(abc.ABC):
    @abc.abstractmethod
    def get_extra_columns(self) -> list[ExtraColumn]:
        """Search-specific ExtraColumns (appended to path/idx)."""

    @abc.abstractmethod
    def get_schema_sql(self) -> str:
        """DDL for search-specific schema objects."""

    @abc.abstractmethod
    def process_search_data(self, pending: dict) -> dict:
        """Extract backend-specific data from pending catalog entry.
        Returns dict to merge into CatalogStateProcessor.process() result."""

    @abc.abstractmethod
    def build_search_clause(self, query_val, lang_val, pname_func):
        """Returns (where_sql, params_dict, rank_expr_or_none)."""

    @property
    def rank_ascending(self) -> bool:
        """True if lower scores = more relevant (BM25). False for tsvector."""
        return False

    def uncatalog_extra(self) -> dict:
        """Column: None pairs for uncatalog. Default: empty."""
        return {}

    @classmethod
    @abc.abstractmethod
    def detect(cls, dsn) -> bool: ...
```

### TsvectorBackend

Extracts the current Level 1 logic — **identical SQL**, just moved:

- `get_extra_columns()`: returns the existing weighted tsvector ExtraColumn (setweight A/B/D)
- `get_schema_sql()`: returns `""` (tsvector DDL already in main schema)
- `process_search_data()`: returns `{}` (no extra keys needed)
- `build_search_clause()`: returns the existing `searchable_text @@ plainto_tsquery(...)` clause + `ts_rank_cd(...)` rank expression
- `rank_ascending`: `False` (higher = better)
- `detect()`: always `True`

### BM25Backend

```python
class BM25Backend(SearchBackend):
    def __init__(self, tokenizer_name="pgcatalog_default", tokenizer_config=None):
        self.tokenizer_name = tokenizer_name
        self.tokenizer_config = tokenizer_config or _DEFAULT_TOKENIZER_CONFIG
```

- `get_extra_columns()`: returns **two** ExtraColumns:
  1. The same weighted tsvector as TsvectorBackend (keeps GIN pre-filtering)
  2. `ExtraColumn("search_bm25", "CASE WHEN %(search_bm25)s IS NOT NULL THEN tokenize(%(search_bm25)s, '{tokenizer_name}') ELSE NULL END")`

- `get_schema_sql()`: returns DDL for:
  - `CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE`
  - `CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE`
  - `ALTER TABLE object_state ADD COLUMN IF NOT EXISTS search_bm25 bm25vector`
  - Idempotent tokenizer creation via `DO $$ ... EXCEPTION WHEN duplicate_object ... $$`
  - `CREATE INDEX IF NOT EXISTS idx_os_search_bm25 ON object_state USING bm25 (search_bm25 bm25_ops)`

- `process_search_data(pending)`: builds combined text with title boost:
  ```python
  title = idx.get("Title", "")
  # 3x title repetition for field boosting
  parts = [title, title, title, description, body]
  return {"search_bm25": " ".join(filter(None, parts)) or None}
  ```

- `build_search_clause()`: returns:
  - WHERE: same tsvector `@@` clause (GIN pre-filter)
  - Rank: `search_bm25 <&> to_bm25query('idx_os_search_bm25', tokenize(%(p_bm25q)s, '{tokenizer_name}'))`

- `rank_ascending`: `True` (BM25 scores are negative, ascending = most relevant first)
- `uncatalog_extra()`: `{"search_bm25": None}`
- `detect(dsn)`: queries `pg_extension` for both `vchord_bm25` and `pg_tokenizer`

### Module-level singleton

```python
_active_backend: SearchBackend | None = None

def get_backend() -> SearchBackend:
    """Returns active backend (defaults to TsvectorBackend)."""

def set_backend(backend: SearchBackend) -> None: ...

def detect_and_set_backend(dsn: str | None) -> SearchBackend:
    """Called at startup. Tries BM25Backend.detect(), falls back to Tsvector."""
```

### Default tokenizer config

```toml
pre_tokenizer = "unicode_segmentation"
[[character_filters]]
to_lowercase = {}
[[token_filters]]
skip_non_alphanumeric = {}
[[token_filters]]
stemmer = { language = "english" }
```

Phase 3 will add multilingual/CJK tokenizer configuration.

## Step 3: Modify `config.py`

### `register_catalog_processor()` — add detection

Before registering the processor, detect and set the backend:

```python
from plone.pgcatalog.backends import detect_and_set_backend
dsn = getattr(storage, "_dsn", None)
detect_and_set_backend(dsn)
```

### `CatalogStateProcessor.get_extra_columns()` — delegate to backend

```python
def get_extra_columns(self):
    from plone.pgcatalog.backends import get_backend
    from zodb_pgjsonb import ExtraColumn
    return [
        ExtraColumn("path", "%(path)s"),
        ExtraColumn("idx", "%(idx)s"),
    ] + get_backend().get_extra_columns()
```

### `CatalogStateProcessor.get_schema_sql()` — append backend DDL

Add `+ get_backend().get_schema_sql()` to the return value.

### `CatalogStateProcessor.process()` — merge backend data

After building the base result dict, add:
```python
result.update(get_backend().process_search_data(pending))
```

For uncatalog (pending is None):
```python
result.update(get_backend().uncatalog_extra())
```

## Step 4: Modify `query.py`

### `_handle_text()` for SearchableText (idx_key is None)

Replace hardcoded tsvector SQL with backend delegation:

```python
if idx_key is None:
    from plone.pgcatalog.backends import get_backend
    lang_val = self._query.get("Language")
    if isinstance(lang_val, dict):
        lang_val = lang_val.get("query", "")
    lang_val = str(lang_val) if lang_val else ""

    clause, params, rank_expr = get_backend().build_search_clause(
        query_val, lang_val, self._pname
    )
    self.clauses.append(clause)
    self.params.update(params)
    if rank_expr is not None:
        self._text_rank_expr = rank_expr
```

The Title/Description ZCTextIndex path (`idx_key is not None`) stays **unchanged**.

### Auto-ranking direction in `process()`

Replace the hardcoded `DESC` with backend-aware direction:

```python
if self.order_by is None and hasattr(self, "_text_rank_expr"):
    from plone.pgcatalog.backends import get_backend
    direction = "ASC" if get_backend().rank_ascending else "DESC"
    self.order_by = f"{self._text_rank_expr} {direction}"
```

## Step 5: Modify `indexing.py` (test helpers)

Add optional `search_bm25` and `bm25_tokenizer` params to `catalog_object()`:

- When provided, add `search_bm25 = tokenize(%(bm25_text)s, '{tokenizer}')` to UPDATE SET
- `uncatalog_object()`: conditionally NULL `search_bm25` if BM25 backend is active

These are test helpers only — the production write path goes through CatalogStateProcessor.

## Step 6: Modify `catalog.py`

In `clear_catalog_data()` and `refreshCatalog()`, conditionally include `search_bm25 = NULL` when BM25 backend is active.

## Step 7: Tests

### `tests/test_backends.py` — Unit tests (no PG needed)

- `TestSearchBackendInterface`: both backends are instances of SearchBackend
- `TestTsvectorBackend`:
  - `get_extra_columns()` returns 1 column (searchable_text)
  - `process_search_data()` returns `{}`
  - `build_search_clause()` returns tsvector SQL + ts_rank_cd ranking
  - `rank_ascending` is False
  - `detect()` returns True
- `TestBM25Backend`:
  - `get_extra_columns()` returns 2 columns (searchable_text + search_bm25)
  - `process_search_data()` combines text with 3x title boost
  - `process_search_data()` handles missing title/description/body gracefully
  - `build_search_clause()` returns tsvector WHERE + BM25 `<&>` ranking
  - `rank_ascending` is True
  - `uncatalog_extra()` includes `search_bm25: None`
  - `detect()` returns False on bad DSN
- `TestBackendSingleton`: get/set/detect_and_set lifecycle

### `tests/test_bm25_integration.py` — Integration tests (skippable)

Module-level skip: `pytestmark = pytest.mark.skipif(not BM25Backend.detect(DSN), ...)`

- `TestBM25Write`: tokenize + store, verify bm25vector non-NULL
- `TestBM25Ranking`: title match ranks higher than body-only via `<&>` scoring
- `TestBM25QueryBuilder`: query builder uses BM25 ranking expr when backend active

### `tests/conftest.py`

Add `pg_conn_with_bm25` fixture (installs BM25 schema on top of catalog, skips if unavailable).

### Existing tests

All 571+ tests run with `TsvectorBackend` (the default) and must pass **unchanged**. The refactoring moves code into `TsvectorBackend` but produces identical SQL.

## Step 8: CHANGES.md

```markdown
## 1.0.0b7 (unreleased)

### Added

- Optional BM25 ranking via VectorChord-BM25 extension. When `vchord_bm25`
  and `pg_tokenizer` extensions are detected at startup, search results are
  automatically ranked using BM25 (IDF, term saturation, length normalization)
  instead of `ts_rank_cd`. Title matches are boosted via combined text.
  Vanilla PostgreSQL installations continue using Level 1 weighted tsvector
  ranking with no changes needed.
  **Requires:** `vchord_bm25` + `pg_tokenizer` PostgreSQL extensions.
  **Note:** Full catalog reindex required after enabling.

- `SearchBackend` abstraction: thin interface for swappable search/ranking
  backends. `TsvectorBackend` (always available) and `BM25Backend` (optional).
  Backend auto-detected at Zope startup.
```

## Step 9: Commit + PR

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Keep tsvector column with BM25 | GIN index enables fast boolean pre-filtering; BM25 only for ranking |
| Title 3x repetition for boosting | Simple, effective; no upstream field-boost API yet |
| CASE WHEN NULL handling in value_expr | Safely propagates NULL for uncatalog without calling tokenize(NULL) |
| Backend singleton (not registry) | Two backends, no plugins — keep it simple |
| `rank_ascending` property | BM25 scores are negative; direction must be configurable |
| Idempotent tokenizer DDL | `DO $$ EXCEPTION WHEN duplicate_object $$` prevents startup errors |
| BM25 tests skippable | Not all dev/CI environments have extensions installed |

## Data Flow: BM25 Write Path

```
catalog_object() → set_pending(zoid, {path, idx, searchable_text})
  → ZODB tpc_vote → CatalogStateProcessor.process()
    → base: {path, idx, searchable_text}
    → backend.process_search_data() → {search_bm25: "title title title desc body"}
    → merged result: {path, idx, searchable_text, search_bm25}
  → _batch_write_objects():
    ExtraColumn("searchable_text", setweight(A) || setweight(B) || setweight(D))
    ExtraColumn("search_bm25", CASE WHEN ... THEN tokenize(...) ELSE NULL END)
    → INSERT ... VALUES (..., weighted_tsvector, tokenize(combined))
      ON CONFLICT DO UPDATE SET ...
```

## Data Flow: BM25 Query Path

```
searchResults({"SearchableText": "security policy"})
  → build_query() → _handle_text()
    → backend.build_search_clause()
      WHERE: searchable_text @@ plainto_tsquery(...)  [GIN pre-filter]
      RANK:  search_bm25 <&> to_bm25query('idx_os_search_bm25', tokenize(...))
  → process(): auto-ranking → ORDER BY rank ASC  [BM25: ascending]
  → _run_search(): SELECT zoid, path FROM ... WHERE ... ORDER BY ...
```

## Risk Assessment

| Risk | Mitigation |
|---|---|
| tokenize() adds write latency | Only when BM25 active; tokenization is fast (microseconds) |
| BM25 index build time on large tables | DDL deferred by zodb-pgjsonb if lock conflict |
| create_text_analyzer not idempotent | Wrapped in DO/EXCEPTION block |
| Extension removal breaks queries | detect() at startup → falls back to TsvectorBackend |
| Title repetition is crude boosting | Adequate for Phase 2; Phase 3 can refine with custom tokenizers |
| Existing tests break from refactoring | TsvectorBackend produces identical SQL to current hardcoded version |
