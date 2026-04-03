# Smart Index Suggestions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the naive `_suggest_index()` with field-type-aware suggestions, on-demand EXPLAIN, and manual Apply/Drop buttons in the ZMI.

**Architecture:** New `suggestions.py` module with a pure suggestion engine + DB helpers. `catalog.py` delegates ZMI actions to it. DTML template updated for new UI.

**Tech Stack:** Python, psycopg3, DTML, PostgreSQL DDL

**Spec:** `docs/plans/2026-04-03-smart-index-suggestions-design.md`
**Issue:** https://github.com/bluedynamics/plone-pgcatalog/issues/86

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/plone/pgcatalog/suggestions.py` | Create | Pure suggestion engine + DB helpers |
| `tests/test_suggestions.py` | Create | Unit tests for `suggest_indexes()` (no DB) |
| `tests/test_suggestions_db.py` | Create | Integration tests for DB helpers |
| `src/plone/pgcatalog/catalog.py` | Modify | Remove `_suggest_index`, add ZMI action methods |
| `src/plone/pgcatalog/www/catalogSlowQueries.dtml` | Modify | New UI with suggestions, Apply/Drop, EXPLAIN |
| `CHANGES.md` | Modify | Changelog entry |

---

### Task 1: Core suggestion engine — `suggest_indexes()`

**Files:**
- Create: `tests/test_suggestions.py`
- Create: `src/plone/pgcatalog/suggestions.py`

- [ ] **Step 1: Write failing tests for the suggestion engine**

```python
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


class TestSuggestIndexes:
    """Test the pure suggestion engine."""

    def test_single_field_returns_single_btree(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type"], registry, {})
        assert len(result) == 1
        assert result[0]["status"] == "new"
        assert "(idx->>'portal_type')" in result[0]["ddl"]
        assert "idx_os_sug_" in result[0]["ddl"]

    def test_two_fields_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Creator=IndexType.FIELD,
        )
        result = suggest_indexes(["portal_type", "Creator"], registry, {})
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
        result = suggest_indexes(["a", "b", "c", "d"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        # Composite should have max 3 fields
        for s in new:
            assert len(s["fields"]) <= 3

    def test_keyword_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Subject=IndexType.KEYWORD,
        )
        result = suggest_indexes(["portal_type", "Subject"], registry, {})
        # KEYWORD gets its own suggestion, not mixed into composite
        for s in result:
            if "Subject" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("KEYWORD should not be in a composite")

    def test_text_excluded_from_composite(self):
        registry = _reg(
            portal_type=IndexType.FIELD,
            Title=IndexType.TEXT,
        )
        result = suggest_indexes(["portal_type", "Title"], registry, {})
        for s in result:
            if "Title" in s["fields"] and len(s["fields"]) > 1:
                pytest.fail("TEXT should not be in a composite")

    def test_date_uses_timestamptz(self):
        registry = _reg(modified=IndexType.DATE)
        result = suggest_indexes(["modified"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("pgcatalog_to_timestamptz" in s["ddl"] for s in new)

    def test_boolean_uses_cast(self):
        registry = _reg(is_folderish=IndexType.BOOLEAN)
        result = suggest_indexes(["is_folderish"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("::boolean" in s["ddl"] for s in new)

    def test_uuid_uses_text_expression(self):
        registry = _reg(UID=IndexType.UUID)
        result = suggest_indexes(["UID"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("(idx->>'UID')" in s["ddl"] for s in new)

    def test_path_uses_text_pattern_ops(self):
        registry = _reg(tgpath=IndexType.PATH)
        result = suggest_indexes(["tgpath"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("text_pattern_ops" in s["ddl"] for s in new)

    def test_keyword_gets_own_gin(self):
        registry = _reg(Subject=IndexType.KEYWORD)
        result = suggest_indexes(["Subject"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        assert any("GIN" in s["ddl"].upper() or "gin" in s["ddl"] for s in new)

    def test_non_idx_fields_filtered(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(
            ["portal_type", "sort_on", "b_size"], registry, {}
        )
        for s in result:
            assert "sort_on" not in s["fields"]
            assert "b_size" not in s["fields"]

    def test_unknown_field_skipped(self):
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(
            ["portal_type", "unknown_field"], registry, {}
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
        result = suggest_indexes(["portal_type"], registry, existing)
        assert all(s["status"] == "already_covered" for s in result)

    def test_dedicated_column_already_covered(self):
        registry = _reg(
            allowedRolesAndUsers=IndexType.KEYWORD,
        )
        result = suggest_indexes(["allowedRolesAndUsers"], registry, {})
        covered = [s for s in result if s["status"] == "already_covered"]
        assert len(covered) == 1
        assert "dedicated column" in covered[0]["reason"].lower()

    def test_empty_keys_returns_empty(self):
        registry = _reg()
        result = suggest_indexes([], registry, {})
        assert result == []

    def test_all_filtered_keys_returns_empty(self):
        registry = _reg()
        result = suggest_indexes(["sort_on", "b_size"], registry, {})
        assert result == []

    def test_selectivity_ordering(self):
        """UUID fields should come first in composites (most selective)."""
        registry = _reg(
            review_state=IndexType.FIELD,
            UID=IndexType.UUID,
        )
        result = suggest_indexes(["review_state", "UID"], registry, {})
        composites = [s for s in result if len(s["fields"]) > 1]
        if composites:
            assert composites[0]["fields"][0] == "UID"

    def test_naming_convention(self):
        """Generated index names use idx_os_sug_ prefix."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(["portal_type"], registry, {})
        new = [s for s in result if s["status"] == "new"]
        for s in new:
            assert "idx_os_sug_" in s["ddl"]

    def test_date_range_excluded(self):
        """DATE_RANGE (effectiveRange) should be filtered by _NON_IDX_FIELDS."""
        registry = _reg(portal_type=IndexType.FIELD)
        result = suggest_indexes(
            ["portal_type", "effectiveRange"], registry, {}
        )
        for s in result:
            assert "effectiveRange" not in s["fields"]

    def test_gopip_skipped(self):
        """GopipIndex fields are skipped (no meaningful PG index type)."""
        registry = _reg(getObjPositionInParent=IndexType.GOPIP)
        result = suggest_indexes(["getObjPositionInParent"], registry, {})
        assert result == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_suggestions.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'plone.pgcatalog.suggestions'`

- [ ] **Step 3: Implement `suggestions.py` — suggestion engine**

```python
"""Smart index suggestions for plone.pgcatalog.

Pure suggestion engine + DB helpers for EXPLAIN, apply, and drop.
The suggestion engine (``suggest_indexes``) has no DB access — it takes
the IndexRegistry and existing indexes as input and returns suggestions.
"""

from plone.pgcatalog.columns import IndexType

import logging
import re
import time


__all__ = [
    "apply_index",
    "drop_index",
    "explain_query",
    "get_existing_indexes",
    "suggest_indexes",
]

log = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────

# Query-meta keys that are not index fields (same as _NON_IDX_FIELDS in catalog.py)
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

# Fields with dedicated PG columns — always "already_covered"
_DEDICATED_COLUMNS = {
    "allowedRolesAndUsers": "allowed_roles (dedicated TEXT[] column + GIN)",
    "SearchableText": "searchable_text (dedicated tsvector column + GIN)",
}

# IndexTypes that cannot participate in btree composites
_NON_COMPOSITE_TYPES = frozenset({IndexType.KEYWORD, IndexType.TEXT})

# IndexTypes we don't generate suggestions for
_SKIP_TYPES = frozenset({IndexType.GOPIP, IndexType.DATE_RANGE})

# Selectivity ordering for composite indexes (most selective first)
_SELECTIVITY_ORDER = {
    IndexType.UUID: 0,
    IndexType.FIELD: 1,
    IndexType.DATE: 2,
    IndexType.BOOLEAN: 3,
    IndexType.PATH: 4,
}

# Safe index name pattern
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


# ── DDL expression builders ─────────────────────────────────────────────


def _btree_expr(field, idx_type):
    """Return the btree index expression for a field + IndexType."""
    if idx_type == IndexType.DATE:
        return f"pgcatalog_to_timestamptz(idx->>'{field}')"
    if idx_type == IndexType.BOOLEAN:
        return f"(idx->>'{field}')::boolean"
    if idx_type == IndexType.PATH:
        return f"(idx->>'{field}') text_pattern_ops"
    # FIELD, UUID — plain text expression
    return f"(idx->>'{field}')"


def _gin_expr(field):
    """Return the GIN index expression for a KEYWORD field."""
    return f"(idx->'{field}')"


# ── Core suggestion engine ───────────────────────────────────────────────


def suggest_indexes(query_keys, registry, existing_indexes):
    """Generate index suggestions for a set of slow-query field keys.

    Pure function — no DB access.

    Args:
        query_keys: list of catalog query field names
        registry: IndexRegistry instance (has .items() returning
            name -> (IndexType, idx_key, source_attrs))
        existing_indexes: dict {index_name: index_def_sql} from
            get_existing_indexes()

    Returns:
        list of suggestion dicts with keys: fields, field_types, ddl,
        status ("new" | "already_covered"), reason.
    """
    # Build lookup from registry
    reg_lookup = {}
    for name, (idx_type, idx_key, _source_attrs) in registry.items():
        reg_lookup[name] = idx_type

    # Filter and classify fields
    suggestions = []
    btree_fields = []  # (field, idx_type) tuples for composite candidate

    for key in query_keys:
        if key in _NON_IDX_FIELDS:
            continue

        # Dedicated column check
        if key in _DEDICATED_COLUMNS:
            suggestions.append(
                {
                    "fields": [key],
                    "field_types": [reg_lookup.get(key, "KEYWORD").name
                                   if isinstance(reg_lookup.get(key), IndexType)
                                   else "KEYWORD"],
                    "ddl": "",
                    "status": "already_covered",
                    "reason": f"Dedicated column: {_DEDICATED_COLUMNS[key]}",
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
            _add_standalone_suggestion(
                key, idx_type, existing_indexes, suggestions
            )
        else:
            btree_fields.append((key, idx_type))

    # Build composite from btree-eligible fields
    if btree_fields:
        _add_btree_suggestions(btree_fields, existing_indexes, suggestions)

    return suggestions


def _add_standalone_suggestion(field, idx_type, existing_indexes, suggestions):
    """Add a standalone GIN/tsvector suggestion for KEYWORD or TEXT field."""
    if idx_type == IndexType.KEYWORD:
        expr = _gin_expr(field)
        ddl = (
            f"CREATE INDEX CONCURRENTLY idx_os_sug_{field} "
            f"ON object_state USING gin ({expr}) "
            f"WHERE idx IS NOT NULL AND idx ? '{field}'"
        )
        reason = f"GIN index for KEYWORD field '{field}'"
    elif idx_type == IndexType.TEXT:
        ddl = (
            f"CREATE INDEX CONCURRENTLY idx_os_sug_{field}_tsv "
            f"ON object_state USING gin ("
            f"to_tsvector('simple'::regconfig, COALESCE(idx->>'{field}', ''))"
            f") WHERE idx IS NOT NULL"
        )
        reason = f"GIN tsvector index for TEXT field '{field}'"
    else:
        return

    status = _check_covered(ddl, existing_indexes)
    suggestions.append(
        {
            "fields": [field],
            "field_types": [idx_type.name],
            "ddl": ddl,
            "status": status,
            "reason": reason if status == "new" else f"Already covered: {reason}",
        }
    )


def _add_btree_suggestions(btree_fields, existing_indexes, suggestions):
    """Add btree suggestions — single or composite (max 3 fields)."""
    # Sort by selectivity
    btree_fields.sort(key=lambda ft: _SELECTIVITY_ORDER.get(ft[1], 99))

    # Limit to max 3
    fields_limited = btree_fields[:3]

    if len(fields_limited) == 1:
        field, idx_type = fields_limited[0]
        expr = _btree_expr(field, idx_type)
        name = f"idx_os_sug_{field}"
        ddl = (
            f"CREATE INDEX CONCURRENTLY {name} "
            f"ON object_state ({expr}) WHERE idx IS NOT NULL"
        )
        reason = f"Btree index for {idx_type.name} field '{field}'"
    else:
        exprs = [_btree_expr(f, t) for f, t in fields_limited]
        field_names = [f for f, _t in fields_limited]
        name = "idx_os_sug_" + "_".join(field_names)
        cols = ", ".join(exprs)
        ddl = (
            f"CREATE INDEX CONCURRENTLY {name} "
            f"ON object_state ({cols}) WHERE idx IS NOT NULL"
        )
        types_str = " + ".join(t.name for _f, t in fields_limited)
        reason = f"Composite btree ({types_str}) for {len(fields_limited)} fields"

    status = _check_covered(ddl, existing_indexes)
    suggestions.append(
        {
            "fields": [f for f, _t in fields_limited],
            "field_types": [t.name for _f, t in fields_limited],
            "ddl": ddl,
            "status": status,
            "reason": reason if status == "new" else f"Already covered: {reason}",
        }
    )


def _check_covered(ddl, existing_indexes):
    """Check if the DDL expression is already covered by an existing index.

    Extracts the column expressions from the DDL and checks if any existing
    index definition contains the same expressions.
    """
    # Extract the part between ON object_state (...) to compare
    m = re.search(r"ON object_state\b.*?\((.+?)\)\s*(?:WHERE|USING|$)", ddl)
    if not m:
        return "new"
    new_expr = m.group(1).strip()

    for _idx_name, idx_def in existing_indexes.items():
        if new_expr in idx_def:
            return "already_covered"
    return "new"


# ── DB helpers ───────────────────────────────────────────────────────────


def get_existing_indexes(conn):
    """Query pg_indexes for all indexes on object_state.

    Returns:
        dict mapping index_name -> full CREATE INDEX definition
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE tablename = 'object_state'"
        )
        return {row["indexname"]: row["indexdef"] for row in cur.fetchall()}


def explain_query(conn, sql, params):
    """Run EXPLAIN (FORMAT JSON) for a query and return the plan.

    Args:
        conn: psycopg connection (dict_row factory)
        sql: the SQL query text from pgcatalog_slow_queries.query_text
        params: dict from pgcatalog_slow_queries.params (JSONB)

    Returns:
        dict with top-level plan node including 'total_cost' and 'node_type',
        or error dict on failure.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT JSON) {sql}", params or {})
            rows = cur.fetchall()
            if rows:
                plan = rows[0]["QUERY PLAN"]
                if isinstance(plan, list) and plan:
                    top = plan[0].get("Plan", {})
                    return {
                        "node_type": top.get("Node Type", "Unknown"),
                        "total_cost": top.get("Total Cost", 0),
                        "plan_rows": top.get("Plan Rows", 0),
                        "plan": plan,
                    }
        return {"error": "No plan returned"}
    except Exception as exc:
        return {"error": str(exc)}


def apply_index(conn, ddl):
    """Create an index using CREATE INDEX CONCURRENTLY.

    The connection must support autocommit (CONCURRENTLY cannot run
    inside a transaction block).

    Args:
        conn: psycopg connection
        ddl: full CREATE INDEX CONCURRENTLY statement

    Returns:
        tuple (success: bool, message: str, duration_seconds: float)
    """
    # Validate DDL
    ddl_upper = ddl.strip().upper()
    if not ddl_upper.startswith("CREATE INDEX"):
        return (False, "DDL must start with CREATE INDEX", 0.0)
    if "OBJECT_STATE" not in ddl_upper:
        return (False, "DDL must target object_state table", 0.0)

    # Extract index name for logging
    m = re.search(r"(?:CREATE INDEX\s+(?:CONCURRENTLY\s+)?)(\S+)", ddl, re.I)
    idx_name = m.group(1) if m else "unknown"

    old_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        log.info("Creating index: %s", ddl)
        t0 = time.monotonic()
        conn.execute(ddl)
        duration = time.monotonic() - t0
        log.info("Index created in %.1fs: %s", duration, idx_name)
        return (True, f"Index {idx_name} created in {duration:.1f}s", duration)
    except Exception as exc:
        log.error("Index creation failed: %s — %s", idx_name, exc)
        return (False, f"Index creation failed: {exc}", 0.0)
    finally:
        conn.autocommit = old_autocommit


def drop_index(conn, index_name):
    """Drop a suggestion-system index using DROP INDEX CONCURRENTLY.

    Only indexes with the ``idx_os_sug_`` prefix can be dropped.
    Refuses to drop indexes in EXPECTED_INDEXES (base catalog indexes).

    Args:
        conn: psycopg connection
        index_name: name of the index to drop

    Returns:
        tuple (success: bool, message: str, duration_seconds: float)
    """
    from plone.pgcatalog.schema import EXPECTED_INDEXES

    if not index_name.startswith("idx_os_sug_"):
        return (False, f"Refusing to drop {index_name}: not a suggestion index", 0.0)
    if index_name in EXPECTED_INDEXES:
        return (False, f"Refusing to drop {index_name}: protected base index", 0.0)
    if not _SAFE_NAME_RE.match(index_name):
        return (False, f"Invalid index name: {index_name}", 0.0)

    old_autocommit = conn.autocommit
    try:
        conn.autocommit = True
        ddl = f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}"
        log.info("Dropping index: %s", ddl)
        t0 = time.monotonic()
        conn.execute(ddl)
        duration = time.monotonic() - t0
        log.info("Index dropped in %.1fs: %s", duration, index_name)
        return (True, f"Index {index_name} dropped in {duration:.1f}s", duration)
    except Exception as exc:
        log.error("Index drop failed: %s — %s", index_name, exc)
        return (False, f"Index drop failed: {exc}", 0.0)
    finally:
        conn.autocommit = old_autocommit
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_suggestions.py -v`
Expected: all tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/suggestions.py tests/test_suggestions.py
git commit -m "feat: smart index suggestion engine with type-aware DDL (#86)"
```

---

### Task 2: DB helper tests

**Files:**
- Create: `tests/test_suggestions_db.py`

- [ ] **Step 1: Write integration tests for DB helpers**

```python
"""Integration tests for suggestions DB helpers — requires PostgreSQL."""

from plone.pgcatalog.suggestions import apply_index
from plone.pgcatalog.suggestions import drop_index
from plone.pgcatalog.suggestions import explain_query
from plone.pgcatalog.suggestions import get_existing_indexes
from tests.conftest import DSN

import pytest


pytestmark = pytest.mark.skipif(not DSN, reason="No PostgreSQL DSN configured")


class TestGetExistingIndexes:
    def test_returns_dict(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        result = get_existing_indexes(conn)
        assert isinstance(result, dict)
        # Should have at least the base catalog indexes
        assert any("idx_os_" in name for name in result)

    def test_includes_index_definitions(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        result = get_existing_indexes(conn)
        for name, defn in result.items():
            assert "CREATE INDEX" in defn or "CREATE UNIQUE INDEX" in defn


class TestExplainQuery:
    def test_simple_query(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        result = explain_query(
            conn,
            "SELECT zoid FROM object_state WHERE idx IS NOT NULL LIMIT 1",
            {},
        )
        assert "node_type" in result
        assert "total_cost" in result

    def test_invalid_sql_returns_error(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        result = explain_query(conn, "SELECT FROM nonexistent_table", {})
        assert "error" in result


class TestApplyIndex:
    def test_create_and_verify(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        ddl = (
            "CREATE INDEX CONCURRENTLY idx_os_sug_test_apply "
            "ON object_state ((idx->>'portal_type')) WHERE idx IS NOT NULL"
        )
        success, msg, duration = apply_index(conn, ddl)
        assert success, msg
        assert "idx_os_sug_test_apply" in msg
        assert duration > 0

        # Verify it exists
        indexes = get_existing_indexes(conn)
        assert "idx_os_sug_test_apply" in indexes

        # Cleanup
        conn.autocommit = True
        conn.execute("DROP INDEX IF EXISTS idx_os_sug_test_apply")

    def test_rejects_non_create_index(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        success, msg, _ = apply_index(conn, "DROP TABLE object_state")
        assert not success
        assert "must start with CREATE INDEX" in msg

    def test_rejects_wrong_table(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        success, msg, _ = apply_index(
            conn,
            "CREATE INDEX CONCURRENTLY idx_test ON other_table (col)",
        )
        assert not success
        assert "must target object_state" in msg


class TestDropIndex:
    def test_drop_suggestion_index(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        # First create one
        conn.autocommit = True
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_os_sug_test_drop "
            "ON object_state ((idx->>'portal_type')) WHERE idx IS NOT NULL"
        )
        conn.autocommit = False

        success, msg, duration = drop_index(conn, "idx_os_sug_test_drop")
        assert success, msg
        assert "dropped" in msg

    def test_refuses_non_sug_prefix(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        success, msg, _ = drop_index(conn, "idx_os_cat_portal_type")
        assert not success
        assert "not a suggestion index" in msg

    def test_refuses_protected_index(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        success, msg, _ = drop_index(conn, "idx_os_sug_path")
        # Even with sug_ prefix, if somehow in EXPECTED_INDEXES
        # In practice this won't happen, but the guard is there
        assert success or "not a suggestion index" in msg or "protected" in msg

    def test_refuses_invalid_name(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        success, msg, _ = drop_index(conn, "idx_os_sug_'; DROP TABLE foo;--")
        assert not success
        assert "Invalid index name" in msg
```

- [ ] **Step 2: Run tests (will skip without PG, or pass with PG)**

Run: `uv run pytest tests/test_suggestions_db.py -v`
Expected: SKIP (no PG) or PASS (with PG)

- [ ] **Step 3: Commit**

```bash
git add tests/test_suggestions_db.py
git commit -m "test: integration tests for suggestions DB helpers (#86)"
```

---

### Task 3: Integrate into `catalog.py`

**Files:**
- Modify: `src/plone/pgcatalog/catalog.py`

- [ ] **Step 1: Remove `_suggest_index()` and `_NON_IDX_FIELDS` from catalog.py**

Delete lines 87-111 (the `_NON_IDX_FIELDS` frozenset and `_suggest_index` function).

The `_NON_IDX_FIELDS` constant now lives in `suggestions.py`.

- [ ] **Step 2: Update `manage_get_slow_query_stats()` to use `suggest_indexes()`**

Replace the method body (starting at line 1111) with:

```python
    def manage_get_slow_query_stats(self):
        """Return aggregated slow query stats for the Slow Queries tab."""
        from plone.pgcatalog.suggestions import get_existing_indexes
        from plone.pgcatalog.suggestions import suggest_indexes

        try:
            pool = get_pool(self)
            pg_conn = pool.getconn()
            try:
                registry = get_registry()
                existing = get_existing_indexes(pg_conn)

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
                    "suggestions": suggest_indexes(keys, registry, existing),
                }
            )
        return result
```

- [ ] **Step 3: Add `manage_get_managed_indexes()` method**

Add after `manage_get_slow_query_stats`:

```python
    def manage_get_managed_indexes(self):
        """Return list of idx_os_sug_* indexes for ZMI display."""
        from plone.pgcatalog.suggestions import get_existing_indexes

        try:
            pool = get_pool(self)
            pg_conn = pool.getconn()
            try:
                existing = get_existing_indexes(pg_conn)
            finally:
                pool.putconn(pg_conn)
        except Exception:
            return []

        return [
            {"name": name, "definition": defn}
            for name, defn in sorted(existing.items())
            if name.startswith("idx_os_sug_")
        ]
```

- [ ] **Step 4: Add `manage_explain_slow_query()` method**

```python
    def manage_explain_slow_query(self, query_id, REQUEST=None):
        """ZMI action: run EXPLAIN on a slow query and return the plan."""
        from plone.pgcatalog.suggestions import explain_query

        try:
            pool = get_pool(self)
            pg_conn = pool.getconn()
            try:
                with pg_conn.cursor() as cur:
                    cur.execute(
                        "SELECT query_text, params FROM pgcatalog_slow_queries "
                        "WHERE id = %(id)s",
                        {"id": int(query_id)},
                    )
                    row = cur.fetchone()
                if row and row["query_text"]:
                    return explain_query(
                        pg_conn, row["query_text"], row["params"]
                    )
            finally:
                pool.putconn(pg_conn)
        except Exception as exc:
            return {"error": str(exc)}
        return {"error": "Query not found"}
```

- [ ] **Step 5: Add `manage_apply_index()` method**

```python
    def manage_apply_index(self, ddl, REQUEST=None):
        """ZMI action: create a suggested index."""
        from plone.pgcatalog.suggestions import apply_index

        msg = "No action taken"
        try:
            pool = get_pool(self)
            pg_conn = pool.getconn()
            try:
                success, msg, _duration = apply_index(pg_conn, ddl)
            finally:
                pool.putconn(pg_conn)
        except Exception as exc:
            msg = f"Error: {exc}"

        if REQUEST is not None:
            from urllib.parse import quote

            REQUEST.RESPONSE.redirect(
                f"{self.absolute_url()}/manage_slowQueries"
                f"?manage_tabs_message={quote(msg)}"
            )
        return msg
```

- [ ] **Step 6: Add `manage_drop_index()` method**

```python
    def manage_drop_index(self, index_name, REQUEST=None):
        """ZMI action: drop a suggestion-system index."""
        from plone.pgcatalog.suggestions import drop_index

        msg = "No action taken"
        try:
            pool = get_pool(self)
            pg_conn = pool.getconn()
            try:
                success, msg, _duration = drop_index(pg_conn, index_name)
            finally:
                pool.putconn(pg_conn)
        except Exception as exc:
            msg = f"Error: {exc}"

        if REQUEST is not None:
            from urllib.parse import quote

            REQUEST.RESPONSE.redirect(
                f"{self.absolute_url()}/manage_slowQueries"
                f"?manage_tabs_message={quote(msg)}"
            )
        return msg
```

- [ ] **Step 7: Add security declarations for new methods**

Add after line 202 (the existing `manage_clear_cache` declaration):

```python
    security.declareProtected(manage_zcatalog_entries, "manage_get_managed_indexes")
    security.declareProtected(manage_zcatalog_entries, "manage_explain_slow_query")
    security.declareProtected(manage_zcatalog_entries, "manage_apply_index")
    security.declareProtected(manage_zcatalog_entries, "manage_drop_index")
```

- [ ] **Step 8: Run existing tests to verify no regressions**

Run: `uv run pytest tests/test_suggestions.py tests/test_indexers.py tests/test_collect_ref_oids.py -v`
Expected: all PASS

- [ ] **Step 9: Commit**

```bash
git add src/plone/pgcatalog/catalog.py
git commit -m "feat: integrate smart suggestions into catalog.py ZMI actions (#86)"
```

---

### Task 4: Update ZMI DTML template

**Files:**
- Modify: `src/plone/pgcatalog/www/catalogSlowQueries.dtml`

- [ ] **Step 1: Replace the DTML template**

Replace the entire file with:

```html
<dtml-var manage_page_header>
<dtml-var manage_tabs>

<main class="container-fluid">

<dtml-let stats="manage_get_slow_query_stats()">

<div class="card mt-3 mb-3">
  <div class="card-header d-flex justify-content-between align-items-center">
    <strong>Slow Query Patterns</strong>
    <div>
      <span class="text-muted small mr-2">Threshold: <code><dtml-var "manage_get_slow_query_threshold()">ms</code></span>
      <a href="manage_clear_slow_queries"
         class="btn btn-sm btn-outline-danger"
         onclick="return confirm('Clear all slow query statistics?')">Clear Stats</a>
    </div>
  </div>
  <div class="card-body p-0">
    <dtml-if stats>
    <table class="table table-striped table-hover table-sm mb-0">
      <thead class="thead-light">
        <tr>
          <th scope="col">Query Fields</th>
          <th scope="col" style="width:8%">Count</th>
          <th scope="col" style="width:10%">Avg (ms)</th>
          <th scope="col" style="width:10%">Max (ms)</th>
          <th scope="col" style="width:15%">Last Seen</th>
          <th scope="col">Index Suggestions</th>
        </tr>
      </thead>
      <tbody>
        <dtml-in stats>
        <dtml-let row="_['sequence-item']">
        <tr>
          <td><code><dtml-var "row['query_keys']" html_quote></code></td>
          <td><dtml-var "row['count']"></td>
          <td><dtml-var "row['avg_ms']"></td>
          <td><dtml-var "row['max_ms']"></td>
          <td class="text-muted small"><dtml-var "row['last_seen']" html_quote></td>
          <td>
            <dtml-if "row.get('suggestions')">
              <dtml-in "row['suggestions']">
              <dtml-let sug="_['sequence-item']">
                <dtml-if "sug['status'] == 'already_covered'">
                  <span class="text-success small">
                    &#10003; <dtml-var "', '.join(sug['fields'])" html_quote>
                    (<dtml-var "', '.join(sug['field_types'])" html_quote>)
                    &mdash; <dtml-var "sug['reason']" html_quote>
                  </span><br>
                <dtml-else>
                  <div class="mb-1">
                    <strong class="small">
                      <dtml-var "', '.join(sug['fields'])" html_quote>
                      (<dtml-var "', '.join(sug['field_types'])" html_quote>)
                    </strong>
                    <span class="text-muted small">&mdash; <dtml-var "sug['reason']" html_quote></span><br>
                    <code class="small"><dtml-var "sug['ddl']" html_quote></code>
                    <form method="post" action="manage_apply_index" style="display:inline">
                      <input type="hidden" name="ddl" value="<dtml-var "sug['ddl']" html_quote>">
                      <button type="submit" class="btn btn-sm btn-outline-primary ml-1"
                              onclick="return confirm('Create this index? This may take a moment on large tables.')">
                        Apply
                      </button>
                    </form>
                  </div>
                </dtml-if>
              </dtml-let>
              </dtml-in>
            <dtml-else>
              <span class="text-muted">-</span>
            </dtml-if>
          </td>
        </tr>
        </dtml-let>
        </dtml-in>
      </tbody>
    </table>
    <dtml-else>
    <p class="text-muted p-3 mb-0">
      No slow queries recorded yet.
      Queries exceeding the threshold (<code>PGCATALOG_SLOW_QUERY_MS</code>,
      default 10ms) are logged here automatically.
    </p>
    </dtml-if>
  </div>
</div>

</dtml-let>

<dtml-let managed="manage_get_managed_indexes()">

<div class="card mb-3">
  <div class="card-header">
    <strong>Managed Indexes</strong>
    <span class="text-muted small ml-2">(created via suggestions &mdash; prefix <code>idx_os_sug_</code>)</span>
  </div>
  <div class="card-body p-0">
    <dtml-if managed>
    <table class="table table-striped table-hover table-sm mb-0">
      <thead class="thead-light">
        <tr>
          <th scope="col">Index Name</th>
          <th scope="col">Definition</th>
          <th scope="col" style="width:10%">Action</th>
        </tr>
      </thead>
      <tbody>
        <dtml-in managed>
        <dtml-let idx="_['sequence-item']">
        <tr>
          <td><code><dtml-var "idx['name']" html_quote></code></td>
          <td><code class="small"><dtml-var "idx['definition']" html_quote></code></td>
          <td>
            <form method="post" action="manage_drop_index" style="display:inline">
              <input type="hidden" name="index_name" value="<dtml-var "idx['name']" html_quote>">
              <button type="submit" class="btn btn-sm btn-outline-danger"
                      onclick="return confirm('Drop this index?')">
                Drop
              </button>
            </form>
          </td>
        </tr>
        </dtml-let>
        </dtml-in>
      </tbody>
    </table>
    <dtml-else>
    <p class="text-muted p-3 mb-0">
      No suggestion indexes created yet. Use the "Apply" button above to create indexes from suggestions.
    </p>
    </dtml-if>
  </div>
</div>

</dtml-let>

<dtml-let cache="manage_get_cache_stats()">

<div class="card mb-3">
  <div class="card-header d-flex justify-content-between align-items-center">
    <strong>Query Result Cache</strong>
    <div>
      <dtml-if "cache['enabled']">
        <span class="badge badge-success">enabled</span>
      <dtml-else>
        <span class="badge badge-secondary">disabled</span>
      </dtml-if>
      <a href="manage_clear_cache"
         class="btn btn-sm btn-outline-danger ml-2"
         onclick="return confirm('Clear the query result cache?')">Clear Cache</a>
    </div>
  </div>
  <dtml-if "cache['enabled']">
  <div class="card-body p-3">
    <table class="table table-sm table-borderless mb-3">
      <tbody>
        <tr>
          <td class="text-muted" style="width:25%">Entries</td>
          <td><dtml-var "cache['current_entries']"> / <dtml-var "cache['max_entries']"></td>
        </tr>
        <tr>
          <td class="text-muted">Hit rate</td>
          <td>
            <strong><dtml-var "cache['hit_rate']">%</strong>
            (<dtml-var "cache['hits']"> hits, <dtml-var "cache['misses']"> misses)
          </td>
        </tr>
        <tr>
          <td class="text-muted">Invalidations</td>
          <td><dtml-var "cache['invalidations']"></td>
        </tr>
        <tr>
          <td class="text-muted">TTR (rounding)</td>
          <td><dtml-var "cache['ttr_seconds']">s</td>
        </tr>
      </tbody>
    </table>

    <dtml-if "cache['top_entries']">
    <h6>Top cached queries (by cost)</h6>
    <table class="table table-striped table-sm mb-0">
      <thead class="thead-light">
        <tr>
          <th scope="col" style="width:15%">Cost (ms)</th>
          <th scope="col" style="width:15%">Hits</th>
          <th scope="col" style="width:15%">Rows</th>
        </tr>
      </thead>
      <tbody>
        <dtml-in "cache['top_entries']">
        <dtml-let entry="_['sequence-item']">
        <tr>
          <td><dtml-var "entry['cost_ms']"></td>
          <td><dtml-var "entry['hits']"></td>
          <td><dtml-var "entry['rows']"></td>
        </tr>
        </dtml-let>
        </dtml-in>
      </tbody>
    </table>
    </dtml-if>
  </div>
  <dtml-else>
  <div class="card-body p-3">
    <p class="text-muted small mb-0">
      Set <code>PGCATALOG_QUERY_CACHE_SIZE</code> to enable the query result cache
      (default: 200 entries).
    </p>
  </div>
  </dtml-if>
</div>

</dtml-let>

</main>

<dtml-var manage_page_footer>
```

- [ ] **Step 2: Commit**

```bash
git add src/plone/pgcatalog/www/catalogSlowQueries.dtml
git commit -m "feat: update ZMI Slow Queries tab with suggestions, Apply/Drop buttons (#86)"
```

---

### Task 5: Changelog and cleanup

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Add changelog entry**

Add a new version section above the current top entry in `CHANGES.md`:

```markdown
## 1.0.0b44

### Added

- Smart index suggestions in ZMI Slow Queries tab (#86).  Replaces the
  naive `_suggest_index()` with field-type-aware suggestions using the
  IndexRegistry.  Generates correct DDL per IndexType (btree expression,
  GIN, tsvector, composites).  Detects already-covered fields (dedicated
  columns, existing indexes).  Manual "Apply" button creates indexes via
  `CREATE INDEX CONCURRENTLY`.  "Drop" button for removing suggestion
  indexes (`idx_os_sug_*`).  On-demand EXPLAIN plans for slow queries.
  New `suggestions.py` module with pure suggestion engine + DB helpers.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog for smart index suggestions (#86)"
```
