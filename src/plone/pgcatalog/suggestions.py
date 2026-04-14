"""Smart index suggestions for plone.pgcatalog.

Pure suggestion engine + DB helpers for EXPLAIN, apply, and drop.
The suggestion engine (``suggest_indexes``) has no DB access — it takes
the IndexRegistry and existing indexes as input and returns suggestions.
"""

from plone.pgcatalog.columns import IndexType

import contextlib
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

# Fields with dedicated PG columns or indexes — always "already_covered"
_DEDICATED_FIELDS = {
    "allowedRolesAndUsers": "allowed_roles (dedicated TEXT[] column + GIN)",
    "SearchableText": "searchable_text (dedicated tsvector column + GIN)",
    "object_provides": "object_provides (dedicated TEXT[] column + GIN)",
    "Subject": "idx_os_cat_subject_gin (dedicated GIN index)",
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
        return f"((idx->>'{field}')::boolean)"
    if idx_type == IndexType.PATH:
        return f"(idx->>'{field}') text_pattern_ops"
    # FIELD, UUID — plain text expression
    return f"(idx->>'{field}')"


def _gin_expr(field):
    """Return the GIN index expression for a KEYWORD field."""
    return f"(idx->'{field}')"


# ── Core suggestion engine ───────────────────────────────────────────────


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

    Returns:
        list of suggestion dicts with keys: fields, field_types, ddl,
        status ("new" | "already_covered"), reason.
    """
    # Build lookup from registry
    reg_lookup = {}
    for name, (idx_type, _idx_key, _source_attrs) in registry.items():
        reg_lookup[name] = idx_type

    # Filter and classify fields
    suggestions = []
    btree_fields = []  # (field, idx_type) tuples for composite candidate

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
    """Check if the suggested index already exists.

    Two checks:
    1. Exact name match (catches re-apply of same suggestion).
       Name is lowercased because PostgreSQL folds unquoted
       identifiers to lowercase in pg_indexes.indexname.
    2. Normalized expression match (catches existing idx_os_cat_*
       indexes that cover the same columns with different naming).
    """
    # Check 1: case-insensitive index name match
    m = re.search(r"(?:CREATE INDEX\s+(?:CONCURRENTLY\s+)?)(\S+)", ddl, re.I)
    if m and m.group(1).lower() in existing_indexes:
        return "already_covered"

    # Check 2: normalize and compare column expressions
    norm = _normalize_idx_expr(ddl)
    if norm:
        for _name, idx_def in existing_indexes.items():
            if norm in _normalize_idx_expr(idx_def):
                return "already_covered"
    return "new"


def _normalize_idx_expr(ddl):
    """Extract and normalize the column expression from a CREATE INDEX DDL.

    Produces a canonical form that compares equal across:
    - whitespace differences (including around ``->>``)
    - ``::text`` casts that PG adds on ingest
    - redundant paren wrappers PG adds around each expression
    """
    # Prefer WHERE-anchored extraction; fall back to end-of-string
    # when the DDL has no WHERE clause.  A single greedy pattern with
    # ``(WHERE|$)`` would over-capture when WHERE itself contains
    # parens (e.g. ``WHERE (idx IS NOT NULL)``) — the greedy ``.+``
    # would extend past the WHERE clause to the final ``)``.
    m = re.search(r"\((.+)\)\s+WHERE\b", ddl, re.I)
    if not m:
        m = re.search(r"\((.+)\)\s*$", ddl, re.I | re.S)
    if not m:
        return ""
    expr = m.group(1)
    # Strip PG's explicit ::text casts
    expr = re.sub(r"::text\b", "", expr)
    # Squeeze whitespace around JSON arrow operators — generated form
    # has no spaces (idx->>'x'), PG-stored form has them (idx ->> 'x').
    expr = re.sub(r"\s*(->>?|#>>?|#>)\s*", r"\1", expr)
    # Collapse runs of whitespace
    expr = re.sub(r"\s+", " ", expr).strip()
    # Iteratively collapse redundant paren wrappers — PG stores
    # ((expr)) where the generator emits (expr), and a single-pass
    # regex with a non-greedy group misses nested cases.
    while True:
        new = re.sub(r"\(\s*\(([^()]+)\)\s*\)", r"(\1)", expr)
        if new == expr:
            break
        expr = new
    return expr


# ── DB helpers ───────────────────────────────────────────────────────────


def get_existing_indexes(conn):  # pragma: no cover
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


def explain_query(conn, sql, params):  # pragma: no cover
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


_DEFAULT_INDEX_TIMEOUT = "5min"


def apply_index(conn, ddl, timeout=_DEFAULT_INDEX_TIMEOUT):
    """Create an index using CREATE INDEX CONCURRENTLY.

    The connection must support autocommit (CONCURRENTLY cannot run
    inside a transaction block).

    Before building, checks for and drops any INVALID index with the
    same name (left behind by a previously aborted CONCURRENTLY build).

    Sets ``statement_timeout`` to prevent indefinite hangs when other
    sessions hold long-running REPEATABLE READ transactions (#100).
    The default timeout is 5 minutes; if exceeded, the build is aborted
    and can be retried later.

    Args:
        conn: psycopg connection
        ddl: full CREATE INDEX CONCURRENTLY statement
        timeout: PG interval string for statement_timeout (default 5min)

    Returns:
        tuple (success: bool, message: str, duration_seconds: float)
    """
    # Validate DDL
    ddl_upper = ddl.strip().upper()
    if not ddl_upper.startswith("CREATE INDEX"):
        return (False, "DDL must start with CREATE INDEX", 0.0)
    if "OBJECT_STATE" not in ddl_upper:
        return (False, "DDL must target object_state table", 0.0)

    # Extract and validate index name
    m = re.search(r"(?:CREATE INDEX\s+(?:CONCURRENTLY\s+)?)(\S+)", ddl, re.I)
    idx_name = m.group(1) if m else "unknown"
    if not _SAFE_NAME_RE.match(idx_name):
        return (False, f"Invalid index name: {idx_name!r}", 0.0)

    # Validate timeout format (e.g. "5min", "300s", "300000ms")
    if not re.match(r"^\d+\s*(ms|s|min|h)?$", timeout):
        return (False, f"Invalid timeout format: {timeout!r}", 0.0)

    old_autocommit = conn.autocommit
    try:
        conn.autocommit = True

        # Pre-flight: query pg_index for any index with this name.
        # Three cases:
        #   - valid index exists: idempotent success no-op (#119)
        #   - INVALID index from aborted CIC: drop and retry
        #   - no index: proceed to CREATE INDEX
        # relname is always lowercase in pg_class; match case-insensitively.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT i.indisvalid FROM pg_index i "
                "JOIN pg_class c ON c.oid = i.indexrelid "
                "WHERE c.relname = %s",
                (idx_name.lower(),),
            )
            row = cur.fetchone()
        if row is not None:
            # psycopg returns dict_row or tuple_row depending on the
            # caller's factory — handle both.
            is_valid = row["indisvalid"] if hasattr(row, "keys") else row[0]
            if is_valid:
                log.info(
                    "Index %s already exists and is valid — no-op",
                    idx_name,
                )
                return (
                    True,
                    f"Index {idx_name} already exists (no-op)",
                    0.0,
                )
            log.warning(
                "Dropping INVALID index %s (aborted previous build)",
                idx_name,
            )
            conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {idx_name}")

        conn.execute(f"SET statement_timeout = '{timeout}'")
        log.info("Creating index (timeout=%s): %s", timeout, ddl)
        t0 = time.monotonic()
        conn.execute(ddl)
        duration = time.monotonic() - t0
        log.info("Index created in %.1fs: %s", duration, idx_name)
        return (
            True,
            f"Index {idx_name} created in {duration:.1f}s",
            duration,
        )
    except Exception as exc:
        log.error("Index creation failed: %s — %s", idx_name, exc)
        return (False, f"Index creation failed: {exc}", 0.0)
    finally:
        with contextlib.suppress(Exception):
            conn.execute("SET statement_timeout = 0")
        conn.autocommit = old_autocommit


def drop_index(conn, index_name):  # pragma: no cover
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
        return (
            False,
            f"Refusing to drop {index_name}: not a suggestion index",
            0.0,
        )
    if index_name in EXPECTED_INDEXES:
        return (
            False,
            f"Refusing to drop {index_name}: protected base index",
            0.0,
        )
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
        return (
            True,
            f"Index {index_name} dropped in {duration:.1f}s",
            duration,
        )
    except Exception as exc:
        log.error("Index drop failed: %s — %s", index_name, exc)
        return (False, f"Index drop failed: {exc}", 0.0)
    finally:
        conn.autocommit = old_autocommit
