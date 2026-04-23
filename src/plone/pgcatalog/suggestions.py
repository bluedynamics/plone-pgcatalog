"""Smart index suggestions for plone.pgcatalog.

Pure suggestion engine + DB helpers for EXPLAIN, apply, and drop.
The suggestion engine (``suggest_indexes``) has no DB access — it takes
the IndexRegistry and existing indexes as input and returns suggestions.
"""

from dataclasses import dataclass
from dataclasses import field
from plone.pgcatalog.columns import IndexType

import contextlib
import logging
import os
import re
import time


__all__ = [
    "Bundle",
    "BundleMember",
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

# Hard cap on columns in a composite btree suggestion — beyond three
# the write-amplification cost outweighs read savings in real Plone
# catalogs.  Sort covering column counts against this cap.
_MAX_COMPOSITE_COLUMNS = 3

# Selectivity threshold below which an equality filter gets baked into
# a partial index's WHERE clause.  Configurable via env var for
# production tuning without a code change.
_PARTIAL_PREDICATE_SELECTIVITY_THRESHOLD = float(
    os.environ.get("PGCATALOG_PARTIAL_SELECTIVITY_THRESHOLD", "0.1")
)

# IndexTypes whose equality values are eligible for partial-predicate
# scoping.  DATE excluded — values are timestamps, rarely appear in
# MCV, partial-DATE scoping is typically an anti-pattern.
_PARTIAL_SCOPING_ELIGIBLE_TYPES = frozenset(
    {IndexType.FIELD, IndexType.BOOLEAN, IndexType.PATH, IndexType.UUID}
)

# ── Selectivity probing (request-scoped cache) ─────────────────────────
#
# The cache is module-level — catalog.py resets it via
# _reset_probe_cache() at the start of each manage_get_slow_query_stats
# call, so the scope is effectively "one ZMI tab load".  Single-thread
# WSGI request model makes this safe.

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

    # Step 1: pg_stats MCV.
    attname = f"(idx ->> '{key}'::text)"
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

    # Step 2: live COUNT fallback.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS c FROM object_state "
            "WHERE idx IS NOT NULL AND idx->>%s = %s",
            (key, value),
        )
        count = cur.fetchone()["c"]
        cur.execute(
            "SELECT reltuples::bigint AS t FROM pg_class WHERE relname = 'object_state'"
        )
        total = max(cur.fetchone()["t"], 1)
    sel = count / total
    _probe_cache[cache_key] = sel
    return sel


# Safe index name pattern
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


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
        # Dedicated KEYWORD fields (e.g. Subject) already have a plain GIN
        # index — the dedicated bundle emits an "already_covered" notice.
        # Strip them from the filter list before building the GIN bundle to
        # avoid duplicate suggestions.
        non_dedicated_fields = [
            ff for ff in filter_fields if ff[0] not in _DEDICATED_FIELDS
        ]
        bundle = _build_keyword_gin_bundle(
            non_dedicated_fields, partial_where_terms, existing_indexes
        )
        return [bundle] if bundle is not None else []

    if shape == "MIXED":
        bundle = _build_hybrid_bundle(
            filter_fields, sort_field, partial_where_terms, existing_indexes
        )
        return [bundle] if bundle is not None else []

    # TEXT_ONLY / UNKNOWN → silent skip.
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

        # Dedicated columns are handled elsewhere (they emit an
        # already_covered reason).  Non-KEYWORD dedicated fields do NOT
        # participate in the filter shape — drop here.  KEYWORD dedicated
        # fields (e.g. Subject) are kept so that a MIXED shape can be
        # detected when they appear alongside btree-eligible fields;
        # the KEYWORD_ONLY dispatch path strips them before building the
        # GIN bundle, while the MIXED path passes them through so a
        # partial GIN scoped by co-occurring equality filters — genuinely
        # more selective than the dedicated plain GIN — is suggested.
        if key in _DEDICATED_FIELDS and reg_lookup.get(key) != IndexType.KEYWORD:
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


def _extract_sort_field(params, registry):
    """Return ``(field_name, IndexType)`` for a composite-eligible sort
    column, or ``None``.

    Plone emits the sort key under various param names — plain
    ``sort_on`` for direct catalog searches, ``p_sort_on_1`` for
    restapi-generated queries, etc.  Substring matching on
    ``"sort_on"`` is the pragmatic fit.

    Only btree-composite-eligible types are returned — KEYWORD, TEXT,
    GOPIP, and DATE_RANGE cannot be trailing columns of a btree index.
    """
    if not params:
        return None

    sort_value = None
    for param_name, value in params.items():
        if "sort_on" in param_name:
            sort_value = value
            break
    if not sort_value:
        return None

    # Registry lookup.  items() returns name -> (IndexType, idx_key, source_attrs).
    for name, (idx_type, _idx_key, _source_attrs) in registry.items():
        if name == sort_value:
            if idx_type in _NON_COMPOSITE_TYPES:
                return None
            if idx_type in _SKIP_TYPES:
                return None
            return (name, idx_type)
    return None


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
        (name, idx_type)
        for (name, idx_type, _op, _val) in filter_fields
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
            if partial_where_terms
            else "plain GIN"
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

    if not members:
        return None

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


def _build_hybrid_bundle(
    filter_fields, sort_field, partial_where_terms, existing_indexes
):
    """Build a MIXED-shape Bundle — one btree composite + N partial GINs.

    The btree member handles the btree-eligible filter axes plus any
    sort covering (reuses T1 logic).  The GIN members handle the
    KEYWORD filters, each scoped by the same ``partial_where_terms``
    so the planner can BitmapAnd them.

    The btree member does NOT gain a partial WHERE — PR α skips
    T2 (partial btree) as YAGNI.

    Returns None when filter_fields contains neither a btree-eligible
    nor a KEYWORD entry.
    """
    btree_candidates = [
        (n, t, op, v)
        for (n, t, op, v) in filter_fields
        if t != IndexType.KEYWORD and t != IndexType.TEXT
    ]
    keyword_candidates = [
        (n, t, op, v) for (n, t, op, v) in filter_fields if t == IndexType.KEYWORD
    ]

    if not btree_candidates and not keyword_candidates:
        return None

    members = []

    btree_bundle = _build_btree_bundle(btree_candidates, sort_field, existing_indexes)
    if btree_bundle is not None:
        members.extend(btree_bundle.members)

    gin_bundle = _build_keyword_gin_bundle(
        keyword_candidates, partial_where_terms, existing_indexes
    )
    if gin_bundle is not None:
        members.extend(gin_bundle.members)

    if not members:
        return None

    all_field_names = [m.fields[0] for m in members]
    bundle_name = "hybrid-" + "-".join(all_field_names)
    rationale = (
        f"Hybrid bundle for MIXED filter shape: "
        f"btree covers {', '.join(f for (f, _, _, _) in btree_candidates)} "
        f"+ GIN covers {', '.join(f for (f, _, _, _) in keyword_candidates)}"
    )
    if partial_where_terms:
        rationale += f"; partial predicate scopes GIN by {len(partial_where_terms)} equality filter(s)"
    return Bundle(
        name=bundle_name,
        rationale=rationale,
        shape_classification="MIXED",
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


def _build_btree_bundle(filter_fields, sort_field, existing_indexes):
    """Build a single-member Bundle holding one btree composite index.

    Wraps the PR 2 btree-composite logic (selectivity ordering, 3-column
    cap including sort covering, dedupe when sort field is already a
    filter column) into the Bundle output shape.

    Returns None when filter_fields is empty.
    """
    if not filter_fields:
        return None

    btree_pairs = [(name, idx_type) for (name, idx_type, _op, _val) in filter_fields]

    btree_pairs = sorted(btree_pairs, key=lambda ft: _SELECTIVITY_ORDER.get(ft[1], 99))

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
    rationale = (
        f"Btree composite for filter shape BTREE_ONLY on {', '.join(field_names)}"
    )
    return Bundle(
        name=bundle_name,
        rationale=rationale,
        shape_classification="BTREE_ONLY",
        members=[member],
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
