"""Query translation: ZCatalog query dict → SQL WHERE + ORDER BY + LIMIT.

Translates Plone/ZCatalog-style query dicts into parameterized SQL queries
against the object_state table (with catalog columns from plone.pgcatalog).

All user-supplied values go through psycopg parameterized queries — never
string-formatted into SQL.  Index names are resolved dynamically via the
``IndexRegistry`` populated from ZCatalog's registered indexes.
"""

from datetime import UTC
from plone.pgcatalog.columns import ensure_date_param as _ensure_date_param
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from psycopg.types.json import Json
from typing import ClassVar

import logging
import re


log = logging.getLogger(__name__)

# Keys in the query dict that are NOT index names
_QUERY_META_KEYS = frozenset(
    {
        "sort_on",
        "sort_order",
        "sort_limit",
        "b_start",
        "b_size",
        "show_inactive",
    }
)

# Path validation pattern
_PATH_RE = re.compile(r"^/[a-zA-Z0-9._/@+\-]*$")

# Maximum number of paths in a single path query (DoS prevention)
_MAX_PATHS = 100


def _lookup_translator(name):
    """Look up an IPGIndexTranslator utility for a given index name.

    Returns the translator or None if not found.
    """
    try:
        from plone.pgcatalog.interfaces import IPGIndexTranslator
        from zope.component import queryUtility

        return queryUtility(IPGIndexTranslator, name=name)
    except Exception:
        return None


def build_query(query_dict):
    """Translate a ZCatalog query dict into SQL components.

    Args:
        query_dict: ZCatalog-style query dict (e.g. from catalog())

    Returns:
        dict with keys:
            where: str — full WHERE clause (without 'WHERE' keyword)
            params: dict — query parameters for psycopg
            order_by: str|None — ORDER BY expression (without 'ORDER BY')
            limit: int|None
            offset: int
    """
    builder = _QueryBuilder()
    builder.process(query_dict)
    return builder.result()


def apply_security_filters(query_dict, roles, show_inactive=False):
    """Inject security and effectiveRange filters into a query dict.

    This function is meant to be called by the catalog tool's searchResults()
    before passing the query to build_query().

    Args:
        query_dict: ZCatalog-style query dict (will NOT be mutated)
        roles: list of allowed roles/users (e.g. ["Anonymous", "user:admin"])
        show_inactive: if True, skip effectiveRange injection

    Returns:
        new query dict with security filters added
    """
    from datetime import datetime

    result = dict(query_dict)

    # Inject allowedRolesAndUsers (always, unless already present)
    if "allowedRolesAndUsers" not in result:
        result["allowedRolesAndUsers"] = {
            "query": list(roles),
            "operator": "or",
        }

    # Inject effectiveRange (unless show_inactive or already present)
    if (
        not show_inactive
        and "effectiveRange" not in result
        and not result.get("show_inactive")
    ):
        result["effectiveRange"] = datetime.now(UTC)

    # Remove show_inactive from the dict (it's a meta-key, not an index)
    result.pop("show_inactive", None)

    return result


def _execute_query(conn, query_dict, columns="zoid, path, idx, state"):
    """Execute a catalog query and return result rows.

    Internal convenience function for testing.  The ``columns`` parameter
    is interpolated into SQL, so callers must only pass trusted constants.

    Args:
        conn: psycopg connection (with dict_row factory)
        query_dict: ZCatalog-style query dict
        columns: SQL column list to SELECT (must be a trusted constant)

    Returns:
        list of row dicts
    """
    qr = build_query(query_dict)
    sql = f"SELECT {columns} FROM object_state WHERE {qr['where']}"
    if qr["order_by"]:
        sql += f" ORDER BY {qr['order_by']}"
    if qr["limit"]:
        sql += f" LIMIT {qr['limit']}"
    if qr["offset"]:
        sql += f" OFFSET {qr['offset']}"

    with conn.cursor() as cur:
        cur.execute(sql, qr["params"])
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Internal builder
# ---------------------------------------------------------------------------


class _QueryBuilder:
    def __init__(self):
        self.clauses = []
        self.params = {}
        self.order_by = None
        self.limit = None
        self.offset = 0
        self._counter = 0

    def _pname(self, prefix):
        """Generate a unique parameter name."""
        self._counter += 1
        return f"p_{prefix}_{self._counter}"

    def result(self):
        where = " AND ".join(self.clauses) if self.clauses else "idx IS NOT NULL"
        return {
            "where": where,
            "params": self.params,
            "order_by": self.order_by,
            "limit": self.limit,
            "offset": self.offset,
        }

    def process(self, query_dict):
        # Store full query dict for cross-index lookups (e.g. Language)
        self._query = query_dict

        # Always filter for cataloged objects
        self.clauses.append("idx IS NOT NULL")

        # Process each index query
        for key, value in query_dict.items():
            if key in _QUERY_META_KEYS:
                continue
            self._process_index(key, value)

        # Sort — normalize to lists (ZCatalog/Plone can pass either)
        sort_on = query_dict.get("sort_on")
        if sort_on:
            sort_order = query_dict.get("sort_order", "ascending")
            if isinstance(sort_on, str):
                sort_on = [sort_on]
            if isinstance(sort_order, str):
                sort_order = [sort_order]
            self._process_sort(sort_on, sort_order)

        # Auto-rank by relevance when SearchableText is queried without
        # explicit sort_on.  Title(A) > Description(B) > body(D).
        if self.order_by is None and hasattr(self, "_text_rank_expr"):
            from plone.pgcatalog.backends import get_backend

            direction = "ASC" if get_backend().rank_ascending else "DESC"
            self.order_by = f"{self._text_rank_expr} {direction}"

        # Limit/offset
        sort_limit = query_dict.get("sort_limit")
        b_start = query_dict.get("b_start", 0)
        b_size = query_dict.get("b_size")

        if sort_limit:
            self.limit = int(sort_limit)
        elif b_size:
            self.limit = int(b_size)
        if b_start:
            self.offset = int(b_start)

    # -- dispatch -----------------------------------------------------------

    _HANDLERS: ClassVar[dict[IndexType, str]] = {
        IndexType.FIELD: "_handle_field",
        IndexType.KEYWORD: "_handle_keyword",
        IndexType.DATE: "_handle_date",
        IndexType.BOOLEAN: "_handle_boolean",
        IndexType.DATE_RANGE: "_handle_date_range",
        IndexType.UUID: "_handle_uuid",
        IndexType.TEXT: "_handle_text",
        IndexType.PATH: "_handle_path",
        IndexType.GOPIP: "_handle_field",  # same as field
    }

    def _process_index(self, name, raw):
        registry = get_registry()
        entry = registry.get(name)
        if entry is None:
            # Fallback: look for an IPGIndexTranslator utility
            translator = _lookup_translator(name)
            if translator is not None:
                spec = _normalize_query(raw)
                sql_fragment, params = translator.query(name, raw, spec)
                self.clauses.append(sql_fragment)
                self.params.update(params)
                return
            # Fall back to simple JSONB field query for unregistered indexes
            # (e.g. Language, TranslationGroup from plone.app.multilingual).
            spec = _normalize_query(raw)
            self._handle_field(name, name, spec)
            return

        idx_type, idx_key, _source_attrs = entry
        spec = _normalize_query(raw)
        handler = getattr(self, self._HANDLERS[idx_type])
        handler(name, idx_key, spec)

    # -- FieldIndex / GopipIndex --------------------------------------------

    def _handle_field(self, name, idx_key, spec):
        query_val = spec.get("query")
        not_val = spec.get("not")
        range_spec = spec.get("range")

        if query_val is not None:
            if range_spec:
                self._field_range(idx_key, query_val, range_spec)
            elif isinstance(query_val, (list, tuple)):
                p = self._pname(name)
                self.clauses.append(f"idx->>'{idx_key}' = ANY(%({p})s)")
                self.params[p] = [str(v) for v in query_val]
            else:
                p = self._pname(name)
                self.clauses.append(f"idx @> %({p})s::jsonb")
                self.params[p] = Json({idx_key: query_val})

        if not_val is not None:
            if isinstance(not_val, (list, tuple)):
                p = self._pname(name + "_not")
                self.clauses.append(f"NOT (idx->>'{idx_key}' = ANY(%({p})s))")
                self.params[p] = [str(v) for v in not_val]
            else:
                p = self._pname(name + "_not")
                self.clauses.append(f"idx->>'{idx_key}' != %({p})s")
                self.params[p] = str(not_val)

    def _field_range(self, idx_key, value, range_spec):
        if range_spec in ("min:max", "minmax") and isinstance(value, (list, tuple)):
            p_min = self._pname(idx_key + "_min")
            p_max = self._pname(idx_key + "_max")
            self.clauses.append(
                f"(idx->>'{idx_key}' >= %({p_min})s"
                f" AND idx->>'{idx_key}' <= %({p_max})s)"
            )
            self.params[p_min] = str(value[0])
            self.params[p_max] = str(value[1])
        elif range_spec == "min":
            p = self._pname(idx_key)
            self.clauses.append(f"idx->>'{idx_key}' >= %({p})s")
            self.params[p] = str(value)
        elif range_spec == "max":
            p = self._pname(idx_key)
            self.clauses.append(f"idx->>'{idx_key}' <= %({p})s")
            self.params[p] = str(value)

    # -- KeywordIndex -------------------------------------------------------

    def _handle_keyword(self, name, idx_key, spec):
        query_val = spec.get("query")
        if query_val is None:
            return

        operator = spec.get("operator", "or")

        query_val = [query_val] if isinstance(query_val, str) else list(query_val)

        if operator == "and":
            # All values must be present → JSONB containment
            p = self._pname(name)
            self.clauses.append(f"idx @> %({p})s::jsonb")
            self.params[p] = Json({idx_key: query_val})
        else:
            # Any value present → ?| overlap
            p = self._pname(name)
            self.clauses.append(f"idx->'{idx_key}' ?| %({p})s")
            self.params[p] = query_val

    # -- DateIndex ----------------------------------------------------------

    def _handle_date(self, name, idx_key, spec):
        query_val = spec.get("query")
        range_spec = spec.get("range")

        if query_val is None:
            return

        if range_spec in ("min:max", "minmax") and isinstance(query_val, (list, tuple)):
            min_val = _ensure_date_param(query_val[0])
            max_val = _ensure_date_param(query_val[1])
            p_min = self._pname(idx_key + "_min")
            p_max = self._pname(idx_key + "_max")
            self.clauses.append(
                f"(pgcatalog_to_timestamptz(idx->>'{idx_key}') >= %({p_min})s"
                f" AND pgcatalog_to_timestamptz(idx->>'{idx_key}') <= %({p_max})s)"
            )
            self.params[p_min] = min_val
            self.params[p_max] = max_val
        elif range_spec == "min":
            val = _ensure_date_param(query_val)
            p = self._pname(idx_key)
            self.clauses.append(
                f"pgcatalog_to_timestamptz(idx->>'{idx_key}') >= %({p})s"
            )
            self.params[p] = val
        elif range_spec == "max":
            val = _ensure_date_param(query_val)
            p = self._pname(idx_key)
            self.clauses.append(
                f"pgcatalog_to_timestamptz(idx->>'{idx_key}') <= %({p})s"
            )
            self.params[p] = val
        else:
            # Exact date match
            val = _ensure_date_param(query_val)
            p = self._pname(idx_key)
            self.clauses.append(
                f"pgcatalog_to_timestamptz(idx->>'{idx_key}') = %({p})s"
            )
            self.params[p] = val

    # -- BooleanIndex -------------------------------------------------------

    def _handle_boolean(self, name, idx_key, spec):
        query_val = spec.get("query")
        if query_val is None:
            return
        p = self._pname(name)
        self.clauses.append(f"idx @> %({p})s::jsonb")
        self.params[p] = Json({idx_key: bool(query_val)})

    # -- DateRangeIndex (effectiveRange) ------------------------------------

    def _handle_date_range(self, name, idx_key, spec):
        query_val = spec.get("query")
        if query_val is None:
            return
        val = _ensure_date_param(query_val)
        p = self._pname("effrange")
        self.clauses.append(
            f"(pgcatalog_to_timestamptz(idx->>'effective') <= %({p})s"
            f" AND (pgcatalog_to_timestamptz(idx->>'expires') >= %({p})s"
            f" OR idx->>'expires' IS NULL))"
        )
        self.params[p] = val

    # -- UUIDIndex ----------------------------------------------------------

    def _handle_uuid(self, name, idx_key, spec):
        query_val = spec.get("query")
        if query_val is None:
            return
        p = self._pname(name)
        self.clauses.append(f"idx @> %({p})s::jsonb")
        self.params[p] = Json({idx_key: str(query_val)})

    # -- ZCTextIndex (SearchableText / Title / Description) -----------------

    def _handle_text(self, name, idx_key, spec):
        query_val = spec.get("query")
        if not query_val:
            return

        if idx_key is None:
            # SearchableText → delegate to active search backend.
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
        else:
            # Title / Description / addon ZCTextIndex →
            # tsvector expression on idx JSONB, 'simple' config.
            # Expression matches the GIN index created in schema.py /
            # _ensure_text_indexes() for index-backed queries.
            p = self._pname(name)
            self.clauses.append(
                f"to_tsvector('simple'::regconfig, "
                f"COALESCE(idx->>'{idx_key}', '')) "
                f"@@ plainto_tsquery('simple'::regconfig, %({p})s)"
            )
            self.params[p] = str(query_val)

    # -- ExtendedPathIndex --------------------------------------------------

    def _handle_path(self, name, idx_key, spec):
        query_val = spec.get("query")
        if query_val is None:
            return

        depth = spec.get("depth", -1)
        navtree = spec.get("navtree", False)
        navtree_start = spec.get("navtree_start", 0)

        paths = [query_val] if isinstance(query_val, str) else list(query_val)

        if len(paths) > _MAX_PATHS:
            raise ValueError(
                f"Too many paths in query ({len(paths)}), maximum is {_MAX_PATHS}"
            )

        for path in paths:
            _validate_path(path)

        # All path indexes (built-in "path" and additional like "tgpath")
        # store their data in idx JSONB and query via expression indexes.
        key = name if idx_key is None else idx_key
        expr_path = f"idx->>'{key}'"
        expr_parent = f"idx->>'{key}_parent'"
        expr_depth = f"(idx->>'{key}_depth')::integer"

        if navtree:
            self._path_navtree(expr_path, expr_parent, paths[0], depth, navtree_start)
        elif depth == 0:
            self._path_exact(expr_path, paths)
        elif depth == 1:
            self._path_children(expr_parent, paths)
        elif depth > 1:
            self._path_limited(expr_path, expr_depth, paths[0], depth)
        else:
            # depth=-1: full subtree (self + all descendants)
            self._path_subtree(expr_path, paths)

    def _path_subtree(self, expr_path, paths):
        """depth=-1: self + all descendants."""
        if len(paths) == 1:
            p = self._pname("path")
            p_like = self._pname("path_like")
            self.clauses.append(
                f"({expr_path} = %({p})s OR {expr_path} LIKE %({p_like})s)"
            )
            self.params[p] = paths[0]
            self.params[p_like] = paths[0].rstrip("/") + "/%"
        else:
            parts = []
            for i, path in enumerate(paths):
                p = self._pname(f"path_{i}")
                p_like = self._pname(f"path_like_{i}")
                parts.append(
                    f"({expr_path} = %({p})s OR {expr_path} LIKE %({p_like})s)"
                )
                self.params[p] = path
                self.params[p_like] = path.rstrip("/") + "/%"
            self.clauses.append(f"({' OR '.join(parts)})")

    def _path_exact(self, expr_path, paths):
        """depth=0: exact object(s)."""
        if len(paths) == 1:
            p = self._pname("path")
            self.clauses.append(f"{expr_path} = %({p})s")
            self.params[p] = paths[0]
        else:
            p = self._pname("paths")
            self.clauses.append(f"{expr_path} = ANY(%({p})s)")
            self.params[p] = paths

    def _path_children(self, expr_parent, paths):
        """depth=1: direct children only (NOT self)."""
        if len(paths) == 1:
            p = self._pname("parent")
            self.clauses.append(f"{expr_parent} = %({p})s")
            self.params[p] = paths[0]
        else:
            p = self._pname("parents")
            self.clauses.append(f"{expr_parent} = ANY(%({p})s)")
            self.params[p] = paths

    def _path_limited(self, expr_path, expr_depth, path, depth):
        """depth=N (N>1): subtree limited to N levels below path."""
        from plone.pgcatalog.columns import compute_path_info

        _, base_depth = compute_path_info(path)
        max_depth = base_depth + depth

        p_like = self._pname("path_like")
        p_depth = self._pname("max_depth")
        self.clauses.append(
            f"({expr_path} LIKE %({p_like})s AND {expr_depth} <= %({p_depth})s)"
        )
        self.params[p_like] = path.rstrip("/") + "/%"
        self.params[p_depth] = max_depth

    def _path_navtree(self, expr_path, expr_parent, path, depth, navtree_start):
        """navtree=True: navigation tree query."""
        parts = [p for p in path.split("/") if p]

        if depth == 0:
            # Breadcrumbs: exact objects at each path prefix
            prefixes = []
            for i in range(navtree_start, len(parts)):
                prefixes.append("/" + "/".join(parts[: i + 1]))
            if not prefixes:
                self.clauses.append("FALSE")
                return
            p = self._pname("breadcrumbs")
            self.clauses.append(f"{expr_path} = ANY(%({p})s)")
            self.params[p] = prefixes
        else:
            # depth=1 (default navtree): siblings at each level along path
            parent_paths = []
            for i in range(navtree_start, len(parts)):
                if i == 0:
                    parent_paths.append("/")
                else:
                    parent_paths.append("/" + "/".join(parts[:i]))
            if not parent_paths:
                self.clauses.append("FALSE")
                return
            p = self._pname("navtree_parents")
            self.clauses.append(f"{expr_parent} = ANY(%({p})s)")
            self.params[p] = parent_paths

    # -- sort ---------------------------------------------------------------

    def _process_sort(self, sort_on_list, sort_order_list):
        """Build ORDER BY from one or more sort keys.

        Args:
            sort_on_list: list of index names
            sort_order_list: list of order strings ("ascending"/"descending"/
                "reverse").  If shorter than sort_on_list, the last value
                is reused for remaining keys.
        """
        registry = get_registry()
        parts = []

        for i, sort_on in enumerate(sort_on_list):
            order_str = sort_order_list[min(i, len(sort_order_list) - 1)]
            direction = "DESC" if order_str in ("descending", "reverse") else "ASC"

            entry = registry.get(sort_on)
            if entry is None:
                translator = _lookup_translator(sort_on)
                if translator is not None:
                    expr = translator.sort(sort_on)
                    if expr is not None:
                        parts.append(f"{expr} {direction}")
                else:
                    log.warning("Unknown sort index %r — ignoring", sort_on)
                continue

            idx_type, idx_key, _source_attrs = entry
            if idx_key is None:
                if idx_type == IndexType.PATH:
                    idx_key = sort_on
                else:
                    continue

            if idx_type == IndexType.DATE:
                expr = f"pgcatalog_to_timestamptz(idx->>'{idx_key}')"
            elif idx_type == IndexType.GOPIP:
                expr = f"(idx->>'{idx_key}')::integer"
            elif idx_type == IndexType.BOOLEAN:
                expr = f"(idx->>'{idx_key}')::boolean"
            else:
                expr = f"idx->>'{idx_key}'"

            parts.append(f"{expr} {direction}")

        if parts:
            self.order_by = ", ".join(parts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_query(raw):
    """Normalize a ZCatalog query value to a spec dict.

    Simple values become {'query': value}.  Dicts pass through.
    """
    if isinstance(raw, dict):
        return raw
    return {"query": raw}


def _validate_path(path):
    """Validate a path string.  Raises ValueError on invalid input."""
    if not isinstance(path, str):
        raise ValueError(f"Path must be a string, got {type(path).__name__}")
    if not _PATH_RE.match(path):
        raise ValueError(f"Invalid path: {path!r}")
