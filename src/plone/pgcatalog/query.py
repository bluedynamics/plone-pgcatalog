"""Query translation: ZCatalog query dict → SQL WHERE + ORDER BY + LIMIT.

Translates Plone/ZCatalog-style query dicts into parameterized SQL queries
against the object_state table (with catalog columns from plone.pgcatalog).

All user-supplied values go through psycopg parameterized queries — never
string-formatted into SQL.  Index key names are whitelisted via KNOWN_INDEXES.
"""

import logging
import re
from datetime import date
from datetime import datetime

from psycopg.types.json import Json

from plone.pgcatalog.columns import KNOWN_INDEXES
from plone.pgcatalog.columns import IndexType

log = logging.getLogger(__name__)

# Keys in the query dict that are NOT index names
_QUERY_META_KEYS = frozenset({
    "sort_on",
    "sort_order",
    "sort_limit",
    "b_start",
    "b_size",
    "show_inactive",
})

# Path validation pattern
_PATH_RE = re.compile(r"^/[a-zA-Z0-9._/@+\-]*$")


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
    from datetime import timezone

    result = dict(query_dict)

    # Inject allowedRolesAndUsers (always, unless already present)
    if "allowedRolesAndUsers" not in result:
        result["allowedRolesAndUsers"] = {
            "query": list(roles),
            "operator": "or",
        }

    # Inject effectiveRange (unless show_inactive or already present)
    if not show_inactive and "effectiveRange" not in result:
        if not result.get("show_inactive"):
            result["effectiveRange"] = datetime.now(timezone.utc)

    # Remove show_inactive from the dict (it's a meta-key, not an index)
    result.pop("show_inactive", None)

    return result


def execute_query(conn, query_dict, columns="zoid, path, idx, state"):
    """Execute a catalog query and return result rows.

    Convenience function that builds SQL from a query dict and executes it.

    Args:
        conn: psycopg connection (with dict_row factory)
        query_dict: ZCatalog-style query dict
        columns: SQL column list to SELECT

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
        # Always filter for cataloged objects
        self.clauses.append("idx IS NOT NULL")

        # Process each index query
        for key, value in query_dict.items():
            if key in _QUERY_META_KEYS:
                continue
            self._process_index(key, value)

        # Sort
        sort_on = query_dict.get("sort_on")
        if sort_on:
            sort_order = query_dict.get("sort_order", "ascending")
            self._process_sort(sort_on, sort_order)

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

    _HANDLERS = {
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
        if name not in KNOWN_INDEXES:
            log.warning("Unknown catalog index %r — skipping", name)
            return

        idx_type, idx_key = KNOWN_INDEXES[name]
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

        if isinstance(query_val, str):
            query_val = [query_val]
        else:
            query_val = list(query_val)

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

        if range_spec in ("min:max", "minmax") and isinstance(
            query_val, (list, tuple)
        ):
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
            # SearchableText → tsvector full-text search
            p_text = self._pname("text")
            p_lang = self._pname("lang")
            self.clauses.append(
                f"searchable_text @@ plainto_tsquery(%({p_lang})s::regconfig, %({p_text})s)"
            )
            self.params[p_text] = str(query_val)
            self.params[p_lang] = "simple"
        else:
            # Title / Description — treat as field exact match
            p = self._pname(name)
            self.clauses.append(f"idx @> %({p})s::jsonb")
            self.params[p] = Json({idx_key: query_val})

    # -- ExtendedPathIndex --------------------------------------------------

    def _handle_path(self, name, idx_key, spec):
        query_val = spec.get("query")
        if query_val is None:
            return

        depth = spec.get("depth", -1)
        navtree = spec.get("navtree", False)
        navtree_start = spec.get("navtree_start", 0)

        if isinstance(query_val, str):
            paths = [query_val]
        else:
            paths = list(query_val)

        for path in paths:
            _validate_path(path)

        if navtree:
            self._path_navtree(paths[0], depth, navtree_start)
        elif depth == 0:
            self._path_exact(paths)
        elif depth == 1:
            self._path_children(paths)
        elif depth > 1:
            self._path_limited(paths[0], depth)
        else:
            # depth=-1: full subtree (self + all descendants)
            self._path_subtree(paths)

    def _path_subtree(self, paths):
        """depth=-1: self + all descendants."""
        if len(paths) == 1:
            p = self._pname("path")
            p_like = self._pname("path_like")
            self.clauses.append(f"(path = %({p})s OR path LIKE %({p_like})s)")
            self.params[p] = paths[0]
            self.params[p_like] = paths[0].rstrip("/") + "/%"
        else:
            parts = []
            for i, path in enumerate(paths):
                p = self._pname(f"path_{i}")
                p_like = self._pname(f"path_like_{i}")
                parts.append(f"(path = %({p})s OR path LIKE %({p_like})s)")
                self.params[p] = path
                self.params[p_like] = path.rstrip("/") + "/%"
            self.clauses.append(f"({' OR '.join(parts)})")

    def _path_exact(self, paths):
        """depth=0: exact object(s)."""
        if len(paths) == 1:
            p = self._pname("path")
            self.clauses.append(f"path = %({p})s")
            self.params[p] = paths[0]
        else:
            p = self._pname("paths")
            self.clauses.append(f"path = ANY(%({p})s)")
            self.params[p] = paths

    def _path_children(self, paths):
        """depth=1: direct children only (NOT self)."""
        if len(paths) == 1:
            p = self._pname("parent")
            self.clauses.append(f"parent_path = %({p})s")
            self.params[p] = paths[0]
        else:
            p = self._pname("parents")
            self.clauses.append(f"parent_path = ANY(%({p})s)")
            self.params[p] = paths

    def _path_limited(self, path, depth):
        """depth=N (N>1): subtree limited to N levels below path."""
        from plone.pgcatalog.columns import compute_path_info

        _, base_depth = compute_path_info(path)
        max_depth = base_depth + depth

        p_like = self._pname("path_like")
        p_depth = self._pname("max_depth")
        self.clauses.append(
            f"(path LIKE %({p_like})s AND path_depth <= %({p_depth})s)"
        )
        self.params[p_like] = path.rstrip("/") + "/%"
        self.params[p_depth] = max_depth

    def _path_navtree(self, path, depth, navtree_start):
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
            self.clauses.append(f"path = ANY(%({p})s)")
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
            self.clauses.append(f"parent_path = ANY(%({p})s)")
            self.params[p] = parent_paths

    # -- sort ---------------------------------------------------------------

    def _process_sort(self, sort_on, sort_order):
        if sort_on not in KNOWN_INDEXES:
            log.warning("Unknown sort index %r — ignoring", sort_on)
            return

        idx_type, idx_key = KNOWN_INDEXES[sort_on]
        if idx_key is None:
            return  # Can't sort on composite/special indexes

        direction = "DESC" if sort_order in ("descending", "reverse") else "ASC"

        if idx_type == IndexType.DATE:
            expr = f"pgcatalog_to_timestamptz(idx->>'{idx_key}')"
        elif idx_type == IndexType.GOPIP:
            expr = f"(idx->>'{idx_key}')::integer"
        elif idx_type == IndexType.BOOLEAN:
            expr = f"(idx->>'{idx_key}')::boolean"
        else:
            expr = f"idx->>'{idx_key}'"

        self.order_by = f"{expr} {direction}"


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


def _ensure_date_param(value):
    """Convert a date-like value to something psycopg can bind as timestamptz."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    # Zope DateTime (duck-typed)
    if hasattr(value, "asdatetime"):
        return value.asdatetime()
    if hasattr(value, "ISO8601"):
        return value.ISO8601()
    return str(value)


def _validate_path(path):
    """Validate a path string.  Raises ValueError on invalid input."""
    if not isinstance(path, str):
        raise ValueError(f"Path must be a string, got {type(path).__name__}")
    if not _PATH_RE.match(path):
        raise ValueError(f"Invalid path: {path!r}")
