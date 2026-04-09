"""Write path: catalog/uncatalog/reindex operations on object_state.

These functions write catalog data (idx JSONB, path, searchable_text) to
the object_state table in PostgreSQL.  They operate at the SQL level and
are independent of the Plone CatalogTool — the CatalogTool subclass calls
these after extracting values via plone.indexer.
"""

from plone.pgcatalog.columns import compute_path_info
from plone.pgcatalog.columns import extract_extra_idx_columns
from plone.pgcatalog.columns import get_extra_idx_columns
from psycopg.types.json import Json


# Sentinel for detecting "not provided" vs None
_SENTINEL = object()

# Weighted tsvector SQL template for field-boosted relevance ranking.
# Title gets weight A (highest), Description weight B, body text weight D.
# {idx_expr} is the SQL expression yielding the idx JSONB value.
_WEIGHTED_TSVECTOR = (
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE({idx_expr}->>'Title', '')), 'A') || "
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE({idx_expr}->>'Description', '')), 'B') || "
    "setweight(to_tsvector({lang_expr}::regconfig, {text_expr}), 'D')"
)


def catalog_object(conn, zoid, path, idx, searchable_text=None, language="simple"):
    """Write full catalog data for an object.

    Args:
        conn: psycopg connection
        zoid: integer object id (must already exist in object_state)
        path: physical path string, e.g. "/plone/folder/doc"
        idx: dict of computed index/metadata values for idx JSONB
        searchable_text: plain text for full-text search (optional)
        language: PostgreSQL text search config name (default "simple")
    """
    parent_path, path_depth = compute_path_info(path)

    # Store path data in idx JSONB for unified path queries
    idx.setdefault("path", path)
    idx.setdefault("path_parent", parent_path)
    idx.setdefault("path_depth", path_depth)

    # Extract registered extra idx columns (pops from idx → dedicated columns)
    extra = extract_extra_idx_columns(idx)
    extra_set_clauses = "".join(
        f",\n                {col} = %({col})s" for col in extra
    )

    params = {
        "zoid": zoid,
        "path": path,
        "parent_path": parent_path,
        "path_depth": path_depth,
        "idx": Json(idx),
        **extra,
    }

    if searchable_text is not None:
        tsvector_sql = _WEIGHTED_TSVECTOR.format(
            idx_expr="%(idx)s::jsonb",
            lang_expr="%(lang)s",
            text_expr="%(text)s",
        )
        params["text"] = searchable_text
        params["lang"] = language
        conn.execute(
            f"""
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s,
                searchable_text = {tsvector_sql}{extra_set_clauses}
            WHERE zoid = %(zoid)s
            """,
            params,
        )
    else:
        conn.execute(
            f"""
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s,
                searchable_text = NULL{extra_set_clauses}
            WHERE zoid = %(zoid)s
            """,
            params,
        )


def uncatalog_object(conn, zoid):
    """Clear all catalog data for an object.

    Sets path, parent_path, path_depth, idx, searchable_text, and all
    extra idx columns to NULL.
    The base object_state row (zoid, tid, state, etc.) is preserved.

    Args:
        conn: psycopg connection
        zoid: integer object id
    """
    from plone.pgcatalog.backends import get_backend

    extra_nulls = get_backend().uncatalog_extra()
    extra_sql = "".join(f",\n            {col} = NULL" for col in extra_nulls)

    # Also NULL all extra idx columns
    for col in get_extra_idx_columns():
        extra_sql += f",\n            {col.column_name} = NULL"

    conn.execute(
        f"""
        UPDATE object_state SET
            path = NULL,
            parent_path = NULL,
            path_depth = NULL,
            idx = NULL,
            searchable_text = NULL{extra_sql}
        WHERE zoid = %(zoid)s
        """,
        {"zoid": zoid},
    )


def reindex_object(
    conn, zoid, idx_updates, searchable_text=_SENTINEL, language="simple"
):
    """Update specific idx keys and/or searchable_text for an object.

    Merges idx_updates into the existing idx JSONB (||).  Keys present in
    idx_updates overwrite existing values; keys not mentioned are preserved.

    If idx_updates contains registered extra idx column keys, those are
    extracted and written to their dedicated columns as well.

    Args:
        conn: psycopg connection
        zoid: integer object id
        idx_updates: dict of idx keys to update (merged into existing idx)
        searchable_text: if provided, update the tsvector; if omitted, leave unchanged
        language: PostgreSQL text search config name (default "simple")
    """
    # Extract any extra idx columns from updates
    extra = extract_extra_idx_columns(idx_updates)
    extra_set_clauses = "".join(
        f",\n                {col} = %({col})s"
        for col, val in extra.items()
        if val is not None
    )
    extra_params = {col: val for col, val in extra.items() if val is not None}

    if searchable_text is _SENTINEL:
        conn.execute(
            f"""
            UPDATE object_state SET
                idx = COALESCE(idx, '{{}}'::jsonb) || %(updates)s::jsonb{extra_set_clauses}
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "updates": Json(idx_updates),
                **extra_params,
            },
        )
    elif searchable_text is not None:
        merged_idx = "COALESCE(idx, '{}'::jsonb) || %(updates)s::jsonb"
        tsvector_sql = _WEIGHTED_TSVECTOR.format(
            idx_expr=f"({merged_idx})",
            lang_expr="%(lang)s",
            text_expr="%(text)s",
        )
        conn.execute(
            f"""
            UPDATE object_state SET
                idx = {merged_idx},
                searchable_text = {tsvector_sql}{extra_set_clauses}
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "updates": Json(idx_updates),
                "text": searchable_text,
                "lang": language,
                **extra_params,
            },
        )
    else:
        conn.execute(
            f"""
            UPDATE object_state SET
                idx = COALESCE(idx, '{{}}'::jsonb) || %(updates)s::jsonb,
                searchable_text = NULL{extra_set_clauses}
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "updates": Json(idx_updates),
                **extra_params,
            },
        )
