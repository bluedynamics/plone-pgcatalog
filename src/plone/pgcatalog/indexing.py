"""Write path: catalog/uncatalog/reindex operations on object_state.

These functions write catalog data (idx JSONB, path, searchable_text) to
the object_state table in PostgreSQL.  They operate at the SQL level and
are independent of the Plone CatalogTool — the CatalogTool subclass calls
these after extracting values via plone.indexer.
"""

from plone.pgcatalog.columns import compute_path_info
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

    if searchable_text is not None:
        tsvector_sql = _WEIGHTED_TSVECTOR.format(
            idx_expr="%(idx)s::jsonb",
            lang_expr="%(lang)s",
            text_expr="%(text)s",
        )
        conn.execute(
            f"""
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s,
                searchable_text = {tsvector_sql}
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "path": path,
                "parent_path": parent_path,
                "path_depth": path_depth,
                "idx": Json(idx),
                "text": searchable_text,
                "lang": language,
            },
        )
    else:
        conn.execute(
            """
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s,
                searchable_text = NULL
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "path": path,
                "parent_path": parent_path,
                "path_depth": path_depth,
                "idx": Json(idx),
            },
        )


def uncatalog_object(conn, zoid):
    """Clear all catalog data for an object.

    Sets path, parent_path, path_depth, idx, and searchable_text to NULL.
    The base object_state row (zoid, tid, state, etc.) is preserved.

    Args:
        conn: psycopg connection
        zoid: integer object id
    """
    conn.execute(
        """
        UPDATE object_state SET
            path = NULL,
            parent_path = NULL,
            path_depth = NULL,
            idx = NULL,
            searchable_text = NULL
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

    Args:
        conn: psycopg connection
        zoid: integer object id
        idx_updates: dict of idx keys to update (merged into existing idx)
        searchable_text: if provided, update the tsvector; if omitted, leave unchanged
        language: PostgreSQL text search config name (default "simple")
    """
    if searchable_text is _SENTINEL:
        conn.execute(
            """
            UPDATE object_state SET
                idx = COALESCE(idx, '{}'::jsonb) || %(updates)s::jsonb
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "updates": Json(idx_updates),
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
                searchable_text = {tsvector_sql}
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "updates": Json(idx_updates),
                "text": searchable_text,
                "lang": language,
            },
        )
    else:
        conn.execute(
            """
            UPDATE object_state SET
                idx = COALESCE(idx, '{}'::jsonb) || %(updates)s::jsonb,
                searchable_text = NULL
            WHERE zoid = %(zoid)s
            """,
            {
                "zoid": zoid,
                "updates": Json(idx_updates),
            },
        )
