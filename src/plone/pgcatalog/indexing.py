"""Write path: catalog/uncatalog/reindex operations on object_state.

These functions write catalog data (idx JSONB, path, searchable_text) to
the object_state table in PostgreSQL.  They operate at the SQL level and
are independent of the Plone CatalogTool — the CatalogTool subclass calls
these after extracting values via plone.indexer.
"""

from psycopg.types.json import Json

from plone.pgcatalog.columns import compute_path_info


# Sentinel for detecting "not provided" vs None
_SENTINEL = object()


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

    if searchable_text is not None:
        conn.execute(
            """
            UPDATE object_state SET
                path = %(path)s,
                parent_path = %(parent_path)s,
                path_depth = %(path_depth)s,
                idx = %(idx)s,
                searchable_text = to_tsvector(%(lang)s::regconfig, %(text)s)
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


def reindex_object(conn, zoid, idx_updates, searchable_text=_SENTINEL, language="simple"):
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
        conn.execute(
            """
            UPDATE object_state SET
                idx = COALESCE(idx, '{}'::jsonb) || %(updates)s::jsonb,
                searchable_text = to_tsvector(%(lang)s::regconfig, %(text)s)
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
