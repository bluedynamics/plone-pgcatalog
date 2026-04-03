"""CatalogStateProcessor for zodb-pgjsonb integration.

Integrates with zodb-pgjsonb's state processor infrastructure to write
catalog index data as extra PG columns atomically alongside the object
state during tpc_vote.
"""

from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.pending import _MISSING
from plone.pgcatalog.pending import pop_all_partial_pending
from plone.pgcatalog.pending import pop_all_pending_moves
from plone.pgcatalog.pending import pop_pending
from plone.pgcatalog.schema import CATALOG_COLUMNS
from plone.pgcatalog.schema import CATALOG_FUNCTIONS
from plone.pgcatalog.schema import CATALOG_INDEXES
from plone.pgcatalog.schema import CATALOG_LANG_FUNCTION
from plone.pgcatalog.schema import RRULE_FUNCTIONS
from plone.pgcatalog.schema import SLOW_QUERY_TABLE
from plone.pgcatalog.schema import TEXT_EXTRACTION_QUEUE
from psycopg.types.json import Json
from zodb_pgjsonb import ExtraColumn

import json
import logging
import os


__all__ = ["ANNOTATION_KEY", "CatalogStateProcessor"]


log = logging.getLogger(__name__)

# ── Tika configuration (opt-in via env vars) ─────────────────────────

TIKA_URL = os.environ.get("PGCATALOG_TIKA_URL", "").strip()

_DEFAULT_CONTENT_TYPES = (
    "application/pdf,"
    "application/msword,"
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document,"
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
    "application/vnd.openxmlformats-officedocument.presentationml.presentation,"
    "application/vnd.oasis.opendocument.text,"
    "application/vnd.oasis.opendocument.spreadsheet,"
    "application/rtf,"
    "image/jpeg,image/png,image/tiff,image/webp,image/gif"
)

TIKA_CONTENT_TYPES = {
    ct.strip()
    for ct in os.environ.get(
        "PGCATALOG_TIKA_CONTENT_TYPES", _DEFAULT_CONTENT_TYPES
    ).split(",")
    if ct.strip()
}


def _should_extract(content_type):
    """Check if a content type should be sent to Tika for extraction."""
    if not content_type:
        return False
    return content_type in TIKA_CONTENT_TYPES


def _collect_ref_oids(state):
    """Extract integer zoids from all ``@ref`` markers in a JSON state.

    *state* may be a dict (already parsed) or a JSON string (from the
    ``decode_zodb_record_for_pg_json`` fast path).

    Returns a list of int zoids found in the state.  Handles both
    compact forms: ``{"@ref": "hex_oid"}`` and
    ``{"@ref": ["hex_oid", "mod.Cls"]}``.
    """
    if isinstance(state, str):
        try:
            state = json.loads(state)
        except (json.JSONDecodeError, TypeError):
            return []

    refs = []

    def _walk(obj):
        if isinstance(obj, dict):
            ref = obj.get("@ref")
            if ref is not None:
                hex_oid = ref[0] if isinstance(ref, list) else ref
                if isinstance(hex_oid, str) and len(hex_oid) == 16:
                    try:
                        refs.append(int(hex_oid, 16))
                    except ValueError:
                        pass
            else:
                for v in obj.values():
                    _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(state)
    return refs


# Annotation key set by catalog_object() on persistent objects.
# The processor pops it from the JSON state before writing to PG.
ANNOTATION_KEY = "_pgcatalog_pending"


class CatalogStateProcessor:
    """Extracts ``_pgcatalog_pending`` from object state -> extra PG columns.

    Works with zodb-pgjsonb's state processor infrastructure.
    When ``_pgcatalog_pending`` is a dict, catalog data is written.
    When it is ``None`` (sentinel), all catalog columns are NULLed (uncatalog).
    """

    def get_extra_columns(self):
        return [
            ExtraColumn("path", "%(path)s"),
            ExtraColumn("parent_path", "%(parent_path)s"),
            ExtraColumn("path_depth", "%(path_depth)s"),
            ExtraColumn("idx", "%(idx)s"),
            ExtraColumn("allowed_roles", "%(allowed_roles)s"),
            *get_backend().get_extra_columns(),
        ]

    def get_schema_sql(self):
        """Return DDL for catalog columns, functions, and indexes.

        Applied by PGJsonbStorage.register_state_processor() using
        the storage's own connection -- no REPEATABLE READ lock conflicts.
        Includes rrule_plpgsql functions for DateRecurringIndex support.
        Appends backend-specific DDL (e.g. BM25 column + index).
        When Tika is configured, also includes queue table + merge function.
        """
        backend = get_backend()
        base = (
            CATALOG_COLUMNS
            + CATALOG_FUNCTIONS
            + CATALOG_LANG_FUNCTION
            + CATALOG_INDEXES
            + RRULE_FUNCTIONS
            + backend.get_schema_sql()
        )
        base += SLOW_QUERY_TABLE
        if TIKA_URL:
            base += TEXT_EXTRACTION_QUEUE
            base += backend.get_extraction_update_sql()
        return base

    def __init__(self):
        # Accumulated per-transaction Tika extraction candidates.
        # Populated during process(), consumed during finalize().
        self._tika_candidates = []

    def process(self, zoid, class_mod, class_name, state):
        # Look up pending data from the thread-local store (set by
        # catalog_object / uncatalog_object via set_pending).
        pending = pop_pending(zoid)
        if pending is _MISSING:
            # Also check state dict for backward compat / direct use.
            # state may be a JSON string (from decode_zodb_record_for_pg_json)
            # or a dict (from decode_zodb_record_for_pg).
            state_dict = state
            if isinstance(state, str):
                try:
                    state_dict = json.loads(state)
                except (json.JSONDecodeError, TypeError):
                    return None
            if isinstance(state_dict, dict) and ANNOTATION_KEY in state_dict:
                pending = state_dict.pop(ANNOTATION_KEY)
            else:
                return None

        log.debug(
            "CatalogStateProcessor.process: zoid=%d class=%s.%s",
            zoid,
            class_mod,
            class_name,
        )

        if pending is None:
            # Uncatalog sentinel: NULL all catalog columns
            result = {
                "path": None,
                "parent_path": None,
                "path_depth": None,
                "idx": None,
                "searchable_text": None,
                "allowed_roles": None,
            }
            result.update(get_backend().uncatalog_extra())
            return result

        # Accumulate Tika extraction candidates
        if TIKA_URL:
            content_type = pending.get("content_type")
            if _should_extract(content_type):
                blob_refs = _collect_ref_oids(state)
                if blob_refs:
                    self._tika_candidates.append(
                        {
                            "zoid": zoid,
                            "content_type": content_type,
                            "blob_refs": blob_refs,
                        }
                    )

        # Normal catalog: return column values
        idx = pending.get("idx")
        # Extract allowedRolesAndUsers as a dedicated TEXT[] column
        allowed = idx.get("allowedRolesAndUsers") if idx else None
        result = {
            "path": pending.get("path"),
            "parent_path": idx.get("path_parent") if idx else None,
            "path_depth": idx.get("path_depth") if idx else None,
            "idx": Json(idx) if idx else None,
            "searchable_text": pending.get("searchable_text"),
            "allowed_roles": allowed if isinstance(allowed, list) else None,
        }
        result.update(get_backend().process_search_data(pending))
        return result

    def finalize(self, cursor):
        """Execute partial idx updates, bulk path moves, and enqueue Tika jobs.

        Called by zodb-pgjsonb after batch object writes, using the
        same cursor (same PG transaction).  Partial updates are
        registered by ``reindexObject(idxs=[...])`` for objects that
        don't need full ZODB serialization.

        Bulk path moves are registered by the move optimization wrappers
        (``plone.pgcatalog.move``) and executed here as single SQL UPDATEs
        per moved subtree.

        When Tika is configured, also checks which committed zoids have
        associated blobs and enqueues them for text extraction.
        """
        partial = pop_all_partial_pending()
        if partial:
            for zoid, idx_updates in partial.items():
                cursor.execute(
                    "UPDATE object_state SET "
                    "idx = COALESCE(idx, '{}'::jsonb) || %(patch)s::jsonb "
                    "WHERE zoid = %(zoid)s AND idx IS NOT NULL",
                    {"zoid": zoid, "patch": Json(idx_updates)},
                )

        # Execute bulk path moves (one SQL per moved subtree)
        moves = pop_all_pending_moves()
        for old_prefix, new_prefix, depth_delta in moves:
            cursor.execute(
                """
                UPDATE object_state SET
                    path = %(new)s || substring(path FROM length(%(old)s) + 1),
                    parent_path = %(new)s || substring(parent_path FROM length(%(old)s) + 1),
                    path_depth = path_depth + %(dd)s,
                    idx = idx || jsonb_build_object(
                        'path',
                        %(new)s || substring(idx->>'path' FROM length(%(old)s) + 1),
                        'path_parent',
                        %(new)s || substring(idx->>'path_parent' FROM length(%(old)s) + 1),
                        'path_depth',
                        (idx->>'path_depth')::int + %(dd)s
                    )
                WHERE path LIKE %(like)s
                  AND idx IS NOT NULL
                """,
                {
                    "old": old_prefix,
                    "new": new_prefix,
                    "dd": depth_delta,
                    "like": old_prefix + "/%",
                },
            )
            log.info(
                "Bulk path update: %s -> %s (%+d depth)",
                old_prefix,
                new_prefix,
                depth_delta,
            )

        # Enqueue Tika extraction jobs for blobs in this transaction
        if TIKA_URL and self._tika_candidates:
            self._enqueue_tika_jobs(cursor)

    def _enqueue_tika_jobs(self, cursor):
        """Enqueue text extraction jobs for blobs committed in this txn.

        Content objects (File/Image) and their Blob sub-objects have
        *different* ZODB oids.  ``process()`` extracts ``@ref`` oids
        from the content state; we look those up in ``blob_state`` to
        find the actual blob zoid + tid.  The queue stores both so the
        worker can fetch blob data (blob_zoid) and update
        searchable_text on the content (zoid).
        """
        candidates = self._tika_candidates
        self._tika_candidates = []

        # Collect all referenced oids across all candidates
        all_refs = set()
        for c in candidates:
            all_refs.update(c["blob_refs"])
        if not all_refs:
            return

        # Find which referenced oids actually have blobs (latest tid per oid)
        cursor.execute(
            "SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state "
            "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
            {"zoids": list(all_refs)},
        )
        blob_rows = {row[0]: row[1] for row in cursor.fetchall()}
        if not blob_rows:
            return

        for c in candidates:
            content_zoid = c["zoid"]
            content_type = c.get("content_type")
            for ref_zoid in c["blob_refs"]:
                if ref_zoid in blob_rows:
                    cursor.execute(
                        "INSERT INTO text_extraction_queue "
                        "  (zoid, blob_zoid, tid, content_type) "
                        "VALUES (%(zoid)s, %(blob_zoid)s, %(tid)s, %(ct)s) "
                        "ON CONFLICT (blob_zoid, tid) DO NOTHING",
                        {
                            "zoid": content_zoid,
                            "blob_zoid": ref_zoid,
                            "tid": blob_rows[ref_zoid],
                            "ct": content_type,
                        },
                    )
