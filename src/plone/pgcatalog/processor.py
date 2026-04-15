"""CatalogStateProcessor for zodb-pgjsonb integration.

Integrates with zodb-pgjsonb's state processor infrastructure to write
catalog index data as extra PG columns atomically alongside the object
state during tpc_vote.
"""

from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.columns import compute_path_info
from plone.pgcatalog.columns import extract_extra_idx_columns
from plone.pgcatalog.columns import get_extra_idx_columns
from plone.pgcatalog.pending import _MISSING
from plone.pgcatalog.pending import pop_all_partial_pending
from plone.pgcatalog.pending import pop_all_pending_moves
from plone.pgcatalog.pending import pop_pending
from plone.pgcatalog.schema import CATALOG_CHANGE_SEQ
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
        extra = [
            ExtraColumn(col.column_name, col.value_expr)
            for col in get_extra_idx_columns()
        ]
        return [
            ExtraColumn("path", "%(path)s"),
            ExtraColumn("parent_path", "%(parent_path)s"),
            ExtraColumn("path_depth", "%(path_depth)s"),
            ExtraColumn("idx", "%(idx)s"),
            *extra,
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
        base += CATALOG_CHANGE_SEQ
        base += SLOW_QUERY_TABLE
        if TIKA_URL:
            base += TEXT_EXTRACTION_QUEUE
            base += backend.get_extraction_update_sql()
        return base

    def __init__(self):
        # Accumulated per-transaction Tika extraction candidates.
        # Populated during process(), consumed during finalize().
        self._tika_candidates = []
        # Flag: did this transaction modify catalog data?
        # Set by process() when it returns column data.
        # Consumed by finalize() to increment pgcatalog_change_seq.
        self._catalog_changed = False

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

        self._catalog_changed = True

        if pending is None:
            # Uncatalog sentinel: NULL all catalog columns
            result = {
                "path": None,
                "parent_path": None,
                "path_depth": None,
                "idx": None,
                "searchable_text": None,
            }
            for col in get_extra_idx_columns():
                result[col.column_name] = None
            result.update(get_backend().uncatalog_extra())
            return result

        # Accumulate Tika extraction candidates.
        # MIME type comes from the idx JSONB (mime_type catalog index),
        # which is reliably extracted by the IndexRegistry (#90).
        if TIKA_URL:
            idx_data = pending.get("idx")
            content_type = idx_data.get("mime_type") if idx_data else None
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

        # Normal catalog: extract registered extra idx columns
        idx = pending.get("idx")
        extra_values = extract_extra_idx_columns(idx)

        # Path data lives in typed columns only.  parent_path and path_depth
        # are derived from the canonical `path` field — not from idx.
        # See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md (#132)
        path = pending.get("path")
        if path:
            parent_path, path_depth = compute_path_info(path)
        else:
            parent_path, path_depth = None, None

        result = {
            "path": path,
            "parent_path": parent_path,
            "path_depth": path_depth,
            "idx": Json(idx) if idx else None,
            "searchable_text": pending.get("searchable_text"),
            **extra_values,
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
                # Extract extra idx columns before merging into idx JSONB
                extra = extract_extra_idx_columns(idx_updates)
                extra_set = "".join(
                    f", {col} = %({col})s"
                    for col, val in extra.items()
                    if val is not None
                )
                extra_params = {
                    col: val for col, val in extra.items() if val is not None
                }
                cursor.execute(
                    "UPDATE object_state SET "
                    "idx = COALESCE(idx, '{}'::jsonb) || %(patch)s::jsonb"
                    f"{extra_set} "
                    "WHERE zoid = %(zoid)s AND idx IS NOT NULL",
                    {"zoid": zoid, "patch": Json(idx_updates), **extra_params},
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

        # Increment catalog change counter if any catalog data was modified.
        # Used by the query cache instead of MAX(tid) — avoids invalidation
        # on non-catalog ZODB writes (scales, sessions, etc.) (#94).
        catalog_changed = self._catalog_changed or partial or moves
        self._catalog_changed = False
        if catalog_changed:
            try:
                cursor.execute("SELECT nextval('pgcatalog_change_seq')")
            except Exception:
                pass  # sequence may not exist yet (first startup)

    def _enqueue_tika_jobs(self, cursor):
        """Enqueue text extraction jobs for blobs committed in this txn.

        Content objects (File/Image) reference blobs either directly
        (legacy/Archetypes: content state carries a ``ZODB.blob.Blob``
        ``@ref``) or via a wrapper (Dexterity NamedBlobFile/Image:
        content state ``@ref`` points at the wrapper, whose own state
        carries the ``ZODB.blob.Blob`` ``@ref``).

        Resolution is two-step:
        1. Look up all candidate refs in ``blob_state``.
        2. For refs with no blob row, fetch their ``object_state`` row
           (the wrapper), extract inner ``@ref`` OIDs, look those up in
           ``blob_state``.

        The queue row stores the *inner* Blob OID as ``blob_zoid`` —
        the worker fetches blob data from ``blob_state`` using it.
        """
        candidates = self._tika_candidates
        self._tika_candidates = []

        # Collect all referenced oids across all candidates
        all_refs = set()
        for c in candidates:
            all_refs.update(c["blob_refs"])
        if not all_refs:
            return

        # Step 1: direct lookup in blob_state
        cursor.execute(
            "SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state "
            "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
            {"zoids": list(all_refs)},
        )
        blob_rows = {row["zoid"]: row["tid"] for row in cursor.fetchall()}

        # Step 2: resolve wrapper refs via object_state second hop
        # (Dexterity NamedBlobFile/Image: content -> wrapper -> blob)
        wrapper_to_inner = {}  # wrapper_oid -> list[inner_oid]
        unresolved = all_refs - set(blob_rows)
        if unresolved:
            cursor.execute(
                "SELECT DISTINCT ON (zoid) zoid, state FROM object_state "
                "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
                {"zoids": list(unresolved)},
            )
            inner_refs = set()
            for row in cursor.fetchall():
                wrapper_oid, wrapper_state = row["zoid"], row["state"]
                inner = _collect_ref_oids(wrapper_state)
                if inner:
                    wrapper_to_inner[wrapper_oid] = inner
                    inner_refs.update(inner)

            if inner_refs:
                cursor.execute(
                    "SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state "
                    "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
                    {"zoids": list(inner_refs)},
                )
                for row in cursor.fetchall():
                    blob_rows[row["zoid"]] = row["tid"]

        if not blob_rows:
            return

        for c in candidates:
            content_zoid = c["zoid"]
            content_type = c.get("content_type")
            for ref_zoid in c["blob_refs"]:
                if ref_zoid in blob_rows:
                    # Direct hit: content ref is a Blob OID
                    self._insert_queue_row(
                        cursor,
                        content_zoid,
                        ref_zoid,
                        blob_rows[ref_zoid],
                        content_type,
                    )
                elif ref_zoid in wrapper_to_inner:
                    # Wrapper hit: content ref is a NamedBlob* wrapper;
                    # enqueue for each resolvable inner Blob OID.
                    for inner_zoid in wrapper_to_inner[ref_zoid]:
                        if inner_zoid in blob_rows:
                            self._insert_queue_row(
                                cursor,
                                content_zoid,
                                inner_zoid,
                                blob_rows[inner_zoid],
                                content_type,
                            )

    def _insert_queue_row(self, cursor, zoid, blob_zoid, tid, content_type):
        cursor.execute(
            "INSERT INTO text_extraction_queue "
            "  (zoid, blob_zoid, tid, content_type) "
            "VALUES (%(zoid)s, %(blob_zoid)s, %(tid)s, %(ct)s) "
            "ON CONFLICT (blob_zoid, tid) DO NOTHING",
            {
                "zoid": zoid,
                "blob_zoid": blob_zoid,
                "tid": tid,
                "ct": content_type,
            },
        )
