"""CatalogStateProcessor for zodb-pgjsonb integration.

Integrates with zodb-pgjsonb's state processor infrastructure to write
catalog index data as extra PG columns atomically alongside the object
state during tpc_vote.
"""

from plone.pgcatalog.backends import get_backend
from plone.pgcatalog.pending import _MISSING
from plone.pgcatalog.pending import pop_all_partial_pending
from plone.pgcatalog.pending import pop_pending
from plone.pgcatalog.schema import CATALOG_COLUMNS
from plone.pgcatalog.schema import CATALOG_FUNCTIONS
from plone.pgcatalog.schema import CATALOG_INDEXES
from plone.pgcatalog.schema import CATALOG_LANG_FUNCTION
from plone.pgcatalog.schema import RRULE_FUNCTIONS
from plone.pgcatalog.schema import TEXT_EXTRACTION_QUEUE
from psycopg.types.json import Json
from zodb_pgjsonb import ExtraColumn

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
            ExtraColumn("idx", "%(idx)s"),
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
            # Also check state dict for backward compat / direct use
            if isinstance(state, dict) and ANNOTATION_KEY in state:
                pending = state.pop(ANNOTATION_KEY)
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
                "idx": None,
                "searchable_text": None,
            }
            result.update(get_backend().uncatalog_extra())
            return result

        # Accumulate Tika extraction candidates
        if TIKA_URL:
            content_type = pending.get("content_type")
            if _should_extract(content_type):
                self._tika_candidates.append(
                    {"zoid": zoid, "content_type": content_type}
                )

        # Normal catalog: return column values
        idx = pending.get("idx")
        result = {
            "path": pending.get("path"),
            "idx": Json(idx) if idx else None,
            "searchable_text": pending.get("searchable_text"),
        }
        result.update(get_backend().process_search_data(pending))
        return result

    def finalize(self, cursor):
        """Execute partial idx updates and enqueue Tika extraction jobs.

        Called by zodb-pgjsonb after batch object writes, using the
        same cursor (same PG transaction).  Partial updates are
        registered by ``reindexObject(idxs=[...])`` for objects that
        don't need full ZODB serialization.

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

        # Enqueue Tika extraction jobs for blobs in this transaction
        if TIKA_URL and self._tika_candidates:
            self._enqueue_tika_jobs(cursor)

    def _enqueue_tika_jobs(self, cursor):
        """Enqueue text extraction jobs for blobs committed in this txn."""
        candidates = self._tika_candidates
        self._tika_candidates = []

        zoids = [c["zoid"] for c in candidates]
        # Find which zoids actually have blobs (latest tid per zoid)
        cursor.execute(
            "SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state "
            "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
            {"zoids": zoids},
        )
        blob_rows = {row[0]: row[1] for row in cursor.fetchall()}
        if not blob_rows:
            return

        ct_by_zoid = {c["zoid"]: c.get("content_type") for c in candidates}
        for zoid, tid in blob_rows.items():
            cursor.execute(
                "INSERT INTO text_extraction_queue (zoid, tid, content_type) "
                "VALUES (%(zoid)s, %(tid)s, %(ct)s) "
                "ON CONFLICT (zoid, tid) DO NOTHING",
                {"zoid": zoid, "tid": tid, "ct": ct_by_zoid.get(zoid)},
            )
