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
from psycopg.types.json import Json
from zodb_pgjsonb import ExtraColumn

import logging


__all__ = ["ANNOTATION_KEY", "CatalogStateProcessor"]


log = logging.getLogger(__name__)


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
        """
        return (
            CATALOG_COLUMNS
            + CATALOG_FUNCTIONS
            + CATALOG_LANG_FUNCTION
            + CATALOG_INDEXES
            + RRULE_FUNCTIONS
            + get_backend().get_schema_sql()
        )

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
        """Execute partial idx updates via JSONB merge.

        Called by zodb-pgjsonb after batch object writes, using the
        same cursor (same PG transaction).  Partial updates are
        registered by ``reindexObject(idxs=[...])`` for objects that
        don't need full ZODB serialization.
        """
        partial = pop_all_partial_pending()
        if not partial:
            return

        for zoid, idx_updates in partial.items():
            cursor.execute(
                "UPDATE object_state SET "
                "idx = COALESCE(idx, '{}'::jsonb) || %(patch)s::jsonb "
                "WHERE zoid = %(zoid)s AND idx IS NOT NULL",
                {"zoid": zoid, "patch": Json(idx_updates)},
            )
