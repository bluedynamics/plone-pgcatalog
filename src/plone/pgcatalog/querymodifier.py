"""IQueryModifier that drops malformed rows before ``parseFormquery``.

``plone.app.querystring.queryparser.parseFormquery`` does
``row.get("i", None)`` and then sets ``query[row.index] = value`` — so a
row without an ``i`` field produces a ``parsedquery[None] = ...`` entry.
The downstream ``catalog(**parsedquery)`` in
``plone.app.querystring.querybuilder._makequery`` then fails at the
Python level with ``TypeError: keywords must be strings`` before any
catalog code runs.

This is an upstream Plone gotcha, not a pgcatalog bug — but under
pgcatalog it became the visible failure mode for editors trying to
repair their own Subject/tag fields via the Collection edit widget
(stored-query corruption from the earlier compat issues).  Filtering
incomplete rows here unblocks the UI so content can be fixed
editorially.

Registered as an ``IQueryModifier`` utility with a stable name so it
runs deterministically in the modifier chain (``_makequery`` sorts by
name).
"""

from plone.app.querystring.interfaces import IQueryModifier
from zope.interface import implementer

import logging


log = logging.getLogger(__name__)


@implementer(IQueryModifier)
class SanitizeRowsModifier:
    """Drop query rows whose ``i`` (index name) is missing or non-string.

    Operates on the raw form-query (list of row dicts or Zope
    ``HTTPRequest.record`` instances) that ``_makequery`` hands to its
    modifier chain before ``parseFormquery``.  Non-list inputs are
    returned untouched.
    """

    def __call__(self, query):
        if not isinstance(query, (list, tuple)):
            return query

        clean = []
        dropped = 0
        for row in query:
            index = row.get("i") if hasattr(row, "get") else None
            if isinstance(index, str) and index:
                clean.append(row)
            else:
                dropped += 1

        if dropped:
            log.warning(
                "SanitizeRowsModifier: dropped %d query row(s) without valid "
                "'i' field (upstream plone.app.querystring gotcha — would "
                "have produced parsedquery[None]=...)",
                dropped,
            )
        return clean if isinstance(query, list) else tuple(clean)
