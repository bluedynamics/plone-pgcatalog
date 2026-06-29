"""Conditional SearchableText indexers that skip portal_transforms for Tika.

When ``PGCATALOG_TIKA_URL`` is configured, blob text extraction is handled
asynchronously by the Tika worker (merged into ``searchable_text`` as tsvector
weight 'C').  The synchronous ``portal_transforms`` pipeline (pdftotext, wv, …)
is then skipped to avoid duplicate work and ``BROKEN transform`` log spam when
the transform binaries are deliberately absent.

Two indexing paths read blobs and are overridden here:

* ``SearchableText_file`` (stock ``plone.app.contenttypes``, bound to ``IFile``)
  — replaced by :func:`SearchableText_file_override` (#41).
* the per-field ``NamedfileFieldConverter`` used by
  ``plone.app.dexterity.textindexer``'s dynamic SearchableText indexer for
  fields marked *searchable* — replaced by
  :class:`TikaAwareNamedfileFieldConverter` (#114).

When ``PGCATALOG_TIKA_URL`` is NOT set, both delegate to the original
implementation so the full transform pipeline runs as before.
"""

from plone.app.contenttypes.indexers import SearchableText
from plone.app.contenttypes.indexers import (
    SearchableText_file as _original_searchable_text_file_factory,
)
from plone.app.contenttypes.interfaces import IFile
from plone.indexer import indexer

import os


# Unwrap the DelegatingIndexerFactory to get the plain function.
_original_searchable_text_file = _original_searchable_text_file_factory.callable


def SearchableText_file_override(obj):
    """SearchableText for IFile — skips transforms when Tika is active."""
    tika_url = os.environ.get("PGCATALOG_TIKA_URL", "").strip()
    if tika_url:
        return SearchableText(obj)
    return _original_searchable_text_file(obj)


indexer_SearchableText_file_override = indexer(IFile)(SearchableText_file_override)


# ── textindexer NamedBlobFile converter override (#114) ─────────────────────
# plone.app.dexterity.textindexer is an optional path; import defensively so
# plone.pgcatalog stays importable without it.  The overrides.zcml registration
# is gated on ``installed plone.app.dexterity`` so the adapter is only wired up
# where the stock converter exists.
try:
    from plone.app.dexterity.textindexer.converters import NamedfileFieldConverter
    from plone.base.utils import safe_text
except ImportError:  # pragma: no cover - minimal installs without textindexer
    NamedfileFieldConverter = None

if NamedfileFieldConverter is not None:

    class TikaAwareNamedfileFieldConverter(NamedfileFieldConverter):
        """NamedBlobFile field converter that defers to Tika when active.

        The stock converter runs ``portal_transforms`` on the blob to produce
        indexable text.  When ``PGCATALOG_TIKA_URL`` is set the async worker
        already extracts that text, so we skip the transform and return only
        the filename (kept searchable immediately).  Body text is merged later
        by the worker.  See #114; the secondary-blob enqueue gap is #184.
        """

        def convert(self):
            tika_url = os.environ.get("PGCATALOG_TIKA_URL", "").strip()
            if not tika_url:
                return super().convert()
            storage = self.field.interface(self.context)
            data = self.field.get(storage)
            if not data or data.getSize() == 0:
                return ""
            return safe_text(data.filename or "")
