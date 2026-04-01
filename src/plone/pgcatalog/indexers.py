"""Conditional SearchableText indexer for IFile.

When ``PGCATALOG_TIKA_URL`` is configured, skips the expensive
``portal_transforms`` pipeline (pdftotext, wv, BFS graph traversal)
and returns only Title + Description.  The Tika async worker
extracts blob text and merges it into ``searchable_text`` as
tsvector weight 'C'.

When ``PGCATALOG_TIKA_URL`` is NOT set, delegates to the original
``plone.app.contenttypes.indexers.SearchableText_file`` so the
full transform pipeline runs as before.
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
