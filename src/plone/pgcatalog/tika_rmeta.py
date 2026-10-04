"""Parsing for Tika's ``PUT /rmeta/text`` responses.

``PUT /tika`` returns body text only, which for an image without OCR is
zero characters even when the file carries a caption.  ``/rmeta/text``
returns one JSON object per document, the container first and embedded
documents after it, each with its text under a content key.

Two measured facts shape this module.

Concatenating every entry's content does **not** duplicate anything: on a
ZIP of two text files the container's content is the file *names* only and
the children hold the text, on both 3.2.3 and 4.1.0.  Word order does
differ from ``/tika``, which interleaves names with contents, so phrase
proximity in ``searchable_text`` shifts.

And moving off ``/tika`` fixes a regression rather than only adding text.
Tika 4 returns **Markdown** from the plain-text endpoint, so a link
becomes ``[die Akte](https://example.org/akte)`` and its target is
tokenised into ``searchable_text``: five lexemes where the same sentence
gives two, one of them with the closing bracket glued on.  ``tk:content``
is plain.
"""

__all__ = [
    "CONTENT_KEYS",
    "MAX_RESPONSE_BYTES",
    "METADATA_FIELDS_DEFAULT",
    "extract_text",
]

# Tika 4 renamed the metadata namespace: X-TIKA:content became
# tk:content, resourceName became tk:resource-name.  Dublin Core keys and
# Content-Type are unchanged.  Reading only the 3.x name against a 4.x
# server returns empty text for every document, which reads as "no text in
# this file" rather than as a bug, so both are tried per entry and the 4.x
# name wins.
CONTENT_KEYS = ("tk:content", "X-TIKA:content")

METADATA_FIELDS_DEFAULT = (
    "dc:title",
    "dc:description",
    "dc:subject",
    "dc:creator",
    "meta:keyword",
)

# A document with many embedded resources produces one entry per resource,
# three for a two-file ZIP, so the body is not bounded by the source size.
# 32 MiB is far above any real document and far below anything that would
# trouble the worker.
MAX_RESPONSE_BYTES = 33_554_432


def _as_text(value):
    if isinstance(value, (list, tuple)):
        return " ".join(str(v) for v in value if v)
    return str(value) if value else ""


def _content_of(entry):
    """One entry's body text, under whichever major's key is present."""
    for key in CONTENT_KEYS:
        if key in entry:
            return _as_text(entry[key])
    return ""


def extract_text(payload, fields):
    """Body text of every entry, plus whitelisted container metadata.

    Returns "" for anything that is not a non-empty list of dicts, so a
    truncated or non-JSON-shaped body fails the one job rather than the
    worker loop.
    """
    if not isinstance(payload, list) or not payload:
        return ""
    entries = [entry for entry in payload if isinstance(entry, dict)]
    if not entries:
        return ""

    parts = []
    for key in fields:
        # Metadata from the container only: an embedded document's title
        # is rarely about the object being catalogued.
        value = _as_text(entries[0].get(key))
        if value:
            parts.append(value)
    for entry in entries:
        content = _content_of(entry).strip()
        if content:
            parts.append(content)
    return "\n".join(parts)
