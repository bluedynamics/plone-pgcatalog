"""MIME type normalisation for the Tika extraction gate.

``_should_extract`` used to compare the catalogued ``mime_type`` against a
set by exact string, so ``text/plain; charset=utf-8`` and
``APPLICATION/PDF`` never matched although Plone can hold both.

Blanket parameter stripping is not the fix either: Tika's own
supported-type set contains parameterised entries such as
``audio/ogg; codecs=opus`` and ``audio/ogg; codecs=speex``, so dropping
parameters would make an explicit parameterised configuration entry
unreachable.  The full normalised value is therefore tried first and the
bare type second.
"""

__all__ = ["matches", "normalise"]


def normalise(content_type):
    """*content_type* lowercased, with parameter spacing canonicalised.

    Returns None for None, empty and whitespace-only input, which is what
    callers treat as "no opinion".
    """
    if not content_type or not content_type.strip():
        return None
    parts = [part.strip() for part in content_type.strip().lower().split(";")]
    essence, params = parts[0], [part for part in parts[1:] if part]
    return "; ".join([essence, *params])


def matches(content_type, allowed):
    """Whether *content_type* is in *allowed*, full value before bare type."""
    normalised = normalise(content_type)
    if normalised is None:
        return False
    if normalised in allowed:
        return True
    return normalised.split(";")[0] in allowed
