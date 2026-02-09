"""Index name registry and value conversion for plone.pgcatalog.

Maps ZCatalog index names → idx JSONB keys and defines value conversion
from Python/Zope types to JSON-safe types.

Design: idx keys use the exact ZCatalog index/metadata name — no translation.
This keeps query mapping trivial and brain attribute access natural.
"""

from datetime import date
from datetime import datetime
from enum import Enum


class IndexType(Enum):
    """ZCatalog index types we handle natively."""

    FIELD = "FieldIndex"
    KEYWORD = "KeywordIndex"
    DATE = "DateIndex"
    BOOLEAN = "BooleanIndex"
    DATE_RANGE = "DateRangeIndex"
    UUID = "UUIDIndex"
    TEXT = "ZCTextIndex"
    PATH = "ExtendedPathIndex"
    GOPIP = "GopipIndex"


# --------------------------------------------------------------------------
# Known indexes: name → (IndexType, idx_key | None)
#
# idx_key=None means the index is handled specially (path, searchable_text)
# or is composite (effectiveRange uses effective + expires).
# --------------------------------------------------------------------------

KNOWN_INDEXES = {
    # FieldIndex
    "Creator": (IndexType.FIELD, "Creator"),
    "Type": (IndexType.FIELD, "Type"),
    "getId": (IndexType.FIELD, "getId"),
    "id": (IndexType.FIELD, "id"),
    "in_reply_to": (IndexType.FIELD, "in_reply_to"),
    "portal_type": (IndexType.FIELD, "portal_type"),
    "review_state": (IndexType.FIELD, "review_state"),
    "sortable_title": (IndexType.FIELD, "sortable_title"),
    # KeywordIndex
    "Subject": (IndexType.KEYWORD, "Subject"),
    "allowedRolesAndUsers": (IndexType.KEYWORD, "allowedRolesAndUsers"),
    "getRawRelatedItems": (IndexType.KEYWORD, "getRawRelatedItems"),
    "object_provides": (IndexType.KEYWORD, "object_provides"),
    # DateIndex
    "Date": (IndexType.DATE, "Date"),
    "created": (IndexType.DATE, "created"),
    "effective": (IndexType.DATE, "effective"),
    "end": (IndexType.DATE, "end"),
    "expires": (IndexType.DATE, "expires"),
    "modified": (IndexType.DATE, "modified"),
    "start": (IndexType.DATE, "start"),
    # BooleanIndex
    "is_default_page": (IndexType.BOOLEAN, "is_default_page"),
    "is_folderish": (IndexType.BOOLEAN, "is_folderish"),
    "exclude_from_nav": (IndexType.BOOLEAN, "exclude_from_nav"),
    # DateRangeIndex (composite — uses effective + expires, no own key)
    "effectiveRange": (IndexType.DATE_RANGE, None),
    # UUIDIndex
    "UID": (IndexType.UUID, "UID"),
    # ZCTextIndex (full-text → separate tsvector column)
    "SearchableText": (IndexType.TEXT, None),
    "Title": (IndexType.TEXT, "Title"),
    "Description": (IndexType.TEXT, "Description"),
    # ExtendedPathIndex (separate path column)
    "path": (IndexType.PATH, None),
    # GopipIndex
    "getObjPositionInParent": (IndexType.GOPIP, "getObjPositionInParent"),
}

# --------------------------------------------------------------------------
# Metadata columns: names that should be stored in idx JSONB for brain access.
# These overlap with index names where the value is the same.
# Metadata-only entries (not indexes) are listed separately.
# --------------------------------------------------------------------------

METADATA_ONLY = {
    "CreationDate",
    "EffectiveDate",
    "ExpirationDate",
    "ModificationDate",
    "getIcon",
    "getObjSize",
    "getRemoteUrl",
    "image_scales",
    "listCreators",
    "location",
    "mime_type",
}

# All idx keys: union of index keys + metadata-only keys.
ALL_IDX_KEYS = (
    {key for _, key in KNOWN_INDEXES.values() if key is not None}
    | METADATA_ONLY
)


# --------------------------------------------------------------------------
# Value conversion: Python/Zope types → JSON-safe values
# --------------------------------------------------------------------------


def convert_value(value):
    """Convert a Python/Zope value to a JSON-safe value for idx JSONB.

    Handles: DateTime, datetime, date, bool, int, float, str, list, tuple,
    None, and falls back to str() for unknown types.
    """
    if value is None:
        return None

    # Bool must come before int (bool is a subclass of int)
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float, str)):
        return value

    # Zope DateTime → ISO 8601
    if _is_zope_datetime(value):
        return _convert_zope_datetime(value)

    # Python datetime/date → ISO 8601
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    # Sequences → lists
    if isinstance(value, (list, tuple, set, frozenset)):
        return [convert_value(v) for v in value]

    # Dict → recurse
    if isinstance(value, dict):
        return {k: convert_value(v) for k, v in value.items()}

    # Fallback: string representation
    return str(value)


def _is_zope_datetime(value):
    """Check if value is a Zope DateTime object (duck-typing)."""
    return hasattr(value, "ISO8601") and callable(value.ISO8601)


def _convert_zope_datetime(dt):
    """Convert a Zope DateTime to ISO 8601 string."""
    return dt.ISO8601()


# --------------------------------------------------------------------------
# Path utilities
# --------------------------------------------------------------------------


def compute_path_info(path):
    """Compute parent_path and path_depth from a path string.

    Args:
        path: physical path like "/plone/folder/doc"

    Returns:
        (parent_path, path_depth) tuple.
        parent_path: "/plone/folder" (parent's path)
        path_depth: 3 (number of non-empty components)
    """
    parts = [p for p in path.split("/") if p]
    depth = len(parts)

    parent = "/" if depth <= 1 else "/" + "/".join(parts[:-1])

    return parent, depth
