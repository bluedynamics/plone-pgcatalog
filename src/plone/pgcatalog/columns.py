"""Dynamic index registry and value conversion for plone.pgcatalog.

Provides an ``IndexRegistry`` that dynamically discovers indexes from the
ZCatalog's registered index objects (populated via GenericSetup catalog.xml
imports).  Each index is mapped to an ``IndexType`` for query translation
and an ``idx_key`` for JSONB storage, plus ``source_attrs`` for value
extraction (from ``indexed_attr`` in catalog.xml).

Design: idx keys use the exact ZCatalog index name — no translation.
This keeps query mapping trivial and brain attribute access natural.
"""

from datetime import date
from datetime import datetime
from enum import Enum

import logging


log = logging.getLogger(__name__)


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
# Meta-type → IndexType mapping
# --------------------------------------------------------------------------

META_TYPE_MAP = {
    "FieldIndex": IndexType.FIELD,
    "KeywordIndex": IndexType.KEYWORD,
    "DateIndex": IndexType.DATE,
    "BooleanIndex": IndexType.BOOLEAN,
    "DateRangeIndex": IndexType.DATE_RANGE,
    "UUIDIndex": IndexType.UUID,
    "ZCTextIndex": IndexType.TEXT,
    "ExtendedPathIndex": IndexType.PATH,
    "PathIndex": IndexType.PATH,
    "GopipIndex": IndexType.GOPIP,
}

# Indexes with special PG handling (idx_key=None): dedicated columns or
# composite logic that can't be expressed as a simple JSONB key.
SPECIAL_INDEXES = frozenset({"SearchableText", "effectiveRange", "path"})


# --------------------------------------------------------------------------
# Dynamic index registry
# --------------------------------------------------------------------------


class IndexRegistry:
    """Dynamic index registry populated from ZCatalog's registered indexes.

    Starts empty — populated via ``sync_from_catalog()`` which reads
    ``catalog._catalog.indexes`` and ``catalog._catalog.schema``.

    Each index entry is a tuple ``(IndexType, idx_key, source_attrs)`` where:
    - ``idx_key``: JSONB key in the ``idx`` column (usually the index name),
      or ``None`` for special indexes (SearchableText, effectiveRange, path).
    - ``source_attrs``: list of object attribute names to extract via
      ``getattr(wrapper, attr)``, from ``getIndexSourceNames()``.
    """

    def __init__(self):
        self._indexes = {}
        self._metadata = set()

    def sync_from_catalog(self, catalog):
        """Populate registry from a ZCatalog tool's internal catalog.

        Reads ``catalog._catalog.indexes`` for queryable indexes and
        ``catalog._catalog.schema`` for metadata columns.
        """
        try:
            zcatalog_indexes = catalog._catalog.indexes
        except AttributeError:
            return

        for name, index_obj in zcatalog_indexes.items():
            if name in self._indexes:
                continue  # already registered

            meta_type = getattr(index_obj, "meta_type", None)
            if meta_type is None:
                continue

            idx_type = META_TYPE_MAP.get(meta_type)
            if idx_type is None:
                log.debug("Unknown index meta_type %r for %r — skipping", meta_type, name)
                continue

            # Read source attributes from index object
            source_attrs = None
            if hasattr(index_obj, "getIndexSourceNames"):
                try:
                    source_attrs = list(index_obj.getIndexSourceNames())
                except Exception:
                    pass
            if not source_attrs:
                source_attrs = [name]

            # Special indexes get idx_key=None
            idx_key = None if name in SPECIAL_INDEXES else name

            self._indexes[name] = (idx_type, idx_key, source_attrs)

        # Metadata columns from catalog schema
        try:
            schema = catalog._catalog.schema
            for col_name in schema:
                self._metadata.add(col_name)
        except AttributeError:
            pass

    def register(self, name, idx_type, idx_key, source_attrs=None):
        """Manually register an index.

        Args:
            name: index name (query key)
            idx_type: IndexType enum value
            idx_key: JSONB key (usually same as name)
            source_attrs: list of attribute names for extraction
                          (defaults to [idx_key])
        """
        if source_attrs is None:
            source_attrs = [idx_key] if idx_key is not None else [name]
        self._indexes[name] = (idx_type, idx_key, source_attrs)

    def add_metadata(self, name):
        """Register a metadata-only column name."""
        self._metadata.add(name)

    @property
    def metadata(self):
        """Set of metadata column names."""
        return self._metadata

    # -- dict-like API ---

    def __contains__(self, name):
        return name in self._indexes

    def __getitem__(self, name):
        return self._indexes[name]

    def __len__(self):
        return len(self._indexes)

    def get(self, name, default=None):
        return self._indexes.get(name, default)

    def items(self):
        return self._indexes.items()

    def keys(self):
        return self._indexes.keys()

    def values(self):
        return self._indexes.values()


# Module-level singleton
_registry = IndexRegistry()


def get_registry():
    """Return the module-level IndexRegistry singleton."""
    return _registry


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
