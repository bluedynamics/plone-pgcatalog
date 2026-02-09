"""Catalog brain and result sequence for plone.pgcatalog.

PGCatalogBrain is a lightweight object backed by a PG row from object_state.
Attribute access reads from idx JSONB (computed values) first, then falls
back to state JSONB (object attributes).

CatalogSearchResults wraps a list of brains with actual_result_count for
batched queries where LIMIT < total matching rows.
"""


class PGCatalogBrain:
    """Lightweight catalog brain backed by a PostgreSQL row.

    Implements the essential ICatalogBrain interface without requiring
    Zope Acquisition or Record infrastructure.

    Args:
        row: dict with keys zoid, path, idx, state (from object_state SELECT)
        catalog: reference to the catalog tool (for getObject traversal)
    """

    __slots__ = ("_row", "_catalog")

    def __init__(self, row, catalog=None):
        self._row = row
        self._catalog = catalog

    # -- ICatalogBrain methods -----------------------------------------------

    def getPath(self):
        """Get the physical path for this record."""
        return self._row["path"]

    def getURL(self, relative=0):
        """Generate a URL for this record.

        In standalone mode (no request), returns the path.
        When integrated with Zope (Phase 6), uses request.physicalPathToURL.
        """
        if self._catalog is not None:
            request = getattr(self._catalog, "REQUEST", None)
            if request is not None:
                return request.physicalPathToURL(self.getPath(), relative)
        return self.getPath()

    def getObject(self):
        """Return the object for this record.

        Requires a catalog with traversal support (Phase 6 integration).
        Returns None if the object cannot be found.
        """
        if self._catalog is None:
            return None
        try:
            return self._catalog.restrictedTraverse(self.getPath())
        except (KeyError, AttributeError):
            return None

    def _unrestrictedGetObject(self):
        """Return the object without security checks."""
        if self._catalog is None:
            return None
        try:
            return self._catalog.unrestrictedTraverse(self.getPath())
        except (KeyError, AttributeError):
            return None

    def getRID(self):
        """Return the record ID (zoid) for this object."""
        return self._row["zoid"]

    @property
    def data_record_id_(self):
        """ZCatalog compatibility: record ID."""
        return self._row["zoid"]

    def has_key(self, key):
        """Check if brain has this field."""
        return key in self

    def __contains__(self, name):
        """Check if brain has this field."""
        idx = self._row.get("idx")
        if idx and name in idx:
            return True
        state = self._row.get("state")
        if state and name in state:
            return True
        return name in ("path", "zoid", "getPath", "getURL", "getRID")

    # -- attribute access from idx + state -----------------------------------

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        row = object.__getattribute__(self, "_row")

        # 1. Check idx JSONB (computed index/metadata values)
        idx = row.get("idx")
        if idx and name in idx:
            return idx[name]

        # 2. Check state JSONB (object attributes)
        state = row.get("state")
        if state and name in state:
            return state[name]

        raise AttributeError(name)

    def __repr__(self):
        return f"<PGCatalogBrain zoid={self._row.get('zoid')} path={self._row.get('path')!r}>"


class CatalogSearchResults:
    """Result sequence from a catalog query.

    Wraps a list of PGCatalogBrain objects and tracks actual_result_count
    for batched queries (where LIMIT truncates results).

    Supports iteration, len, indexing, and slicing.
    """

    __slots__ = ("_brains", "_actual_result_count")

    def __init__(self, brains, actual_result_count=None):
        self._brains = list(brains)
        self._actual_result_count = (
            actual_result_count if actual_result_count is not None else len(self._brains)
        )

    @property
    def actual_result_count(self):
        """Total matching rows (before LIMIT/OFFSET)."""
        return self._actual_result_count

    def __len__(self):
        return len(self._brains)

    def __iter__(self):
        return iter(self._brains)

    def __getitem__(self, index):
        result = self._brains[index]
        if isinstance(index, slice):
            return CatalogSearchResults(result, self._actual_result_count)
        return result

    def __bool__(self):
        return bool(self._brains)

    def __repr__(self):
        return (
            f"<CatalogSearchResults len={len(self._brains)}"
            f" actual={self._actual_result_count}>"
        )
