"""Catalog brain and result sequence for plone.pgcatalog.

PGCatalogBrain is a lightweight object backed by a PG row from object_state.
Attribute access reads from idx JSONB (catalog metadata).  State is NOT
fetched — use getObject() if you need the full object.

CatalogSearchResults wraps a list of brains with actual_result_count for
batched queries where LIMIT < total matching rows.
"""

from Products.ZCatalog.interfaces import ICatalogBrain
from ZTUtils.Lazy import Lazy
from zope.interface import implementer


@implementer(ICatalogBrain)
class PGCatalogBrain:
    """Lightweight catalog brain backed by a PostgreSQL row.

    Implements the essential ICatalogBrain interface without requiring
    Zope Acquisition or Record infrastructure.

    Supports two modes:
    - **Eager** (default): row contains ``idx`` dict, metadata access is direct.
    - **Lazy**: row has no ``idx``; on first metadata access, triggers a batch
      load via ``_result_set._load_idx_batch()`` which fetches idx for all
      brains in the result set in a single query.

    Args:
        row: dict with keys zoid, path (and optionally idx)
        catalog: reference to the catalog tool (for getObject traversal)
    """

    __slots__ = ("_catalog", "_row", "_result_set")

    def __init__(self, row, catalog=None):
        self._row = row
        self._catalog = catalog
        self._result_set = None

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
        if idx is None:
            result_set = object.__getattribute__(self, "_result_set")
            if result_set is not None:
                result_set._load_idx_batch()
                idx = self._row.get("idx")
        if idx and name in idx:
            return True
        return name in ("path", "zoid", "getPath", "getURL", "getRID")

    # -- attribute access from idx JSONB --------------------------------------

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        row = object.__getattribute__(self, "_row")

        # Fast path: idx already in row (eager mode or already batch-loaded)
        idx = row.get("idx")
        if idx is not None:
            if name in idx:
                return idx[name]
            raise AttributeError(name)

        # Lazy path: trigger batch load via result set
        result_set = object.__getattribute__(self, "_result_set")
        if result_set is not None:
            result_set._load_idx_batch()
            idx = row.get("idx")
            if idx is not None and name in idx:
                return idx[name]

        raise AttributeError(name)

    def __repr__(self):
        return f"<PGCatalogBrain zoid={self._row.get('zoid')} path={self._row.get('path')!r}>"


class CatalogSearchResults(Lazy):
    """Result sequence from a catalog query.

    Wraps a list of PGCatalogBrain objects and tracks actual_result_count
    for batched queries (where LIMIT truncates results).

    Inherits from ZTUtils.Lazy so plone.restapi's LazyCatalogResultSerializer
    can serialize it via the existing ISerializeToJson adapter.

    Supports lazy idx loading: when ``pool`` is provided, the main query
    skips the ``idx`` column.  On first metadata access on any brain,
    ``_load_idx_batch()`` fetches idx for ALL brains in a single query.
    """

    def __init__(self, brains, actual_result_count=None, pool=None):
        self._brains = list(brains)
        self.actual_result_count = (
            actual_result_count if actual_result_count is not None else len(self._brains)
        )
        self._pool = pool
        self._idx_loaded = pool is None  # eager if no pool

    def _load_idx_batch(self):
        """Batch-load idx for all brains in this result set.

        Called on first metadata access on any brain.  Issues a single
        SELECT for all zoids, populates each brain's ``_row["idx"]``.
        """
        if self._idx_loaded:
            return
        self._idx_loaded = True
        if not self._brains or self._pool is None:
            return

        brain_map = {b.getRID(): b for b in self._brains}
        zoids = list(brain_map.keys())

        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT zoid, idx FROM object_state "
                    "WHERE zoid = ANY(%(zoids)s)",
                    {"zoids": zoids},
                    prepare=True,
                )
                for row in cur:
                    brain = brain_map.get(row["zoid"])
                    if brain is not None:
                        brain._row["idx"] = row["idx"]
        finally:
            self._pool.putconn(conn)

    def __len__(self):
        return len(self._brains)

    def __iter__(self):
        return iter(self._brains)

    def __getitem__(self, index):
        result = self._brains[index]
        if isinstance(index, slice):
            sr = CatalogSearchResults(
                result, self.actual_result_count, pool=self._pool,
            )
            # Re-wire brains to new result set if idx not yet loaded
            if not self._idx_loaded:
                for brain in sr._brains:
                    brain._result_set = sr
            return sr
        return result

    def __bool__(self):
        return bool(self._brains)

    def __repr__(self):
        return (
            f"<CatalogSearchResults len={len(self._brains)}"
            f" actual={self.actual_result_count}>"
        )
