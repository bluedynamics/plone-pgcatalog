"""Catalog brain and result sequence for plone.pgcatalog.

PGCatalogBrain is a lightweight object backed by a PG row from object_state.
Attribute access reads from idx JSONB (catalog metadata).  State is NOT
fetched — use getObject() if you need the full object.

CatalogSearchResults wraps a list of brains with actual_result_count for
batched queries where LIMIT < total matching rows.
"""

from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.extraction import decode_meta
from Products.ZCatalog.interfaces import ICatalogBrain
from ZODB.utils import p64
from zope.component.hooks import getSite
from zope.globalrequest import getRequest
from zope.interface import implementer
from zope.interface.common.sequence import IFiniteSequence
from ZTUtils.Lazy import Lazy

import logging
import os


log = logging.getLogger(__name__)

_PREFETCH_BATCH = int(os.environ.get("PGCATALOG_PREFETCH_BATCH", "100"))


def _traversal_root():
    """Return a traversable Zope root (Application), or None.

    Brains avoid holding a reference to the catalog tool so they can
    be cached / pickled / re-queued without dragging the Acquisition
    chain along.  The root is resolved at call time via
    ``zope.component.hooks.getSite`` first (works in request and
    thread contexts that have a local-site hook), then falls back to
    the Zope traversal root from ``zope.globalrequest.getRequest``.

    Returns ``None`` when neither is available — the caller (``getObject``
    / ``_unrestrictedGetObject``) then reports "object not found"
    rather than raising, matching the long-standing ZCatalog contract.
    """
    site = getSite()
    if site is not None:
        return site.getPhysicalRoot()
    request = getRequest()
    if request is not None:
        parents = getattr(request, "PARENTS", None)
        if parents:
            return parents[-1]
    return None


@implementer(ICatalogBrain)
class PGCatalogBrain:
    """Lightweight catalog brain backed by a PostgreSQL row.

    Implements the essential ICatalogBrain interface without requiring
    Zope Acquisition or Record infrastructure.  Deliberately does NOT
    store a reference to the catalog tool — traversal resolves the
    portal root lazily via ``_traversal_root()``, so a brain remains
    safe to cache / pickle across request boundaries.

    Supports two modes:
    - **Eager** (default): row contains ``idx`` dict, metadata access is direct.
    - **Lazy**: row has no ``idx``; on first metadata access, triggers a batch
      load via ``_result_set._load_idx_batch()`` which fetches idx for all
      brains in the result set in a single query.

    Args:
        row: dict with keys zoid, path (and optionally idx)
        catalog: accepted for backward compatibility with callers still
            passing the catalog tool — ignored.  Kept until the next
            major version for signature compatibility.
    """

    __slots__ = ("_result_set", "_row")

    def __init__(self, row, catalog=None):
        # ``catalog`` is accepted for backward compat with callers
        # still passing the tool — intentionally unused.
        del catalog
        self._row = row
        self._result_set = None

    # -- ICatalogBrain methods -----------------------------------------------

    def getPath(self):
        """Get the physical path for this record."""
        return self._row["path"]

    def getURL(self, relative=0):
        """Generate a URL for this record.

        Uses ``zope.globalrequest.getRequest()`` — no reference to the
        catalog tool.  Keeping brains catalog-independent lets callers
        cache / pickle / re-queue them without dragging the acquisition
        chain along.  Returns the plain path in standalone / script
        mode when no request is active.
        """
        request = getRequest()
        if request is not None:
            return request.physicalPathToURL(self.getPath(), relative)
        return self.getPath()

    def _maybe_prefetch(self):
        """Trigger ZODB object prefetch if this brain is in a result set.

        Warms the storage ``_load_cache`` so that subsequent
        ``getObject()`` / ``_unrestrictedGetObject()`` calls on nearby
        brains hit the cache instead of issuing individual SQL queries.
        """
        if _PREFETCH_BATCH <= 0:
            return
        result_set = object.__getattribute__(self, "_result_set")
        if result_set is None:
            return
        result_set._maybe_prefetch_objects(self)

    def getObject(self):
        """Return the object for this record, or None if not found.

        Mirrors upstream ``Products.ZCatalog.CatalogBrains.AbstractCatalogBrain``
        semantics: the catalog filter already authorized access to the
        target object, so intermediate containers are traversed
        unrestricted; only the leaf is permission-checked.  Without this
        split, sites with a restricted parent folder (common pattern —
        e.g. an internal calendar container publishing public events)
        raise ``Unauthorized`` on the parent even though the user is
        allowed to see the target.

        Resolves the traversal root lazily via ``_traversal_root()`` so
        brains stay catalog-independent (cache-friendly).
        """
        root = _traversal_root()
        if root is None:
            return None
        self._maybe_prefetch()
        path = self.getPath().split("/")
        if not path:
            return None
        try:
            parent = root.unrestrictedTraverse(path[:-1]) if len(path) > 1 else root
            return parent.restrictedTraverse(path[-1])
        except (KeyError, AttributeError):
            return None

    def _unrestrictedGetObject(self):
        """Return the object without security checks, or None if not found."""
        root = _traversal_root()
        if root is None:
            return None
        self._maybe_prefetch()
        try:
            return root.unrestrictedTraverse(self.getPath())
        except (KeyError, AttributeError):
            return None

    @property
    def getId(self):
        """Return the object ID (last path segment).

        Property (not method) for compat with standard ZCatalog brains
        where ``brain.getId`` returns the string value from metadata,
        not a bound method.
        """
        idx = self._row.get("idx")
        if idx and "getId" in idx:
            return idx["getId"]
        path = self._row.get("path", "")
        return path.rsplit("/", 1)[-1] if path else ""

    def pretty_title_or_id(self):
        """Return the title if available, otherwise the id."""
        idx = self._row.get("idx")
        if idx:
            title = idx.get("Title")
            if title:
                return title
        return self.getId

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
        # Check dedicated meta column first
        meta_col = self._row.get("meta")
        if meta_col is not None and name in meta_col:
            return True
        if idx:
            if name in idx:
                return True
            # Fallback: pre-migration data with @meta still in idx
            meta = idx.get("@meta")
            if meta is not None and name in meta:
                return True
        return name in ("path", "zoid", "getPath", "getURL", "getRID")

    # -- attribute access from idx JSONB --------------------------------------

    def _resolve_from_idx(self, name, idx):
        """Return value from idx for known fields, raise AttributeError for unknown.

        Resolution order:

        1. Dedicated ``meta`` column (codec-encoded non-JSON-native metadata).
        2. Fallback: ``idx["@meta"]`` (pre-migration data still in idx).
        3. Top-level ``idx[name]`` — JSON-native values (str, int, bool, …)
           and converted index data.
        4. Known field not present → ``None`` (Missing Value, matching ZCatalog).
        5. Unknown field → ``AttributeError`` (lets callers fall back to
           ``getObject()``).
        """
        row = object.__getattribute__(self, "_row")

        # Check dedicated meta column first (new path)
        meta_col = row.get("meta")
        if meta_col is not None and name in meta_col:
            decoded = self._decode_meta_source(meta_col)
            return decoded.get(name)

        if idx is not None:
            # Fallback: pre-migration data with @meta still in idx
            meta = idx.get("@meta")
            if meta is not None and name in meta:
                decoded = self._decode_meta_source(meta)
                return decoded.get(name)
            # Fall back to top-level idx (JSON-native values + index data)
            if name in idx:
                return idx[name]
        registry = get_registry()
        if name in registry or name in registry.metadata:
            return None
        raise AttributeError(name)

    def _decode_meta_source(self, meta_dict):
        """Decode a meta dict via the Rust codec (cached).

        Works with both the dedicated ``meta`` column and the legacy
        ``idx["@meta"]`` sub-dict.  Result is cached in
        ``_row["_meta_decoded"]`` so subsequent accesses skip the codec.
        """
        row = object.__getattribute__(self, "_row")
        cached = row.get("_meta_decoded")
        if cached is not None:
            return cached
        decoded = decode_meta(meta_dict)
        row["_meta_decoded"] = decoded
        return decoded

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)

        row = object.__getattribute__(self, "_row")

        # Fast path: idx already in row (eager mode or already batch-loaded)
        idx = row.get("idx")
        if idx is not None:
            return self._resolve_from_idx(name, idx)

        # Lazy path: trigger batch load via result set
        result_set = object.__getattribute__(self, "_result_set")
        if result_set is not None:
            result_set._load_idx_batch()
            idx = row.get("idx")
            if idx is not None:
                return self._resolve_from_idx(name, idx)

        # No idx at all — still distinguish known vs unknown fields
        return self._resolve_from_idx(name, None)

    def __repr__(self):
        return f"<PGCatalogBrain zoid={self._row.get('zoid')} path={self._row.get('path')!r}>"


@implementer(IFiniteSequence)
class CatalogSearchResults(Lazy):
    """Result sequence from a catalog query.

    Wraps a list of PGCatalogBrain objects and tracks actual_result_count
    for batched queries (where LIMIT truncates results).

    Inherits from ZTUtils.Lazy so plone.restapi's LazyCatalogResultSerializer
    can serialize it via the existing ISerializeToJson adapter.

    Supports lazy idx loading: when ``conn`` is provided, the main query
    skips the ``idx`` column.  On first metadata access on any brain,
    ``_load_idx_batch()`` fetches idx for ALL brains in a single query
    using the same connection (and thus the same REPEATABLE READ snapshot).
    """

    def __init__(self, brains, actual_result_count=None, conn=None, catalog=None):
        self._brains = list(brains)
        self.actual_result_count = (
            actual_result_count
            if actual_result_count is not None
            else len(self._brains)
        )
        self._conn = conn
        self._catalog = catalog  # used only for ZODB prefetch (needs _p_jar)
        self._idx_loaded = conn is None  # eager if no conn
        self._prefetched_ranges = set()  # set of (start, end) tuples
        # Build index for O(1) brain → position lookup (used by prefetch)
        self._brain_index = {id(b): i for i, b in enumerate(self._brains)}

    def _load_idx_batch(self):
        """Batch-load idx for all brains in this result set.

        Called on first metadata access on any brain.  Issues a single
        SELECT for all zoids, populates each brain's ``_row["idx"]``.
        Uses the same connection as the original search query, so idx
        data is consistent with the REPEATABLE READ snapshot.
        """
        if self._idx_loaded:
            return
        self._idx_loaded = True
        if not self._brains or self._conn is None:
            return

        brain_map = {b.getRID(): b for b in self._brains}
        zoids = list(brain_map.keys())

        with self._conn.cursor() as cur:
            try:
                cur.execute(
                    "SELECT zoid, idx, meta FROM object_state"
                    " WHERE zoid = ANY(%(zoids)s)",
                    {"zoids": zoids},
                    prepare=True,
                )
            except Exception:
                # Column 'meta' may not exist yet if DDL was deferred and
                # not yet applied (e.g. first read after upgrade).  Fall
                # back to querying without it (#105).
                self._conn.rollback()
                cur.execute(
                    "SELECT zoid, idx FROM object_state WHERE zoid = ANY(%(zoids)s)",
                    {"zoids": zoids},
                    prepare=True,
                )
            for row in cur:
                brain = brain_map.get(row["zoid"])
                if brain is not None:
                    brain._row["idx"] = row["idx"]
                    if "meta" in row:
                        brain._row["meta"] = row["meta"]

    def _maybe_prefetch_objects(self, brain):
        """Prefetch ZODB objects for a batch of brains around *brain*.

        Warms the storage ``_load_cache`` so subsequent ``getObject()``
        calls hit the cache instead of making individual SQL queries.

        Uses ``storage.load_multiple(oids)`` when available (backwards
        compatible — skips silently if storage lacks the method).
        """
        if _PREFETCH_BATCH <= 0:
            return

        brain_id = id(brain)
        idx = self._brain_index.get(brain_id)
        if idx is None:
            return

        # Check if this index is already in a prefetched range.
        for start, end in self._prefetched_ranges:
            if start <= idx < end:
                return

        # Determine the batch range.
        start = idx
        end = min(idx + _PREFETCH_BATCH, len(self._brains))
        self._prefetched_ranges.add((start, end))

        batch = self._brains[start:end]
        if not batch:
            return

        # Get storage from the result-set's catalog reference (brains
        # are catalog-independent — the tool lives only on the
        # transient result set).
        if self._catalog is None:
            return
        jar = getattr(self._catalog, "_p_jar", None)
        if jar is None:
            return
        storage = getattr(jar, "_storage", None)
        if storage is None or not hasattr(storage, "load_multiple"):
            return

        oids = [p64(b.getRID()) for b in batch]
        try:
            storage.load_multiple(oids)
        except Exception:
            log.debug("prefetch failed", exc_info=True)

    def __len__(self):
        return len(self._brains)

    def __iter__(self):
        return iter(self._brains)

    def __getitem__(self, index):
        result = self._brains[index]
        if isinstance(index, slice):
            sr = CatalogSearchResults(
                result,
                self.actual_result_count,
                conn=self._conn,
                catalog=self._catalog,
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
