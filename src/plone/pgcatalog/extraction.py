"""Index/metadata value extraction from Plone content objects.

Provides functions to extract index values, metadata, and searchable text
from wrapped indexable objects.  Used by ``PlonePGCatalogTool`` methods
that prepare data for PostgreSQL storage.
"""

from plone.pgcatalog.columns import compute_path_info
from plone.pgcatalog.columns import convert_value
from plone.pgcatalog.columns import get_registry
from plone.pgcatalog.columns import IndexType
from plone.pgcatalog.interfaces import IPGIndexTranslator

import logging
import pickle


log = logging.getLogger(__name__)


def _is_json_native(value):
    """Check if *value* survives a JSON/JSONB round-trip unchanged.

    Returns ``True`` for types that map 1:1 to JSON scalars, arrays, or
    objects (str, int, float, bool, None, and homogeneous lists/dicts of
    these).  Returns ``False`` for everything else (DateTime, datetime,
    date, sets, custom objects, …).
    """
    if value is None:
        return True
    if isinstance(value, bool):  # before int — bool is a subclass of int
        return True
    if isinstance(value, (int, float, str)):
        return True
    if isinstance(value, (list, tuple)):
        return all(_is_json_native(v) for v in value)
    if isinstance(value, dict):
        return all(_is_json_native(v) for v in value.values())
    return False


def decode_meta(coded_meta):
    """Decode a codec-encoded ``@meta`` dict back to original Python types.

    Args:
        coded_meta: dict from ``idx["@meta"]`` (as loaded from JSONB)

    Returns:
        dict mapping field names to their original Python values
        (e.g. DateTime objects instead of ISO strings).
    """
    from zodb_json_codec import dict_to_pickle

    pickle_bytes = dict_to_pickle(coded_meta)
    return pickle.loads(pickle_bytes)


def _path_value_to_string(value):
    """Convert a path index value to a string path.

    Path indexers may return a tuple/list of path components (e.g. tgpath
    returns ``('uuid1', 'uuid2', 'uuid3')``), or a string path.
    Returns ``None`` if the value is empty or not convertible.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value if value else None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        return "/" + "/".join(str(p) for p in value)
    return str(value)


def wrap_object(obj, catalog):
    """Wrap an object with IIndexableObject for plone.indexer."""
    from plone.indexer.interfaces import IIndexableObject
    from zope.component import queryMultiAdapter

    wrapper = queryMultiAdapter((obj, catalog), IIndexableObject)
    return wrapper if wrapper is not None else obj


def obj_to_zoid(obj):
    """Extract the integer zoid from a persistent object's _p_oid."""
    oid = getattr(obj, "_p_oid", None)
    if oid is None:
        return None
    return int.from_bytes(oid, "big")


def extract_idx(wrapper, idxs=None):
    """Extract all idx values from a wrapped indexable object.

    Iterates the dynamic ``IndexRegistry`` for indexes (using
    ``source_attrs`` for attribute lookup) and metadata columns.
    Indexes with ``idx_key=None`` (special: SearchableText,
    effectiveRange, path) are skipped — they have dedicated columns.

    PATH-type indexes with ``idx_key`` set (additional path indexes
    like ``tgpath``) store the path value plus derived ``_parent``
    and ``_depth`` keys in the idx JSONB.
    """
    registry = get_registry()
    idx = {}

    # Extract index values
    for name, (idx_type, idx_key, source_attrs) in registry.items():
        if idx_key is None:
            continue  # composite/special (path, SearchableText, effectiveRange)
        if idxs and name not in idxs:
            continue  # partial reindex — skip unrequested indexes
        try:
            value = None
            for attr in source_attrs:
                value = getattr(wrapper, attr, None)
                if callable(value):
                    value = value()
                if value is not None:
                    break
            if idx_type == IndexType.PATH:
                # Additional path index — store path + parent + depth
                path_str = _path_value_to_string(value)
                if path_str:
                    parent, depth = compute_path_info(path_str)
                    idx[idx_key] = path_str
                    idx[f"{idx_key}_parent"] = parent
                    idx[f"{idx_key}_depth"] = depth
            else:
                idx[idx_key] = convert_value(value)
        except Exception:
            pass  # indexer raised — skip this field

    # Extract metadata columns.
    # JSON-native values (str, int, float, bool, None) go into top-level idx.
    # Non-native values (DateTime, datetime, date, …) are collected separately,
    # pickled as a dict, and stored under idx["@meta"] via the Rust codec.
    # This preserves original types for brain attribute access while keeping
    # index data in top-level idx for PG queries.
    meta_nonstandard = {}
    for meta_name in registry.metadata:
        if idxs and meta_name not in idxs:
            continue
        try:
            value = getattr(wrapper, meta_name, None)
            if callable(value):
                value = value()
            if value is None:
                if meta_name not in idx:
                    idx[meta_name] = None
            elif _is_json_native(value):
                if meta_name not in idx:
                    idx[meta_name] = value
            else:
                meta_nonstandard[meta_name] = value
        except Exception:
            pass  # indexer raised — skip this field

    if meta_nonstandard:
        try:
            from zodb_json_codec import pickle_to_dict

            pickled = pickle.dumps(meta_nonstandard, protocol=3)
            idx["@meta"] = pickle_to_dict(pickled)
        except Exception:
            pass  # unpicklable metadata — skip @meta encoding

    # IPGIndexTranslator fallback: custom extractors
    extract_from_translators(wrapper, idx, idxs=idxs)

    return idx


def extract_from_translators(wrapper, idx, idxs=None):
    """Call IPGIndexTranslator.extract() for all registered translators.

    When ``idxs`` is provided, only calls translators whose name
    is in the filter list.
    """
    try:
        from zope.component import getUtilitiesFor

        for name, translator in getUtilitiesFor(IPGIndexTranslator):
            if idxs and name not in idxs:
                continue  # skip unrequested translator
            try:
                extra = translator.extract(wrapper, name)
                if extra and isinstance(extra, dict):
                    idx.update(extra)
            except Exception:
                pass  # translator raised — skip
    except Exception:
        pass  # no component architecture available


def extract_searchable_text(wrapper):
    """Extract SearchableText from a wrapped indexable object."""
    try:
        value = getattr(wrapper, "SearchableText", None)
        if callable(value):
            value = value()
        return value if isinstance(value, str) else None
    except Exception:
        return None


def extract_content_type(wrapper):
    """Extract MIME content type from a content object's primary field.

    Tries IPrimaryFieldInfo (Dexterity) first, then falls back to
    a ``content_type`` attribute on the wrapper or underlying object.

    Returns:
        str or None
    """
    # Try Dexterity primary field
    try:
        from plone.rfc822.interfaces import IPrimaryFieldInfo

        info = IPrimaryFieldInfo(wrapper, None)
        if info is not None:
            field_value = info.value
            ct = getattr(field_value, "contentType", None)
            if ct:
                return ct
    except Exception:
        pass
    # Fallback: content_type attribute
    ct = getattr(wrapper, "content_type", None)
    if callable(ct):
        try:
            ct = ct()
        except Exception:
            return None
    return ct if isinstance(ct, str) else None
