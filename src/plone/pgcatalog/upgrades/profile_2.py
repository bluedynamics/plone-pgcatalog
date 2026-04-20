"""Profile v1 -> v2 migration.

Moves ``_CatalogCompat.indexes`` (a plain ``PersistentMapping``) to
``_CatalogCompat._raw_indexes`` so the new ``indexes`` property can
wrap reads via ``_CatalogIndexesView``.  Also sets ``__parent__`` on
the compat if missing, so ``aq_parent`` works through bare attribute
access (descriptors on Implicit classes receive bare ``self``).

See ``docs/plans/2026-04-15-wrap-catalog-indexes-attr.md`` (#137).
"""

import logging


log = logging.getLogger(__name__)


def migrate_catalog_indexes(context, *, _test_inject_jar=False):
    """Rename ``indexes`` -> ``_raw_indexes`` on the catalog's ``_catalog``.

    Accepts either a ``_CatalogCompat`` instance (for unit tests) or a
    GenericSetup environment (in which case the Plone catalog is resolved
    to its ``_catalog`` shim).

    Idempotent.  Marks the instance persistent-dirty so ZODB commits
    capture the rename and the ``__parent__`` pointer.

    The ``_test_inject_jar`` kwarg is a test-only escape hatch: when True
    and the compat has no ``_p_jar``, a no-op jar is attached so that
    ``_p_changed = True`` can be observed.  Production GenericSetup
    callers never pass this — real compats loaded from ZODB already
    carry a jar, so the flag transition works without intervention.
    """
    compat, catalog = _resolve_compat(context)
    if compat is None:
        log.warning("migrate_catalog_indexes: no _CatalogCompat found; skipping")
        return

    state = compat.__dict__
    mutated = False

    # -- Migrate the indexes attribute name ------------------------------
    if "_raw_indexes" in state:
        # Already migrated.  Clean up any stale shadow attribute.
        if "indexes" in state:
            del state["indexes"]
            mutated = True
            log.info(
                "migrate_catalog_indexes: removed stale 'indexes' "
                "attr (already had '_raw_indexes')"
            )
    elif "indexes" in state:
        state["_raw_indexes"] = state.pop("indexes")
        mutated = True
        log.info(
            "migrate_catalog_indexes: renamed 'indexes' -> '_raw_indexes' (%d entries)",
            len(state["_raw_indexes"]),
        )
    else:
        # Neither attribute present -- create an empty raw mapping so
        # the property can still serve a usable view.
        from persistent.mapping import PersistentMapping

        state["_raw_indexes"] = PersistentMapping()
        mutated = True
        log.info(
            "migrate_catalog_indexes: no 'indexes' attribute to "
            "migrate; created empty _raw_indexes"
        )

    # -- Ensure __parent__ is set so aq_parent() can find the tool -------
    if catalog is not None and state.get("__parent__") is not catalog:
        state["__parent__"] = catalog
        mutated = True
        log.info("migrate_catalog_indexes: set __parent__ on compat")

    if mutated:
        if _test_inject_jar and getattr(compat, "_p_jar", None) is None:
            # Test-only: Persistent._p_changed needs a jar to flip True.
            # Never fires in GenericSetup (real compats carry a real jar).
            compat._p_jar = _NoOpJar()
        compat._p_changed = True


class _NoOpJar:
    """Minimal stand-in for a ZODB Connection.

    Persistent's ``_p_changed`` setter requires ``_p_jar`` to be non-None
    before it will honour a transition to ``True``.  Attaching this to an
    otherwise free-floating compat lets the migration mark it dirty even in
    unit tests.  ZODB never sees this jar because real compats already carry
    a real one.
    """

    def register(self, _obj):
        """Accept the registration and do nothing (ZODB jar protocol)."""


def _resolve_compat(context):
    """Return ``(_CatalogCompat, catalog_tool)`` for either a compat
    instance or a GenericSetup context.  Either element may be None.

    GenericSetup hands two shapes to upgrade handlers depending on how
    they're invoked:

    * ``UpgradeStep.doStep`` (the normal v1->v2 path) passes the
      ``portal_setup`` tool itself — acquisition-wrapped inside the
      Plone site.
    * ``UpgradeDepends.doStep`` / export+import helpers pass an
      ``ImportContext`` exposing ``getSite()``.

    Both must resolve to the site's ``portal_catalog._catalog``.  The
    earlier version of this function only understood the ``ImportContext``
    path and silently returned ``(None, None)`` for the setup-tool case,
    so the migration no-op'd on every site that GenericSetup invokes via
    the normal upgrade button (see #139).
    """
    from Acquisition import aq_parent
    from plone.pgcatalog.maintenance import _CatalogCompat

    if isinstance(context, _CatalogCompat):
        # Unit-test shortcut: caller passed compat directly.
        try:
            catalog = aq_parent(context)
        except Exception:
            catalog = None
        if catalog is None:
            catalog = context.__dict__.get("__parent__")
        return context, catalog

    # ImportContext-style callers first (preserves existing path).
    # portal_setup-tool path: the setup tool's acquisition parent is
    # the Plone site root.
    getSite = getattr(context, "getSite", None)
    site = getSite() if getSite is not None else aq_parent(context)

    if site is None:
        return None, None
    catalog = getattr(site, "portal_catalog", None)
    if catalog is None:
        return None, None
    compat = getattr(catalog, "_catalog", None)
    if compat is None:
        return None, catalog
    return compat, catalog
