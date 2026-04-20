"""Tests for the wrapping behavior of _CatalogCompat.indexes.

See: docs/plans/2026-04-15-wrap-catalog-indexes-attr.md
Issue: bluedynamics/plone-pgcatalog#137
"""

from persistent.mapping import PersistentMapping
from unittest import mock


def _fresh_compat():
    """Build a _CatalogCompat with no catalog parent (tests defensive path)."""
    from plone.pgcatalog.maintenance import _CatalogCompat

    return _CatalogCompat()


# ── View with no reachable catalog: must raise, not return raw ───────────


class TestViewWithoutCatalog:
    """Pre-#146 behavior: getitem returned the raw ZCatalog index when
    aq_parent returned None.  That masked silent "empty results" bugs.
    New contract: the view raises RuntimeError when no catalog is
    reachable.  Callers that intentionally work without a catalog now
    need to set __parent__ explicitly.
    """

    def test_getitem_raises_when_no_catalog_reachable(self):
        import pytest

        compat = _fresh_compat()
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        with (
            mock.patch("plone.pgcatalog.maintenance.getSite", return_value=None),
            pytest.raises(RuntimeError, match="cannot find portal_catalog"),
        ):
            _ = compat.indexes["portal_type"]

    def test_view_keys_are_unwrapped_strings(self):
        compat = _fresh_compat()
        compat._raw_indexes["a"] = mock.Mock()
        compat._raw_indexes["b"] = mock.Mock()
        assert set(compat.indexes.keys()) == {"a", "b"}

    def test_view_contains_and_len(self):
        compat = _fresh_compat()
        compat._raw_indexes["a"] = mock.Mock()
        assert "a" in compat.indexes
        assert "missing" not in compat.indexes
        assert len(compat.indexes) == 1

    def test_view_iteration_yields_keys(self):
        compat = _fresh_compat()
        compat._raw_indexes["a"] = mock.Mock()
        compat._raw_indexes["b"] = mock.Mock()
        assert sorted(iter(compat.indexes)) == ["a", "b"]


# ── Wrapper contract with a PG catalog parent — indexes get wrapped ──────


class TestViewWithPgCatalog:
    def _setup(self, pg_conn_with_catalog):
        """Build a PlonePGCatalogTool + _CatalogCompat chained by parent pointer."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        tool._catalog = _CatalogCompat(parent=tool)
        return tool

    def _register_field_index(self, tool, name="portal_type"):
        raw = mock.Mock(id=name, meta_type="FieldIndex")
        tool._catalog._raw_indexes[name] = raw
        return raw

    def test_getitem_returns_pg_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex

        tool = self._setup(pg_conn_with_catalog)
        raw = self._register_field_index(tool, "portal_type")

        result = tool._catalog.indexes["portal_type"]

        assert isinstance(result, PGIndex)
        assert result._wrapped is raw

    def test_get_returns_pg_index(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex

        tool = self._setup(pg_conn_with_catalog)
        self._register_field_index(tool, "portal_type")

        result = tool._catalog.indexes.get("portal_type")
        assert isinstance(result, PGIndex)

    def test_get_missing_returns_default(self, pg_conn_with_catalog):
        tool = self._setup(pg_conn_with_catalog)
        assert tool._catalog.indexes.get("missing") is None
        assert tool._catalog.indexes.get("missing", "sentinel") == "sentinel"

    def test_items_yields_wrapped(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex

        tool = self._setup(pg_conn_with_catalog)
        self._register_field_index(tool, "a")
        self._register_field_index(tool, "b")

        pairs = dict(tool._catalog.indexes.items())
        assert set(pairs.keys()) == {"a", "b"}
        for v in pairs.values():
            assert isinstance(v, PGIndex)

    def test_values_yields_wrapped(self, pg_conn_with_catalog):
        from plone.pgcatalog.pgindex import PGIndex

        tool = self._setup(pg_conn_with_catalog)
        self._register_field_index(tool, "a")

        vs = list(tool._catalog.indexes.values())
        assert len(vs) == 1
        assert isinstance(vs[0], PGIndex)

    def test_special_index_not_wrapped(self, pg_conn_with_catalog):
        """`idx_key=None` indexes (path, SearchableText, effectiveRange) return raw."""
        from plone.pgcatalog.columns import get_registry
        from plone.pgcatalog.columns import IndexType

        registry = get_registry()
        registry.register("path", IndexType.PATH, None)

        tool = self._setup(pg_conn_with_catalog)
        raw = mock.Mock(id="path", meta_type="ExtendedPathIndex")
        tool._catalog._raw_indexes["path"] = raw

        # Special indexes skip wrapping — receive raw back
        result = tool._catalog.indexes["path"]
        assert result is raw


# ── Mutations go to raw mapping, no wrapping ────────────────────────────


class TestViewMutations:
    def test_setitem_writes_to_raw(self):
        compat = _fresh_compat()
        idx = mock.Mock(id="x", meta_type="FieldIndex")
        compat.indexes["x"] = idx
        assert compat._raw_indexes["x"] is idx

    def test_delitem_removes_from_raw(self):
        compat = _fresh_compat()
        compat._raw_indexes["x"] = mock.Mock()
        del compat.indexes["x"]
        assert "x" not in compat._raw_indexes

    def test_update_writes_to_raw(self):
        compat = _fresh_compat()
        compat.indexes.update({"a": mock.Mock(), "b": mock.Mock()})
        assert set(compat._raw_indexes.keys()) == {"a", "b"}


# ── Upgrade step migrates legacy persisted state ────────────────────────


class TestProfileUpgradeV1ToV2:
    def test_migrate_moves_indexes_attr_to_raw_indexes(self):
        """Legacy compat has compat.indexes: PersistentMapping — migrate it."""
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.upgrades.profile_2 import migrate_catalog_indexes

        compat = _CatalogCompat.__new__(_CatalogCompat)
        # Simulate legacy persisted state: `indexes` as plain PersistentMapping.
        legacy = PersistentMapping()
        legacy["portal_type"] = mock.Mock()
        compat.__dict__["indexes"] = legacy
        # And no `_raw_indexes` yet
        assert "_raw_indexes" not in compat.__dict__

        migrate_catalog_indexes(compat)

        assert "_raw_indexes" in compat.__dict__
        assert compat._raw_indexes is legacy
        assert "indexes" not in compat.__dict__

    def test_migrate_is_idempotent(self):
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.upgrades.profile_2 import migrate_catalog_indexes

        compat = _CatalogCompat()  # already has _raw_indexes
        assert "_raw_indexes" in compat.__dict__

        migrate_catalog_indexes(compat)  # no-op
        migrate_catalog_indexes(compat)  # no-op again
        assert "_raw_indexes" in compat.__dict__

    def test_migrate_marks_persistent_dirty(self):
        """After migration the Persistent instance must be marked changed so ZODB
        commits the rename."""
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.upgrades.profile_2 import migrate_catalog_indexes

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["indexes"] = PersistentMapping()
        compat._p_changed = False  # simulate freshly loaded

        # The unjarred-compat branch of the migration requires an explicit
        # opt-in so production GenericSetup calls can't accidentally
        # install a no-op jar.
        migrate_catalog_indexes(compat, _test_inject_jar=True)
        assert compat._p_changed is True

    def test_migrate_from_portal_setup_tool_context(self):
        """GenericSetup calls upgrade handlers with portal_setup as context.

        Regression for #139: the upgrade was silently no-op on production
        because ``_resolve_compat`` only understood ImportContext (getSite())
        and returned (None, None) for the setup tool, logging a warning and
        leaving the persisted state untouched even though GS bumped the
        profile version to 2.
        """
        from OFS.Folder import Folder
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.upgrades.profile_2 import migrate_catalog_indexes

        # Build the shape GS gives us: portal_setup acquisition-wrapped
        # inside a Plone site that owns portal_catalog -> _CatalogCompat.
        site = Folder("plone")
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        compat = _CatalogCompat.__new__(_CatalogCompat)
        legacy = PersistentMapping({"portal_type": mock.Mock()})
        compat.__dict__["indexes"] = legacy
        tool._catalog = compat
        site.portal_catalog = tool

        class _SetupTool(Folder):
            pass

        setup = _SetupTool("portal_setup")
        site.portal_setup = setup
        # GS passes the acquisition-wrapped setup tool
        wrapped_setup = site.portal_setup

        migrate_catalog_indexes(wrapped_setup, _test_inject_jar=True)

        assert "_raw_indexes" in compat.__dict__, (
            "migration must rename legacy 'indexes' attr when invoked "
            "with the portal_setup tool context (real GS path)"
        )
        assert compat._raw_indexes is legacy
        assert "indexes" not in compat.__dict__


# ── Self-healing property survives unmigrated legacy state ──────────────


class TestPropertySelfHealsLegacyState:
    """If a site was installed with profile v2 directly (fresh install) or
    the upgrade step silently no-op'd, ``_CatalogCompat`` may still have
    the legacy ``indexes`` attribute in ``__dict__`` and no
    ``_raw_indexes``.  Any AttributeError inside the property is
    swallowed by Acquisition and the tool's ``indexes()`` method is
    returned instead, producing ``'function' object has no attribute
    'keys'`` on ``catalog.indexes.keys()``.  The property must handle
    that state itself instead of raising.
    """

    def test_property_migrates_on_first_access(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat.__new__(_CatalogCompat)
        legacy = PersistentMapping({"portal_type": mock.Mock()})
        compat.__dict__["indexes"] = legacy
        # no _raw_indexes

        view = compat.indexes
        assert list(view.keys()) == ["portal_type"]
        # side effect: state migrated in-place
        assert "_raw_indexes" in compat.__dict__
        assert compat._raw_indexes is legacy
        assert "indexes" not in compat.__dict__

    def test_property_creates_empty_when_no_legacy(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat.__new__(_CatalogCompat)
        # No indexes and no _raw_indexes — pathological but must not crash
        view = compat.indexes
        assert list(view.keys()) == []
        assert "_raw_indexes" in compat.__dict__

    def test_tool_indexes_method_works_on_unmigrated_compat(self):
        """Regression for the observed production traceback:
            AttributeError: 'function' object has no attribute 'keys'
        at plone.pgcatalog.catalog:287.
        """
        from OFS.Folder import Folder
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["indexes"] = PersistentMapping({"a": mock.Mock()})
        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._catalog = compat
        site = Folder("plone")
        site.portal_catalog = tool

        result = site.portal_catalog.indexes()
        assert result == ["a"]


# ── getIndex still works (existing API) ────────────────────────────────


class TestGetIndexMethod:
    def test_get_index_via_method(self, pg_conn_with_catalog):
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.pgindex import PGIndex

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        tool._catalog = _CatalogCompat(parent=tool)
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        tool._catalog._raw_indexes["portal_type"] = raw

        result = tool._catalog.getIndex("portal_type")
        assert isinstance(result, PGIndex)
        assert result._wrapped is raw


# ── _resolve_catalog: three-step lookup, no silent None ──────────────────


class TestResolveCatalog:
    def test_returns_explicit_parent_first(self):
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        compat = _CatalogCompat()
        tool = mock.Mock(name="tool-via-parent")
        compat.__dict__["__parent__"] = tool

        assert _resolve_catalog(compat) is tool

    def test_falls_through_to_acquisition_parent(self):
        from Acquisition import Implicit
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        class _ParentHolder(Implicit):
            pass

        parent = _ParentHolder()
        compat = _CatalogCompat()
        wrapped = compat.__of__(parent)  # Acquisition wrapper

        assert _resolve_catalog(wrapped) is parent

    def test_falls_through_to_get_site_portal_catalog(self):
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        compat = _CatalogCompat()
        tool = mock.Mock(name="tool-via-get-site")
        site = mock.Mock(portal_catalog=tool)

        with mock.patch("plone.pgcatalog.maintenance.getSite", return_value=site):
            assert _resolve_catalog(compat) is tool

    def test_raises_runtimeerror_when_all_three_fail(self):
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.maintenance import _resolve_catalog

        import pytest

        compat = _CatalogCompat()
        with (
            mock.patch("plone.pgcatalog.maintenance.getSite", return_value=None),
            pytest.raises(RuntimeError, match="cannot find portal_catalog"),
        ):
            _resolve_catalog(compat)


# ── Self-heal: __parent__ gets persisted on first indexes access ─────────


class TestParentSelfHeal:
    def test_property_heals_missing_parent_via_get_site(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["_raw_indexes"] = PersistentMapping()
        compat.__dict__["schema"] = PersistentMapping()
        # no __parent__
        compat._p_changed = False

        tool = mock.Mock(name="portal_catalog")
        site = mock.Mock(portal_catalog=tool)

        # Need a fake jar so ``_p_changed = True`` sticks under Persistent.
        from plone.pgcatalog.upgrades.profile_2 import _NoOpJar

        compat._p_jar = _NoOpJar()

        with mock.patch("plone.pgcatalog.maintenance.getSite", return_value=site):
            _view = compat.indexes  # trigger property

        assert compat.__dict__.get("__parent__") is tool
        assert compat._p_changed is True

    def test_property_tolerates_get_site_returning_none(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["_raw_indexes"] = PersistentMapping()
        # no __parent__, no site hook

        with mock.patch("plone.pgcatalog.maintenance.getSite", return_value=None):
            _view = compat.indexes  # must not raise

        assert "__parent__" not in compat.__dict__

    def test_property_leaves_existing_parent_untouched(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        explicit = mock.Mock(name="explicit-parent")
        other = mock.Mock(name="get-site-tool")
        site = mock.Mock(portal_catalog=other)

        compat = _CatalogCompat.__new__(_CatalogCompat)
        compat.__dict__["_raw_indexes"] = PersistentMapping()
        compat.__dict__["__parent__"] = explicit

        with mock.patch("plone.pgcatalog.maintenance.getSite", return_value=site):
            _view = compat.indexes

        # Still the explicit parent — self-heal must only set when missing.
        assert compat.__dict__["__parent__"] is explicit


# ── getIndex no longer falls back to the raw index silently ─────────────


class TestGetIndexWithoutParent:
    def test_finds_catalog_via_get_site_returns_wrapped(self, pg_conn_with_catalog):
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.pgindex import PGIndex

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        compat = _CatalogCompat()
        tool._catalog = compat
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        # no __parent__, no acquisition — only getSite works
        site = mock.Mock(portal_catalog=tool)
        with mock.patch("plone.pgcatalog.maintenance.getSite", return_value=site):
            result = compat.getIndex("portal_type")

        assert isinstance(result, PGIndex)
        assert result._wrapped is raw

    def test_raises_runtimeerror_when_all_three_fail(self):
        from plone.pgcatalog.maintenance import _CatalogCompat

        import pytest

        compat = _CatalogCompat()
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        with (
            mock.patch("plone.pgcatalog.maintenance.getSite", return_value=None),
            pytest.raises(RuntimeError, match="cannot find portal_catalog"),
        ):
            compat.getIndex("portal_type")

    def test_explicit_parent_still_works(self, pg_conn_with_catalog):
        """Regression guard — the happy path remains unchanged."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.maintenance import _CatalogCompat
        from plone.pgcatalog.pgindex import PGIndex

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        tool._get_pg_read_connection = lambda: pg_conn_with_catalog
        compat = _CatalogCompat(parent=tool)
        tool._catalog = compat
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw

        result = compat.getIndex("portal_type")
        assert isinstance(result, PGIndex)
