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


# ── Wrapper contract when no catalog context is reachable ─────────────────


class TestViewWithoutCatalog:
    def test_view_returns_raw_on_getitem_when_no_catalog(self):
        """Without an acquisition parent, accessing a key returns the raw index."""
        compat = _fresh_compat()
        raw = mock.Mock(id="portal_type", meta_type="FieldIndex")
        compat._raw_indexes["portal_type"] = raw
        # No acquisition parent — should fall back to raw
        assert compat.indexes["portal_type"] is raw

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
