"""Tests for plone.pgcatalog.setuphandlers — GenericSetup install handler.

Tests the snapshot/replace/restore flow that preserves addon index
definitions when replacing portal_catalog with PlonePGCatalogTool.
"""

from plone.pgcatalog.setuphandlers import _build_extra
from plone.pgcatalog.setuphandlers import _ensure_catalog_indexes
from plone.pgcatalog.setuphandlers import _replace_catalog
from plone.pgcatalog.setuphandlers import _restore_from_snapshot
from plone.pgcatalog.setuphandlers import _snapshot_catalog
from plone.pgcatalog.setuphandlers import install
from unittest import mock


# ---------------------------------------------------------------------------
# Helpers for building mock catalogs with realistic index objects
# ---------------------------------------------------------------------------


def _make_index(meta_type, name, source_attrs=None, **extra_attrs):
    """Create a mock index object with the given meta_type and attributes."""
    idx = mock.Mock()
    idx.meta_type = meta_type
    idx.id = name
    idx.getId.return_value = name
    idx.getIndexSourceNames.return_value = source_attrs or [name]

    # Set extra attributes (since_field, until_field, lexicon_id, etc.)
    for attr, val in extra_attrs.items():
        setattr(idx, attr, val)

    # DateRangeIndex has getSinceField/getUntilField methods
    if meta_type == "DateRangeIndex":
        idx.getSinceField.return_value = extra_attrs.get("since_field", "")
        idx.getUntilField.return_value = extra_attrs.get("until_field", "")

    return idx


def _make_catalog_with_indexes(indexes_dict, metadata=None):
    """Build a mock catalog object with given indexes and metadata.

    indexes_dict: {name: index_mock, ...}
    metadata: list of column names (default: empty)
    """
    catalog = mock.Mock()
    catalog._catalog.indexes.items.return_value = list(indexes_dict.items())
    catalog._catalog.schema.keys.return_value = metadata or []
    catalog.indexes.return_value = list(indexes_dict.keys())

    # Make objectIds() return something (for _replace_catalog check)
    catalog.objectIds.return_value = []

    return catalog


# ===========================================================================
# _snapshot_catalog
# ===========================================================================


class TestSnapshotCatalog:
    def test_captures_field_index(self):
        indexes = {
            "my_field": _make_index("FieldIndex", "my_field", ["my_attr"]),
        }
        catalog = _make_catalog_with_indexes(indexes)
        snap = _snapshot_catalog(catalog)

        assert "my_field" in snap["indexes"]
        entry = snap["indexes"]["my_field"]
        assert entry["meta_type"] == "FieldIndex"
        assert entry["source_attrs"] == ["my_attr"]

    def test_captures_keyword_index(self):
        indexes = {
            "tags": _make_index("KeywordIndex", "tags", ["Subject"]),
        }
        catalog = _make_catalog_with_indexes(indexes)
        snap = _snapshot_catalog(catalog)

        entry = snap["indexes"]["tags"]
        assert entry["meta_type"] == "KeywordIndex"
        assert entry["source_attrs"] == ["Subject"]

    def test_captures_date_range_index(self):
        indexes = {
            "effectiveRange": _make_index(
                "DateRangeIndex",
                "effectiveRange",
                since_field="effective",
                until_field="expires",
            ),
        }
        catalog = _make_catalog_with_indexes(indexes)
        snap = _snapshot_catalog(catalog)

        entry = snap["indexes"]["effectiveRange"]
        assert entry["meta_type"] == "DateRangeIndex"
        assert entry["since_field"] == "effective"
        assert entry["until_field"] == "expires"

    def test_captures_zctext_index(self):
        indexes = {
            "SearchableText": _make_index(
                "ZCTextIndex",
                "SearchableText",
                ["SearchableText"],
                lexicon_id="plone_lexicon",
                index_type="Okapi BM25 Rank",
            ),
        }
        catalog = _make_catalog_with_indexes(indexes)
        snap = _snapshot_catalog(catalog)

        entry = snap["indexes"]["SearchableText"]
        assert entry["meta_type"] == "ZCTextIndex"
        assert entry["lexicon_id"] == "plone_lexicon"
        assert entry["index_type"] == "Okapi BM25 Rank"
        assert entry["source_attrs"] == ["SearchableText"]

    def test_captures_date_recurring_index(self):
        indexes = {
            "start": _make_index(
                "DateRecurringIndex",
                "start",
                attr_recurdef="recurrence",
                attr_until="open_end",
            ),
        }
        catalog = _make_catalog_with_indexes(indexes)
        snap = _snapshot_catalog(catalog)

        entry = snap["indexes"]["start"]
        assert entry["meta_type"] == "DateRecurringIndex"
        assert entry["attr_recurdef"] == "recurrence"
        assert entry["attr_until"] == "open_end"

    def test_captures_date_range_in_range_index(self):
        indexes = {
            "eventrange": _make_index(
                "DateRangeInRangeIndex",
                "eventrange",
                startindex="start",
                endindex="end",
            ),
        }
        catalog = _make_catalog_with_indexes(indexes)
        snap = _snapshot_catalog(catalog)

        entry = snap["indexes"]["eventrange"]
        assert entry["startindex"] == "start"
        assert entry["endindex"] == "end"

    def test_captures_metadata_columns(self):
        catalog = _make_catalog_with_indexes(
            {},
            metadata=["Title", "Description", "getObjSize"],
        )
        snap = _snapshot_catalog(catalog)
        assert snap["metadata"] == ["Title", "Description", "getObjSize"]

    def test_captures_multiple_indexes_and_metadata(self):
        indexes = {
            "portal_type": _make_index("FieldIndex", "portal_type"),
            "Subject": _make_index("KeywordIndex", "Subject"),
            "created": _make_index("DateIndex", "created"),
        }
        catalog = _make_catalog_with_indexes(
            indexes,
            metadata=["Title", "Creator"],
        )
        snap = _snapshot_catalog(catalog)
        assert len(snap["indexes"]) == 3
        assert len(snap["metadata"]) == 2

    def test_handles_missing_catalog_attr(self):
        """Gracefully handles catalog without _catalog attribute."""
        catalog = mock.Mock(spec=[])
        snap = _snapshot_catalog(catalog)
        assert snap["indexes"] == {}
        assert snap["metadata"] == []

    def test_handles_index_without_getIndexSourceNames(self):
        """Falls back to [name] when getIndexSourceNames is missing."""
        idx = mock.Mock(spec=["meta_type", "id"])
        idx.meta_type = "FieldIndex"
        idx.id = "custom"
        indexes = {"custom": idx}
        catalog = _make_catalog_with_indexes(indexes)
        snap = _snapshot_catalog(catalog)
        assert snap["indexes"]["custom"]["source_attrs"] == ["custom"]

    def test_handles_getIndexSourceNames_exception(self):
        """Falls back to [name] when getIndexSourceNames raises."""
        idx = _make_index("FieldIndex", "broken")
        idx.getIndexSourceNames.side_effect = RuntimeError("broken")
        catalog = _make_catalog_with_indexes({"broken": idx})
        snap = _snapshot_catalog(catalog)
        assert snap["indexes"]["broken"]["source_attrs"] == ["broken"]


# ===========================================================================
# _build_extra
# ===========================================================================


class TestBuildExtra:
    def test_field_index_extra(self):
        entry = {"meta_type": "FieldIndex", "source_attrs": ["portal_type"]}
        extra = _build_extra(entry)
        assert extra.indexed_attrs == "portal_type"

    def test_keyword_index_multiple_attrs(self):
        entry = {"meta_type": "KeywordIndex", "source_attrs": ["Subject", "Tags"]}
        extra = _build_extra(entry)
        assert extra.indexed_attrs == "Subject,Tags"

    def test_date_range_index_extra(self):
        entry = {
            "meta_type": "DateRangeIndex",
            "since_field": "effective",
            "until_field": "expires",
        }
        extra = _build_extra(entry)
        assert extra.since_field == "effective"
        assert extra.until_field == "expires"

    def test_zctext_index_extra(self):
        entry = {
            "meta_type": "ZCTextIndex",
            "source_attrs": ["SearchableText"],
            "lexicon_id": "plone_lexicon",
            "index_type": "Okapi BM25 Rank",
        }
        extra = _build_extra(entry)
        assert extra.lexicon_id == "plone_lexicon"
        assert extra.index_type == "Okapi BM25 Rank"
        assert extra.doc_attr == "SearchableText"

    def test_no_source_attrs(self):
        entry = {"meta_type": "FieldIndex"}
        extra = _build_extra(entry)
        assert not hasattr(extra, "indexed_attrs")


# ===========================================================================
# _replace_catalog
# ===========================================================================


class TestReplaceCatalog:
    def test_replaces_existing_catalog(self):
        site = mock.Mock()
        site.objectIds.return_value = ["portal_catalog"]
        _replace_catalog(site)
        site._delObject.assert_called_once_with("portal_catalog")
        site._setObject.assert_called_once()
        args = site._setObject.call_args[0]
        assert args[0] == "portal_catalog"

    def test_creates_catalog_when_none_exists(self):
        site = mock.Mock()
        site.objectIds.return_value = []
        _replace_catalog(site)
        site._delObject.assert_not_called()
        site._setObject.assert_called_once()

    def test_new_catalog_is_PlonePGCatalogTool(self):
        from plone.pgcatalog.catalog import PlonePGCatalogTool

        site = mock.Mock()
        site.objectIds.return_value = []
        _replace_catalog(site)
        new_tool = site._setObject.call_args[0][1]
        assert isinstance(new_tool, PlonePGCatalogTool)


# ===========================================================================
# _restore_from_snapshot
# ===========================================================================


class TestRestoreFromSnapshot:
    def test_restores_addon_index(self):
        """Addon indexes not in the fresh catalog are restored."""
        site = mock.Mock()
        # Fresh catalog has only UID (from core profiles)
        site.portal_catalog.indexes.return_value = ["UID"]
        site.portal_catalog._catalog.schema.keys.return_value = ["Title"]

        snapshot = {
            "indexes": {
                "UID": {
                    "meta_type": "UUIDIndex",
                    "source_attrs": ["UID"],
                },
                "my_addon_index": {
                    "meta_type": "FieldIndex",
                    "source_attrs": ["my_attr"],
                },
            },
            "metadata": ["Title", "my_addon_column"],
        }

        _restore_from_snapshot(site, snapshot)

        # UID should be skipped (already exists), addon index restored
        add_calls = site.portal_catalog.addIndex.call_args_list
        assert len(add_calls) == 1
        assert add_calls[0][0][0] == "my_addon_index"
        assert add_calls[0][0][1] == "FieldIndex"

        # Title should be skipped, addon column restored
        col_calls = site.portal_catalog.addColumn.call_args_list
        assert len(col_calls) == 1
        assert col_calls[0][0][0] == "my_addon_column"

    def test_skips_all_existing_indexes(self):
        """When all snapshot indexes already exist, nothing is restored."""
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = ["UID", "portal_type"]
        site.portal_catalog._catalog.schema.keys.return_value = ["Title"]

        snapshot = {
            "indexes": {
                "UID": {"meta_type": "UUIDIndex", "source_attrs": ["UID"]},
                "portal_type": {
                    "meta_type": "FieldIndex",
                    "source_attrs": ["portal_type"],
                },
            },
            "metadata": ["Title"],
        }

        _restore_from_snapshot(site, snapshot)
        site.portal_catalog.addIndex.assert_not_called()
        site.portal_catalog.addColumn.assert_not_called()

    def test_skips_entry_without_meta_type(self):
        """Entries with meta_type=None are skipped."""
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = []
        site.portal_catalog._catalog.schema.keys.return_value = []

        snapshot = {
            "indexes": {
                "broken": {"meta_type": None, "source_attrs": ["x"]},
            },
            "metadata": [],
        }

        _restore_from_snapshot(site, snapshot)
        site.portal_catalog.addIndex.assert_not_called()

    def test_handles_addIndex_exception(self):
        """addIndex failure is logged but doesn't stop other indexes."""
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = []
        site.portal_catalog._catalog.schema.keys.return_value = []
        site.portal_catalog.addIndex.side_effect = [
            RuntimeError("fail"),
            None,  # second call succeeds
        ]

        snapshot = {
            "indexes": {
                "bad_index": {"meta_type": "FieldIndex", "source_attrs": ["x"]},
                "good_index": {"meta_type": "FieldIndex", "source_attrs": ["y"]},
            },
            "metadata": [],
        }

        _restore_from_snapshot(site, snapshot)
        assert site.portal_catalog.addIndex.call_count == 2

    def test_handles_addColumn_exception(self):
        """addColumn failure is logged but doesn't stop other columns."""
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = []
        site.portal_catalog._catalog.schema.keys.return_value = []
        site.portal_catalog.addColumn.side_effect = [
            RuntimeError("fail"),
            None,
        ]

        snapshot = {
            "indexes": {},
            "metadata": ["bad_col", "good_col"],
        }

        _restore_from_snapshot(site, snapshot)
        assert site.portal_catalog.addColumn.call_count == 2

    def test_passes_extra_to_addIndex(self):
        """The extra object built from snapshot is passed to addIndex."""
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = []
        site.portal_catalog._catalog.schema.keys.return_value = []

        snapshot = {
            "indexes": {
                "effectiveRange": {
                    "meta_type": "DateRangeIndex",
                    "source_attrs": ["effectiveRange"],
                    "since_field": "effective",
                    "until_field": "expires",
                },
            },
            "metadata": [],
        }

        _restore_from_snapshot(site, snapshot)
        extra = site.portal_catalog.addIndex.call_args[0][2]
        assert extra.since_field == "effective"
        assert extra.until_field == "expires"


# ===========================================================================
# install() — integration
# ===========================================================================


class TestInstall:
    def test_skips_without_sentinel_file(self):
        context = mock.Mock()
        context.readDataFile.return_value = None
        install(context)
        context.getSite.assert_not_called()

    def test_skips_replacement_when_already_pgcatalog(self):
        """When catalog is already PlonePGCatalogTool, skip snapshot/replace."""
        from plone.pgcatalog.catalog import PlonePGCatalogTool

        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        site = mock.Mock()
        site.portal_catalog = PlonePGCatalogTool()
        context.getSite.return_value = site

        with (
            mock.patch(
                "plone.pgcatalog.setuphandlers._ensure_catalog_indexes"
            ) as ensure_mock,
            mock.patch("plone.pgcatalog.setuphandlers._remove_lexicons"),
            mock.patch("plone.pgcatalog.setuphandlers._snapshot_catalog") as snap_mock,
            mock.patch(
                "plone.pgcatalog.setuphandlers._replace_catalog"
            ) as replace_mock,
        ):
            install(context)
            # Should ensure indexes but NOT snapshot or replace
            ensure_mock.assert_called_once_with(site)
            snap_mock.assert_not_called()
            replace_mock.assert_not_called()

    def test_snapshots_and_replaces_foreign_catalog(self):
        """When catalog is a different class, snapshot → replace → restore."""
        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        site = mock.Mock()
        # Old catalog is NOT PlonePGCatalogTool
        site.portal_catalog = mock.Mock(spec=["indexes", "_catalog"])
        context.getSite.return_value = site

        with (
            mock.patch(
                "plone.pgcatalog.setuphandlers._snapshot_catalog",
                return_value={
                    "indexes": {"x": {"meta_type": "FieldIndex"}},
                    "metadata": [],
                },
            ) as snap_mock,
            mock.patch(
                "plone.pgcatalog.setuphandlers._replace_catalog"
            ) as replace_mock,
            mock.patch(
                "plone.pgcatalog.setuphandlers._ensure_catalog_indexes"
            ) as ensure_mock,
            mock.patch(
                "plone.pgcatalog.setuphandlers._restore_from_snapshot"
            ) as restore_mock,
            mock.patch("plone.pgcatalog.setuphandlers._remove_lexicons"),
        ):
            install(context)
            snap_mock.assert_called_once()
            replace_mock.assert_called_once_with(site)
            ensure_mock.assert_called_once_with(site)
            restore_mock.assert_called_once()

    def test_no_catalog_skips_snapshot(self):
        """When no portal_catalog exists, snapshot is skipped."""
        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        site = mock.Mock(spec=[])  # no portal_catalog attribute
        context.getSite.return_value = site

        with (
            mock.patch("plone.pgcatalog.setuphandlers._snapshot_catalog") as snap_mock,
            mock.patch("plone.pgcatalog.setuphandlers._replace_catalog"),
            mock.patch("plone.pgcatalog.setuphandlers._ensure_catalog_indexes"),
            mock.patch(
                "plone.pgcatalog.setuphandlers._restore_from_snapshot"
            ) as restore_mock,
            mock.patch("plone.pgcatalog.setuphandlers._remove_lexicons"),
        ):
            install(context)
            snap_mock.assert_not_called()
            restore_mock.assert_not_called()

    def test_restore_called_with_snapshot(self):
        """Verify the snapshot dict is passed to _restore_from_snapshot."""
        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        site = mock.Mock()
        site.portal_catalog = mock.Mock(spec=["indexes", "_catalog"])
        context.getSite.return_value = site

        fake_snapshot = {
            "indexes": {
                "addon_idx": {"meta_type": "FieldIndex", "source_attrs": ["x"]}
            },
            "metadata": ["addon_col"],
        }

        with (
            mock.patch(
                "plone.pgcatalog.setuphandlers._snapshot_catalog",
                return_value=fake_snapshot,
            ),
            mock.patch("plone.pgcatalog.setuphandlers._replace_catalog"),
            mock.patch("plone.pgcatalog.setuphandlers._ensure_catalog_indexes"),
            mock.patch(
                "plone.pgcatalog.setuphandlers._restore_from_snapshot"
            ) as restore_mock,
            mock.patch("plone.pgcatalog.setuphandlers._remove_lexicons"),
        ):
            install(context)
            restore_mock.assert_called_once_with(site, fake_snapshot)

    def test_order_is_snapshot_replace_ensure_restore_lexicons(self):
        """Verify the correct execution order of install steps."""
        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        site = mock.Mock()
        site.portal_catalog = mock.Mock(spec=["indexes", "_catalog"])
        context.getSite.return_value = site

        call_order = []

        with (
            mock.patch(
                "plone.pgcatalog.setuphandlers._snapshot_catalog",
                side_effect=lambda c: (
                    call_order.append("snapshot") or {"indexes": {}, "metadata": []}
                ),
            ),
            mock.patch(
                "plone.pgcatalog.setuphandlers._replace_catalog",
                side_effect=lambda s: call_order.append("replace"),
            ),
            mock.patch(
                "plone.pgcatalog.setuphandlers._ensure_catalog_indexes",
                side_effect=lambda s: call_order.append("ensure"),
            ),
            mock.patch(
                "plone.pgcatalog.setuphandlers._restore_from_snapshot",
                side_effect=lambda s, snap: call_order.append("restore"),
            ),
            mock.patch(
                "plone.pgcatalog.setuphandlers._remove_lexicons",
                side_effect=lambda s: call_order.append("lexicons"),
            ),
        ):
            install(context)

        assert call_order == ["snapshot", "replace", "ensure", "restore", "lexicons"]


# ===========================================================================
# _ensure_catalog_indexes (preserved from before)
# ===========================================================================


class TestEnsureCatalogIndexes:
    def test_skips_if_catalog_has_essential_indexes(self):
        """Skips re-apply when essential Plone indexes (UID, portal_type) exist."""
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = ["UID", "portal_type", "Title"]
        _ensure_catalog_indexes(site)
        # Should not try to run import steps
        assert (
            not hasattr(site, "portal_setup")
            or not site.portal_setup.runImportStepFromProfile.called
        )

    def test_reapplies_if_only_addon_indexes(self):
        """Re-applies Plone defaults when essential indexes are missing."""
        site = mock.Mock()
        # Addon indexes only — no UID, no portal_type
        site.portal_catalog.indexes.return_value = ["my_custom_index", "another_index"]
        _ensure_catalog_indexes(site)
        # Should have called runImportStepFromProfile for Plone profiles
        calls = site.portal_setup.runImportStepFromProfile.call_args_list
        assert len(calls) >= 1
        profile_ids = [c[0][0] for c in calls]
        assert "profile-Products.CMFPlone:plone" in profile_ids

    def test_skips_without_catalog(self):
        site = mock.Mock(spec=[])
        _ensure_catalog_indexes(site)  # Should not raise

    def test_reapplies_profiles_for_fresh_catalog(self):
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = []
        _ensure_catalog_indexes(site)
        # Should have called runImportStepFromProfile for Plone profiles
        calls = site.portal_setup.runImportStepFromProfile.call_args_list
        assert len(calls) >= 1
        profile_ids = [c[0][0] for c in calls]
        assert "profile-Products.CMFPlone:plone" in profile_ids

    def test_handles_indexes_exception(self):
        """If catalog.indexes() raises, fall through to re-apply."""
        site = mock.Mock()
        site.portal_catalog.indexes.side_effect = RuntimeError("broken")
        _ensure_catalog_indexes(site)
        # Should still try to run import steps
        calls = site.portal_setup.runImportStepFromProfile.call_args_list
        assert len(calls) >= 1

    def test_skips_without_portal_setup(self):
        """If site has no portal_setup, logs warning and returns."""
        site = mock.Mock(spec=["portal_catalog"])
        site.portal_catalog.indexes.return_value = []
        # Should not raise
        _ensure_catalog_indexes(site)

    def test_handles_profile_import_exception(self):
        """If runImportStepFromProfile raises, it's caught and logged."""
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = []
        site.portal_setup.runImportStepFromProfile.side_effect = RuntimeError(
            "import failed"
        )
        # Should not raise — all exceptions are caught
        _ensure_catalog_indexes(site)
