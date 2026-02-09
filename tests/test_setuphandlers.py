"""Tests for plone.pgcatalog.setuphandlers — GenericSetup install handler."""

from plone.pgcatalog.setuphandlers import _ensure_catalog_indexes
from plone.pgcatalog.setuphandlers import install
from unittest import mock


class TestInstall:

    def test_skips_without_sentinel_file(self):
        context = mock.Mock()
        context.readDataFile.return_value = None
        install(context)
        context.getSite.assert_not_called()

    def test_calls_ensure_catalog_indexes(self):
        context = mock.Mock()
        context.readDataFile.return_value = "sentinel"
        site = mock.Mock()
        context.getSite.return_value = site
        with mock.patch(
            "plone.pgcatalog.setuphandlers._ensure_catalog_indexes"
        ) as ensure_mock:
            install(context)
            ensure_mock.assert_called_once_with(site)


class TestEnsureCatalogIndexes:

    def test_skips_if_catalog_has_indexes(self):
        site = mock.Mock()
        site.portal_catalog.indexes.return_value = ["UID", "Title"]
        _ensure_catalog_indexes(site)
        # Should not try to run import steps
        assert not hasattr(site, "portal_setup") or \
            not site.portal_setup.runImportStepFromProfile.called

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
