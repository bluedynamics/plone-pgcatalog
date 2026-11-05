"""Tests for pgcatalog's plone.observability content metric provider."""

from plone.pgcatalog.indexing import catalog_object
from tests.conftest import insert_object

import pytest


@pytest.fixture(autouse=True)
def _clear_metric_cache():
    """The provider caches process-globally; reset around each test."""
    from plone.pgcatalog import observability

    observability._cache.clear()
    yield
    observability._cache.clear()


class TestAggregate:
    def test_counts_by_type_and_state_for_given_site(self, pg_conn_with_catalog):
        from plone.pgcatalog.observability import _aggregate

        conn = pg_conn_with_catalog
        for zoid in (1, 2, 3, 4):
            insert_object(conn, zoid=zoid)
        catalog_object(
            conn,
            zoid=1,
            path="/sitea/d1",
            idx={"portal_type": "Document", "review_state": "published"},
        )
        catalog_object(
            conn,
            zoid=2,
            path="/sitea/d2",
            idx={"portal_type": "Document", "review_state": "private"},
        )
        catalog_object(
            conn,
            zoid=3,
            path="/sitea/n1",
            idx={"portal_type": "News Item", "review_state": "published"},
        )
        catalog_object(
            conn,
            zoid=4,
            path="/siteb/d3",
            idx={"portal_type": "Document", "review_state": "published"},
        )
        conn.commit()

        metrics = _aggregate(conn, ["sitea"])

        totals = {
            (m.labels["portal_type"], m.labels["site"]): m.value
            for m in metrics
            if m.name == "plone_content_total"
        }
        states = {
            (m.labels["state"], m.labels["site"]): m.value
            for m in metrics
            if m.name == "plone_content_by_state"
        }
        assert totals == {("Document", "sitea"): 2, ("News Item", "sitea"): 1}
        assert states == {("published", "sitea"): 2, ("private", "sitea"): 1}
        assert all(m.type == "gauge" and m.scope == "global" for m in metrics)
        assert all(m.labels["site"] == "sitea" for m in metrics)

    def test_excludes_null_index_values_and_other_sites(self, pg_conn_with_catalog):
        from plone.pgcatalog.observability import _aggregate

        conn = pg_conn_with_catalog
        for zoid in (10, 11):
            insert_object(conn, zoid=zoid)
        # no portal_type / review_state in idx → excluded
        catalog_object(conn, zoid=10, path="/sitea/x", idx={"Title": "no type"})
        # different site → excluded by the site filter
        catalog_object(
            conn,
            zoid=11,
            path="/siteb/y",
            idx={"portal_type": "Document", "review_state": "published"},
        )
        conn.commit()

        assert _aggregate(conn, ["sitea"]) == []


class _FakeCatalog:
    pass


class _FakeIndex:
    def __init__(self, counts):
        self._counts = counts

    def uniqueValues(self, withLengths=False):
        return list(self._counts.items())


class _FakeZCatalog:
    def __init__(self, indexes):
        self.Indexes = indexes


class _FakeSite:
    def __init__(self, site_id, catalog):
        self._id = site_id
        self._catalog = catalog

    def getId(self):
        return self._id

    def unrestrictedTraverse(self, name, default=None):
        if name == "portal_catalog":
            return self._catalog
        return default


class _FakeApp:
    def __init__(self, sites):
        self._sites = sites

    def objectValues(self):
        return self._sites


def _site(site_id, catalog):
    from plone.base.interfaces import IPloneSiteRoot
    from zope.interface import alsoProvides

    site = _FakeSite(site_id, catalog)
    alsoProvides(site, IPloneSiteRoot)
    return site


def _pg_catalog():
    from plone.pgcatalog.interfaces import IPGCatalogTool
    from zope.interface import alsoProvides

    catalog = _FakeCatalog()
    alsoProvides(catalog, IPGCatalogTool)
    return catalog


class TestSiteSelection:
    def test_splits_sites_by_catalog_backend(self):
        from plone.pgcatalog.observability import _classify_sites

        zcatalog = _FakeCatalog()  # not IPGCatalogTool
        app = _FakeApp(
            [
                _site("sitea", _pg_catalog()),
                _site("siteb", zcatalog),
                _site("sitec", None),  # no catalog: skipped
                _FakeSite("notasite", _pg_catalog()),  # not IPloneSiteRoot
            ]
        )

        assert _classify_sites(app) == (["sitea"], [("siteb", zcatalog)])


class TestZCatalogMetrics:
    def test_counts_from_index_api(self):
        from plone.pgcatalog.observability import _zcatalog_metrics

        catalog = _FakeZCatalog(
            {
                "portal_type": _FakeIndex({"Document": 2, "News Item": 1}),
                "review_state": _FakeIndex({"published": 3}),
            }
        )

        metrics = _zcatalog_metrics("siteb", catalog)

        assert {
            (m.name, tuple(sorted(m.labels.items())), m.value) for m in metrics
        } == {
            (
                "plone_content_total",
                (("portal_type", "Document"), ("site", "siteb")),
                2,
            ),
            (
                "plone_content_total",
                (("portal_type", "News Item"), ("site", "siteb")),
                1,
            ),
            (
                "plone_content_by_state",
                (("site", "siteb"), ("state", "published")),
                3,
            ),
        }
        assert all(m.type == "gauge" and m.scope == "global" for m in metrics)

    def test_missing_index_is_skipped(self):
        from plone.pgcatalog.observability import _zcatalog_metrics

        catalog = _FakeZCatalog({"portal_type": _FakeIndex({"Document": 1})})

        metrics = _zcatalog_metrics("siteb", catalog)

        assert [m.name for m in metrics] == ["plone_content_total"]


class _SpyPool:
    def __init__(self, conn):
        self._conn = conn

    def connection(self):
        import contextlib

        @contextlib.contextmanager
        def _cm():
            yield self._conn

        return _cm()


def _metric(site):
    from plone.observability.metric import Metric

    return Metric(
        name="plone_content_total",
        value=1,
        type="gauge",
        scope="global",
        help="h",
        labels={"portal_type": "Document", "site": site},
    )


class TestCollect:
    def test_yields_nothing_without_sites(self, monkeypatch):
        from plone.pgcatalog import observability

        monkeypatch.setattr(observability, "_classify_sites", lambda ctx: ([], []))
        provider = observability.PGContentMetricProvider(object())
        assert list(provider.collect()) == []

    def test_caches_within_ttl(self, monkeypatch):
        from plone.pgcatalog import observability

        calls = {"n": 0}
        sample = [_metric("sitea")]

        def fake_aggregate(conn, site_ids):
            calls["n"] += 1
            return sample

        monkeypatch.setattr(
            observability, "_classify_sites", lambda ctx: (["sitea"], [])
        )
        monkeypatch.setattr(
            observability, "get_pool", lambda ctx: _SpyPool(conn=object())
        )
        monkeypatch.setattr(observability, "_aggregate", fake_aggregate)
        monkeypatch.setenv("PLONE_OBSERVABILITY_METRICS_CACHE_TTL", "300")

        provider = observability.PGContentMetricProvider(object())
        first = list(provider.collect())
        second = list(observability.PGContentMetricProvider(object()).collect())

        assert first == second == sample
        assert calls["n"] == 1  # second call served from the process-global cache

    def test_covers_pg_and_zcatalog_sites(self, monkeypatch):
        from plone.pgcatalog import observability

        zcatalog = _FakeZCatalog({"portal_type": _FakeIndex({"Document": 5})})
        monkeypatch.setattr(
            observability,
            "_classify_sites",
            lambda ctx: (["sitea"], [("siteb", zcatalog)]),
        )
        monkeypatch.setattr(
            observability, "get_pool", lambda ctx: _SpyPool(conn=object())
        )
        monkeypatch.setattr(
            observability, "_aggregate", lambda conn, ids: [_metric("sitea")]
        )

        metrics = list(observability.PGContentMetricProvider(object()).collect())

        assert [(m.labels["site"], m.value) for m in metrics] == [
            ("sitea", 1),
            ("siteb", 5),
        ]

    def test_degrades_without_pool_and_does_not_cache(self, monkeypatch):
        from plone.pgcatalog import observability

        zcatalog = _FakeZCatalog({"portal_type": _FakeIndex({"Document": 5})})
        monkeypatch.setattr(
            observability,
            "_classify_sites",
            lambda ctx: (["sitea"], [("siteb", zcatalog)]),
        )

        def boom(ctx):
            raise RuntimeError("no pool")

        monkeypatch.setattr(observability, "get_pool", boom)
        provider = observability.PGContentMetricProvider(object())

        # ZCatalog sites are still reported, the PG part is retried next time
        assert [m.labels["site"] for m in provider.collect()] == ["siteb"]
        assert observability._cache == {}


class TestRegistration:
    """Load the real ZCML: plone.observability's metrics, then our overrides."""

    def test_replaces_generic_content_provider(self):
        from OFS.interfaces import IApplication
        from plone.observability.interfaces import IMetricProvider
        from plone.observability.metrics.providers.content import ContentMetricProvider
        from plone.pgcatalog.observability import PGContentMetricProvider
        from plone.testing import zca
        from zope.component import getAdapters
        from zope.configuration import xmlconfig
        from zope.interface import alsoProvides

        zca.pushGlobalRegistry()
        try:
            xmlconfig.string(
                """
                <configure xmlns="http://namespaces.zope.org/zope">
                  <include package="Products.Five" file="meta.zcml" />
                  <include package="Products.GenericSetup" file="meta.zcml" />
                  <include package="plone.observability.metrics" />
                  <includeOverrides package="plone.pgcatalog" file="overrides.zcml" />
                </configure>
                """,
                context=zca.stackConfigurationContext(None),
            )
            app = _FakeApp([])
            alsoProvides(app, IApplication)
            providers = dict(getAdapters((app,), IMetricProvider))
        finally:
            zca.popGlobalRegistry()

        assert isinstance(providers["content"], PGContentMetricProvider)
        assert not any(
            isinstance(provider, ContentMetricProvider)
            for provider in providers.values()
        )
        assert "pgcontent" not in providers
