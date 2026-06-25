"""Tests for the pgcontent plone.observability metric provider."""

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


class TestSiteSelection:
    def test_only_ipgcatalogtool_sites(self):
        from plone.base.interfaces import IPloneSiteRoot
        from plone.pgcatalog.interfaces import IPGCatalogTool
        from plone.pgcatalog.observability import _pg_site_ids
        from zope.interface import alsoProvides

        pg_catalog = _FakeCatalog()
        alsoProvides(pg_catalog, IPGCatalogTool)
        zcatalog = _FakeCatalog()  # not IPGCatalogTool

        pg_site = _FakeSite("sitea", pg_catalog)
        alsoProvides(pg_site, IPloneSiteRoot)
        z_site = _FakeSite("siteb", zcatalog)
        alsoProvides(z_site, IPloneSiteRoot)

        assert _pg_site_ids(_FakeApp([pg_site, z_site])) == ["sitea"]


class _SpyPool:
    def __init__(self, conn):
        self._conn = conn

    def connection(self):
        import contextlib

        @contextlib.contextmanager
        def _cm():
            yield self._conn

        return _cm()


class TestCollect:
    def test_yields_nothing_without_pg_sites(self, monkeypatch):
        from plone.pgcatalog import observability

        monkeypatch.setattr(observability, "_pg_site_ids", lambda ctx: [])
        provider = observability.PGContentMetricProvider(object())
        assert list(provider.collect()) == []

    def test_caches_within_ttl(self, monkeypatch):
        from plone.observability.metric import Metric
        from plone.pgcatalog import observability

        calls = {"n": 0}
        sample = [
            Metric(
                name="plone_content_total",
                value=1,
                type="gauge",
                scope="global",
                help="h",
                labels={"portal_type": "Document", "site": "sitea"},
            )
        ]

        def fake_aggregate(conn, site_ids):
            calls["n"] += 1
            return sample

        monkeypatch.setattr(observability, "_pg_site_ids", lambda ctx: ["sitea"])
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

    def test_degrades_silently_without_pool(self, monkeypatch):
        from plone.pgcatalog import observability

        monkeypatch.setattr(observability, "_pg_site_ids", lambda ctx: ["sitea"])

        def boom(ctx):
            raise RuntimeError("no pool")

        monkeypatch.setattr(observability, "get_pool", boom)
        provider = observability.PGContentMetricProvider(object())
        assert list(provider.collect()) == []
