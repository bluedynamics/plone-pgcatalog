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
