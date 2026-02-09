"""Tests for security filtering + effectiveRange + show_inactive.

Tests both the apply_security_filters() function (unit) and
secured queries against real PG (integration).
"""

from datetime import datetime
from datetime import timezone

from tests.conftest import insert_object

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.query import apply_security_filters
from plone.pgcatalog.query import execute_query


# ---------------------------------------------------------------------------
# Unit tests: apply_security_filters
# ---------------------------------------------------------------------------


class TestApplySecurityFilters:

    def test_injects_roles(self):
        query = {"portal_type": "Document"}
        result = apply_security_filters(query, roles=["Anonymous"])
        assert "allowedRolesAndUsers" in result
        assert result["allowedRolesAndUsers"]["query"] == ["Anonymous"]
        assert result["allowedRolesAndUsers"]["operator"] == "or"

    def test_does_not_overwrite_existing_roles(self):
        query = {
            "portal_type": "Document",
            "allowedRolesAndUsers": {"query": ["Manager"], "operator": "or"},
        }
        result = apply_security_filters(query, roles=["Anonymous"])
        assert result["allowedRolesAndUsers"]["query"] == ["Manager"]

    def test_injects_effective_range(self):
        query = {"portal_type": "Document"}
        result = apply_security_filters(query, roles=["Anonymous"])
        assert "effectiveRange" in result
        assert isinstance(result["effectiveRange"], datetime)

    def test_does_not_overwrite_existing_effective_range(self):
        now = datetime(2025, 6, 15, tzinfo=timezone.utc)
        query = {"portal_type": "Document", "effectiveRange": now}
        result = apply_security_filters(query, roles=["Anonymous"])
        assert result["effectiveRange"] == now

    def test_show_inactive_skips_effective_range(self):
        query = {"portal_type": "Document"}
        result = apply_security_filters(
            query, roles=["Anonymous"], show_inactive=True
        )
        assert "effectiveRange" not in result

    def test_show_inactive_in_query_dict(self):
        query = {"portal_type": "Document", "show_inactive": True}
        result = apply_security_filters(query, roles=["Anonymous"])
        assert "effectiveRange" not in result
        assert "show_inactive" not in result  # cleaned up

    def test_does_not_mutate_original(self):
        query = {"portal_type": "Document"}
        result = apply_security_filters(query, roles=["Anonymous"])
        assert "allowedRolesAndUsers" not in query
        assert "allowedRolesAndUsers" in result

    def test_multiple_roles(self):
        query = {}
        result = apply_security_filters(
            query, roles=["Anonymous", "Authenticated", "user:admin"]
        )
        assert set(result["allowedRolesAndUsers"]["query"]) == {
            "Anonymous",
            "Authenticated",
            "user:admin",
        }


# ---------------------------------------------------------------------------
# Integration tests: secured queries against real PG
# ---------------------------------------------------------------------------


def _setup_security_data(conn):
    """Insert objects with different security settings."""
    objects = [
        {
            "zoid": 400,
            "path": "/plone/public-doc",
            "idx": {
                "portal_type": "Document",
                "allowedRolesAndUsers": ["Anonymous"],
                "effective": "2025-01-01T00:00:00+00:00",
                "expires": None,
            },
        },
        {
            "zoid": 401,
            "path": "/plone/private-doc",
            "idx": {
                "portal_type": "Document",
                "allowedRolesAndUsers": ["Manager", "user:admin"],
                "effective": "2025-01-01T00:00:00+00:00",
                "expires": None,
            },
        },
        {
            "zoid": 402,
            "path": "/plone/auth-doc",
            "idx": {
                "portal_type": "Document",
                "allowedRolesAndUsers": ["Authenticated", "Manager"],
                "effective": "2025-01-01T00:00:00+00:00",
                "expires": None,
            },
        },
        {
            "zoid": 403,
            "path": "/plone/future-doc",
            "idx": {
                "portal_type": "Document",
                "allowedRolesAndUsers": ["Anonymous"],
                "effective": "2030-01-01T00:00:00+00:00",
                "expires": None,
            },
        },
        {
            "zoid": 404,
            "path": "/plone/expired-doc",
            "idx": {
                "portal_type": "Document",
                "allowedRolesAndUsers": ["Anonymous"],
                "effective": "2020-01-01T00:00:00+00:00",
                "expires": "2024-12-31T23:59:59+00:00",
            },
        },
        {
            "zoid": 405,
            "path": "/plone/active-doc",
            "idx": {
                "portal_type": "Document",
                "allowedRolesAndUsers": ["Anonymous"],
                "effective": "2025-01-01T00:00:00+00:00",
                "expires": "2026-12-31T23:59:59+00:00",
            },
        },
    ]
    for obj in objects:
        insert_object(conn, zoid=obj["zoid"])
        catalog_object(conn, zoid=obj["zoid"], path=obj["path"], idx=obj["idx"])
    conn.commit()


def _query_zoids(conn, query_dict):
    rows = execute_query(conn, query_dict, columns="zoid")
    return sorted(row["zoid"] for row in rows)


class TestSecurityFilterIntegration:

    def test_anonymous_sees_public_only(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        query = apply_security_filters(
            {"portal_type": "Document"},
            roles=["Anonymous"],
            show_inactive=True,  # skip date filter for this test
        )
        zoids = _query_zoids(conn, query)
        # Anonymous: 400, 403, 404, 405 (all with Anonymous in roles)
        assert 400 in zoids
        assert 401 not in zoids  # Manager only
        assert 402 not in zoids  # Authenticated only
        assert 403 in zoids
        assert 404 in zoids
        assert 405 in zoids

    def test_authenticated_sees_more(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        query = apply_security_filters(
            {"portal_type": "Document"},
            roles=["Anonymous", "Authenticated"],
            show_inactive=True,
        )
        zoids = _query_zoids(conn, query)
        assert 400 in zoids  # Anonymous
        assert 401 not in zoids  # Manager only
        assert 402 in zoids  # Authenticated
        assert 403 in zoids
        assert 405 in zoids

    def test_admin_sees_private(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        query = apply_security_filters(
            {"portal_type": "Document"},
            roles=["Anonymous", "Authenticated", "Manager", "user:admin"],
            show_inactive=True,
        )
        zoids = _query_zoids(conn, query)
        # Admin sees everything
        assert set(zoids) == {400, 401, 402, 403, 404, 405}

    def test_unrestricted_bypasses_security(self, pg_conn_with_catalog):
        """Without apply_security_filters, no security filter is applied."""
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        # Direct query without security filters
        zoids = _query_zoids(conn, {"portal_type": "Document"})
        assert set(zoids) == {400, 401, 402, 403, 404, 405}


class TestEffectiveRangeIntegration:

    def test_effective_range_filters_future(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        now = datetime(2025, 6, 15, tzinfo=timezone.utc)
        query = apply_security_filters(
            {"portal_type": "Document"},
            roles=["Anonymous"],
        )
        # Override auto-generated effectiveRange with known date
        query["effectiveRange"] = now
        zoids = _query_zoids(conn, query)
        # 400: eff=Jan2025 ✓, exp=None ✓ → in
        # 403: eff=Jan2030 > Jun2025 → out (future)
        # 404: eff=Jan2020 ✓, exp=Dec2024 < Jun2025 → out (expired)
        # 405: eff=Jan2025 ✓, exp=Dec2026 > Jun2025 ✓ → in
        assert set(zoids) == {400, 405}

    def test_show_inactive_bypasses_effective_range(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        query = apply_security_filters(
            {"portal_type": "Document"},
            roles=["Anonymous"],
            show_inactive=True,
        )
        zoids = _query_zoids(conn, query)
        # No effectiveRange filter, but still security filtered
        assert set(zoids) == {400, 403, 404, 405}

    def test_expired_null_never_expires(self, pg_conn_with_catalog):
        """Objects with expires=None never expire."""
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        far_future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        query = apply_security_filters(
            {"portal_type": "Document"},
            roles=["Anonymous"],
        )
        query["effectiveRange"] = far_future
        zoids = _query_zoids(conn, query)
        # 400: exp=None → never expires → in
        # 403: eff=2030 ✓ (before 2099), exp=None → in
        # 404: exp=2024 < 2099 → out
        # 405: exp=2026 < 2099 → out
        assert set(zoids) == {400, 403}

    def test_combined_security_and_date(self, pg_conn_with_catalog):
        """Both security AND effectiveRange applied together."""
        conn = pg_conn_with_catalog
        _setup_security_data(conn)
        now = datetime(2025, 6, 15, tzinfo=timezone.utc)
        # Authenticated user
        query = apply_security_filters(
            {"portal_type": "Document"},
            roles=["Anonymous", "Authenticated"],
        )
        query["effectiveRange"] = now
        zoids = _query_zoids(conn, query)
        # Security: Anon sees 400,403,404,405; Auth also sees 402
        # Date: 403 future→out, 404 expired→out
        # Result: 400, 402, 405
        assert set(zoids) == {400, 402, 405}
