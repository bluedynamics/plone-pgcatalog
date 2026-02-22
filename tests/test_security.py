"""Tests for security filtering + effectiveRange + show_inactive.

Tests both the apply_security_filters() function (unit) and
secured queries against real PG (integration).
"""

from datetime import datetime
from datetime import UTC
from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.query import _execute_query
from plone.pgcatalog.query import _MAX_LIMIT
from plone.pgcatalog.query import _MAX_OFFSET
from plone.pgcatalog.query import _MAX_SEARCH_LENGTH
from plone.pgcatalog.query import _validate_path
from plone.pgcatalog.query import apply_security_filters
from plone.pgcatalog.query import build_query
from tests.conftest import insert_object

import pytest


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
        now = datetime(2025, 6, 15, tzinfo=UTC)
        query = {"portal_type": "Document", "effectiveRange": now}
        result = apply_security_filters(query, roles=["Anonymous"])
        assert result["effectiveRange"] == now

    def test_show_inactive_skips_effective_range(self):
        query = {"portal_type": "Document"}
        result = apply_security_filters(query, roles=["Anonymous"], show_inactive=True)
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
    rows = _execute_query(conn, query_dict, columns="zoid")
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
        now = datetime(2025, 6, 15, tzinfo=UTC)
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
        far_future = datetime(2099, 1, 1, tzinfo=UTC)
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
        now = datetime(2025, 6, 15, tzinfo=UTC)
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


# ---------------------------------------------------------------------------
# Unit tests: hardening fixes (CAT-H1, CAT-H3, CAT-L1, CAT-L3)
# ---------------------------------------------------------------------------


class TestLimitOffsetBounds:
    """CAT-H1: LIMIT/OFFSET clamped to safe maximums."""

    def test_sort_limit_clamped(self):
        qr = build_query({"sort_limit": 999999})
        assert qr["limit"] == _MAX_LIMIT

    def test_b_size_clamped(self):
        qr = build_query({"b_size": 999999})
        assert qr["limit"] == _MAX_LIMIT

    def test_b_start_clamped(self):
        qr = build_query({"b_start": 99999999})
        assert qr["offset"] == _MAX_OFFSET

    def test_within_bounds_unchanged(self):
        qr = build_query({"sort_limit": 50, "b_start": 100})
        assert qr["limit"] == 50
        assert qr["offset"] == 100

    def test_exact_max_limit(self):
        qr = build_query({"b_size": _MAX_LIMIT})
        assert qr["limit"] == _MAX_LIMIT

    def test_exact_max_offset(self):
        qr = build_query({"b_start": _MAX_OFFSET})
        assert qr["offset"] == _MAX_OFFSET


class TestSearchLengthTruncation:
    """CAT-H3: Full-text search queries truncated to _MAX_SEARCH_LENGTH."""

    def test_long_search_text_truncated(self):
        long_query = "a" * (_MAX_SEARCH_LENGTH + 500)
        qr = build_query({"SearchableText": long_query})
        # The param value should be truncated to _MAX_SEARCH_LENGTH
        # Find the search param (it will be in qr["params"])
        text_params = [
            v
            for v in qr["params"].values()
            if isinstance(v, str) and len(v) >= _MAX_SEARCH_LENGTH
        ]
        # All text params should be at most _MAX_SEARCH_LENGTH
        for val in text_params:
            assert len(val) <= _MAX_SEARCH_LENGTH

    def test_short_search_text_unchanged(self):
        short_query = "hello world"
        qr = build_query({"SearchableText": short_query})
        text_params = [
            v for v in qr["params"].values() if isinstance(v, str) and "hello" in v
        ]
        assert any("hello world" in v for v in text_params)


class TestPathValidationNormalization:
    """CAT-L3: _validate_path normalizes double slashes."""

    def test_double_slash_normalized(self):
        result = _validate_path("//plone//documents")
        assert result == "/plone/documents"

    def test_triple_slash_normalized(self):
        result = _validate_path("///plone///docs")
        assert result == "/plone/docs"

    def test_normal_path_unchanged(self):
        result = _validate_path("/plone/documents")
        assert result == "/plone/documents"

    def test_single_slash_unchanged(self):
        result = _validate_path("/")
        assert result == "/"

    def test_invalid_chars_still_rejected(self):
        with pytest.raises(ValueError, match="Invalid path"):
            _validate_path("/plone/<script>")

    def test_non_string_rejected(self):
        with pytest.raises(ValueError, match="Path must be a string"):
            _validate_path(123)


class TestErrorMessageOpacity:
    """CAT-L1: Error messages do not expose internal limit values."""

    def test_too_many_paths_no_limit_in_message(self):
        many_paths = [f"/plone/doc{i}" for i in range(200)]
        with pytest.raises(ValueError, match="Too many paths in query") as exc_info:
            build_query({"path": {"query": many_paths}})
        # Verify the error message does NOT contain the actual limit number
        msg = str(exc_info.value)
        assert "100" not in msg
        assert "200" not in msg
        assert "maximum" not in msg.lower()


# ---------------------------------------------------------------------------
# CAT-H2: RRULE validation tests
# ---------------------------------------------------------------------------


class TestRRULEValidation:
    """Tests for RRULE string validation in DateRecurringIndexTranslator."""

    def test_rejects_string_not_starting_with_rrule_freq(self):
        """RRULE strings not starting with RRULE:FREQ= are rejected."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "DTSTART:20250101T000000Z"

        result = translator.extract(FakeObj(), "start")
        assert "start" in result
        assert "start_recurrence" not in result

    def test_rejects_sql_injection_attempt(self):
        """SQL injection attempts in RRULE strings are rejected."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "'; DROP TABLE object_state; --"

        result = translator.extract(FakeObj(), "start")
        assert "start_recurrence" not in result

    def test_rejects_string_exceeding_max_length(self):
        """RRULE strings exceeding _MAX_RRULE_LENGTH are rejected."""
        from plone.pgcatalog.dri import _MAX_RRULE_LENGTH
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "RRULE:FREQ=DAILY;COUNT=1" + "x" * _MAX_RRULE_LENGTH

        result = translator.extract(FakeObj(), "start")
        assert "start_recurrence" not in result

    def test_accepts_valid_rrule_daily(self):
        """Valid daily RRULE string is accepted."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "RRULE:FREQ=DAILY;COUNT=10"

        result = translator.extract(FakeObj(), "start")
        assert result["start_recurrence"] == "RRULE:FREQ=DAILY;COUNT=10"

    def test_accepts_valid_rrule_weekly(self):
        """Valid weekly RRULE string is accepted."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "RRULE:FREQ=WEEKLY;BYDAY=MO,WE,FR"

        result = translator.extract(FakeObj(), "start")
        assert result["start_recurrence"] == "RRULE:FREQ=WEEKLY;BYDAY=MO,WE,FR"

    def test_accepts_valid_rrule_monthly(self):
        """Valid monthly RRULE string is accepted."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "RRULE:FREQ=MONTHLY;BYMONTHDAY=15"

        result = translator.extract(FakeObj(), "start")
        assert result["start_recurrence"] == "RRULE:FREQ=MONTHLY;BYMONTHDAY=15"

    def test_accepts_valid_rrule_yearly(self):
        """Valid yearly RRULE string is accepted."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "RRULE:FREQ=YEARLY;BYMONTH=12;BYMONTHDAY=25"

        result = translator.extract(FakeObj(), "start")
        assert (
            result["start_recurrence"] == "RRULE:FREQ=YEARLY;BYMONTH=12;BYMONTHDAY=25"
        )

    def test_accepts_valid_rrule_case_insensitive(self):
        """RRULE validation is case-insensitive per RFC 5545."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "rrule:freq=daily;count=5"

        result = translator.extract(FakeObj(), "start")
        assert result["start_recurrence"] == "rrule:freq=daily;count=5"

    def test_rejects_empty_rrule_string(self):
        """Empty RRULE string is treated as no recurrence (falsy)."""
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = ""

        result = translator.extract(FakeObj(), "start")
        assert "start_recurrence" not in result

    def test_accepts_bare_freq_format(self):
        """Bare 'FREQ=...' format (without 'RRULE:' prefix) is accepted.

        Plone's plone.formwidget.recurrence commonly stores bare format,
        and the PL/pgSQL rrule parser accepts it.
        """
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = "FREQ=DAILY;COUNT=10"

        result = translator.extract(FakeObj(), "start")
        assert result["start_recurrence"] == "FREQ=DAILY;COUNT=10"

    def test_max_rrule_length_constant(self):
        """_MAX_RRULE_LENGTH is set to 1000."""
        from plone.pgcatalog.dri import _MAX_RRULE_LENGTH

        assert _MAX_RRULE_LENGTH == 1000

    def test_at_exact_max_length_accepted(self):
        """RRULE string at exactly _MAX_RRULE_LENGTH is accepted."""
        from plone.pgcatalog.dri import _MAX_RRULE_LENGTH
        from plone.pgcatalog.dri import DateRecurringIndexTranslator

        translator = DateRecurringIndexTranslator(
            date_attr="start", recurdef_attr="recurrence"
        )
        # Build a valid RRULE string padded to exactly _MAX_RRULE_LENGTH
        base = "RRULE:FREQ=DAILY;COUNT=1"
        # Pad with a valid-looking comment section (semicolon + chars)
        padding = ";X-PAD=" + "A" * (_MAX_RRULE_LENGTH - len(base) - len(";X-PAD="))
        rrule = base + padding
        assert len(rrule) == _MAX_RRULE_LENGTH

        class FakeObj:
            start = "2025-01-01T00:00:00+00:00"
            recurrence = rrule

        result = translator.extract(FakeObj(), "start")
        assert "start_recurrence" in result


# ---------------------------------------------------------------------------
# CAT-M2: Connection pool leak resilience tests
# ---------------------------------------------------------------------------


class TestReleaseRequestConnectionRobustness:
    """Tests that release_request_connection handles edge cases gracefully."""

    def test_handles_closed_connection(self):
        """release_request_connection skips putconn for closed connections."""
        from plone.pgcatalog.pending import _local
        from plone.pgcatalog.pool import release_request_connection
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_conn.closed = True
        mock_pool = MagicMock()

        _local.pgcat_conn = mock_conn
        _local.pgcat_pool = mock_pool

        release_request_connection()

        # putconn should NOT be called for closed connections
        mock_pool.putconn.assert_not_called()
        # Thread-locals should be cleaned up
        assert _local.pgcat_conn is None
        assert _local.pgcat_pool is None

    def test_handles_open_connection(self):
        """release_request_connection returns open connections to pool."""
        from plone.pgcatalog.pending import _local
        from plone.pgcatalog.pool import release_request_connection
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_pool = MagicMock()

        _local.pgcat_conn = mock_conn
        _local.pgcat_pool = mock_pool

        release_request_connection()

        mock_pool.putconn.assert_called_once_with(mock_conn)
        assert _local.pgcat_conn is None
        assert _local.pgcat_pool is None

    def test_handles_no_connection(self):
        """release_request_connection is a no-op when no connection exists."""
        from plone.pgcatalog.pending import _local
        from plone.pgcatalog.pool import release_request_connection

        _local.pgcat_conn = None
        _local.pgcat_pool = None

        # Should not raise
        release_request_connection()

        assert _local.pgcat_conn is None
        assert _local.pgcat_pool is None

    def test_handles_putconn_exception(self):
        """release_request_connection handles exceptions from putconn gracefully."""
        from plone.pgcatalog.pending import _local
        from plone.pgcatalog.pool import release_request_connection
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_pool = MagicMock()
        mock_pool.putconn.side_effect = Exception("pool error")

        _local.pgcat_conn = mock_conn
        _local.pgcat_pool = mock_pool

        # Should not raise despite putconn failure
        release_request_connection()

        # Thread-locals should still be cleaned up
        assert _local.pgcat_conn is None
        assert _local.pgcat_pool is None
