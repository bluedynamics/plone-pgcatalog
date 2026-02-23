"""Tests for DateRangeInRangeIndex translator (driri.py).

Unit tests for extract/query/sort + integration tests against PostgreSQL.
"""

from datetime import datetime
from datetime import UTC
from plone.pgcatalog.addons_compat.driri import DateRangeInRangeIndexTranslator
from plone.pgcatalog.indexing import catalog_object
from tests.conftest import insert_object

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def translator():
    """A translator configured with start/end indexes."""
    return DateRangeInRangeIndexTranslator(
        startindex="start",
        endindex="end",
    )


# ---------------------------------------------------------------------------
# Unit tests: constructor validation
# ---------------------------------------------------------------------------


class TestConstructorValidation:
    """DateRangeInRangeIndexTranslator rejects unsafe identifier values."""

    def test_accepts_valid_identifiers(self):
        t = DateRangeInRangeIndexTranslator("start", "end")
        assert t.startindex == "start"
        assert t.endindex == "end"

    def test_rejects_single_quote_in_startindex(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            DateRangeInRangeIndexTranslator("start'", "end")

    def test_rejects_single_quote_in_endindex(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            DateRangeInRangeIndexTranslator("start", "end'")

    def test_rejects_sql_injection_in_startindex(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            DateRangeInRangeIndexTranslator("'; DROP TABLE x; --", "end")

    def test_rejects_dash_in_endindex(self):
        with pytest.raises(ValueError, match="Invalid identifier"):
            DateRangeInRangeIndexTranslator("start", "my-end")


# ---------------------------------------------------------------------------
# Unit tests: extract()
# ---------------------------------------------------------------------------


class TestExtract:
    def test_returns_empty_dict(self, translator):
        """Extract is a no-op — underlying indexes handle data."""
        result = translator.extract(object(), "event_dates")
        assert result == {}


# ---------------------------------------------------------------------------
# Unit tests: query() SQL generation
# ---------------------------------------------------------------------------


class TestQuerySQL:
    def test_overlap_query_non_recurring(self, translator):
        """Generates overlap SQL with CASE WHEN for recurrence."""
        sql, params = translator.query(
            "event_dates",
            {"start": datetime(2025, 3, 1), "end": datetime(2025, 3, 31)},
            {"start": datetime(2025, 3, 1), "end": datetime(2025, 3, 31)},
        )
        # Simple overlap clause present
        assert "idx->>'start'" in sql
        assert "idx->>'end'" in sql
        assert "driri_event_dates_start" in params
        assert "driri_event_dates_end" in params

    def test_recurring_branch_present(self, translator):
        """SQL includes CASE WHEN for recurrence support."""
        sql, params = translator.query(
            "event_dates",
            {"start": datetime(2025, 3, 1), "end": datetime(2025, 3, 31)},
            {"start": datetime(2025, 3, 1), "end": datetime(2025, 3, 31)},
        )
        assert "CASE WHEN" in sql
        assert 'rrule."between"' in sql
        assert "start_recurrence" in sql

    def test_missing_start_returns_true(self, translator):
        sql, params = translator.query(
            "event_dates",
            {"end": datetime(2025, 3, 31)},
            {"end": datetime(2025, 3, 31)},
        )
        assert sql == "TRUE"
        assert params == {}

    def test_missing_end_returns_true(self, translator):
        sql, params = translator.query(
            "event_dates",
            {"start": datetime(2025, 3, 1)},
            {"start": datetime(2025, 3, 1)},
        )
        assert sql == "TRUE"
        assert params == {}

    def test_empty_query_returns_true(self, translator):
        sql, params = translator.query("event_dates", {}, {})
        assert sql == "TRUE"
        assert params == {}

    def test_validates_index_name(self):
        t = DateRangeInRangeIndexTranslator("start", "end")
        with pytest.raises(ValueError, match="Invalid identifier"):
            t.query(
                "bad'; DROP TABLE",
                {"start": datetime(2025, 1, 1), "end": datetime(2025, 1, 2)},
                {"start": datetime(2025, 1, 1), "end": datetime(2025, 1, 2)},
            )


# ---------------------------------------------------------------------------
# Unit tests: sort()
# ---------------------------------------------------------------------------


class TestSort:
    def test_sort_by_start(self, translator):
        expr = translator.sort("event_dates")
        assert expr == "pgcatalog_to_timestamptz(idx->>'start')"


# ---------------------------------------------------------------------------
# Integration tests (require PostgreSQL)
# ---------------------------------------------------------------------------


def _setup_events(conn):
    """Insert test events with start/end date ranges.

    Some events have recurrence on the start field.
    """
    events = {
        # Conference: March 10-12 (non-recurring)
        300: {
            "path": "/plone/conference",
            "idx": {
                "portal_type": "Event",
                "start": "2025-03-10T09:00:00+00:00",
                "end": "2025-03-12T17:00:00+00:00",
            },
        },
        # Weekly meeting: every Monday 10:00-11:00, starting Jan 6, 12 weeks
        301: {
            "path": "/plone/weekly-meeting",
            "idx": {
                "portal_type": "Event",
                "start": "2025-01-06T10:00:00+00:00",
                "end": "2025-01-06T11:00:00+00:00",
                "start_recurrence": "FREQ=WEEKLY;BYDAY=MO;COUNT=12",
            },
        },
        # Workshop: April 15-17 (non-recurring)
        302: {
            "path": "/plone/workshop",
            "idx": {
                "portal_type": "Event",
                "start": "2025-04-15T09:00:00+00:00",
                "end": "2025-04-17T17:00:00+00:00",
            },
        },
        # Daily standup: March 3-7, 15min each day (recurring)
        303: {
            "path": "/plone/standup",
            "idx": {
                "portal_type": "Event",
                "start": "2025-03-03T09:00:00+00:00",
                "end": "2025-03-03T09:15:00+00:00",
                "start_recurrence": "FREQ=DAILY;COUNT=5",
            },
        },
        # Past event: January 5 (non-recurring)
        304: {
            "path": "/plone/past-event",
            "idx": {
                "portal_type": "Event",
                "start": "2025-01-05T10:00:00+00:00",
                "end": "2025-01-05T12:00:00+00:00",
            },
        },
    }

    for zoid, data in events.items():
        insert_object(conn, zoid=zoid)
        catalog_object(
            conn,
            zoid=zoid,
            path=data["path"],
            idx=data["idx"],
        )
    conn.commit()
    return events


class TestDRIRIQueryIntegration:
    """Integration: DRIRI overlap queries against real PostgreSQL data."""

    def test_non_recurring_overlap(self, pg_conn_with_catalog):
        """Conference March 10-12 overlaps query March 11-13."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRangeInRangeIndexTranslator("start", "end")
        sql_frag, params = t.query(
            "event_dates",
            {
                "start": datetime(2025, 3, 11, tzinfo=UTC),
                "end": datetime(2025, 3, 13, tzinfo=UTC),
            },
            {},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 300 in zoids  # conference March 10-12 overlaps

    def test_non_recurring_no_overlap(self, pg_conn_with_catalog):
        """Workshop April 15-17 does NOT overlap query March 1-31."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRangeInRangeIndexTranslator("start", "end")
        sql_frag, params = t.query(
            "event_dates",
            {
                "start": datetime(2025, 3, 1, tzinfo=UTC),
                "end": datetime(2025, 3, 31, tzinfo=UTC),
            },
            {},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 302 not in zoids  # workshop April 15-17

    def test_recurring_overlap_via_occurrence(self, pg_conn_with_catalog):
        """Weekly meeting has occurrence on March 3 (Monday) → overlaps March query."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRangeInRangeIndexTranslator("start", "end")
        sql_frag, params = t.query(
            "event_dates",
            {
                "start": datetime(2025, 3, 1, tzinfo=UTC),
                "end": datetime(2025, 3, 31, tzinfo=UTC),
            },
            {},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 301 in zoids  # weekly meeting has March occurrences

    def test_recurring_no_overlap_outside_range(self, pg_conn_with_catalog):
        """Daily standup March 3-7 does NOT overlap May query."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRangeInRangeIndexTranslator("start", "end")
        sql_frag, params = t.query(
            "event_dates",
            {
                "start": datetime(2025, 5, 1, tzinfo=UTC),
                "end": datetime(2025, 5, 31, tzinfo=UTC),
            },
            {},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 303 not in zoids  # standup ended in March

    def test_exact_boundary_overlap(self, pg_conn_with_catalog):
        """Query end matches event start exactly → should overlap."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRangeInRangeIndexTranslator("start", "end")
        # Query ends exactly when conference starts
        sql_frag, params = t.query(
            "event_dates",
            {
                "start": datetime(2025, 3, 8, tzinfo=UTC),
                "end": datetime(2025, 3, 10, 9, 0, 0, tzinfo=UTC),
            },
            {},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 300 in zoids  # conference starts at query end

    def test_march_overview(self, pg_conn_with_catalog):
        """Full March query: find all events overlapping March."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRangeInRangeIndexTranslator("start", "end")
        sql_frag, params = t.query(
            "event_dates",
            {
                "start": datetime(2025, 3, 1, tzinfo=UTC),
                "end": datetime(2025, 3, 31, 23, 59, 59, tzinfo=UTC),
            },
            {},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        # Conference (300): March 10-12 ✓
        assert 300 in zoids
        # Weekly meeting (301): has March Mondays ✓
        assert 301 in zoids
        # Standup (303): March 3-7 ✓
        assert 303 in zoids
        # Workshop (302): April only ✗
        assert 302 not in zoids
        # Past event (304): January only ✗
        assert 304 not in zoids

    def test_sort_by_start(self, pg_conn_with_catalog):
        """Sort uses start date."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRangeInRangeIndexTranslator("start", "end")
        expr = t.sort("event_dates")
        sql = (
            f"SELECT zoid FROM object_state"
            f" WHERE idx IS NOT NULL AND idx->>'portal_type' = 'Event'"
            f" ORDER BY {expr} ASC"
        )
        rows = conn.execute(sql).fetchall()
        zoids = [r["zoid"] for r in rows]
        # Jan 5, Jan 6, Mar 3, Mar 10, Apr 15
        assert zoids == [304, 301, 303, 300, 302]
