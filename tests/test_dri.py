"""Tests for DateRecurringIndex translator (dri.py).

Unit tests for extract/query/sort + integration tests against PostgreSQL
with rrule_plpgsql functions.
"""

from datetime import datetime
from datetime import UTC
from plone.pgcatalog.dri import DateRecurringIndexTranslator
from plone.pgcatalog.indexing import catalog_object
from tests.conftest import insert_object

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def translator():
    """A translator configured like Plone's 'start' index."""
    return DateRecurringIndexTranslator(
        date_attr="start",
        recurdef_attr="recurrence",
        until_attr="",
    )


class _FakeEvent:
    """Minimal event-like object for extract() tests."""

    def __init__(self, start=None, end=None, recurrence=None):
        self.start = start
        self.end = end
        self.recurrence = recurrence


# ---------------------------------------------------------------------------
# Unit tests: extract()
# ---------------------------------------------------------------------------


class TestExtract:
    def test_non_recurring_event(self, translator):
        obj = _FakeEvent(start=datetime(2025, 3, 15, 10, 0, tzinfo=UTC))
        result = translator.extract(obj, "start")
        assert result == {"start": "2025-03-15T10:00:00+00:00"}
        assert "start_recurrence" not in result

    def test_recurring_event(self, translator):
        obj = _FakeEvent(
            start=datetime(2025, 1, 6, 10, 0, tzinfo=UTC),
            recurrence="FREQ=WEEKLY;BYDAY=MO;COUNT=52",
        )
        result = translator.extract(obj, "start")
        assert result["start"] == "2025-01-06T10:00:00+00:00"
        assert result["start_recurrence"] == "FREQ=WEEKLY;BYDAY=MO;COUNT=52"

    def test_none_date(self, translator):
        obj = _FakeEvent(start=None)
        result = translator.extract(obj, "start")
        assert result == {}

    def test_empty_recurrence(self, translator):
        obj = _FakeEvent(
            start=datetime(2025, 3, 15, 10, 0, tzinfo=UTC),
            recurrence="",
        )
        result = translator.extract(obj, "start")
        assert result == {"start": "2025-03-15T10:00:00+00:00"}
        assert "start_recurrence" not in result

    def test_end_index(self):
        t = DateRecurringIndexTranslator("end", "recurrence")
        obj = _FakeEvent(
            end=datetime(2025, 3, 15, 11, 0, tzinfo=UTC),
            recurrence="FREQ=DAILY;COUNT=3",
        )
        result = t.extract(obj, "end")
        assert result["end"] == "2025-03-15T11:00:00+00:00"
        assert result["end_recurrence"] == "FREQ=DAILY;COUNT=3"


# ---------------------------------------------------------------------------
# Unit tests: query() SQL generation
# ---------------------------------------------------------------------------


class TestQuerySQL:
    def test_min_max_range(self, translator):
        sql, params = translator.query(
            "start",
            {
                "query": [datetime(2025, 3, 1), datetime(2025, 3, 31)],
                "range": "min:max",
            },
            {
                "query": [datetime(2025, 3, 1), datetime(2025, 3, 31)],
                "range": "min:max",
            },
        )
        assert 'rrule."between"' in sql
        assert "CASE WHEN" in sql
        assert "BETWEEN" in sql
        assert "dri_start_min" in params
        assert "dri_start_max" in params

    def test_min_range(self, translator):
        sql, params = translator.query(
            "start",
            {"query": datetime(2025, 3, 1), "range": "min"},
            {"query": datetime(2025, 3, 1), "range": "min"},
        )
        assert 'rrule."after"' in sql
        assert "dri_start_min" in params

    def test_max_range(self, translator):
        sql, params = translator.query(
            "start",
            {"query": datetime(2025, 3, 31), "range": "max"},
            {"query": datetime(2025, 3, 31), "range": "max"},
        )
        # max: simple date comparison (no rrule needed)
        assert "rrule" not in sql
        assert "<=" in sql
        assert "dri_start_max" in params

    def test_exact_match(self, translator):
        sql, params = translator.query(
            "start",
            {"query": datetime(2025, 3, 15)},
            {"query": datetime(2025, 3, 15)},
        )
        assert 'rrule."between"' in sql
        assert "dri_start_exact" in params

    def test_none_query(self, translator):
        sql, params = translator.query("start", {}, {})
        assert sql == "TRUE"
        assert params == {}


# ---------------------------------------------------------------------------
# Unit tests: sort()
# ---------------------------------------------------------------------------


class TestSort:
    def test_sort_expression(self, translator):
        expr = translator.sort("start")
        assert expr == "pgcatalog_to_timestamptz(idx->>'start')"


# ---------------------------------------------------------------------------
# Integration tests (require PostgreSQL)
# ---------------------------------------------------------------------------


def _setup_events(conn):
    """Insert test events — recurring and non-recurring.

    Returns dict of zoid → description.
    """
    events = {
        # Non-recurring event: March 15
        200: {
            "path": "/plone/event-single",
            "idx": {
                "portal_type": "Event",
                "start": "2025-03-15T10:00:00+00:00",
            },
        },
        # Weekly recurring event: every Monday starting Jan 6, 12 weeks
        201: {
            "path": "/plone/event-weekly",
            "idx": {
                "portal_type": "Event",
                "start": "2025-01-06T10:00:00+00:00",
                "start_recurrence": "FREQ=WEEKLY;BYDAY=MO;COUNT=12",
            },
        },
        # Daily recurring event: 5 days starting March 10
        202: {
            "path": "/plone/event-daily",
            "idx": {
                "portal_type": "Event",
                "start": "2025-03-10T09:00:00+00:00",
                "start_recurrence": "FREQ=DAILY;COUNT=5",
            },
        },
        # Non-recurring event: April 20
        203: {
            "path": "/plone/event-april",
            "idx": {
                "portal_type": "Event",
                "start": "2025-04-20T14:00:00+00:00",
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


class TestRRuleSchemaInstall:
    """Verify rrule_plpgsql functions are installed correctly."""

    def test_rrule_schema_exists(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        row = conn.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = 'rrule'"
        ).fetchone()
        assert row is not None

    def test_rrule_between_works(self, pg_conn_with_catalog):
        """rrule.between() returns occurrences in a date range."""
        conn = pg_conn_with_catalog
        rows = conn.execute(
            """
            SELECT * FROM rrule."between"(
                'FREQ=DAILY;COUNT=5',
                '2025-01-01 10:00:00+00'::timestamptz,
                '2025-01-01 00:00:00+00'::timestamptz,
                '2025-01-10 00:00:00+00'::timestamptz
            )
            """
        ).fetchall()
        assert len(rows) == 5

    def test_rrule_after_works(self, pg_conn_with_catalog):
        """rrule.after() returns occurrences after a given date."""
        conn = pg_conn_with_catalog
        rows = conn.execute(
            """
            SELECT * FROM rrule."after"(
                'FREQ=WEEKLY;BYDAY=MO;COUNT=52',
                '2025-01-06 10:00:00+00'::timestamptz,
                '2025-03-01 00:00:00+00'::timestamptz,
                3
            )
            """
        ).fetchall()
        assert len(rows) == 3


class TestDRIQueryIntegration:
    """Integration tests: DRI queries against real PostgreSQL data."""

    def test_min_max_recurring_match(self, pg_conn_with_catalog):
        """Weekly event (Jan-Mar) should match March range query."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        # Query: events in March 2025
        # Expected: 200 (single March 15), 201 (weekly includes March Mondays),
        #           202 (daily March 10-14)
        sql = (
            "SELECT zoid FROM object_state WHERE idx IS NOT NULL"
            " AND (CASE WHEN idx->>'start_recurrence' IS NOT NULL"
            " AND idx->>'start_recurrence' != ''"
            " THEN EXISTS ("
            'SELECT 1 FROM rrule."between"('
            "idx->>'start_recurrence',"
            " pgcatalog_to_timestamptz(idx->>'start'),"
            " %(p_min)s::timestamptz, %(p_max)s::timestamptz))"
            " ELSE pgcatalog_to_timestamptz(idx->>'start')"
            " BETWEEN %(p_min)s::timestamptz AND %(p_max)s::timestamptz"
            " END)"
        )
        rows = conn.execute(
            sql,
            {
                "p_min": datetime(2025, 3, 1, tzinfo=UTC),
                "p_max": datetime(2025, 3, 31, 23, 59, 59, tzinfo=UTC),
            },
        ).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 200 in zoids  # single event March 15
        assert 201 in zoids  # weekly Mondays include March
        assert 202 in zoids  # daily March 10-14
        assert 203 not in zoids  # April event

    def test_non_recurring_simple_range(self, pg_conn_with_catalog):
        """Non-recurring event: simple date comparison."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        sql = (
            "SELECT zoid FROM object_state WHERE idx IS NOT NULL"
            " AND pgcatalog_to_timestamptz(idx->>'start')"
            " BETWEEN %(p_min)s::timestamptz AND %(p_max)s::timestamptz"
        )
        rows = conn.execute(
            sql,
            {
                "p_min": datetime(2025, 4, 1, tzinfo=UTC),
                "p_max": datetime(2025, 4, 30, 23, 59, 59, tzinfo=UTC),
            },
        ).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert zoids == [203]  # Only the April event

    def test_recurring_no_match_outside_range(self, pg_conn_with_catalog):
        """Daily 5-day event in March should NOT match June query."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        sql = (
            "SELECT zoid FROM object_state WHERE idx IS NOT NULL"
            " AND (CASE WHEN idx->>'start_recurrence' IS NOT NULL"
            " AND idx->>'start_recurrence' != ''"
            " THEN EXISTS ("
            'SELECT 1 FROM rrule."between"('
            "idx->>'start_recurrence',"
            " pgcatalog_to_timestamptz(idx->>'start'),"
            " %(p_min)s::timestamptz, %(p_max)s::timestamptz))"
            " ELSE pgcatalog_to_timestamptz(idx->>'start')"
            " BETWEEN %(p_min)s::timestamptz AND %(p_max)s::timestamptz"
            " END)"
        )
        rows = conn.execute(
            sql,
            {
                "p_min": datetime(2025, 6, 1, tzinfo=UTC),
                "p_max": datetime(2025, 6, 30, 23, 59, 59, tzinfo=UTC),
            },
        ).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 202 not in zoids  # daily 5-day event ended in March

    def test_translator_query_integration(self, pg_conn_with_catalog):
        """End-to-end: translator.query() generates working SQL."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRecurringIndexTranslator("start", "recurrence")
        sql_frag, params = t.query(
            "start",
            {
                "query": [
                    datetime(2025, 3, 1, tzinfo=UTC),
                    datetime(2025, 3, 31, 23, 59, 59, tzinfo=UTC),
                ],
                "range": "min:max",
            },
            {
                "query": [
                    datetime(2025, 3, 1, tzinfo=UTC),
                    datetime(2025, 3, 31, 23, 59, 59, tzinfo=UTC),
                ],
                "range": "min:max",
            },
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        assert 200 in zoids  # March 15 single
        assert 201 in zoids  # weekly Mondays
        assert 202 in zoids  # daily March 10-14
        assert 203 not in zoids  # April

    def test_translator_min_range(self, pg_conn_with_catalog):
        """min range: recurring event with future occurrences should match."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRecurringIndexTranslator("start", "recurrence")
        sql_frag, params = t.query(
            "start",
            {"query": datetime(2025, 3, 20, tzinfo=UTC), "range": "min"},
            {"query": datetime(2025, 3, 20, tzinfo=UTC), "range": "min"},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        # Weekly event 201: has occurrences after March 20 (March 24, 31)
        assert 201 in zoids
        # April event 203: April 20 >= March 20
        assert 203 in zoids
        # Daily event 202: last occurrence March 14 < March 20
        assert 202 not in zoids

    def test_translator_max_range(self, pg_conn_with_catalog):
        """max range: events with base date <= query date."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRecurringIndexTranslator("start", "recurrence")
        sql_frag, params = t.query(
            "start",
            {"query": datetime(2025, 2, 1, tzinfo=UTC), "range": "max"},
            {"query": datetime(2025, 2, 1, tzinfo=UTC), "range": "max"},
        )
        full_sql = f"SELECT zoid FROM object_state WHERE idx IS NOT NULL AND {sql_frag}"
        rows = conn.execute(full_sql, params).fetchall()
        zoids = sorted(r["zoid"] for r in rows)
        # Weekly event 201: base date Jan 6 <= Feb 1
        assert 201 in zoids
        # All others start after Feb 1
        assert 200 not in zoids
        assert 202 not in zoids
        assert 203 not in zoids

    def test_sort_by_start(self, pg_conn_with_catalog):
        """sort_on=start should sort by base date."""
        conn = pg_conn_with_catalog
        _setup_events(conn)

        t = DateRecurringIndexTranslator("start", "recurrence")
        expr = t.sort("start")
        sql = (
            f"SELECT zoid FROM object_state"
            f" WHERE idx IS NOT NULL AND idx->>'portal_type' = 'Event'"
            f" ORDER BY {expr} ASC"
        )
        rows = conn.execute(sql).fetchall()
        zoids = [r["zoid"] for r in rows]
        # Jan 6, Mar 10, Mar 15, Apr 20
        assert zoids == [201, 202, 200, 203]
