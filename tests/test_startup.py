"""Tests for plone.pgcatalog startup hooks."""

from plone.pgcatalog.startup import _make_analyze_object_state_action
from psycopg.types.json import Json
from tests.conftest import DSN


def _insert_catalog_rows(conn, count=5):
    """Insert minimal object_state rows with idx JSONB so ANALYZE has
    something to populate pg_stats_ext from.  Needs transaction_log
    to satisfy the foreign key.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transaction_log (tid) VALUES (1) ON CONFLICT DO NOTHING"
        )
        for i in range(count):
            cur.execute(
                "INSERT INTO object_state "
                "(zoid, tid, class_mod, class_name, state, state_size, refs, "
                " path, idx) "
                "VALUES (%s, 1, 'mod', 'cls', '{}'::jsonb, 0, '{}'::bigint[], "
                " %s, %s)",
                (
                    i + 100,
                    f"/plone/doc-{i}",
                    Json(
                        {
                            "portal_type": "Document",
                            "review_state": "published",
                            "path": f"/plone/doc-{i}",
                            "path_parent": "/plone",
                            "path_depth": 2,
                            "effective": "2026-01-01T00:00:00+00:00",
                            "expires": "2030-01-01T00:00:00+00:00",
                        }
                    ),
                ),
            )
    conn.commit()


class TestAnalyzeObjectStateAction:
    """Test the deferred ANALYZE object_state startup action."""

    def test_action_populates_pg_stats_ext(self, pg_conn_with_catalog):
        """After ANALYZE, pg_stats_ext has a row for each managed stats
        object once the table has at least one row to analyze.
        """
        _insert_catalog_rows(pg_conn_with_catalog)

        action = _make_analyze_object_state_action()
        action(DSN)

        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                "SELECT statistics_name FROM pg_stats_ext "
                "WHERE tablename = 'object_state' "
                "AND statistics_name = 'stts_os_type_state'"
            )
            row = cur.fetchone()
        assert row is not None, "Expected stats row after ANALYZE on populated table"

    def test_action_skips_when_already_populated(self, pg_conn_with_catalog):
        """Second call skips ANALYZE -- pg_stats_ext.n_distinct is no
        longer NULL, so the skip branch triggers.  Verified behaviourally:
        both calls complete without error.
        """
        _insert_catalog_rows(pg_conn_with_catalog)

        action = _make_analyze_object_state_action()
        action(DSN)  # populates
        action(DSN)  # should take the skip branch and return quickly

    def test_action_is_idempotent_on_empty_table(self, pg_conn_with_catalog):
        """On an empty table ANALYZE is a no-op.  Action must not raise."""
        action = _make_analyze_object_state_action()
        action(DSN)
        action(DSN)
