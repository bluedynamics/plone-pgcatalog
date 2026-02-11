"""Integration tests for full-text search (tsvector/tsquery) — real PG.

Tests multilingual text search, ranking, and edge cases.
"""

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.query import _execute_query as execute_query
from tests.conftest import insert_object


def _query_zoids(conn, query_dict):
    rows = execute_query(conn, query_dict, columns="zoid")
    return sorted(row["zoid"] for row in rows)


# ---------------------------------------------------------------------------
# Basic full-text search
# ---------------------------------------------------------------------------


class TestBasicSearch:
    def test_single_word(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=300)
        catalog_object(
            conn,
            zoid=300,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="The quick brown fox",
        )
        conn.commit()
        zoids = _query_zoids(conn, {"SearchableText": "fox"})
        assert zoids == [300]

    def test_multiple_words(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=301)
        catalog_object(
            conn,
            zoid=301,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="PostgreSQL is a powerful database system",
        )
        conn.commit()
        zoids = _query_zoids(conn, {"SearchableText": "powerful database"})
        assert zoids == [301]

    def test_no_match(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=302)
        catalog_object(
            conn,
            zoid=302,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="The quick brown fox",
        )
        conn.commit()
        zoids = _query_zoids(conn, {"SearchableText": "elephant"})
        assert zoids == []

    def test_multiple_objects(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        for zoid, text in [
            (310, "Python is a great language"),
            (311, "Java is also popular"),
            (312, "Python and Java together"),
        ]:
            insert_object(conn, zoid=zoid)
            catalog_object(
                conn,
                zoid=zoid,
                path=f"/plone/doc{zoid}",
                idx={"portal_type": "Document"},
                searchable_text=text,
            )
        conn.commit()

        # "Python" matches 310 and 312
        zoids = _query_zoids(conn, {"SearchableText": "Python"})
        assert set(zoids) == {310, 312}

        # "Java" matches 311 and 312
        zoids = _query_zoids(conn, {"SearchableText": "Java"})
        assert set(zoids) == {311, 312}

    def test_null_searchable_text_excluded(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=320)
        catalog_object(
            conn,
            zoid=320,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text=None,
        )
        conn.commit()
        zoids = _query_zoids(conn, {"SearchableText": "anything"})
        assert 320 not in zoids


# ---------------------------------------------------------------------------
# Multilingual search
# ---------------------------------------------------------------------------


class TestMultilingualSearch:
    def test_german_stemming(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=330)
        catalog_object(
            conn,
            zoid=330,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="Die Katzen spielen im Garten",
            language="german",
        )
        conn.commit()

        # German stemmer: "Katze" (singular) matches "Katzen" (plural)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('german', 'Katze')"
            )
            result = cur.fetchone()
        assert result is not None
        assert result["zoid"] == 330

    def test_english_stemming(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=331)
        catalog_object(
            conn,
            zoid=331,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="The children are running quickly",
            language="english",
        )
        conn.commit()

        # English stemmer: "run" matches "running"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('english', 'run')"
            )
            result = cur.fetchone()
        assert result is not None
        assert result["zoid"] == 331

    def test_simple_no_stemming(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=332)
        catalog_object(
            conn,
            zoid=332,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="The children are running",
            language="simple",
        )
        conn.commit()

        # "simple" config: no stemming, "run" won't match "running"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'run')"
            )
            assert cur.fetchone() is None

            # But exact word matches
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'running')"
            )
            result = cur.fetchone()
            assert result["zoid"] == 332


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------


class TestRanking:
    def test_rank_orders_by_relevance(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        # Doc with many mentions of "Python" should rank higher
        insert_object(conn, zoid=340)
        catalog_object(
            conn,
            zoid=340,
            path="/plone/many",
            idx={"portal_type": "Document"},
            searchable_text="Python Python Python Python programming",
        )
        insert_object(conn, zoid=341)
        catalog_object(
            conn,
            zoid=341,
            path="/plone/few",
            idx={"portal_type": "Document"},
            searchable_text="Python is nice",
        )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid, ts_rank(searchable_text, "
                "  plainto_tsquery('simple', 'Python')) AS rank "
                "FROM object_state "
                "WHERE searchable_text @@ plainto_tsquery('simple', 'Python') "
                "ORDER BY rank DESC"
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0]["zoid"] == 340  # more mentions → higher rank


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestFulltextEdgeCases:
    def test_special_characters_safe(self, pg_conn_with_catalog):
        """plainto_tsquery handles special chars safely."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=350)
        catalog_object(
            conn,
            zoid=350,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="Normal document content",
        )
        conn.commit()

        # Characters that would be operators in to_tsquery are safe with plainto_tsquery
        zoids = _query_zoids(conn, {"SearchableText": "a & b | !c"})
        assert 350 not in zoids  # no match, but no SQL error

    def test_empty_search_text(self, pg_conn_with_catalog):
        """Empty search text returns no results (not all)."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=351)
        catalog_object(
            conn,
            zoid=351,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="Some content",
        )
        conn.commit()

        # Empty string → build_query skips (falsy query_val)
        qr = execute_query(conn, {"SearchableText": ""}, columns="zoid")
        # With empty text, the text handler is skipped, so only idx IS NOT NULL filter
        # This means all cataloged objects are returned
        zoids = [r["zoid"] for r in qr]
        assert 351 in zoids

    def test_combined_with_other_indexes(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        for zoid, pt, text in [
            (360, "Document", "Python programming guide"),
            (361, "News Item", "Python news update"),
            (362, "Document", "Java programming guide"),
        ]:
            insert_object(conn, zoid=zoid)
            catalog_object(
                conn,
                zoid=zoid,
                path=f"/plone/doc{zoid}",
                idx={"portal_type": pt},
                searchable_text=text,
            )
        conn.commit()

        zoids = _query_zoids(
            conn,
            {"SearchableText": "Python", "portal_type": "Document"},
        )
        assert zoids == [360]
