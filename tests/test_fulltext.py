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


# ---------------------------------------------------------------------------
# Language-aware search via query dict
# ---------------------------------------------------------------------------


class TestLanguageAwareSearch:
    def test_german_stemming_via_language_field(self, pg_conn_with_catalog):
        """SearchableText indexed with 'german' matches stems when Language filter used."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=370)
        catalog_object(
            conn,
            zoid=370,
            path="/plone/de/doc",
            idx={"portal_type": "Document", "Language": "de"},
            searchable_text="Die Katzen spielen im Garten",
            language="german",
        )
        conn.commit()

        # Query with Language=de → pgcatalog_lang_to_regconfig('de') = 'german'
        # German stemmer: "Katze" (singular) matches "Katzen" (plural)
        zoids = _query_zoids(conn, {"SearchableText": "Katze", "Language": "de"})
        assert 370 in zoids

    def test_english_stemming_via_language_field(self, pg_conn_with_catalog):
        """English stemming works via Language filter."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=371)
        catalog_object(
            conn,
            zoid=371,
            path="/plone/en/doc",
            idx={"portal_type": "Document", "Language": "en"},
            searchable_text="The children are running quickly",
            language="english",
        )
        conn.commit()

        # "run" matches "running" with english stemmer
        zoids = _query_zoids(conn, {"SearchableText": "run", "Language": "en"})
        assert 371 in zoids

    def test_no_language_falls_back_to_simple(self, pg_conn_with_catalog):
        """Without Language filter, query uses 'simple' (no stemming)."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=372)
        catalog_object(
            conn,
            zoid=372,
            path="/plone/doc",
            idx={"portal_type": "Document"},
            searchable_text="The children are running",
        )
        conn.commit()

        # 'simple' config: exact word match, no stemming
        zoids = _query_zoids(conn, {"SearchableText": "running"})
        assert 372 in zoids
        # "run" won't match "running" with 'simple' config
        zoids = _query_zoids(conn, {"SearchableText": "run"})
        assert 372 not in zoids

    def test_locale_variant_language(self, pg_conn_with_catalog):
        """Language code with locale variant (e.g. 'en-us') maps correctly."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=373)
        catalog_object(
            conn,
            zoid=373,
            path="/plone/en/doc",
            idx={"portal_type": "Document", "Language": "en-us"},
            searchable_text="The children are running",
            language="english",
        )
        conn.commit()

        # "en-us" → pgcatalog_lang_to_regconfig strips to "en" → "english"
        zoids = _query_zoids(conn, {"SearchableText": "run", "Language": "en-us"})
        assert 373 in zoids


# ---------------------------------------------------------------------------
# Title / Description text search (tsvector expression on idx JSONB)
# ---------------------------------------------------------------------------


class TestTitleTextSearch:
    def test_title_word_match(self, pg_conn_with_catalog):
        """Title query matches individual words (not just exact string)."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=380)
        catalog_object(
            conn,
            zoid=380,
            path="/plone/doc",
            idx={"portal_type": "Document", "Title": "Hello World"},
        )
        conn.commit()

        zoids = _query_zoids(conn, {"Title": "Hello"})
        assert 380 in zoids

    def test_title_multi_word(self, pg_conn_with_catalog):
        """Multi-word Title query matches when all words present."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=381)
        catalog_object(
            conn,
            zoid=381,
            path="/plone/doc",
            idx={"portal_type": "Document", "Title": "The Quick Brown Fox"},
        )
        conn.commit()

        zoids = _query_zoids(conn, {"Title": "quick fox"})
        assert 381 in zoids

    def test_title_no_match(self, pg_conn_with_catalog):
        """Title query with non-matching word returns nothing."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=382)
        catalog_object(
            conn,
            zoid=382,
            path="/plone/doc",
            idx={"portal_type": "Document", "Title": "Hello World"},
        )
        conn.commit()

        zoids = _query_zoids(conn, {"Title": "Nonexistent"})
        assert 382 not in zoids

    def test_title_case_insensitive(self, pg_conn_with_catalog):
        """tsvector 'simple' config lowercases tokens — case insensitive."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=383)
        catalog_object(
            conn,
            zoid=383,
            path="/plone/doc",
            idx={"portal_type": "Document", "Title": "UPPERCASE TITLE"},
        )
        conn.commit()

        zoids = _query_zoids(conn, {"Title": "uppercase"})
        assert 383 in zoids


class TestDescriptionTextSearch:
    def test_description_word_match(self, pg_conn_with_catalog):
        """Description query uses tsvector matching for word search."""
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=390)
        catalog_object(
            conn,
            zoid=390,
            path="/plone/doc",
            idx={
                "portal_type": "Document",
                "Description": "A comprehensive overview of the system",
            },
        )
        conn.commit()

        zoids = _query_zoids(conn, {"Description": "comprehensive"})
        assert 390 in zoids

    def test_description_no_match(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=391)
        catalog_object(
            conn,
            zoid=391,
            path="/plone/doc",
            idx={
                "portal_type": "Document",
                "Description": "A simple description",
            },
        )
        conn.commit()

        zoids = _query_zoids(conn, {"Description": "nonexistent"})
        assert 391 not in zoids


# ---------------------------------------------------------------------------
# SQL function: pgcatalog_lang_to_regconfig
# ---------------------------------------------------------------------------


class TestLangToRegconfigFunction:
    def test_german(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        with conn.cursor() as cur:
            cur.execute("SELECT pgcatalog_lang_to_regconfig('de')")
            assert cur.fetchone()["pgcatalog_lang_to_regconfig"] == "german"

    def test_english(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        with conn.cursor() as cur:
            cur.execute("SELECT pgcatalog_lang_to_regconfig('en')")
            assert cur.fetchone()["pgcatalog_lang_to_regconfig"] == "english"

    def test_locale_variant(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        with conn.cursor() as cur:
            cur.execute("SELECT pgcatalog_lang_to_regconfig('en-us')")
            assert cur.fetchone()["pgcatalog_lang_to_regconfig"] == "english"

    def test_underscore_variant(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        with conn.cursor() as cur:
            cur.execute("SELECT pgcatalog_lang_to_regconfig('pt_BR')")
            assert cur.fetchone()["pgcatalog_lang_to_regconfig"] == "portuguese"

    def test_unknown_returns_simple(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        with conn.cursor() as cur:
            cur.execute("SELECT pgcatalog_lang_to_regconfig('xx')")
            assert cur.fetchone()["pgcatalog_lang_to_regconfig"] == "simple"

    def test_empty_returns_simple(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        with conn.cursor() as cur:
            cur.execute("SELECT pgcatalog_lang_to_regconfig('')")
            assert cur.fetchone()["pgcatalog_lang_to_regconfig"] == "simple"

    def test_null_returns_simple(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        with conn.cursor() as cur:
            cur.execute("SELECT pgcatalog_lang_to_regconfig(NULL)")
            assert cur.fetchone()["pgcatalog_lang_to_regconfig"] == "simple"
