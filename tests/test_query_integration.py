"""Integration tests for plone.pgcatalog.query — real PG queries.

These tests insert test data into PostgreSQL and verify that queries
return correct results.
"""

from datetime import datetime
from datetime import UTC
from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.query import execute_query
from tests.conftest import insert_object


# ---------------------------------------------------------------------------
# Fixtures: insert test data
# ---------------------------------------------------------------------------


def _setup_test_data(conn):
    """Insert a set of test objects for query testing.

    Returns dict of zoid → description for reference.
    """
    objects = {
        100: {
            "path": "/plone/doc1",
            "idx": {
                "portal_type": "Document",
                "review_state": "published",
                "Title": "Hello World",
                "Subject": ["Python", "Zope"],
                "is_folderish": False,
                "created": "2025-01-15T10:30:00+00:00",
                "modified": "2025-06-15T14:00:00+00:00",
                "effective": "2025-01-15T10:30:00+00:00",
                "expires": "2026-12-31T23:59:59+00:00",
                "sortable_title": "hello world",
                "UID": "uid-100",
                "getObjPositionInParent": 1,
                "allowedRolesAndUsers": ["Anonymous"],
            },
            "text": "The quick brown fox jumps over the lazy dog",
        },
        101: {
            "path": "/plone/doc2",
            "idx": {
                "portal_type": "Document",
                "review_state": "private",
                "Title": "Private Doc",
                "Subject": ["Zope"],
                "is_folderish": False,
                "created": "2025-03-01T08:00:00+00:00",
                "modified": "2025-03-01T08:00:00+00:00",
                "effective": "2025-03-01T08:00:00+00:00",
                "expires": None,
                "sortable_title": "private doc",
                "UID": "uid-101",
                "getObjPositionInParent": 2,
                "allowedRolesAndUsers": ["Manager"],
            },
            "text": None,
        },
        102: {
            "path": "/plone/folder1",
            "idx": {
                "portal_type": "Folder",
                "review_state": "published",
                "Title": "Folder One",
                "Subject": ["Python"],
                "is_folderish": True,
                "created": "2025-02-01T12:00:00+00:00",
                "modified": "2025-07-01T12:00:00+00:00",
                "effective": "2025-02-01T12:00:00+00:00",
                "expires": None,
                "sortable_title": "folder one",
                "UID": "uid-102",
                "getObjPositionInParent": 3,
                "allowedRolesAndUsers": ["Anonymous"],
            },
            "text": "Folder with Python content",
        },
        103: {
            "path": "/plone/folder1/sub-doc",
            "idx": {
                "portal_type": "Document",
                "review_state": "published",
                "Title": "Sub Document",
                "Subject": ["Python", "Plone"],
                "is_folderish": False,
                "created": "2025-04-01T09:00:00+00:00",
                "modified": "2025-04-01T09:00:00+00:00",
                "effective": "2025-04-01T09:00:00+00:00",
                "expires": None,
                "sortable_title": "sub document",
                "UID": "uid-103",
                "getObjPositionInParent": 1,
                "allowedRolesAndUsers": ["Anonymous"],
            },
            "text": "A sub document about Python and Plone",
        },
        104: {
            "path": "/plone/news",
            "idx": {
                "portal_type": "News Item",
                "review_state": "published",
                "Title": "Breaking News",
                "Subject": ["News"],
                "is_folderish": False,
                "created": "2025-05-01T16:00:00+00:00",
                "modified": "2025-08-01T16:00:00+00:00",
                "effective": "2025-05-01T16:00:00+00:00",
                "expires": "2025-06-01T00:00:00+00:00",
                "sortable_title": "breaking news",
                "UID": "uid-104",
                "getObjPositionInParent": 4,
                "allowedRolesAndUsers": ["Anonymous"],
            },
            "text": "Breaking news about the latest events",
        },
    }

    for zoid, data in objects.items():
        insert_object(conn, zoid=zoid)
        catalog_object(
            conn,
            zoid=zoid,
            path=data["path"],
            idx=data["idx"],
            searchable_text=data["text"],
        )
    conn.commit()
    return objects


def _query_zoids(conn, query_dict):
    """Execute query and return sorted list of zoids."""
    rows = execute_query(conn, query_dict, columns="zoid")
    return sorted(row["zoid"] for row in rows)


# ---------------------------------------------------------------------------
# FieldIndex queries
# ---------------------------------------------------------------------------


class TestFieldIndexIntegration:
    def test_exact_match(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"portal_type": "Folder"})
        assert zoids == [102]

    def test_multi_value(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"portal_type": {"query": ["Document", "Folder"]}})
        assert set(zoids) == {100, 101, 102, 103}

    def test_not_single(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"portal_type": {"not": "Document"}})
        assert 100 not in zoids
        assert 102 in zoids  # Folder
        assert 104 in zoids  # News Item

    def test_not_list(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"portal_type": {"not": ["Document", "Folder"]}})
        assert zoids == [104]  # Only News Item


# ---------------------------------------------------------------------------
# KeywordIndex queries
# ---------------------------------------------------------------------------


class TestKeywordIndexIntegration:
    def test_or_overlap(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(
            conn, {"Subject": {"query": ["Python", "News"], "operator": "or"}}
        )
        # Python: 100, 102, 103; News: 104
        assert set(zoids) == {100, 102, 103, 104}

    def test_and_containment(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(
            conn, {"Subject": {"query": ["Python", "Zope"], "operator": "and"}}
        )
        # Only doc1 has both Python AND Zope
        assert zoids == [100]

    def test_single_value(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"Subject": "Plone"})
        assert zoids == [103]


# ---------------------------------------------------------------------------
# BooleanIndex queries
# ---------------------------------------------------------------------------


class TestBooleanIndexIntegration:
    def test_true(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"is_folderish": True})
        assert zoids == [102]

    def test_false(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"is_folderish": False})
        assert set(zoids) == {100, 101, 103, 104}


# ---------------------------------------------------------------------------
# DateIndex queries
# ---------------------------------------------------------------------------


class TestDateIndexIntegration:
    def test_range_min(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        # Objects modified on or after 2025-07-01
        dt = datetime(2025, 7, 1, tzinfo=UTC)
        zoids = _query_zoids(conn, {"modified": {"query": dt, "range": "min"}})
        assert set(zoids) == {102, 104}  # modified 2025-07-01 and 2025-08-01

    def test_range_max(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        # Objects modified on or before 2025-04-01
        dt = datetime(2025, 4, 1, 9, 0, 0, tzinfo=UTC)
        zoids = _query_zoids(conn, {"modified": {"query": dt, "range": "max"}})
        assert set(zoids) == {101, 103}  # modified 2025-03-01 and 2025-04-01

    def test_range_min_max(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        dt_min = datetime(2025, 3, 1, tzinfo=UTC)
        dt_max = datetime(2025, 6, 30, tzinfo=UTC)
        zoids = _query_zoids(
            conn, {"modified": {"query": [dt_min, dt_max], "range": "min:max"}}
        )
        assert set(zoids) == {101, 103, 100}


# ---------------------------------------------------------------------------
# DateRangeIndex queries
# ---------------------------------------------------------------------------


class TestDateRangeIndexIntegration:
    def test_effective_range_current(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        # "now" = 2025-06-15: effective <= now AND (expires >= now OR NULL)
        now = datetime(2025, 6, 15, tzinfo=UTC)
        zoids = _query_zoids(conn, {"effectiveRange": now})
        # doc1: eff=Jan15 <= Jun15 ✓, exp=Dec31 >= Jun15 ✓ → in
        # doc2: eff=Mar1 <= Jun15 ✓, exp=NULL → in
        # folder1: eff=Feb1 <= Jun15 ✓, exp=NULL → in
        # sub-doc: eff=Apr1 <= Jun15 ✓, exp=NULL → in
        # news: eff=May1 <= Jun15 ✓, exp=Jun1 >= Jun15? NO → out
        assert set(zoids) == {100, 101, 102, 103}

    def test_effective_range_expired(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        # "now" = 2027-01-15: after doc1's expires (Dec31 2026)
        now = datetime(2027, 1, 15, tzinfo=UTC)
        zoids = _query_zoids(conn, {"effectiveRange": now})
        # doc1: exp=Dec31 2026 < Jan15 2027 → out
        # doc2,folder1,sub-doc: exp=NULL → in
        # news: exp=Jun1 2025 < Jan15 2027 → out
        assert set(zoids) == {101, 102, 103}


# ---------------------------------------------------------------------------
# UUIDIndex queries
# ---------------------------------------------------------------------------


class TestUUIDIndexIntegration:
    def test_exact_match(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"UID": "uid-103"})
        assert zoids == [103]


# ---------------------------------------------------------------------------
# Full-text search
# ---------------------------------------------------------------------------


class TestSearchableTextIntegration:
    def test_find_by_text(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"SearchableText": "fox"})
        assert zoids == [100]

    def test_no_match(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"SearchableText": "nonexistent"})
        assert zoids == []


# ---------------------------------------------------------------------------
# Path queries
# ---------------------------------------------------------------------------


class TestPathIntegration:
    def test_subtree(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"path": "/plone/folder1"})
        assert set(zoids) == {102, 103}  # folder1 + sub-doc

    def test_exact(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"path": {"query": "/plone/folder1", "depth": 0}})
        assert zoids == [102]

    def test_children(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(conn, {"path": {"query": "/plone", "depth": 1}})
        # Direct children of /plone: doc1, doc2, folder1, news (NOT sub-doc)
        assert set(zoids) == {100, 101, 102, 104}


# ---------------------------------------------------------------------------
# Combined queries
# ---------------------------------------------------------------------------


class TestCombinedQueries:
    def test_type_and_state(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(
            conn, {"portal_type": "Document", "review_state": "published"}
        )
        assert set(zoids) == {100, 103}

    def test_type_subject_and_state(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        zoids = _query_zoids(
            conn,
            {
                "portal_type": "Document",
                "review_state": "published",
                "Subject": "Python",
            },
        )
        assert set(zoids) == {100, 103}


# ---------------------------------------------------------------------------
# Sort + Pagination
# ---------------------------------------------------------------------------


class TestSortAndPagination:
    def test_sort_by_title(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        rows = execute_query(
            conn,
            {"portal_type": "Document", "sort_on": "sortable_title"},
            columns="zoid",
        )
        zoids = [r["zoid"] for r in rows]
        assert zoids == [100, 101, 103]  # hello, private, sub

    def test_sort_descending(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        rows = execute_query(
            conn,
            {
                "portal_type": "Document",
                "sort_on": "sortable_title",
                "sort_order": "descending",
            },
            columns="zoid",
        )
        zoids = [r["zoid"] for r in rows]
        assert zoids == [103, 101, 100]  # sub, private, hello

    def test_sort_with_limit(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        rows = execute_query(
            conn,
            {"sort_on": "sortable_title", "sort_limit": 2},
            columns="zoid",
        )
        assert len(rows) == 2

    def test_pagination(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_test_data(conn)
        # Page 1: first 2
        rows1 = execute_query(
            conn,
            {"sort_on": "sortable_title", "b_start": 0, "b_size": 2},
            columns="zoid",
        )
        # Page 2: next 2
        rows2 = execute_query(
            conn,
            {"sort_on": "sortable_title", "b_start": 2, "b_size": 2},
            columns="zoid",
        )
        assert len(rows1) == 2
        assert len(rows2) == 2
        # No overlap
        zoids1 = {r["zoid"] for r in rows1}
        zoids2 = {r["zoid"] for r in rows2}
        assert zoids1.isdisjoint(zoids2)
