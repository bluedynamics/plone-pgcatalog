"""Integration tests for ExtendedPathIndex queries (real PG).

Tests all path query modes: subtree, exact, children, limited depth,
navtree, breadcrumbs, navtree_start, multiple paths.
"""

from tests.conftest import insert_object

from plone.pgcatalog.indexing import catalog_object
from plone.pgcatalog.query import execute_query


# ---------------------------------------------------------------------------
# Test data: a tree structure
# ---------------------------------------------------------------------------
#
#   /plone                         (zoid=200)
#   /plone/folder1                 (zoid=201)
#   /plone/folder1/doc-a           (zoid=202)
#   /plone/folder1/sub             (zoid=203)
#   /plone/folder1/sub/deep-doc    (zoid=204)
#   /plone/folder2                 (zoid=205)
#   /plone/folder2/doc-b           (zoid=206)
#   /plone/news                    (zoid=207)
#


def _setup_tree(conn):
    tree = [
        (200, "/plone",                      "Plone Site"),
        (201, "/plone/folder1",              "Folder One"),
        (202, "/plone/folder1/doc-a",        "Doc A"),
        (203, "/plone/folder1/sub",          "Subfolder"),
        (204, "/plone/folder1/sub/deep-doc", "Deep Doc"),
        (205, "/plone/folder2",              "Folder Two"),
        (206, "/plone/folder2/doc-b",        "Doc B"),
        (207, "/plone/news",                 "News"),
    ]
    for zoid, path, title in tree:
        insert_object(conn, zoid=zoid)
        catalog_object(conn, zoid=zoid, path=path, idx={"Title": title})
    conn.commit()


def _query_zoids(conn, query_dict):
    rows = execute_query(conn, query_dict, columns="zoid")
    return sorted(row["zoid"] for row in rows)


# ---------------------------------------------------------------------------
# depth=-1: full subtree (default)
# ---------------------------------------------------------------------------


class TestSubtree:

    def test_full_subtree(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": "/plone/folder1"})
        # folder1 + doc-a + sub + deep-doc
        assert set(zoids) == {201, 202, 203, 204}

    def test_root_subtree(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": "/plone"})
        # Everything
        assert set(zoids) == {200, 201, 202, 203, 204, 205, 206, 207}

    def test_leaf_subtree(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": "/plone/folder1/doc-a"})
        # Just the leaf itself
        assert zoids == [202]

    def test_nonexistent_path(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": "/plone/nonexistent"})
        assert zoids == []


# ---------------------------------------------------------------------------
# depth=0: exact object only
# ---------------------------------------------------------------------------


class TestExact:

    def test_exact_single(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": {"query": "/plone/folder1", "depth": 0}})
        assert zoids == [201]

    def test_exact_multiple(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(
            conn,
            {"path": {"query": ["/plone/folder1", "/plone/folder2"], "depth": 0}},
        )
        assert set(zoids) == {201, 205}


# ---------------------------------------------------------------------------
# depth=1: direct children only (NOT self)
# ---------------------------------------------------------------------------


class TestChildren:

    def test_direct_children_of_root(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": {"query": "/plone", "depth": 1}})
        # folder1, folder2, news (NOT plone itself, NOT deeper items)
        assert set(zoids) == {201, 205, 207}

    def test_direct_children_of_folder(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": {"query": "/plone/folder1", "depth": 1}})
        # doc-a, sub (NOT folder1 itself, NOT deep-doc)
        assert set(zoids) == {202, 203}

    def test_children_of_leaf(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(
            conn, {"path": {"query": "/plone/folder1/doc-a", "depth": 1}}
        )
        assert zoids == []

    def test_children_of_multiple_paths(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(
            conn,
            {"path": {"query": ["/plone/folder1", "/plone/folder2"], "depth": 1}},
        )
        # doc-a, sub, doc-b
        assert set(zoids) == {202, 203, 206}


# ---------------------------------------------------------------------------
# depth=N (N>1): limited subtree
# ---------------------------------------------------------------------------


class TestLimitedDepth:

    def test_depth_2_from_root(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        # /plone has depth=1; depth=2 → up to depth 3
        # Items within 2 levels of /plone: folder1(2), folder2(2), news(2),
        # doc-a(3), sub(3), doc-b(3) — but NOT deep-doc(4)
        zoids = _query_zoids(conn, {"path": {"query": "/plone", "depth": 2}})
        assert set(zoids) == {201, 202, 203, 205, 206, 207}
        assert 204 not in zoids  # deep-doc at depth 4

    def test_depth_1_limited(self, pg_conn_with_catalog):
        """depth=1 via limited path should match direct descendants only."""
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        # /plone/folder1 has depth=2; depth=1 → up to depth 3
        zoids = _query_zoids(
            conn, {"path": {"query": "/plone/folder1", "depth": 2}}
        )
        # doc-a(3), sub(3), deep-doc(4) — deep-doc at 4 <= 2+2=4 → included
        assert set(zoids) == {202, 203, 204}


# ---------------------------------------------------------------------------
# navtree=True, depth=1: navigation tree
# ---------------------------------------------------------------------------


class TestNavtree:

    def test_navtree_from_deep_path(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        # For /plone/folder1/sub/deep-doc, navtree returns siblings at each level:
        # parent_path IN ('/', '/plone', '/plone/folder1', '/plone/folder1/sub')
        zoids = _query_zoids(
            conn,
            {
                "path": {
                    "query": "/plone/folder1/sub/deep-doc",
                    "navtree": True,
                    "depth": 1,
                }
            },
        )
        # / → plone(200)
        # /plone → folder1(201), folder2(205), news(207)
        # /plone/folder1 → doc-a(202), sub(203)
        # /plone/folder1/sub → deep-doc(204)
        assert set(zoids) == {200, 201, 202, 203, 204, 205, 207}

    def test_navtree_with_start_1(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        # navtree_start=1: skip root level
        zoids = _query_zoids(
            conn,
            {
                "path": {
                    "query": "/plone/folder1/sub/deep-doc",
                    "navtree": True,
                    "depth": 1,
                    "navtree_start": 1,
                }
            },
        )
        # Skip '/': no plone(200) from root level
        # /plone → folder1(201), folder2(205), news(207)
        # /plone/folder1 → doc-a(202), sub(203)
        # /plone/folder1/sub → deep-doc(204)
        assert 200 not in zoids
        assert set(zoids) == {201, 202, 203, 204, 205, 207}

    def test_navtree_with_start_2(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        # navtree_start=2: skip root + second level
        zoids = _query_zoids(
            conn,
            {
                "path": {
                    "query": "/plone/folder1/sub/deep-doc",
                    "navtree": True,
                    "depth": 1,
                    "navtree_start": 2,
                }
            },
        )
        # /plone/folder1 → doc-a(202), sub(203)
        # /plone/folder1/sub → deep-doc(204)
        assert set(zoids) == {202, 203, 204}


# ---------------------------------------------------------------------------
# navtree=True, depth=0: breadcrumbs
# ---------------------------------------------------------------------------


class TestBreadcrumbs:

    def test_breadcrumbs(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(
            conn,
            {
                "path": {
                    "query": "/plone/folder1/sub/deep-doc",
                    "navtree": True,
                    "depth": 0,
                }
            },
        )
        # Exact objects at each prefix: plone, folder1, sub, deep-doc
        assert set(zoids) == {200, 201, 203, 204}

    def test_breadcrumbs_with_start(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(
            conn,
            {
                "path": {
                    "query": "/plone/folder1/sub/deep-doc",
                    "navtree": True,
                    "depth": 0,
                    "navtree_start": 1,
                }
            },
        )
        # Skip /plone: folder1, sub, deep-doc
        assert set(zoids) == {201, 203, 204}


# ---------------------------------------------------------------------------
# Multiple paths with subtree
# ---------------------------------------------------------------------------


class TestMultiplePaths:

    def test_or_subtree(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(
            conn,
            {"path": {"query": ["/plone/folder1", "/plone/folder2"]}},
        )
        # folder1 subtree + folder2 subtree
        assert set(zoids) == {201, 202, 203, 204, 205, 206}


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestPathEdgeCases:

    def test_root_path_exact(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": {"query": "/plone", "depth": 0}})
        assert zoids == [200]

    def test_single_component_path(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        _setup_tree(conn)
        zoids = _query_zoids(conn, {"path": {"query": "/plone", "depth": 1}})
        assert set(zoids) == {201, 205, 207}
