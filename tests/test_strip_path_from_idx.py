"""Tests for stripping path/parent_path/path_depth from idx JSONB.

See: docs/plans/2026-04-15-strip-path-from-idx-jsonb.md
Issue: bluedynamics/plone-pgcatalog#132

Task 1 — failing test scaffold.  Later tasks make these green one by one.
"""

from typing import ClassVar
from unittest import mock


PATH_KEYS_IN_IDX = ("path", "path_parent", "path_depth")


def _build_sql(query_dict):
    """Run build_query() and return the generated SQL + params.

    Mirrors what ``_execute_query`` produces — enough for WHERE-clause
    assertions without hitting the database.
    """
    from plone.pgcatalog.query import build_query

    qr = build_query(query_dict)
    sql = f"SELECT zoid FROM object_state WHERE {qr['where']}"
    if qr["order_by"]:
        sql += f" ORDER BY {qr['order_by']}"
    return sql, qr["params"]


class TestWriterDoesNotDuplicatePath:
    def test_catalog_object_strips_path_keys(self, pg_conn_with_catalog, sample_zoid):
        from plone.pgcatalog.indexing import catalog_object

        idx_in = {"portal_type": "Document", "Title": "T"}
        catalog_object(pg_conn_with_catalog, sample_zoid, "/Plone/doc", idx_in)
        pg_conn_with_catalog.commit()
        row = pg_conn_with_catalog.execute(
            "SELECT path, parent_path, path_depth, idx "
            "FROM object_state WHERE zoid = %s",
            (sample_zoid,),
        ).fetchone()
        assert row["path"] == "/Plone/doc"
        assert row["parent_path"] == "/Plone"
        assert row["path_depth"] == 2
        for key in PATH_KEYS_IN_IDX:
            assert key not in row["idx"], (
                f"{key!r} must not be written to idx JSONB after cleanup"
            )

    def test_pgcatalog_tool_set_pg_annotation_strips_path_keys(self, plone_obj):
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.processor import ANNOTATION_KEY

        tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool)
        with (
            mock.patch.object(
                PlonePGCatalogTool, "_wrap_object", return_value=plone_obj
            ),
            mock.patch.object(PlonePGCatalogTool, "_extract_idx", return_value={}),
            mock.patch.object(
                PlonePGCatalogTool, "_extract_searchable_text", return_value=None
            ),
        ):
            ok = tool._set_pg_annotation(plone_obj, "/Plone/doc")
        assert ok is True
        pending = plone_obj.__dict__[ANNOTATION_KEY]
        for key in PATH_KEYS_IN_IDX:
            assert key not in pending["idx"], (
                f"{key!r} must not be in pending idx after cleanup"
            )

    def test_processor_reads_typed_cols_not_idx(self, sample_pending):
        """CatalogStateProcessor.process must source parent_path/path_depth
        from compute_path_info(path), not from idx.
        """
        from plone.pgcatalog.pending import set_pending
        from plone.pgcatalog.processor import CatalogStateProcessor

        processor = CatalogStateProcessor()
        zoid = 4242
        set_pending(zoid, sample_pending)
        result = processor.process(zoid, "mymod", "MyCls", {})
        assert result["path"] == "/a/b/c"
        assert result["parent_path"] == "/a/b"
        assert result["path_depth"] == 3


class TestBulkMoveDoesNotTouchIdxPathKeys:
    def test_bulk_move_updates_typed_only(self, pg_conn_with_catalog, two_objects_at):
        from plone.pgcatalog.pending import add_pending_move
        from plone.pgcatalog.processor import CatalogStateProcessor

        add_pending_move("/Plone/old", "/Plone/new", 0)
        with pg_conn_with_catalog.cursor() as cursor:
            processor = CatalogStateProcessor()
            processor.finalize(cursor)
        pg_conn_with_catalog.commit()
        for old_path in ("/Plone/old/a", "/Plone/old/b"):
            new_path = old_path.replace("/old/", "/new/")
            row = pg_conn_with_catalog.execute(
                "SELECT path, parent_path, idx FROM object_state WHERE path = %s",
                (new_path,),
            ).fetchone()
            assert row is not None, f"row not found at {new_path}"
            assert row["parent_path"] == "/Plone/new"
            for key in PATH_KEYS_IN_IDX:
                assert key not in row["idx"]


class TestQueryBuilderDispatchesPathToTypedColumns:
    def test_builtin_path_index_uses_typed_columns(self):
        sql, _ = _build_sql({"path": {"query": "/Plone/x", "depth": -1}})
        assert "idx->>'path'" not in sql, (
            f"subtree path query should use typed column, got: {sql}"
        )
        assert "path" in sql  # the typed column must appear

    def test_builtin_path_navtree_uses_typed_parent_path(self):
        sql, _ = _build_sql(
            {"path": {"query": "/Plone/a/b", "navtree": True, "depth": 1}}
        )
        assert "idx->>'path_parent'" not in sql, (
            f"navtree query should use typed parent_path, got: {sql}"
        )
        assert "parent_path" in sql

    def test_builtin_path_depth_uses_typed_path_depth(self):
        sql, _ = _build_sql({"path": {"query": "/Plone", "depth": 2}})
        assert "(idx->>'path_depth')::integer" not in sql, (
            f"depth-limited query should use typed path_depth, got: {sql}"
        )
        assert "path_depth" in sql

    def test_custom_path_index_keeps_jsonb_keys(self):
        sql, _ = _build_sql({"tgpath": {"query": "/Plone/x", "depth": -1}})
        assert "idx->>'tgpath'" in sql, (
            f"custom path index must still query idx JSONB, got: {sql}"
        )


class TestSchemaUsesTypedColumns:
    EXPECTED_INDEXES: ClassVar[list[str]] = [
        "idx_os_cat_path",
        "idx_os_cat_path_pattern",
        "idx_os_cat_path_parent",
        "idx_os_cat_path_depth",
        "idx_os_cat_parent_type",
        "idx_os_cat_path_type",
        "idx_os_cat_path_depth_type",
        "idx_os_cat_nav_visible",
    ]

    def test_path_indexes_reference_typed_columns(self, pg_conn_with_catalog):
        rows = pg_conn_with_catalog.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE tablename='object_state' AND indexname = ANY(%s)",
            (self.EXPECTED_INDEXES,),
        ).fetchall()
        assert rows, "expected catalog path indexes to be present"
        for r in rows:
            assert "idx ->> 'path'" not in r["indexdef"], (
                f"{r['indexname']}: still uses idx->>'path' — migration incomplete"
            )
            assert "idx ->> 'path_parent'" not in r["indexdef"], (
                f"{r['indexname']}: still uses idx->>'path_parent'"
            )
            assert "idx ->> 'path_depth'" not in r["indexdef"], (
                f"{r['indexname']}: still uses idx->>'path_depth'"
            )

    def test_extended_statistics_reference_typed_columns(self, pg_conn_with_catalog):
        rows = pg_conn_with_catalog.execute(
            """
            SELECT stxname, pg_get_statisticsobjdef(oid) AS stxdef
            FROM pg_statistic_ext
            WHERE stxname IN (
                'stts_os_parent_type',
                'stts_os_path_type',
                'stts_os_path_depth_type'
            )
            """
        ).fetchall()
        assert rows, "expected path-related extended statistics to be present"
        for r in rows:
            assert "idx ->> 'path'" not in r["stxdef"]
            assert "idx ->> 'path_parent'" not in r["stxdef"]
            assert "idx ->> 'path_depth'" not in r["stxdef"]


class TestMigrationStripsPathKeys:
    def test_strip_removes_keys_idempotently(self, pg_conn_with_catalog, dirty_rows):
        from plone.pgcatalog.migrations.strip_path_keys import run

        run(pg_conn_with_catalog, batch_size=2)  # forces multiple batches
        rows = pg_conn_with_catalog.execute(
            "SELECT idx FROM object_state WHERE zoid = ANY(%s)",
            (dirty_rows,),
        ).fetchall()
        for r in rows:
            for key in PATH_KEYS_IN_IDX:
                assert key not in r["idx"]

    def test_strip_is_idempotent(self, pg_conn_with_catalog, dirty_rows):
        from plone.pgcatalog.migrations.strip_path_keys import run

        run(pg_conn_with_catalog)
        result = run(pg_conn_with_catalog)
        assert result["rows_updated"] == 0
