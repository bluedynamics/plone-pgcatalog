"""Tests for plone.pgcatalog schema extension on object_state."""

from plone.pgcatalog.schema import EXPECTED_COLUMNS
from plone.pgcatalog.schema import EXPECTED_INDEXES
from plone.pgcatalog.schema import install_catalog_schema


class TestSchemaInstallation:
    """Test that ALTER TABLE DDL installs correctly on object_state."""

    def test_columns_created(self, pg_conn_with_catalog):
        """All catalog columns exist with correct types after install."""
        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'object_state'
                  AND column_name = ANY(%(cols)s)
                ORDER BY column_name
                """,
                {"cols": list(EXPECTED_COLUMNS.keys())},
            )
            rows = cur.fetchall()

        found = {row["column_name"]: row["data_type"] for row in rows}
        for col_name, col_type in EXPECTED_COLUMNS.items():
            assert col_name in found, f"Column {col_name} not found"
            assert found[col_name] == col_type, (
                f"Column {col_name}: expected {col_type}, got {found[col_name]}"
            )

    def test_indexes_created(self, pg_conn_with_catalog):
        """All catalog indexes exist after install."""
        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'object_state'
                  AND indexname = ANY(%(idxs)s)
                """,
                {"idxs": EXPECTED_INDEXES},
            )
            rows = cur.fetchall()

        found = {row["indexname"] for row in rows}
        for idx_name in EXPECTED_INDEXES:
            assert idx_name in found, f"Index {idx_name} not found"

    def test_idempotent(self, pg_conn_with_catalog):
        """Running install_catalog_schema twice does not error."""
        # Already installed by the fixture — run again
        install_catalog_schema(pg_conn_with_catalog)
        pg_conn_with_catalog.commit()

        # Verify columns still correct
        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'object_state'
                  AND column_name = ANY(%(cols)s)
                """,
                {"cols": list(EXPECTED_COLUMNS.keys())},
            )
            rows = cur.fetchall()

        assert len(rows) == len(EXPECTED_COLUMNS)

    def test_non_catalog_rows_unaffected(self, pg_conn_with_catalog):
        """Rows without catalog data have NULL in catalog columns."""
        with pg_conn_with_catalog.cursor() as cur:
            # Insert a base transaction + object (simulating zodb-pgjsonb)
            cur.execute(
                "INSERT INTO transaction_log (tid) VALUES (1)"
            )
            cur.execute(
                """
                INSERT INTO object_state
                    (zoid, tid, class_mod, class_name, state, state_size)
                VALUES (1, 1, 'myapp', 'Foo', '{"title": "hello"}'::jsonb, 42)
                """
            )
        pg_conn_with_catalog.commit()

        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                "SELECT path, parent_path, path_depth, idx, searchable_text "
                "FROM object_state WHERE zoid = 1"
            )
            row = cur.fetchone()

        assert row["path"] is None
        assert row["parent_path"] is None
        assert row["path_depth"] is None
        assert row["idx"] is None
        assert row["searchable_text"] is None

    def test_base_columns_preserved(self, pg_conn_with_catalog):
        """Original object_state columns still exist after catalog extension."""
        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'object_state'
                ORDER BY ordinal_position
                """
            )
            rows = cur.fetchall()

        col_names = [row["column_name"] for row in rows]
        # Base columns from zodb-pgjsonb
        for base_col in ["zoid", "tid", "class_mod", "class_name", "state", "state_size", "refs"]:
            assert base_col in col_names, f"Base column {base_col} missing"

    def test_catalog_columns_nullable(self, pg_conn_with_catalog):
        """Catalog columns allow NULL (non-cataloged objects)."""
        with pg_conn_with_catalog.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'object_state'
                  AND column_name = ANY(%(cols)s)
                """,
                {"cols": list(EXPECTED_COLUMNS.keys())},
            )
            rows = cur.fetchall()

        for row in rows:
            assert row["is_nullable"] == "YES", (
                f"Column {row['column_name']} should be nullable"
            )
