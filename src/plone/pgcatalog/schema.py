"""Schema extension for object_state table (owned by zodb-pgjsonb).

plone.pgcatalog adds catalog columns + indexes to the existing object_state
table via ALTER TABLE.  The base table is created by zodb-pgjsonb; this module
only extends it.
"""


CATALOG_COLUMNS = """\
-- Catalog columns on object_state (plone.pgcatalog extension)
-- path: dedicated column for brain construction (SELECT zoid, path)
-- parent_path/path_depth: legacy columns, path queries use idx JSONB
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS path TEXT;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS parent_path TEXT;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS path_depth INTEGER;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS idx JSONB;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS searchable_text TSVECTOR;
"""

CATALOG_FUNCTIONS = """\
-- Immutable timestamptz cast for expression indexes.
-- The built-in ::timestamptz cast is STABLE (depends on session timezone),
-- but our ISO 8601 dates always include timezone info, making the result
-- deterministic.  This wrapper lets PG accept it in index expressions.
CREATE OR REPLACE FUNCTION pgcatalog_to_timestamptz(text)
RETURNS timestamptz AS $$
    SELECT $1::timestamptz;
$$ LANGUAGE sql IMMUTABLE STRICT;
"""

CATALOG_INDEXES = """\
-- Path column index (for brain SELECT; queries use idx JSONB below)
CREATE INDEX IF NOT EXISTS idx_os_path
    ON object_state (path) WHERE path IS NOT NULL;

-- GIN index on idx JSONB (default jsonb_ops for ?| support)
CREATE INDEX IF NOT EXISTS idx_os_catalog
    ON object_state USING gin (idx) WHERE idx IS NOT NULL;

-- Path expression indexes on idx JSONB (unified path query support)
CREATE INDEX IF NOT EXISTS idx_os_cat_path
    ON object_state ((idx->>'path')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_pattern
    ON object_state USING btree ((idx->>'path') text_pattern_ops) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_parent
    ON object_state ((idx->>'path_parent')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_path_depth
    ON object_state (((idx->>'path_depth')::integer)) WHERE idx IS NOT NULL;

-- Expression indexes for date range queries (B-tree via immutable wrapper)
CREATE INDEX IF NOT EXISTS idx_os_cat_modified
    ON object_state (pgcatalog_to_timestamptz(idx->>'modified')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_created
    ON object_state (pgcatalog_to_timestamptz(idx->>'created')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_effective
    ON object_state (pgcatalog_to_timestamptz(idx->>'effective')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_expires
    ON object_state (pgcatalog_to_timestamptz(idx->>'expires')) WHERE idx IS NOT NULL;

-- Expression indexes for common sort/filter fields
CREATE INDEX IF NOT EXISTS idx_os_cat_sortable_title
    ON object_state ((idx->>'sortable_title')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_portal_type
    ON object_state ((idx->>'portal_type')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_review_state
    ON object_state ((idx->>'review_state')) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_uid
    ON object_state ((idx->>'uid')) WHERE idx IS NOT NULL;

-- Full-text search (GIN on tsvector)
CREATE INDEX IF NOT EXISTS idx_os_searchable_text
    ON object_state USING gin (searchable_text) WHERE searchable_text IS NOT NULL;
"""

# All expected catalog columns with their PG types (for verification)
EXPECTED_COLUMNS = {
    "path": "text",
    "parent_path": "text",
    "path_depth": "integer",
    "idx": "jsonb",
    "searchable_text": "tsvector",
}

# All expected catalog indexes
EXPECTED_INDEXES = [
    "idx_os_path",
    "idx_os_catalog",
    "idx_os_cat_path",
    "idx_os_cat_path_pattern",
    "idx_os_cat_path_parent",
    "idx_os_cat_path_depth",
    "idx_os_cat_modified",
    "idx_os_cat_created",
    "idx_os_cat_effective",
    "idx_os_cat_expires",
    "idx_os_cat_sortable_title",
    "idx_os_cat_portal_type",
    "idx_os_cat_review_state",
    "idx_os_cat_uid",
    "idx_os_searchable_text",
]


def install_catalog_schema(conn):
    """Extend object_state with catalog columns and indexes.

    Prerequisite: the base object_state table must already exist
    (created by zodb-pgjsonb).

    Args:
        conn: psycopg connection (autocommit or in a transaction block)
    """
    conn.execute(CATALOG_COLUMNS)
    conn.execute(CATALOG_FUNCTIONS)
    conn.execute(CATALOG_INDEXES)
