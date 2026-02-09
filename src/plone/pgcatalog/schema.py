"""Schema extension for object_state table (owned by zodb-pgjsonb).

plone.pgcatalog adds catalog columns + indexes to the existing object_state
table via ALTER TABLE.  The base table is created by zodb-pgjsonb; this module
only extends it.
"""


CATALOG_COLUMNS = """\
-- Catalog columns on object_state (plone.pgcatalog extension)
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS path TEXT;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS parent_path TEXT;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS path_depth INTEGER;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS idx JSONB;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS searchable_text TSVECTOR;
"""

CATALOG_INDEXES = """\
-- Path indexes (B-tree)
CREATE INDEX IF NOT EXISTS idx_os_path
    ON object_state (path) WHERE path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_path_pattern
    ON object_state USING btree (path text_pattern_ops) WHERE path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_parent_path
    ON object_state (parent_path) WHERE parent_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_path_depth
    ON object_state (path_depth) WHERE path_depth IS NOT NULL;

-- GIN index on idx JSONB (default jsonb_ops for ?| support)
CREATE INDEX IF NOT EXISTS idx_os_catalog
    ON object_state USING gin (idx) WHERE idx IS NOT NULL;

-- Expression indexes for date range queries (B-tree)
CREATE INDEX IF NOT EXISTS idx_os_cat_modified
    ON object_state (((idx->>'modified')::timestamptz)) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_created
    ON object_state (((idx->>'created')::timestamptz)) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_effective
    ON object_state (((idx->>'effective')::timestamptz)) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_expires
    ON object_state (((idx->>'expires')::timestamptz)) WHERE idx IS NOT NULL;

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
    "idx_os_path_pattern",
    "idx_os_parent_path",
    "idx_os_path_depth",
    "idx_os_catalog",
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
    conn.execute(CATALOG_INDEXES)
