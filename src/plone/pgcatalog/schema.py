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
--
-- SECURITY NOTE: declared IMMUTABLE despite wrapping a STABLE cast.
-- Safe because all stored dates include explicit timezone info (ISO 8601).
-- If a value without timezone were stored, the result would depend on the
-- session timezone setting and PG could cache incorrect index entries.
-- Ensure all date values written to idx JSONB include timezone offsets.
CREATE OR REPLACE FUNCTION pgcatalog_to_timestamptz(text)
RETURNS timestamptz AS $$
    SELECT $1::timestamptz;
$$ LANGUAGE sql IMMUTABLE STRICT;
"""

CATALOG_LANG_FUNCTION = """\
-- Map Plone language codes (ISO 639-1) to PostgreSQL text search configurations.
-- Used at both index time (to_tsvector) and query time (plainto_tsquery).
-- Returns 'simple' for NULL, empty, or unmapped languages.
CREATE OR REPLACE FUNCTION pgcatalog_lang_to_regconfig(lang text)
RETURNS text AS $$
BEGIN
    IF lang IS NULL OR lang = '' THEN
        RETURN 'simple';
    END IF;
    lang := lower(split_part(lang, '-', 1));
    lang := lower(split_part(lang, '_', 1));
    RETURN CASE lang
        WHEN 'ar' THEN 'arabic'
        WHEN 'hy' THEN 'armenian'
        WHEN 'eu' THEN 'basque'
        WHEN 'ca' THEN 'catalan'
        WHEN 'da' THEN 'danish'
        WHEN 'nl' THEN 'dutch'
        WHEN 'en' THEN 'english'
        WHEN 'fi' THEN 'finnish'
        WHEN 'fr' THEN 'french'
        WHEN 'de' THEN 'german'
        WHEN 'el' THEN 'greek'
        WHEN 'hi' THEN 'hindi'
        WHEN 'hu' THEN 'hungarian'
        WHEN 'id' THEN 'indonesian'
        WHEN 'ga' THEN 'irish'
        WHEN 'it' THEN 'italian'
        WHEN 'lt' THEN 'lithuanian'
        WHEN 'ne' THEN 'nepali'
        WHEN 'nb' THEN 'norwegian'
        WHEN 'nn' THEN 'norwegian'
        WHEN 'no' THEN 'norwegian'
        WHEN 'pt' THEN 'portuguese'
        WHEN 'ro' THEN 'romanian'
        WHEN 'ru' THEN 'russian'
        WHEN 'sr' THEN 'serbian'
        WHEN 'es' THEN 'spanish'
        WHEN 'sv' THEN 'swedish'
        WHEN 'ta' THEN 'tamil'
        WHEN 'tr' THEN 'turkish'
        WHEN 'yi' THEN 'yiddish'
        ELSE 'simple'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
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

-- GIN expression indexes for ZCTextIndex fields stored in idx JSONB
-- (Title/Description: word-level matching via tsvector, 'simple' config)
CREATE INDEX IF NOT EXISTS idx_os_cat_title_tsv
    ON object_state USING gin (
        to_tsvector('simple'::regconfig, COALESCE(idx->>'Title', ''))
    ) WHERE idx IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_os_cat_description_tsv
    ON object_state USING gin (
        to_tsvector('simple'::regconfig, COALESCE(idx->>'Description', ''))
    ) WHERE idx IS NOT NULL;
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
    "idx_os_cat_title_tsv",
    "idx_os_cat_description_tsv",
]


def _load_rrule_sql():
    """Load the vendored rrule_plpgsql SQL from the package data file."""
    import pathlib

    sql_path = pathlib.Path(__file__).parent / "rrule_schema.sql"
    return sql_path.read_text()


RRULE_FUNCTIONS = _load_rrule_sql()


def install_catalog_schema(conn):
    """Extend object_state with catalog columns and indexes.

    Prerequisite: the base object_state table must already exist
    (created by zodb-pgjsonb).

    Args:
        conn: psycopg connection (autocommit or in a transaction block)
    """
    conn.execute(CATALOG_COLUMNS)
    conn.execute(CATALOG_FUNCTIONS)
    conn.execute(CATALOG_LANG_FUNCTION)
    conn.execute(CATALOG_INDEXES)
    conn.execute(RRULE_FUNCTIONS)
