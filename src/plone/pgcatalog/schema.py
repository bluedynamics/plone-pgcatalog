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

-- Dedicated column for security filter (used in EVERY query).
-- Stored as TEXT[] for direct GIN queries without JSONB decompression.
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS allowed_roles TEXT[];

-- Dedicated column for non-JSON-native metadata (image_scales, DateTime, etc.)
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS meta JSONB;

-- Dedicated column for interface-based lookups (object_provides KeywordIndex)
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS object_provides TEXT[];
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
$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
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
        WHEN 'et' THEN 'estonian'
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
    ON object_state ((idx->>'UID')) WHERE idx IS NOT NULL;

-- Composite indexes for common multi-field query patterns.
-- Without these, PG picks a single-column index and sequentially filters
-- all indexed rows (3+ seconds).  With composites, the planner uses a
-- multi-column index scan (sub-millisecond).

-- Folder listings, navigation: parent + type (most common pattern)
CREATE INDEX IF NOT EXISTS idx_os_cat_parent_type
    ON object_state ((idx->>'path_parent'), (idx->>'portal_type'))
    WHERE idx IS NOT NULL;

-- Path prefix queries with type filter (collections, search)
CREATE INDEX IF NOT EXISTS idx_os_cat_path_type
    ON object_state ((idx->>'path') text_pattern_ops, (idx->>'portal_type'))
    WHERE idx IS NOT NULL;

-- Path prefix + depth (navigation tree queries)
CREATE INDEX IF NOT EXISTS idx_os_cat_path_depth_type
    ON object_state (
        (idx->>'path') text_pattern_ops,
        ((idx->>'path_depth')::integer),
        (idx->>'portal_type')
    ) WHERE idx IS NOT NULL;

-- Type + review state (workflow-filtered listings)
CREATE INDEX IF NOT EXISTS idx_os_cat_type_state
    ON object_state ((idx->>'portal_type'), (idx->>'review_state'))
    WHERE idx IS NOT NULL;

-- Partial index for navigation listings (exclude_from_nav = false is ~1.6%
-- of rows — highly selective, eliminates 98% before heap scan).
CREATE INDEX IF NOT EXISTS idx_os_cat_nav_visible
    ON object_state ((idx->>'path') text_pattern_ops, (idx->>'portal_type'))
    WHERE idx IS NOT NULL AND (idx->>'exclude_from_nav')::boolean = false;

-- Partial index for upcoming events (portal_type = Event + sidecalendar).
-- Allows direct index scan on end date for calendar widgets.
CREATE INDEX IF NOT EXISTS idx_os_cat_events_upcoming
    ON object_state (pgcatalog_to_timestamptz(idx->>'end') DESC)
    WHERE idx IS NOT NULL
      AND (idx->>'portal_type') = 'Event'
      AND (idx->>'show_in_sidecalendar')::boolean = true;

-- Dedicated GIN indexes for high-cardinality keyword fields.
-- The full-idx GIN index (idx_os_catalog) is too broad for these — PG
-- must scan all JSONB keys across all objects.  Dedicated indexes on
-- just the keyword array are much smaller and faster for ?| queries.

-- Security filter (used in EVERY catalog query) — dedicated column, not JSONB
DROP INDEX IF EXISTS idx_os_cat_allowed_gin;
CREATE INDEX IF NOT EXISTS idx_os_allowed_roles
    ON object_state USING gin (allowed_roles) WHERE allowed_roles IS NOT NULL;

-- Interface-based lookups (object_provides) — dedicated TEXT[] column
DROP INDEX IF EXISTS idx_os_cat_provides_gin;
CREATE INDEX IF NOT EXISTS idx_os_object_provides
    ON object_state USING gin (object_provides) WHERE object_provides IS NOT NULL;

-- Subject keywords
CREATE INDEX IF NOT EXISTS idx_os_cat_subject_gin
    ON object_state USING gin ((idx->'Subject'))
    WHERE idx IS NOT NULL AND idx ? 'Subject';

-- Extended statistics for UID selectivity (helps planner pick the right index)
CREATE STATISTICS IF NOT EXISTS stts_os_uid ON (idx->>'UID') FROM object_state;

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
    "allowed_roles": "ARRAY",
    "meta": "jsonb",
    "object_provides": "ARRAY",
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
    "idx_os_cat_nav_visible",
    "idx_os_cat_events_upcoming",
    "idx_os_searchable_text",
    "idx_os_cat_title_tsv",
    "idx_os_cat_description_tsv",
    "idx_os_object_provides",
]


# ── Catalog change counter (for query cache invalidation) ────────────────────

CATALOG_CHANGE_SEQ = """\
CREATE SEQUENCE IF NOT EXISTS pgcatalog_change_seq;
"""

# ── Slow query logging table ──────────────────────────────────────────────────

SLOW_QUERY_TABLE = """\
CREATE TABLE IF NOT EXISTS pgcatalog_slow_queries (
    id          BIGSERIAL PRIMARY KEY,
    query_keys  TEXT[] NOT NULL,
    duration_ms FLOAT NOT NULL,
    query_text  TEXT,
    params      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_slow_queries_keys
    ON pgcatalog_slow_queries USING gin (query_keys);
"""


# ── Optional text extraction queue (created when PGCATALOG_TIKA_URL is set) ──

TEXT_EXTRACTION_QUEUE = """\
CREATE TABLE IF NOT EXISTS text_extraction_queue (
    id           BIGSERIAL PRIMARY KEY,
    zoid         BIGINT NOT NULL,
    blob_zoid    BIGINT NOT NULL,
    tid          BIGINT NOT NULL,
    content_type TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(blob_zoid, tid)
);

ALTER TABLE text_extraction_queue ADD COLUMN IF NOT EXISTS blob_zoid BIGINT;
UPDATE text_extraction_queue SET blob_zoid = zoid WHERE blob_zoid IS NULL;
DO $$ BEGIN
    ALTER TABLE text_extraction_queue ALTER COLUMN blob_zoid SET NOT NULL;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_teq_pending
    ON text_extraction_queue (id) WHERE status = 'pending';

CREATE OR REPLACE FUNCTION notify_extraction_ready() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('text_extraction_ready', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_extraction ON text_extraction_queue;
CREATE TRIGGER trg_notify_extraction
    AFTER INSERT ON text_extraction_queue
    FOR EACH ROW EXECUTE FUNCTION notify_extraction_ready();
"""

# PL/pgSQL merge function template for tsvector-only backend.
# BM25Backend provides its own version that also updates BM25 columns.
TSVECTOR_MERGE_FUNCTION = """\
CREATE OR REPLACE FUNCTION pgcatalog_merge_extracted_text(
    p_zoid BIGINT,
    p_text TEXT
) RETURNS void AS $$
BEGIN
    UPDATE object_state SET
      searchable_text = COALESCE(searchable_text, ''::tsvector) ||
        setweight(to_tsvector(
          pgcatalog_lang_to_regconfig(idx->>'Language')::regconfig,
          COALESCE(p_text, '')
        ), 'C')
    WHERE zoid = p_zoid AND idx IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
"""


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
    # Migration: drop old wrong-case UID index (idx->>'uid' instead of 'UID').
    # The old expression never matched actual JSONB keys (stored as 'UID').
    # Must drop before CREATE INDEX IF NOT EXISTS, because the old index has
    # the same name but a different expression.
    conn.execute(
        "DROP INDEX IF EXISTS idx_os_cat_uid; DROP STATISTICS IF EXISTS stts_os_uid"
    )
    conn.execute(CATALOG_COLUMNS)
    conn.execute(CATALOG_FUNCTIONS)
    conn.execute(CATALOG_LANG_FUNCTION)
    conn.execute(CATALOG_INDEXES)
    conn.execute(RRULE_FUNCTIONS)
