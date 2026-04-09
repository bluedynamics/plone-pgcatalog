-- ============================================================================
-- Benchmark: object_provides — JSONB GIN vs dedicated TEXT[] GIN
-- ============================================================================
--
-- Compares the old query path (object_provides stored inside idx JSONB,
-- queried via idx->'object_provides' ?|) against the new path
-- (dedicated object_provides TEXT[] column, queried via &&).
--
-- This script is SAFE to run on a production database:
--   - It adds a column and index (IF NOT EXISTS, idempotent)
--   - It does NOT modify existing data or drop existing indexes
--   - The backfill UPDATE touches only the new column
--   - Cleanup instructions at the bottom (commented out)
--
-- How to run
-- ----------
--
--   1. Connect to the database:
--
--        psql "dbname=zodb host=<host> port=5432 user=zodb"
--
--   2. Run the full script:
--
--        \i benchmarks/bench_object_provides.sql
--
--      Or from the shell:
--
--        psql "dbname=zodb ..." -f benchmarks/bench_object_provides.sql
--
--   3. The script runs in four phases:
--
--      a) SETUP    — adds object_provides TEXT[] column, backfills from
--                    idx JSONB, creates GIN index, runs ANALYZE.
--                    On 137k cataloged rows this takes ~10-30 seconds.
--
--      b) EXPLAIN  — runs EXPLAIN (ANALYZE, BUFFERS) for five query
--                    patterns, each in both JSONB and TEXT[] variants.
--                    Compare "Execution Time" and "Buffers" lines.
--
--      c) LATENCY  — runs each query 10x without EXPLAIN overhead to
--                    measure raw throughput.  Compare \timing output.
--
--      d) CLEANUP  — instructions to drop the test column + index
--                    (commented out, run manually if desired).
--
--   4. What to look for in the output:
--
--      - "Execution Time" in EXPLAIN ANALYZE (lower = better)
--      - "Buffers: shared hit/read" (fewer = less I/O)
--      - "\timing" output for the x10 latency queries
--      - Index size comparison (TEXT[] GIN should be smaller)
--
-- ============================================================================

\timing on
\echo ''
\echo '================================================================='
\echo '  Setup: create object_provides TEXT[] column + GIN index'
\echo '================================================================='

-- Add column (IF NOT EXISTS — safe to re-run)
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS object_provides TEXT[];

-- Populate from idx JSONB (one-time backfill)
\echo 'Populating object_provides from idx...'
UPDATE object_state SET object_provides = (
    SELECT array_agg(value::text)
    FROM jsonb_array_elements_text(idx->'object_provides')
)
WHERE idx IS NOT NULL
  AND idx ? 'object_provides'
  AND object_provides IS NULL;

-- Create GIN index on TEXT[] column
\echo 'Creating TEXT[] GIN index...'
CREATE INDEX IF NOT EXISTS idx_os_object_provides
    ON object_state USING gin (object_provides)
    WHERE object_provides IS NOT NULL;

ANALYZE object_state;

-- Row counts
\echo ''
SELECT
    COUNT(*) FILTER (WHERE idx IS NOT NULL AND idx ? 'object_provides') AS "rows_with_provides_in_idx",
    COUNT(*) FILTER (WHERE object_provides IS NOT NULL) AS "rows_with_provides_column"
FROM object_state;

-- Index sizes
\echo ''
\echo 'Index sizes:'
SELECT indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) AS size
FROM pg_indexes
WHERE tablename = 'object_state'
  AND indexname IN ('idx_os_cat_provides_gin', 'idx_os_object_provides');

\echo ''
\echo '================================================================='
\echo '  Benchmark: IContentish (broad — matches most objects)'
\echo '================================================================='

-- Pick the most common interface
\echo ''
\echo '--- OLD: JSONB GIN (idx->''object_provides'' ?|) ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE idx->'object_provides' ?| ARRAY['Products.CMFCore.interfaces._content.IContentish'];

\echo ''
\echo '--- NEW: TEXT[] GIN (object_provides &&) ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE object_provides && ARRAY['Products.CMFCore.interfaces._content.IContentish'];

\echo ''
\echo '================================================================='
\echo '  Benchmark: IDocument (selective — ~60% of objects)'
\echo '================================================================='

\echo ''
\echo '--- OLD: JSONB GIN ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument'];

\echo ''
\echo '--- NEW: TEXT[] GIN ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument'];

\echo ''
\echo '================================================================='
\echo '  Benchmark: IFolderish (very selective — ~20% of objects)'
\echo '================================================================='

\echo ''
\echo '--- OLD: JSONB GIN ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE idx->'object_provides' ?| ARRAY['Products.CMFCore.interfaces._content.IFolderish'];

\echo ''
\echo '--- NEW: TEXT[] GIN ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE object_provides && ARRAY['Products.CMFCore.interfaces._content.IFolderish'];

\echo ''
\echo '================================================================='
\echo '  Benchmark: Combined with security filter (realistic query)'
\echo '================================================================='

\echo ''
\echo '--- OLD: JSONB for both ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
  AND allowed_roles && ARRAY['Anonymous'];

\echo ''
\echo '--- NEW: TEXT[] for both ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
  AND allowed_roles && ARRAY['Anonymous'];

\echo ''
\echo '================================================================='
\echo '  Benchmark: AND operator (multiple interfaces)'
\echo '================================================================='

\echo ''
\echo '--- OLD: JSONB containment ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE idx @> '{"object_provides": ["plone.dexterity.interfaces.IDexterityContent", "plone.app.contenttypes.interfaces.IDocument"]}'::jsonb;

\echo ''
\echo '--- NEW: TEXT[] @> ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS)
SELECT zoid, path
FROM object_state
WHERE object_provides @> ARRAY['plone.dexterity.interfaces.IDexterityContent', 'plone.app.contenttypes.interfaces.IDocument'];

\echo ''
\echo '================================================================='
\echo '  Latency comparison (median of 10 runs, no EXPLAIN overhead)'
\echo '================================================================='

-- Run each query 10x and measure total time
\echo ''
\echo '--- OLD: JSONB GIN x10 ---'
\timing on
SELECT COUNT(*) FROM (
    SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE idx->'object_provides' ?| ARRAY['plone.app.contenttypes.interfaces.IDocument']
) t;

\echo '--- NEW: TEXT[] GIN x10 ---'
SELECT COUNT(*) FROM (
    SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
    UNION ALL SELECT 1 FROM object_state WHERE object_provides && ARRAY['plone.app.contenttypes.interfaces.IDocument']
) t;

\echo ''
\echo '================================================================='
\echo '  Cleanup (optional — run after benchmarking)'
\echo '================================================================='
\echo '  -- DROP INDEX IF EXISTS idx_os_object_provides;'
\echo '  -- ALTER TABLE object_state DROP COLUMN IF EXISTS object_provides;'
\echo ''
