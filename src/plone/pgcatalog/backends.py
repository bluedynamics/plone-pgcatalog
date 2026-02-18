"""Search backend abstraction for swappable ranking strategies.

Provides ``TsvectorBackend`` (always available, vanilla PostgreSQL) and
``BM25Backend`` (optional, requires ``vchord_bm25`` + ``pg_tokenizer``
extensions).  The active backend is auto-detected at Zope startup and
accessed via ``get_backend()``.
"""

import abc
import logging


log = logging.getLogger(__name__)


# ── Weighted tsvector SQL (shared by both backends) ──────────────────

_WEIGHTED_TSVECTOR_EXPR = (
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE(%(idx)s::jsonb->>'Title', '')), 'A') || "
    "setweight(to_tsvector('simple'::regconfig, "
    "COALESCE(%(idx)s::jsonb->>'Description', '')), 'B') || "
    "setweight(to_tsvector("
    "pgcatalog_lang_to_regconfig(%(idx)s::jsonb->>'Language')"
    "::regconfig, %(searchable_text)s), 'D')"
)


# ── Abstract base ────────────────────────────────────────────────────


class SearchBackend(abc.ABC):
    """Thin interface for swappable search/ranking backends."""

    @abc.abstractmethod
    def get_extra_columns(self):
        """Search-specific ExtraColumns (appended to path/idx).

        Returns:
            list[ExtraColumn]
        """

    @abc.abstractmethod
    def get_schema_sql(self):
        """DDL for search-specific schema objects.

        Returns:
            str
        """

    @abc.abstractmethod
    def process_search_data(self, pending):
        """Extract backend-specific data from pending catalog entry.

        Args:
            pending: dict with path, idx, searchable_text keys

        Returns:
            dict to merge into CatalogStateProcessor.process() result
        """

    @abc.abstractmethod
    def build_search_clause(self, query_val, lang_val, pname_func):
        """Build SQL clause for SearchableText filtering and ranking.

        Args:
            query_val: search query string
            lang_val: language code string (may be empty)
            pname_func: callable(prefix) -> unique param name

        Returns:
            tuple of (where_sql, params_dict, rank_expr_or_none)
        """

    @property
    def rank_ascending(self):
        """True if lower scores = more relevant (BM25). False for tsvector."""
        return False

    def uncatalog_extra(self):
        """Column: None pairs for uncatalog. Default: empty."""
        return {}

    def install_schema(self, conn):
        """Execute schema DDL statement-by-statement.

        Default implementation sends get_schema_sql() as one string.
        Override for backends that need per-statement execution
        (e.g. CREATE EXTENSION + DO blocks).
        """
        sql = self.get_schema_sql()
        if sql:
            conn.execute(sql)

    @classmethod
    @abc.abstractmethod
    def detect(cls, dsn):
        """Detect whether this backend is available.

        Args:
            dsn: PostgreSQL DSN string or None

        Returns:
            bool
        """


# ── Tsvector (Level 1, always available) ─────────────────────────────


class TsvectorBackend(SearchBackend):
    """Vanilla PostgreSQL tsvector ranking with ts_rank_cd."""

    def get_extra_columns(self):
        from zodb_pgjsonb import ExtraColumn

        return [
            ExtraColumn("searchable_text", _WEIGHTED_TSVECTOR_EXPR),
        ]

    def get_schema_sql(self):
        return ""

    def process_search_data(self, pending):
        return {}

    def build_search_clause(self, query_val, lang_val, pname_func):
        p_text = pname_func("text")
        p_lang = pname_func("lang")

        where = (
            f"searchable_text @@ plainto_tsquery("
            f"pgcatalog_lang_to_regconfig(%({p_lang})s)::regconfig, "
            f"%({p_text})s)"
        )

        rank = (
            f"ts_rank_cd("
            f"'{{0.1, 0.2, 0.4, 1.0}}'::float4[], "
            f"searchable_text, "
            f"plainto_tsquery("
            f"pgcatalog_lang_to_regconfig(%({p_lang})s)::regconfig, "
            f"%({p_text})s))"
        )

        params = {
            p_text: str(query_val),
            p_lang: str(lang_val) if lang_val else "",
        }

        return where, params, rank

    @classmethod
    def detect(cls, dsn):
        return True


# ── BM25 (Level 2, requires extensions) ──────────────────────────────

_DEFAULT_TOKENIZER_CONFIG = """\
model = "bert_base_uncased"
pre_tokenizer = "unicode_segmentation"
[[character_filters]]
to_lowercase = {}
[[token_filters]]
skip_non_alphanumeric = {}
[[token_filters]]
stemmer = { english_porter2 = {} }
"""


class BM25Backend(SearchBackend):
    """BM25 ranking via VectorChord-BM25 extension.

    Keeps tsvector for GIN-indexed boolean pre-filtering.  Adds a
    ``search_bm25`` column (``bm25vector`` type) for BM25 ranking.
    """

    def __init__(self, tokenizer_name="pgcatalog_default", tokenizer_config=None):
        self.tokenizer_name = tokenizer_name
        self.tokenizer_config = tokenizer_config or _DEFAULT_TOKENIZER_CONFIG

    def get_extra_columns(self):
        from zodb_pgjsonb import ExtraColumn

        return [
            ExtraColumn("searchable_text", _WEIGHTED_TSVECTOR_EXPR),
            ExtraColumn(
                "search_bm25",
                f"CASE WHEN %(search_bm25)s::text IS NOT NULL "
                f"THEN tokenize(%(search_bm25)s::text, '{self.tokenizer_name}') "
                f"ELSE NULL END",
            ),
        ]

    def get_schema_sql(self):
        return (
            "CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE;\n"
            "CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE;\n"
            "ALTER TABLE object_state "
            "ADD COLUMN IF NOT EXISTS search_bm25 bm25vector;\n"
            f"DO $$ BEGIN "
            f"PERFORM create_tokenizer('{self.tokenizer_name}', $cfg$\n"
            f"{self.tokenizer_config}$cfg$);\n"
            f"EXCEPTION WHEN OTHERS THEN NULL; END $$;\n"
            "CREATE INDEX IF NOT EXISTS idx_os_search_bm25 "
            "ON object_state USING bm25 (search_bm25 bm25_ops);\n"
        )

    def install_schema(self, conn):
        """Execute BM25 schema DDL statement-by-statement.

        CREATE EXTENSION + DO $$ blocks require per-statement execution;
        multi-statement strings fail silently in transactional connections.
        """
        conn.execute("CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE")
        conn.execute("CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE")
        conn.execute(
            "ALTER TABLE object_state ADD COLUMN IF NOT EXISTS search_bm25 bm25vector"
        )
        conn.execute(
            f"DO $$ BEGIN "
            f"PERFORM create_tokenizer('{self.tokenizer_name}', $cfg$\n"
            f"{self.tokenizer_config}$cfg$);\n"
            f"EXCEPTION WHEN OTHERS THEN NULL; END $$"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_os_search_bm25 "
            "ON object_state USING bm25 (search_bm25 bm25_ops)"
        )

    def process_search_data(self, pending):
        idx = pending.get("idx") or {}
        title = idx.get("Title", "") or ""
        description = idx.get("Description", "") or ""
        body = pending.get("searchable_text", "") or ""
        # 3x title repetition for field boosting
        parts = [title, title, title, description, body]
        combined = " ".join(filter(None, parts)) or None
        return {"search_bm25": combined}

    def build_search_clause(self, query_val, lang_val, pname_func):
        p_text = pname_func("text")
        p_lang = pname_func("lang")
        p_bm25q = pname_func("bm25q")

        # GIN pre-filter: same tsvector clause as TsvectorBackend
        where = (
            f"searchable_text @@ plainto_tsquery("
            f"pgcatalog_lang_to_regconfig(%({p_lang})s)::regconfig, "
            f"%({p_text})s)"
        )

        # BM25 ranking via <&> operator
        rank = (
            f"search_bm25 <&> to_bm25query("
            f"'idx_os_search_bm25', "
            f"tokenize(%({p_bm25q})s, '{self.tokenizer_name}'))"
        )

        params = {
            p_text: str(query_val),
            p_lang: str(lang_val) if lang_val else "",
            p_bm25q: str(query_val),
        }

        return where, params, rank

    @property
    def rank_ascending(self):
        return True

    def uncatalog_extra(self):
        return {"search_bm25": None}

    @classmethod
    def detect(cls, dsn):
        """Check whether vchord_bm25 and pg_tokenizer are available.

        Checks ``pg_available_extensions`` (not ``pg_extension``) so that
        detection works before ``CREATE EXTENSION`` has been executed.
        """
        if not dsn:
            return False
        try:
            import psycopg

            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM pg_available_extensions "
                    "WHERE name IN ('vchord_bm25', 'pg_tokenizer')"
                )
                row = cur.fetchone()
                return row[0] == 2
        except Exception:
            return False


# ── Module-level singleton ───────────────────────────────────────────

_active_backend = None


def get_backend():
    """Returns the active search backend (defaults to TsvectorBackend)."""
    global _active_backend
    if _active_backend is None:
        _active_backend = TsvectorBackend()
    return _active_backend


def set_backend(backend):
    """Set the active search backend."""
    global _active_backend
    _active_backend = backend


def reset_backend():
    """Reset to default (TsvectorBackend). Mainly for tests."""
    global _active_backend
    _active_backend = None


def detect_and_set_backend(dsn):
    """Auto-detect and activate the best available backend.

    Called at startup. Tries BM25Backend.detect(), falls back to Tsvector.

    Returns:
        The activated SearchBackend instance.
    """
    if BM25Backend.detect(dsn):
        backend = BM25Backend()
        set_backend(backend)
        log.info("BM25 search backend activated (vchord_bm25 + pg_tokenizer detected)")
        return backend
    backend = TsvectorBackend()
    set_backend(backend)
    log.info("Tsvector search backend activated (default)")
    return backend
