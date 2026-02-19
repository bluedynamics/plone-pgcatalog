"""Search backend abstraction for swappable ranking strategies.

Provides ``TsvectorBackend`` (always available, vanilla PostgreSQL) and
``BM25Backend`` (optional, requires ``vchord_bm25`` + ``pg_tokenizer``
extensions).  The active backend is auto-detected at Zope startup and
accessed via ``get_backend()``.

Phase 3 adds per-language BM25 columns: each configured language gets its
own ``bm25vector`` column with a language-specific tokenizer (stemmer /
segmenter).  A fallback column handles unconfigured languages.
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


# ── Language → tokenizer configuration ───────────────────────────────

# Maps ISO 639-1 codes to pg_tokenizer config overrides.
# Keys not present here use the default (unicode_segmentation, no stemmer).
LANG_TOKENIZER_MAP = {
    # Western languages with Snowball stemmers
    "ar": {"stemmer": "arabic"},
    "hy": {"stemmer": "armenian"},
    "eu": {"stemmer": "basque"},
    "ca": {"stemmer": "catalan"},
    "da": {"stemmer": "danish"},
    "nl": {"stemmer": "dutch"},
    "en": {"stemmer": "english_porter2"},
    "et": {"stemmer": "estonian"},
    "fi": {"stemmer": "finnish"},
    "fr": {"stemmer": "french"},
    "de": {"stemmer": "german"},
    "el": {"stemmer": "greek"},
    "hi": {"stemmer": "hindi"},
    "hu": {"stemmer": "hungarian"},
    "id": {"stemmer": "indonesian"},
    "ga": {"stemmer": "irish"},
    "it": {"stemmer": "italian"},
    "lt": {"stemmer": "lithuanian"},
    "ne": {"stemmer": "nepali"},
    "nb": {"stemmer": "norwegian"},
    "nn": {"stemmer": "norwegian"},
    "no": {"stemmer": "norwegian"},
    "pt": {"stemmer": "portuguese"},
    "ro": {"stemmer": "romanian"},
    "ru": {"stemmer": "russian"},
    "sr": {"stemmer": "serbian"},
    "es": {"stemmer": "spanish"},
    "sv": {"stemmer": "swedish"},
    "ta": {"stemmer": "tamil"},
    "tr": {"stemmer": "turkish"},
    "yi": {"stemmer": "yiddish"},
    # CJK languages with dedicated segmenters
    "zh": {"pre_tokenizer": "jieba"},
    "ja": {"pre_tokenizer": "lindera"},
    "ko": {"pre_tokenizer": "lindera"},
}


def _build_tokenizer_toml(lang_code=None):
    """Build pg_tokenizer TOML config for a language.

    Args:
        lang_code: ISO 639-1 code, or None for the fallback (no stemmer).

    Returns:
        TOML config string for ``create_tokenizer()``.
    """
    cfg = LANG_TOKENIZER_MAP.get(lang_code, {}) if lang_code else {}
    pre_tok = cfg.get("pre_tokenizer", "unicode_segmentation")
    stemmer = cfg.get("stemmer")

    lines = [
        'model = "bert_base_uncased"',
    ]
    # CJK segmenters (jieba, lindera) need table format: [pre_tokenizer]\njieba = {}
    # Standard tokenizer uses string format: pre_tokenizer = "unicode_segmentation"
    if pre_tok in ("jieba", "lindera"):
        lines += ["[pre_tokenizer]", f"{pre_tok} = {{}}"]
    else:
        lines += [f'pre_tokenizer = "{pre_tok}"']
    lines += ["[[character_filters]]", "to_lowercase = {}"]
    if pre_tok == "unicode_segmentation":
        lines += ["[[token_filters]]", "skip_non_alphanumeric = {}"]
    if stemmer:
        lines += ["[[token_filters]]", f"stemmer = {{ {stemmer} = {{}} }}"]
    return "\n".join(lines) + "\n"


def _normalize_lang(lang):
    """Normalize a language code: 'pt-br' → 'pt', 'zh-CN' → 'zh'."""
    if not lang:
        return ""
    return lang.lower().split("-")[0].split("_")[0]


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


# ── BM25 (Level 2+3, requires extensions) ────────────────────────────


class BM25Backend(SearchBackend):
    """BM25 ranking via VectorChord-BM25 extension.

    Keeps tsvector for GIN-indexed boolean pre-filtering.  Each configured
    language gets its own ``bm25vector`` column with a language-specific
    tokenizer.  A fallback column handles unconfigured languages and
    cross-language search.
    """

    def __init__(self, languages=None, tokenizer_prefix="pgcatalog"):
        self.tokenizer_prefix = tokenizer_prefix
        # Normalize language codes: "pt-br" → "pt", "zh-CN" → "zh"
        raw = languages or ["en"]
        self.languages = list(dict.fromkeys(_normalize_lang(lang) for lang in raw))

    def _tok_name(self, lang=None):
        """Tokenizer name for a language (or fallback)."""
        if lang:
            return f"{self.tokenizer_prefix}_{lang}"
        return f"{self.tokenizer_prefix}_default"

    def _col_name(self, lang=None):
        """Column name for a language (or fallback)."""
        if lang:
            return f"search_bm25_{lang}"
        return "search_bm25"

    def _idx_name(self, lang=None):
        """Index name for a language (or fallback)."""
        if lang:
            return f"idx_os_search_bm25_{lang}"
        return "idx_os_search_bm25"

    def get_extra_columns(self):
        from zodb_pgjsonb import ExtraColumn

        cols = [ExtraColumn("searchable_text", _WEIGHTED_TSVECTOR_EXPR)]

        # Per-language columns
        for lang in self.languages:
            col = self._col_name(lang)
            tok = self._tok_name(lang)
            cols.append(
                ExtraColumn(
                    col,
                    f"CASE WHEN %({col})s::text IS NOT NULL "
                    f"THEN tokenize(%({col})s::text, '{tok}') "
                    f"ELSE NULL END",
                )
            )

        # Fallback column (multilingual, no stemmer)
        tok = self._tok_name()
        cols.append(
            ExtraColumn(
                "search_bm25",
                f"CASE WHEN %(search_bm25)s::text IS NOT NULL "
                f"THEN tokenize(%(search_bm25)s::text, '{tok}') "
                f"ELSE NULL END",
            )
        )
        return cols

    def get_schema_sql(self):
        parts = [
            "CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE;\n",
            "CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE;\n",
        ]

        # Per-language columns + tokenizers + indexes
        for lang in self.languages:
            col = self._col_name(lang)
            tok = self._tok_name(lang)
            idx = self._idx_name(lang)
            toml = _build_tokenizer_toml(lang)
            parts.append(
                f"ALTER TABLE object_state ADD COLUMN IF NOT EXISTS {col} bm25vector;\n"
            )
            parts.append(
                f"DO $$ BEGIN "
                f"PERFORM create_tokenizer('{tok}', $cfg$\n{toml}$cfg$);\n"
                f"EXCEPTION WHEN OTHERS THEN NULL; END $$;\n"
            )
            parts.append(
                f"CREATE INDEX IF NOT EXISTS {idx} "
                f"ON object_state USING bm25 ({col} bm25_ops);\n"
            )

        # Fallback column + tokenizer + index
        fallback_toml = _build_tokenizer_toml(None)
        parts.append(
            "ALTER TABLE object_state "
            "ADD COLUMN IF NOT EXISTS search_bm25 bm25vector;\n"
        )
        parts.append(
            f"DO $$ BEGIN "
            f"PERFORM create_tokenizer('{self._tok_name()}', $cfg$\n"
            f"{fallback_toml}$cfg$);\n"
            f"EXCEPTION WHEN OTHERS THEN NULL; END $$;\n"
        )
        parts.append(
            "CREATE INDEX IF NOT EXISTS idx_os_search_bm25 "
            "ON object_state USING bm25 (search_bm25 bm25_ops);\n"
        )
        return "".join(parts)

    def install_schema(self, conn):
        """Execute BM25 schema DDL statement-by-statement.

        CREATE EXTENSION + DO $$ blocks require per-statement execution;
        multi-statement strings fail silently in transactional connections.
        """
        conn.execute("CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE")
        conn.execute("CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE")

        # Per-language columns + tokenizers + indexes
        for lang in self.languages:
            col = self._col_name(lang)
            tok = self._tok_name(lang)
            idx = self._idx_name(lang)
            toml = _build_tokenizer_toml(lang)
            conn.execute(
                f"ALTER TABLE object_state ADD COLUMN IF NOT EXISTS {col} bm25vector"
            )
            conn.execute(
                f"DO $$ BEGIN "
                f"PERFORM create_tokenizer('{tok}', $cfg$\n{toml}$cfg$);\n"
                f"EXCEPTION WHEN OTHERS THEN NULL; END $$"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {idx} "
                f"ON object_state USING bm25 ({col} bm25_ops)"
            )

        # Fallback column + tokenizer + index
        fallback_toml = _build_tokenizer_toml(None)
        conn.execute(
            "ALTER TABLE object_state ADD COLUMN IF NOT EXISTS search_bm25 bm25vector"
        )
        conn.execute(
            f"DO $$ BEGIN "
            f"PERFORM create_tokenizer('{self._tok_name()}', $cfg$\n"
            f"{fallback_toml}$cfg$);\n"
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

        # Determine document language
        lang = _normalize_lang(idx.get("Language", ""))

        result = {}
        # Populate language-specific column (if configured)
        for cfg_lang in self.languages:
            col = self._col_name(cfg_lang)
            result[col] = combined if lang == cfg_lang else None

        # Always populate fallback column
        result["search_bm25"] = combined
        return result

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

        # Determine BM25 ranking column based on search language
        lang = _normalize_lang(lang_val)
        if lang and lang in self.languages:
            col = self._col_name(lang)
            idx = self._idx_name(lang)
            tok = self._tok_name(lang)
        else:
            col = "search_bm25"
            idx = "idx_os_search_bm25"
            tok = self._tok_name()

        # BM25 ranking via <&> operator
        rank = f"{col} <&> to_bm25query('{idx}', tokenize(%({p_bm25q})s, '{tok}'))"

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
        result = {"search_bm25": None}
        for lang in self.languages:
            result[self._col_name(lang)] = None
        return result

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


def detect_and_set_backend(dsn, languages=None):
    """Auto-detect and activate the best available backend.

    Called at startup. Tries BM25Backend.detect(), falls back to Tsvector.

    Args:
        dsn: PostgreSQL DSN string or None
        languages: list of ISO 639-1 language codes for BM25 columns,
                   or None for default (["en"]).

    Returns:
        The activated SearchBackend instance.
    """
    if BM25Backend.detect(dsn):
        backend = BM25Backend(languages=languages)
        set_backend(backend)
        log.info(
            "BM25 search backend activated (languages=%s)",
            backend.languages,
        )
        return backend
    backend = TsvectorBackend()
    set_backend(backend)
    log.info("Tsvector search backend activated (default)")
    return backend
