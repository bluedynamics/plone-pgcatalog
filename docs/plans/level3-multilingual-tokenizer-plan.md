# Plan: Phase 3 — Per-Language BM25 Ranking & Multilingual Tokenizer Configuration

## Summary

Phase 3 replaces the single-tokenizer BM25 backend with **per-language BM25 columns**, giving every configured language its own stemmer/segmenter for optimal ranking quality. A universal fallback column (no stemmer) ensures BM25 benefits (IDF, term saturation, length normalization) for unconfigured languages too.

This addresses the needs of real-world multilingual Plone sites — including full EU coverage (50+ Plone languages, from Basque to Yiddish), Asian languages (Chinese via jieba, Japanese via lindera), and regional variants (pt-br, etc.).

**Prerequisite:** Phase 1 (weighted tsvector) and Phase 2 (BM25 backend) are merged on `main`.

Working directory: `sources/plone-pgcatalog/`

## Current State (Phase 2)

- `BM25Backend` uses a **single** tokenizer (English Porter2) for all content
- All content goes into one `search_bm25` column regardless of language
- The tsvector path already does per-language stemming via `pgcatalog_lang_to_regconfig()` — 31 languages mapped
- Plone supports 50+ languages including regional variants (pt-br, zh-cn, etc.)
- pg_tokenizer provides 25+ Snowball stemmers + jieba (Chinese) + lindera (Japanese/Korean) pre-tokenizers

## Architecture: Per-Language BM25 Columns

### Why Per-Language?

The BM25 index is tied to the tokenizer used at index creation. A single "multilingual" tokenizer without stemming loses language-specific ranking quality — German "Häuser" won't match "Haus", French "maisons" won't match "maison". For sites serving citizens in their own language, this matters.

### Design

```
┌─────────────────────────────────────────────────────────┐
│ object_state table                                      │
├──────────────┬──────────────┬────────┬─────────────────┤
│ search_bm25  │search_bm25_en│ ...    │search_bm25_zh   │
│ (fallback)   │(English docs)│        │(Chinese docs)   │
│ ALL docs     │ NULL if ≠ en │        │ NULL if ≠ zh    │
├──────────────┼──────────────┼────────┼─────────────────┤
│ multilingual │ english_     │        │ jieba           │
│ tokenizer    │ porter2      │        │ segmenter       │
│ (no stemmer) │ stemmer      │        │ (no stemmer)    │
└──────────────┴──────────────┴────────┴─────────────────┘

Write path:
  Document (lang=de) → populate search_bm25_de + search_bm25 (fallback)
                        all other language columns stay NULL

Query path:
  Search (lang=de)   → rank using search_bm25_de (German stemmer)
  Search (lang=??)   → rank using search_bm25 (fallback, no stemmer)
  tsvector pre-filter always uses per-language stemming (unchanged)
```

### Two-Column Strategy Per Document

Every document populates exactly **two** BM25 columns:

1. **Language-specific column** (`search_bm25_{lang}`) — tokenized with the language's stemmer/segmenter for optimal ranking when searching in that language
2. **Fallback column** (`search_bm25`) — tokenized with multilingual tokenizer (no stemmer) for cross-language search and unconfigured languages

The extra tokenization is cheap (microseconds per document at write time) and enables BM25 ranking for *all* searches, not just language-specific ones.

### Storage Efficiency

- NULL `bm25vector` columns consume zero storage (PostgreSQL TOAST)
- BM25 indexes are sparse — only non-NULL values are indexed
- For a site with 10 configured languages, each document adds ~2 bm25vectors (one language-specific + one fallback), not 11

## pg_tokenizer Language Support

### Snowball Stemmers (25 languages)

| Stemmer Name | ISO 639-1 | Language |
|---|---|---|
| `arabic` | ar | Arabic |
| `armenian` | hy | Armenian |
| `basque` | eu | Basque |
| `catalan` | ca | Catalan |
| `danish` | da | Danish |
| `dutch` | nl | Dutch |
| `english_porter2` | en | English |
| `estonian` | et | Estonian |
| `finnish` | fi | Finnish |
| `french` | fr | French |
| `german` | de | German |
| `greek` | el | Greek |
| `hindi` | hi | Hindi |
| `hungarian` | hu | Hungarian |
| `indonesian` | id | Indonesian |
| `irish` | ga | Irish |
| `italian` | it | Italian |
| `lithuanian` | lt | Lithuanian |
| `nepali` | ne | Nepali |
| `norwegian` | nb/nn/no | Norwegian |
| `portuguese` | pt | Portuguese |
| `romanian` | ro | Romanian |
| `russian` | ru | Russian |
| `serbian` | sr | Serbian |
| `spanish` | es | Spanish |
| `swedish` | sv | Swedish |
| `tamil` | ta | Tamil |
| `turkish` | tr | Turkish |
| `yiddish` | yi | Yiddish |

### CJK Pre-Tokenizers

| Pre-Tokenizer | Languages | Notes |
|---|---|---|
| `jieba` | zh (Chinese) | Native Chinese word segmentation |
| `lindera` | ja (Japanese), ko (Korean) | Morphological analysis engine |
| `unicode_segmentation` | all others | Unicode Annex #29 word boundaries |

### Token Filters

| Filter | Description |
|---|---|
| `stemmer` | Snowball stemmer (see table above) |
| `skip_non_alphanumeric` | Skip non-alphanumeric tokens |
| `stopwords` | Custom stop word dictionaries |
| `synonym` | Custom synonym dictionaries |
| `pg_dict` | PostgreSQL dictionary integration |

### Character Filters

| Filter | Description |
|---|---|
| `to_lowercase` | Convert to lowercase |
| `unicode_normalization` | NFC, NFD, NFKC, NFKD normalization |

## Language → Tokenizer Config Mapping

### `LANG_TOKENIZER_MAP`

Maps ISO 639-1 codes to pg_tokenizer configuration. Regional variants (pt-br, zh-cn) are normalized to base codes.

```python
LANG_TOKENIZER_MAP = {
    # ── Western languages with Snowball stemmers ──────────
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

    # ── CJK languages with dedicated segmenters ───────────
    "zh": {"pre_tokenizer": "jieba"},
    "ja": {"pre_tokenizer": "lindera"},
    "ko": {"pre_tokenizer": "lindera"},

    # ── Languages without stemmers (BM25 IDF/saturation still helps) ──
    # These use unicode_segmentation + no stemmer (same as fallback)
    # Not listed here — they use the fallback column automatically
}
```

### TOML Generation

Each language config generates a TOML string for `create_tokenizer()`:

```python
def _build_tokenizer_toml(lang_code):
    """Build pg_tokenizer TOML config for a language."""
    cfg = LANG_TOKENIZER_MAP.get(lang_code, {})
    pre_tok = cfg.get("pre_tokenizer", "unicode_segmentation")
    stemmer = cfg.get("stemmer")

    lines = [
        'model = "bert_base_uncased"',
        f'pre_tokenizer = "{pre_tok}"',
        "[[character_filters]]",
        "to_lowercase = {}",
    ]
    if pre_tok == "unicode_segmentation":
        lines += ["[[token_filters]]", "skip_non_alphanumeric = {}"]
    if stemmer:
        lines += ["[[token_filters]]", f"stemmer = {{ {stemmer} = {{}} }}"]
    return "\n".join(lines)
```

## Configuration

### Environment Variables

```bash
# Comma-separated list of language codes to create dedicated BM25 columns for
PGCATALOG_BM25_LANGUAGES=en,de,fr,es,it,nl,pt,zh,ja

# Or use "auto" to detect from portal_languages at startup
PGCATALOG_BM25_LANGUAGES=auto

# Custom tokenizer name prefix (default: pgcatalog)
PGCATALOG_BM25_TOKENIZER_PREFIX=pgcatalog
```

### `auto` Mode

When `PGCATALOG_BM25_LANGUAGES=auto`, read the site's supported languages from `portal_languages` at startup:

```python
def _detect_languages(site):
    """Get supported languages from Plone's language tool."""
    lang_tool = getattr(site, "portal_languages", None)
    if lang_tool is None:
        return ["en"]
    return list(lang_tool.getSupportedLanguages())
```

### Default

If not set, default to `en` only (backward compatible with Phase 2 — single English tokenizer, single column).

## Files to Create/Modify

| File | Change |
|------|--------|
| `src/plone/pgcatalog/backends.py` | Per-language BM25 columns in `BM25Backend`, `LANG_TOKENIZER_MAP`, TOML generation |
| `src/plone/pgcatalog/config.py` | Read `PGCATALOG_BM25_LANGUAGES`, pass to `BM25Backend()`, auto-detect mode |
| `src/plone/pgcatalog/columns.py` | Add `et` (Estonian) to `_LANG_TO_REGCONFIG` (supported by PG 17) |
| `example/create_site.py` | PAM setup, trilingual content import (en/de/zh), translation linking |
| `example/fetch_wikipedia.py` | Fetch articles in 3 languages via langlinks API |
| `example/seed_data.json.gz` | Trilingual seed data (~800 EN + matching DE + matching ZH articles) |
| `example/requirements.txt` | Verify plone.app.multilingual is available |
| `example/README.md` | Multilingual example with language-specific search demos |
| `tests/test_backends.py` | Tests for per-language tokenizer configs |
| `tests/test_bm25_integration.py` | Integration tests for multilingual BM25 |
| `ARCHITECTURE.md` | Document per-language BM25 architecture |
| `CHANGES.md` | Changelog entry |

## Step 1: Worktree + Branch

```bash
cd /home/jensens/ws/cdev/z3blobs/sources/plone-pgcatalog
git worktree add .worktrees/phase3-multilingual -b feature/multilingual-tokenizer
```

## Step 2: Update `backends.py`

### Add `LANG_TOKENIZER_MAP` and TOML generation

(See Language → Tokenizer Config Mapping section above)

### Rewrite `BM25Backend.__init__`

```python
class BM25Backend(SearchBackend):
    def __init__(self, languages=None, tokenizer_prefix="pgcatalog"):
        self.tokenizer_prefix = tokenizer_prefix
        # Configured languages get dedicated BM25 columns
        self.languages = languages or ["en"]
        # Normalize: "pt-br" → "pt", "zh-cn" → "zh"
        self.languages = [
            lang.lower().split("-")[0].split("_")[0]
            for lang in self.languages
        ]
        # Deduplicate (nb/nn/no → all map to "norwegian" stemmer, but keep separate columns)
        self.languages = list(dict.fromkeys(self.languages))
```

### `get_extra_columns()` — per-language columns + fallback

```python
def get_extra_columns(self):
    from zodb_pgjsonb import ExtraColumn
    cols = [ExtraColumn("searchable_text", _WEIGHTED_TSVECTOR_EXPR)]

    # Per-language columns
    for lang in self.languages:
        tok_name = f"{self.tokenizer_prefix}_{lang}"
        col_name = f"search_bm25_{lang}"
        cols.append(ExtraColumn(
            col_name,
            f"CASE WHEN %({col_name})s::text IS NOT NULL "
            f"THEN tokenize(%({col_name})s::text, '{tok_name}') "
            f"ELSE NULL END",
        ))

    # Fallback column (multilingual, no stemmer)
    cols.append(ExtraColumn(
        "search_bm25",
        f"CASE WHEN %(search_bm25)s::text IS NOT NULL "
        f"THEN tokenize(%(search_bm25)s::text, '{self.tokenizer_prefix}_default') "
        f"ELSE NULL END",
    ))
    return cols
```

### `install_schema()` — per-language DDL

```python
def install_schema(self, conn):
    conn.execute("CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE")
    conn.execute("CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE")

    # Per-language tokenizers + columns + indexes
    for lang in self.languages:
        tok_name = f"{self.tokenizer_prefix}_{lang}"
        col_name = f"search_bm25_{lang}"
        idx_name = f"idx_os_search_bm25_{lang}"
        toml_cfg = _build_tokenizer_toml(lang)

        conn.execute(
            f"ALTER TABLE object_state "
            f"ADD COLUMN IF NOT EXISTS {col_name} bm25vector"
        )
        conn.execute(
            f"DO $$ BEGIN "
            f"PERFORM create_tokenizer('{tok_name}', $cfg$\n{toml_cfg}\n$cfg$);\n"
            f"EXCEPTION WHEN OTHERS THEN NULL; END $$"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {idx_name} "
            f"ON object_state USING bm25 ({col_name} bm25_ops)"
        )

    # Fallback tokenizer + column + index
    fallback_toml = _build_tokenizer_toml(None)  # no stemmer
    conn.execute(
        "ALTER TABLE object_state "
        "ADD COLUMN IF NOT EXISTS search_bm25 bm25vector"
    )
    conn.execute(
        f"DO $$ BEGIN "
        f"PERFORM create_tokenizer('{self.tokenizer_prefix}_default', $cfg$\n{fallback_toml}\n$cfg$);\n"
        f"EXCEPTION WHEN OTHERS THEN NULL; END $$"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_os_search_bm25 "
        "ON object_state USING bm25 (search_bm25 bm25_ops)"
    )
```

### `process_search_data()` — route to language column + fallback

```python
def process_search_data(self, pending):
    idx = pending.get("idx") or {}
    title = idx.get("Title", "") or ""
    description = idx.get("Description", "") or ""
    body = pending.get("searchable_text", "") or ""
    # 3x title repetition for field boosting
    parts = [title, title, title, description, body]
    combined = " ".join(filter(None, parts)) or None

    # Determine document language
    lang = (idx.get("Language") or "").lower().split("-")[0].split("_")[0]

    result = {}
    # Populate language-specific column (if configured)
    for cfg_lang in self.languages:
        col_name = f"search_bm25_{cfg_lang}"
        result[col_name] = combined if lang == cfg_lang else None

    # Always populate fallback column
    result["search_bm25"] = combined
    return result
```

### `build_search_clause()` — language-aware ranking

```python
def build_search_clause(self, query_val, lang_val, pname_func):
    p_text = pname_func("text")
    p_lang = pname_func("lang")
    p_bm25q = pname_func("bm25q")

    # GIN pre-filter (unchanged, per-language via tsvector)
    where = (
        f"searchable_text @@ plainto_tsquery("
        f"pgcatalog_lang_to_regconfig(%({p_lang})s)::regconfig, "
        f"%({p_text})s)"
    )

    # Determine BM25 ranking column
    lang = (str(lang_val) if lang_val else "").lower().split("-")[0].split("_")[0]
    if lang in self.languages:
        col = f"search_bm25_{lang}"
        idx = f"idx_os_search_bm25_{lang}"
        tok = f"{self.tokenizer_prefix}_{lang}"
    else:
        col = "search_bm25"
        idx = "idx_os_search_bm25"
        tok = f"{self.tokenizer_prefix}_default"

    rank = (
        f"{col} <&> to_bm25query("
        f"'{idx}', "
        f"tokenize(%({p_bm25q})s, '{tok}'))"
    )

    params = {
        p_text: str(query_val),
        p_lang: str(lang_val) if lang_val else "",
        p_bm25q: str(query_val),
    }
    return where, params, rank
```

### `uncatalog_extra()` — NULL all BM25 columns

```python
def uncatalog_extra(self):
    result = {"search_bm25": None}
    for lang in self.languages:
        result[f"search_bm25_{lang}"] = None
    return result
```

## Step 3: Update `config.py`

### Read configuration from environment

```python
import os

def _get_bm25_languages():
    """Read configured BM25 languages from environment."""
    env_val = os.environ.get("PGCATALOG_BM25_LANGUAGES", "en")
    if env_val.lower() == "auto":
        return None  # signal to detect from portal_languages
    return [lang.strip() for lang in env_val.split(",") if lang.strip()]
```

### Update `register_catalog_processor()`

```python
def register_catalog_processor(event):
    # ... existing code ...
    dsn = getattr(storage, "_dsn", None)

    languages = _get_bm25_languages()
    if languages is None:
        # "auto" mode: detect from site
        languages = _detect_languages_from_site(event)

    detect_and_set_backend(dsn, languages=languages)
```

### Update `detect_and_set_backend()`

```python
def detect_and_set_backend(dsn, languages=None):
    if BM25Backend.detect(dsn):
        backend = BM25Backend(languages=languages)
        set_backend(backend)
        log.info(
            "BM25 search backend activated "
            f"(languages={backend.languages})"
        )
        return backend
    # ... fallback to TsvectorBackend ...
```

## Step 4: Update `columns.py`

Add Estonian (`et`) to `_LANG_TO_REGCONFIG` — it's supported by both PostgreSQL 17 and pg_tokenizer but currently missing from the mapping.

## Step 5: Multilingual Example Site

### Goal

Replace the current English-only example with a **trilingual Plone site** (English, German, Chinese) using `plone.app.multilingual` (PAM) with properly linked translations. This demonstrates per-language BM25 ranking across Western and CJK languages.

### Seed Data: Trilingual Wikipedia Articles

#### `fetch_wikipedia.py` rewrite

1. Start from the existing ~800 English article titles (geography topics)
2. For each English article, query Wikipedia's langlinks API for German and Chinese translations
3. Fetch the article text in each available language
4. Output: `seed_data.json.gz`

```json
[
  {
    "id": "mount-everest",
    "en": {"title": "Mount Everest", "description": "...", "text": "..."},
    "de": {"title": "Mount Everest", "description": "...", "text": "..."},
    "zh": {"title": "珠穆朗玛峰", "description": "...", "text": "..."}
  }
]
```

Not every article will have all three languages — realistic and fine.

**Expected yield:** ~600-700 with DE translations, ~500-600 with ZH translations.

#### Wikipedia API calls

```python
# 1. Get langlinks
GET https://en.wikipedia.org/w/api.php?action=query&titles=Mount+Everest
    &prop=langlinks&lllang=de|zh&format=json

# 2. Get article extract in target language
GET https://{lang}.wikipedia.org/w/api.php?action=query&titles={title}
    &prop=extracts&explaintext=1&format=json
```

Rate limiting: 200ms delay between requests.

### `create_site.py` rewrite

#### PAM Setup

```python
from plone.app.multilingual.browser.setup import SetupMultilingualSite

def setup_multilingual(site):
    lang_tool = site.portal_languages
    lang_tool.supported_langs = ["en", "de", "zh"]
    lang_tool.setDefaultLanguage("en")

    installer = get_installer(site)
    installer.install_product("plone.app.multilingual")

    setup = SetupMultilingualSite()
    setup.setupSite(site)
    transaction.commit()
```

#### Content Import with Translation Linking

```python
from plone.app.multilingual.interfaces import ITranslationManager

def import_multilingual_content(site, seed_data):
    for article in seed_data:
        # Create English (canonical) in /en/library/
        doc_en = create_document(site["en"]["library"], article, "en")

        # Create German translation in /de/library/ and link
        if "de" in article:
            doc_de = create_document(site["de"]["library"], article, "de")
            ITranslationManager(doc_en).register_translation("de", doc_de)

        # Create Chinese translation in /zh/library/ and link
        if "zh" in article:
            doc_zh = create_document(site["zh"]["library"], article, "zh")
            ITranslationManager(doc_en).register_translation("zh", doc_zh)

        # Publish all versions
        publish_all(...)
```

#### Library Folder Structure

```
/Plone/
  /en/library/           ← ~800 English articles
  /de/library/           ← ~600 German translations (linked)
  /zh/library/           ← ~500 Chinese translations (linked)
```

### Environment Configuration for the Example

```bash
export PGCATALOG_BM25_LANGUAGES=en,de,zh
```

This creates:
- `search_bm25_en` column with English Porter2 stemmer
- `search_bm25_de` column with German Snowball stemmer
- `search_bm25_zh` column with jieba Chinese segmenter
- `search_bm25` fallback column (multilingual, no stemmer)

### Search Testing Scenarios

| Query | Language | BM25 Column Used | Expected Behavior |
|---|---|---|---|
| `volcano` | en | `search_bm25_en` | English stemmer: "volcanoes" matches too |
| `Vulkan` | de | `search_bm25_de` | German stemmer: "Vulkane" matches too |
| `火山` | zh | `search_bm25_zh` | jieba segments correctly |
| `Mount Everest` | en | `search_bm25_en` | Title boost (3x) ranks it highest |
| `珠穆朗玛峰` | zh | `search_bm25_zh` | Chinese Mount Everest article |
| `volcano` | (none) | `search_bm25` | Fallback: BM25 without stemming |

### SQL Examples for README

```sql
-- Search German content with BM25 ranking
SELECT path, idx->>'Title' AS title
FROM object_state
WHERE searchable_text @@ plainto_tsquery('german', 'Vulkan')
  AND idx->>'Language' = 'de'
ORDER BY search_bm25_de <&> to_bm25query(
    'idx_os_search_bm25_de',
    tokenize('Vulkan', 'pgcatalog_de')
) ASC
LIMIT 10;

-- Count articles by language
SELECT idx->>'Language' AS lang, count(*)
FROM object_state
WHERE idx->>'portal_type' = 'Document'
GROUP BY 1 ORDER BY 2 DESC;

-- Cross-language search (fallback BM25)
SELECT path, idx->>'Title', idx->>'Language'
FROM object_state
WHERE searchable_text @@ plainto_tsquery('simple', 'Everest')
ORDER BY search_bm25 <&> to_bm25query(
    'idx_os_search_bm25',
    tokenize('Everest', 'pgcatalog_default')
) ASC
LIMIT 10;
```

## Step 6: Tests

### Unit tests in `tests/test_backends.py`

- `TestLangTokenizerMap`:
  - All 31 existing `_LANG_TO_REGCONFIG` languages have tokenizer configs
  - CJK languages use correct pre-tokenizers (jieba, lindera)
  - Regional variants normalize correctly (pt-br → pt)
  - `_build_tokenizer_toml()` produces valid TOML for each language

- `TestBM25BackendMultilingual`:
  - `BM25Backend(languages=["en", "de", "zh"])` creates 4 ExtraColumns (3 language + 1 fallback)
  - `process_search_data()` routes to correct language column
  - `process_search_data()` always populates fallback
  - `build_search_clause(lang="de")` uses `search_bm25_de` column
  - `build_search_clause(lang="")` uses fallback column
  - `build_search_clause(lang="sv")` uses fallback when sv not configured
  - `uncatalog_extra()` NULLs all columns
  - `install_schema()` generates DDL for all languages + fallback

- `TestBackwardCompat`:
  - `BM25Backend()` (no args) defaults to `["en"]` — single column like Phase 2
  - Existing tests pass without modification

### Integration tests in `tests/test_bm25_integration.py`

(Skipped without BM25 extensions)

- `TestMultilingualBM25Write`:
  - English document populates `search_bm25_en` + `search_bm25`, not `search_bm25_de`
  - German document populates `search_bm25_de` + `search_bm25`, not `search_bm25_en`

- `TestMultilingualBM25Ranking`:
  - German stemmer: "Häuser" query matches "Haus" document via `search_bm25_de`
  - Cross-language search uses fallback column

- `TestBM25ChineseJieba`:
  - Chinese text tokenized correctly via jieba
  - Chinese search query matches Chinese documents

### Existing tests

All 608+ tests pass unchanged — default `BM25Backend()` uses `languages=["en"]`, producing the same single-column behavior as Phase 2.

## Step 7: Documentation

### ARCHITECTURE.md additions

```markdown
### Per-Language BM25 Columns

Each configured language gets its own `bm25vector` column with a
language-specific tokenizer (stemmer + segmenter). A fallback column
handles unconfigured languages.

**Configuration:**
    PGCATALOG_BM25_LANGUAGES=en,de,fr,es,zh,ja

**Write path:** Each document populates its language column + fallback.
**Query path:** Search uses the language-specific column for ranking.

Supported: 25+ Snowball stemmers (EU languages), jieba (Chinese),
lindera (Japanese/Korean).
```

### CHANGES.md

```markdown
## 1.0.0b8 (unreleased)

### Added

- Per-language BM25 columns: each configured language gets its own
  `bm25vector` column with a language-specific tokenizer. Supports
  25+ Snowball stemmers (Arabic to Yiddish), jieba (Chinese), and
  lindera (Japanese/Korean). Configure via `PGCATALOG_BM25_LANGUAGES`
  environment variable. Fallback column for unconfigured languages.
  **Note:** Changing languages requires full catalog reindex.

- Trilingual example site: English, German, Chinese Wikipedia articles
  with plone.app.multilingual translation linking.
```

## Step 8: Commit + PR

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Per-language BM25 columns | Each language deserves optimal stemming/segmentation for ranking quality |
| Dual write (language + fallback) | Cross-language search still gets BM25 ranking benefits |
| Language from `idx["Language"]` | Plone indexes Language in the catalog; available at write time |
| Fallback column for unconfigured languages | BM25 IDF/saturation/normalization still improves ranking even without stemming |
| Environment variable configuration | Zero changes to zodb-pgjsonb; container-friendly |
| `auto` mode from portal_languages | Adapt to site configuration without manual env var management |
| Default to `["en"]` | Backward compatible with Phase 2 |
| NULL columns = zero storage | PostgreSQL TOAST handles this efficiently |
| Sparse BM25 indexes | Only non-NULL values indexed; no performance overhead for unused columns |

## Risk Assessment

| Risk | Mitigation |
|---|---|
| Many columns (20+ languages) | NULL columns are free; sparse indexes are efficient; admin controls which languages |
| jieba/lindera not available in all pg_tokenizer builds | Graceful error at create_tokenizer; language falls back to fallback column |
| Schema migration (Phase 2 → Phase 3) | New columns added with `ADD COLUMN IF NOT EXISTS`; existing `search_bm25` becomes fallback |
| Changing language list requires reindex | Document clearly; make it a manual step |
| tokenize() called twice per document | Microseconds per call; negligible vs. total write latency |
| BM25 index per column increases WAL | Sparse indexes only contain non-NULL rows; WAL overhead proportional to actual content |
| Wikipedia API rate limiting during seed fetch | 200ms delay; cache in seed_data.json.gz |
| Not all articles have DE/ZH translations | Expected; monolingual content in multilingual site is normal |

## Scope Boundaries

**In scope:**
- Per-language BM25 columns architecture
- `LANG_TOKENIZER_MAP` with 25+ Snowball stemmers + jieba + lindera
- `PGCATALOG_BM25_LANGUAGES` env var + `auto` mode
- Trilingual example site (EN/DE/ZH) with PAM translation linking
- Tests for per-language routing and ranking
- Documentation

**Out of scope (future):**
- ZConfig integration (needs zodb-pgjsonb changes)
- Control panel UI in Plone for language selection
- Custom stopword/synonym dictionaries per language
- Per-language field boosting weights (currently uniform 3x title)
- Benchmarking suite (separate effort)

## Data Flow: Multilingual Write Path

```
catalog_object() → set_pending(zoid, {path, idx, searchable_text})
  → ZODB tpc_vote → CatalogStateProcessor.process()
    → base: {path, idx, searchable_text}
    → backend.process_search_data():
        lang = idx["Language"] = "de"
        combined = "title title title description body"
        → {search_bm25_en: None,
           search_bm25_de: combined,   ← language-specific
           search_bm25_zh: None,
           search_bm25: combined}      ← fallback (always)
    → _batch_write_objects():
        ExtraColumn("search_bm25_de",
          "CASE WHEN ... THEN tokenize(..., 'pgcatalog_de') ...")
        ExtraColumn("search_bm25",
          "CASE WHEN ... THEN tokenize(..., 'pgcatalog_default') ...")
        → INSERT ... (NULL, german_bm25vec, NULL, fallback_bm25vec)
```

## Data Flow: Multilingual Query Path

```
searchResults({"SearchableText": "Vulkan", "Language": "de"})
  → build_query() → _handle_text()
    → backend.build_search_clause(query="Vulkan", lang="de")
      lang "de" in configured_languages → use search_bm25_de
      WHERE: searchable_text @@ plainto_tsquery('german', 'Vulkan')
      RANK:  search_bm25_de <&> to_bm25query(
               'idx_os_search_bm25_de',
               tokenize('Vulkan', 'pgcatalog_de'))
  → process(): ORDER BY rank ASC (BM25: ascending)
  → German documents ranked with German stemmer
```

## References

- [pg_tokenizer.rs documentation](https://github.com/tensorchord/pg_tokenizer.rs/blob/main/docs/05-text-analyzer.md)
- [VectorChord-BM25 multilingual blog post](https://blog.vectorchord.ai/vectorchord-bm25-introducing-pgtokenizera-standalone-multilingual-tokenizer-for-advanced-search)
- [Snowball stemmer project](https://snowballstem.org/projects.html)
