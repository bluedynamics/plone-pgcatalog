# BM25 Integration Analysis for plone-pgcatalog

**Architectural options for bringing BM25 relevance ranking to Plone's PostgreSQL catalog.**

*February 2026*

---

## Table of Contents

1. [Goals and Constraints](#1-goals-and-constraints)
2. [Current State: What We Have Today](#2-current-state-what-we-have-today)
3. [The Search Experience Gap](#3-the-search-experience-gap)
4. [Architecture: Progressive Enhancement](#4-architecture-progressive-enhancement)
5. [Extension Selection Analysis](#5-extension-selection-analysis)
6. [Recommended Architecture](#6-recommended-architecture)
7. [Implementation Sketch](#7-implementation-sketch)
8. [Risk Assessment](#8-risk-assessment)
9. [Decision Summary](#9-decision-summary)

---

## 1. Goals and Constraints

### What We Want

1. **Vanilla PostgreSQL works out of the box.** An admin installs plone-pgcatalog with an unmodified `postgres:17` image and gets working full-text search — no extensions required.

2. **Better search for those who want it.** Admins who need better relevance ranking can install a BM25 extension and plone-pgcatalog automatically takes advantage of it.

3. **Best possible search experience.** The user typing a search term into Plone's search bar should get results ranked by *relevance*, not by modification date or insertion order. A match in the title should rank higher than the same match buried in the body.

### Hard Constraints

| Constraint | Rationale |
|---|---|
| No mandatory external extensions | Plone must work with any standard PG installation |
| No data duplication | We cannot maintain separate index structures outside PG |
| Transactional consistency | Search results must reflect committed state (ZODB guarantee) |
| Multilingual support | Plone serves content in 30+ languages; CJK is a real-world need |
| CNPG / HA compatibility | Production Plone often runs on Kubernetes with CloudNativePG |
| Open-source friendly licensing | Plone is GPL-2.0; the recommended stack should be distributable |

### Factors to Weigh

| Factor | Weight | Why |
|---|---|---|
| **Search result quality** | Critical | This is what end users experience every day |
| **Performance** | High | Search must be fast (< 100ms for typical queries) |
| **Operational simplicity** | High | Plone admins are not search engine specialists |
| **Multilingual coverage** | High | Plone is used globally |
| **License compatibility** | Medium | Affects distribution and adoption |
| **Maintenance burden** | Medium | Supporting multiple backends costs development time |
| **Future-proofness** | Medium | The BM25 extension landscape is still evolving |

---

## 2. Current State: What We Have Today

### Write Path

```
catalog_object() → set_pending(zoid, {path, idx, searchable_text})
    → ZODB tpc_vote() → CatalogStateProcessor
        → ExtraColumn: to_tsvector(pgcatalog_lang_to_regconfig(Language), text)
        → Written atomically to object_state table
```

The `searchable_text` column is a **dedicated tsvector**, language-aware via the object's `Language` field. Title and Description are stored in the `idx` JSONB column and searched via GIN expression indexes using the `simple` config (no stemming).

### Query Path

```
searchResults({"SearchableText": "term", "Language": "de"})
    → build_query() → _handle_text()
        → WHERE searchable_text @@ plainto_tsquery(regconfig, term)
    → _run_search() → SELECT zoid, path FROM object_state WHERE ...
```

### What's Missing

1. **No relevance ranking by default.** Results come back in insertion order (or whatever `sort_on` is set to). The `ts_rank()` function exists in tests but is not wired into the query builder.

2. **No field-weighted ranking.** A match in the title counts the same as a match in the body text. There is no combined ranking across SearchableText + Title + Description.

3. **No BM25.** The built-in `ts_rank()` uses term frequency only — no IDF, no length normalization, no term saturation.

4. **Title/Description use `simple` config.** No stemming on these fields because GIN expression indexes require a constant `regconfig`. Searching for "running" won't match a Title containing "run".

---

## 3. The Search Experience Gap

Consider a Plone site with 50,000 content items. A user searches for "security policy".

### Today (no ranking)

Results come back in arbitrary or date-based order. A page titled "Security Policy" buried on page 3 because 200 other documents mention "security" somewhere in their body. The user must scroll and hunt.

### With field-weighted ts_rank (no extension)

Results are ranked by term frequency with title weighted higher. The "Security Policy" page moves to the top. But a news item that mentions "security" 50 times in the body may still outrank it, because `ts_rank` has no term frequency saturation.

### With BM25

The "Security Policy" page is ranked first because:
- Both query terms appear in a short text (title) → high length normalization score
- "policy" is rarer than "security" across the corpus → IDF boosts it
- The 50-mention news item saturates after a few occurrences → does not dominate

**This is the experience difference that matters.** BM25 produces the results users expect.

---

## 4. Architecture: Progressive Enhancement

Rather than choosing one extension or trying to support all three equally, the architecture should be **layered**:

```
┌──────────────────────────────────────────────────────────┐
│  Level 2: BM25 Extension (optional)                      │
│  Full BM25 ranking with IDF, saturation, normalization   │
│  Activated: extension detected at startup                │
├──────────────────────────────────────────────────────────┤
│  Level 1: Weighted ts_rank (built-in enhancement)        │
│  Field-weighted relevance: Title(A) > Description(B) >   │
│  SearchableText(D), using ts_rank_cd with setweight()    │
│  Activated: always (vanilla PG, zero extensions)         │
├──────────────────────────────────────────────────────────┤
│  Level 0: Boolean match (current state)                  │
│  tsvector @@ tsquery — match/no-match filtering          │
│  No ranking, no field weighting                          │
└──────────────────────────────────────────────────────────┘
```

### Level 0 → Level 1: Quick Win, No Extensions

This is achievable with **vanilla PostgreSQL** and represents the biggest bang-for-the-buck improvement.

**What changes:**

- When `sort_on` is not explicitly set and `SearchableText` is in the query, **automatically rank by relevance** instead of returning unordered results.
- Use `ts_rank_cd()` (cover density ranking — considers term proximity) instead of `ts_rank()`.
- Combine Title + Description + SearchableText into a weighted ranking expression:

```sql
SELECT zoid, path,
  ts_rank_cd(
    '{0.1, 0.2, 0.4, 1.0}'::float4[],
    setweight(to_tsvector('simple', COALESCE(idx->>'Title', '')), 'A') ||
    setweight(to_tsvector('simple', COALESCE(idx->>'Description', '')), 'B') ||
    searchable_text,
    plainto_tsquery(pgcatalog_lang_to_regconfig(%(lang)s)::regconfig, %(text)s)
  ) AS _relevance
FROM object_state
WHERE searchable_text @@ plainto_tsquery(...)
ORDER BY _relevance DESC
```

**Pros:** Huge UX improvement, zero dependencies, works everywhere.
**Cons:** Still no IDF or saturation. Title/Description tsvectors computed at query time (not indexed), adding some CPU cost per query.

**Optimization option:** Pre-combine Title/Description into the `searchable_text` column at write time using `setweight()`, so ranking at query time is a single `ts_rank_cd()` call on the existing indexed column. This requires changing the write path.

### Level 1 → Level 2: BM25 via Extension

When a BM25 extension is detected, replace the ranking function:

- **pg_textsearch:** Replace `ts_rank_cd()` with the `<@>` operator
- **ParadeDB:** Create a parallel BM25 index, use `pdb.score()` for ranking
- **VectorChord-BM25:** Maintain `bm25vector` column, use `<&>` for ranking

The key architectural question is: **do we need to support all three, or should we pick one?**

---

## 5. Extension Selection Analysis

### Support All Three?

| Aspect | Effort | Benefit |
|---|---|---|
| **Abstraction layer** | High — 3 different APIs, index types, write paths | Maximum admin choice |
| **Testing** | 3x integration test matrices | Confidence across all backends |
| **Documentation** | 3 deployment guides | Clear admin guidance |
| **Maintenance** | Every change tested against 3 backends | Long-term cost |
| **Bug surface** | Each backend has unique edge cases | More support burden |

**Verdict: Not worth it.** The three extensions have fundamentally different architectures (native tsvector vs. Tantivy index vs. bm25vector column). Abstracting them behind a common interface would either be leaky (exposing backend-specific limitations) or lowest-common-denominator (losing each backend's strengths). The maintenance burden of testing against three backends is significant for a project with limited contributors.

### Support Two (Vanilla PG + One Extension)?

This is the sweet spot. Vanilla PG as the baseline, one BM25 extension as the enhanced path. The question becomes: **which one?**

### Head-to-Head for Plone

#### pg_textsearch (Tiger Data)

| Criterion | Assessment |
|---|---|
| **Integration fit** | Best — uses same tsvector/tsquery, minimal write-path changes |
| **License** | PostgreSQL License (most permissive) |
| **Multilingual** | 29+ languages via PG regconfig (no CJK) |
| **Cloud-native / CNPG** | **Disqualified** — no WAL support, indexes don't replicate |
| **Maturity** | Preview release, not production-ready |
| **Field boosting** | Not yet documented |

**Assessment:** The best technical fit *if* it were production-ready. But the lack of WAL support is a hard blocker for CNPG deployments, and preview status means we cannot recommend it for production Plone sites. **Revisit in 6-12 months.**

#### pg_search (ParadeDB)

| Criterion | Assessment |
|---|---|
| **Integration fit** | Moderate — own index engine, different write/query path |
| **License** | AGPL-3.0 (Community) / commercial (Enterprise) |
| **Multilingual** | 18 stemmers + strong CJK (jieba, lindera, ICU) |
| **Cloud-native / CNPG** | Best — official Helm chart, CNPG images |
| **Maturity** | Production-ready (210+ releases) |
| **Field boosting** | Arbitrary float boosts per field |
| **HA / WAL replication** | Enterprise only (commercial license) |

**Assessment:** The most feature-complete and mature option. Arbitrary field boosting and CJK support are strong. However:

- **AGPL-3.0** is not a hard blocker (the extension runs server-side in PG, not linked into Python code), but it complicates distribution guidance. Some organizations have blanket AGPL policies.
- **HA requires Enterprise license.** Community edition BM25 indexes don't replicate to standbys. For CNPG clusters, this means search only works on the primary (or indexes must be rebuilt after failover).
- **Different index engine** means maintaining a parallel BM25 index alongside the existing tsvector column. This is additional storage and write overhead.

#### VectorChord-BM25

| Criterion | Assessment |
|---|---|
| **Integration fit** | Moderate — explicit tokenization step, bm25vector column |
| **License** | Apache-2.0 (fully permissive) |
| **Multilingual** | 30 stemmers (most comprehensive) + CJK (jieba, lindera, LLM) |
| **Cloud-native / CNPG** | Good — CNPG images exist, WAL replication confirmed |
| **Maturity** | Newer (2025), but actively developed |
| **Field boosting** | Not documented |
| **HA / WAL replication** | Yes — all writes use GenericXLog (confirmed via source code) |

**Assessment:** Best license story (Apache-2.0), most languages, and the BM25 index **replicates via WAL** — confirmed by source code analysis (all writes use `GenericXLogStart()`/`GenericXLogFinish()` through PG's native buffer manager). This is a decisive advantage over both ParadeDB Community (no WAL replication) and pg_textsearch (no WAL at all). The explicit tokenization step (text → bm25vector) maps well to plone-pgcatalog's `CatalogStateProcessor` pattern — we already transform text at write time. The dependency on `pg_tokenizer.rs` adds complexity but also provides the most powerful tokenization pipeline. Custom model training could be compelling for specialized Plone sites.

### Scoring Matrix

| Criterion (weight) | pg_textsearch | pg_search | VectorChord-BM25 |
|---|---|---|---|
| Search quality (5) | 4 — BM25 but no field boost | 5 — BM25 + arbitrary boosts | 4 — BM25, no field boost yet |
| PG-native integration (4) | 5 — same tsvector | 2 — own engine | 3 — own column type |
| License (4) | 5 — PostgreSQL | 2 — AGPL | 5 — Apache-2.0 |
| CNPG / HA (4) | 1 — no WAL | 3 — Enterprise only | 5 — WAL confirmed |
| Multilingual (3) | 3 — no CJK | 4 — 18 + CJK | 5 — 30 + CJK + custom |
| Maturity (3) | 1 — preview | 5 — production | 3 — newer |
| Operational simplicity (3) | 4 — no deps | 3 — Helm chart helps | 2 — two extensions |
| **Weighted Total** | **82 / 130** | **89 / 130** | **102 / 130** |

*Scoring: 1-5 per criterion, multiplied by weight.*

---

## 6. Recommended Architecture

### Two-Track Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                    plone-pgcatalog                               │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  SearchBackend (abstract)                                  │  │
│  │                                                            │  │
│  │  write_searchable(zoid, title, description, text, lang)    │  │
│  │  build_search_query(text, lang) → (sql, params)            │  │
│  │  build_ranking_expr(text, lang) → sql                      │  │
│  │  get_extra_columns() → [ExtraColumn, ...]                  │  │
│  │  get_schema_sql() → str                                    │  │
│  │  detect() → bool  (class method)                           │  │
│  └───────────┬──────────────────────┬────────────────────────┘  │
│              │                      │                            │
│  ┌───────────▼───────────┐  ┌──────▼─────────────────────────┐  │
│  │  TsvectorBackend      │  │  BM25Backend                    │  │
│  │  (always available)   │  │  (when extension detected)      │  │
│  │                       │  │                                  │  │
│  │  - setweight(A/B/D)   │  │  - bm25vector column            │  │
│  │  - ts_rank_cd()       │  │  - BM25 index + scoring         │  │
│  │  - plainto_tsquery()  │  │  - language-aware tokenizer      │  │
│  │  - vanilla PG only    │  │  - requires vchord_bm25 +       │  │
│  │                       │  │    pg_tokenizer                  │  │
│  └───────────────────────┘  └──────────────────────────────────┘  │
│                                                                  │
│  Backend selected at startup via detect():                       │
│  1. Try BM25Backend.detect() → check pg_extension for vchord    │
│  2. Fallback: TsvectorBackend (always works)                    │
└─────────────────────────────────────────────────────────────────┘
```

### Why VectorChord-BM25 as the BM25 Backend

1. **Apache-2.0 license.** No friction for distribution, no organizational policy concerns. Plone can freely recommend it in documentation.

2. **WAL replication confirmed.** Source code analysis shows every write uses PostgreSQL's `GenericXLogStart()`/`GenericXLogFinish()` API, and all storage goes through the native buffer manager. A TensorChord team member explicitly confirmed: *"VectorChord natively supports WAL of Postgres."* CNPG standby replicas will have fully working BM25 indexes without paying for an enterprise license. This is the single biggest differentiator for production Plone deployments.

3. **Most multilingual.** 30 stemmer languages (matching PG's full set), plus CJK via Jieba/Lindera, plus LLM-based tokenizers for mixed-language content, plus custom model training.

4. **Write-path alignment.** The explicit tokenization step (`text → bm25vector`) maps perfectly to the existing `CatalogStateProcessor` pattern. We already transform text at write time — adding a `bm25vector` column is the same pattern.

5. **Future potential.** Custom model training means specialized Plone sites (medical, legal, government) can train domain-specific vocabularies on their own content. No other extension offers this.

### Why NOT the Others

- **pg_textsearch:** Revisit when WAL support ships and it leaves preview. Could become the ideal Level 1.5 (better ranking than ts_rank_cd, same tsvector infrastructure, no extra column).

- **ParadeDB:** If an admin has ParadeDB Enterprise deployed for other reasons, we should not *prevent* them from using it. But we should not *recommend* it as the default BM25 path due to AGPL licensing and the HA-requires-Enterprise constraint. A future third backend could be added if there is community demand.

### Backend Abstraction: Keep It Thin

The abstraction should be **minimal** — just enough to swap the search/ranking behavior without over-engineering:

```python
class SearchBackend:
    """Abstract interface for full-text search backends."""

    def get_extra_columns(self):
        """Return ExtraColumn list for CatalogStateProcessor."""
        raise NotImplementedError

    def get_schema_sql(self):
        """Return SQL DDL for search-specific columns/indexes/functions."""
        raise NotImplementedError

    def build_search_clause(self, text, lang, param_prefix):
        """Return (where_sql, params) for text matching."""
        raise NotImplementedError

    def build_ranking_expr(self, text, lang, param_prefix):
        """Return (sql_expr, params) for ORDER BY relevance."""
        raise NotImplementedError

    @classmethod
    def detect(cls, connection):
        """Return True if this backend is available on the given PG connection."""
        raise NotImplementedError
```

This is **5 methods**, not a framework. The rest of pgcatalog (security filters, path queries, field indexes, keyword indexes, pagination, brain loading) stays unchanged.

---

## 7. Implementation Sketch

### Phase 1: Level 1 — Weighted ts_rank_cd (Vanilla PG)

**Goal:** Every plone-pgcatalog installation gets relevance-ranked search results out of the box.

#### Important: Plone's SearchableText Already Includes Title + Description

Plone's default `SearchableText()` method on content types returns **title + description + body text concatenated** as a single string. The `_extract_searchable_text()` in `catalog.py` receives this pre-combined value — we do not get the individual components separately.

This means the current `searchable_text` tsvector column already contains title and description words. The question is how to make those title/description words rank higher.

#### Write-Path Option A: Prepend Weighted Title + Description (Recommended)

Since `idx` JSONB already contains Title and Description as separate keys (extracted via `_extract_idx()`), we can use them to build a weighted tsvector — even though they also appear inside SearchableText:

```sql
-- ExtraColumn for searchable_text becomes:
setweight(to_tsvector('simple'::regconfig, %(title_text)s), 'A') ||
setweight(to_tsvector('simple'::regconfig, %(description_text)s), 'B') ||
setweight(
  to_tsvector(
    pgcatalog_lang_to_regconfig(%(idx)s::jsonb->>'Language')::regconfig,
    %(searchable_text)s
  ), 'D'
)
```

Where `%(title_text)s` and `%(description_text)s` are extracted from `idx->>'Title'` and `idx->>'Description'` at write time.

**Yes, this creates duplicate tokens.** The word "Security" from a title "Security Policy" will appear:
- At position 1 with weight **A** (from the separate title tsvector)
- At some position with weight **D** (from inside the SearchableText blob)

**This is fine — and even desirable.** The `||` operator on tsvectors merges positions with their weights intact. `ts_rank_cd()` considers all positions and their weights. The A-weighted position dominates the D-weighted one in scoring, which is exactly what we want: title words rank higher than body-only words. The storage overhead of duplicate position entries is negligible (a few bytes per overlapping token).

**Implementation:** `_set_pg_annotation()` already extracts both `idx` (containing Title/Description) and `searchable_text`. We just need to pass `idx->>'Title'` and `idx->>'Description'` as additional parameters to the ExtraColumn expression.

#### Write-Path Option B: Single Weighted Blob (Simpler But Less Precise)

Alternatively, we could skip the separate Title/Description extraction and weight the entire SearchableText as a single D-weight tsvector, relying on the existing GIN expression indexes on Title/Description for field-specific matching. This is what we have today — it just needs ranking added.

**Verdict:** Option A is better. The extra write-path complexity is minimal (we already have the data), and the ranking improvement is significant.

#### Query-Path Change

When `SearchableText` is in the query and no explicit `sort_on` is set, add relevance ranking:

```sql
SELECT zoid, path,
  ts_rank_cd(
    '{0.1, 0.2, 0.4, 1.0}'::float4[],
    searchable_text,
    plainto_tsquery(pgcatalog_lang_to_regconfig(%(lang)s)::regconfig, %(text)s)
  ) AS _relevance
FROM object_state
WHERE searchable_text @@ plainto_tsquery(...)
  AND {security_filters}
ORDER BY _relevance DESC
LIMIT %(limit)s
```

**Impact:** Title matches rank ~10x higher than body matches (due to weight A vs D). Cover density ranking (`ts_rank_cd`) additionally rewards term proximity. This produces dramatically better results than unordered output.

**No new columns, no new indexes.** The existing `searchable_text` tsvector column and GIN index are reused. The only change is what gets stored in it (weighted tsvector with A/B/D labels) and how results are sorted (ranked).

**Requires full reindex on upgrade** — the existing tsvector values need to be regenerated with weight labels.

### Phase 2: Level 2 — BM25 via VectorChord-BM25

**Goal:** When `vchord_bm25` + `pg_tokenizer` are installed, use true BM25 ranking.

**Detection** (at startup, in `register_catalog_processor()`):

```sql
SELECT EXISTS(
  SELECT 1 FROM pg_extension WHERE extname = 'vchord_bm25'
);
```

**Write-path addition:**

Add a `search_bm25` column of type `bm25vector`:

```sql
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS search_bm25 bm25vector;
```

The `CatalogStateProcessor` populates it at write time:

```sql
-- Tokenize combined text with configured analyzer
tokenize(%(combined_text)s, %(tokenizer_name)s)
```

The tokenizer name is configurable (default: a language-aware analyzer created at startup).

**Query-path change:**

When BM25 backend is active, replace ranking:

```sql
SELECT zoid, path,
  search_bm25 <&> to_bm25query(
    'idx_os_search_bm25',
    tokenize(%(text)s, %(tokenizer_name)s)
  ) AS _relevance
FROM object_state
WHERE search_bm25 <&> to_bm25query(...) < 0  -- negative scores = matches
ORDER BY _relevance
LIMIT %(limit)s
```

The boolean tsvector match (`@@`) is kept as a **pre-filter** (leveraging the existing GIN index), with BM25 scoring applied for ranking. Alternatively, if the BM25 index alone is fast enough, the tsvector filter can be dropped.

**Field boosting consideration:**

VectorChord-BM25 does not currently document per-field boosting. Two options:

1. **Concatenate with field markers** — prepend "TITLE: " or repeat title text to boost it (crude but effective).
2. **Weighted score combination** — compute separate BM25 scores for title and body, combine arithmetically:
   ```sql
   (title_bm25 <&> query) * 10.0 + (body_bm25 <&> query) * 1.0 AS _relevance
   ```
   This requires two `bm25vector` columns but gives precise control.
3. **Wait for upstream** — field boosting may be added to VectorChord-BM25 (active development).

**Recommended:** Start with option 1 (title repetition in combined text), move to option 2 if users need finer control.

### Phase 3: Multilingual Tokenizer Setup

**Goal:** Automatic language-aware tokenizer configuration.

At startup, when BM25 backend is detected:

```sql
-- Create a language-aware text analyzer
SELECT create_text_analyzer('pgcatalog_default', $$
pre_tokenizer = "unicode_segmentation"
[[character_filters]]
to_lowercase = {}
[[token_filters]]
stemmer = "english_porter2"
$$);
```

For multilingual sites, create per-language analyzers and select the right one at write time based on the object's `Language` field. The `CatalogStateProcessor` already has access to `idx->>'Language'`.

For CJK sites:

```sql
SELECT create_text_analyzer('pgcatalog_cjk', $$
[pre_tokenizer.jieba]
[[character_filters]]
to_lowercase = {}
$$);
```

This configuration should be **site-configurable** — either via `zope.conf` settings or a control panel.

---

## 8. Risk Assessment

### Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| VectorChord-BM25 WAL replication edge cases | Very Low | Medium | Source code confirms GenericXLog usage; validate with CNPG failover test before Phase 2 release |
| Performance regression from ts_rank_cd computation | Low | Medium | ts_rank_cd on indexed tsvector is fast; benchmark with 100K+ documents |
| Tokenizer setup complexity confuses admins | Medium | Medium | Sensible defaults, auto-detection, clear docs |
| VectorChord-BM25 project abandoned | Low | Medium | Level 1 (vanilla PG) remains fully functional; backend is swappable |
| pg_textsearch ships WAL support and becomes superior | Medium | Low (positive) | Thin abstraction makes adding a third backend straightforward |
| Write-path overhead from bm25vector computation | Low | Medium | Tokenization is fast; benchmark at scale |

### Architectural Risks

| Risk | Mitigation |
|---|---|
| Over-engineering the backend abstraction | Keep it to 5 methods; no framework, no plugin registry |
| Breaking existing installations on upgrade | Phase 1 changes the searchable_text column content — requires reindex |
| Two-column overhead (tsvector + bm25vector) | tsvector stays for boolean matching (GIN); bm25vector adds ~20% storage |

### Licensing Risk

| Extension | License | Risk for Plone |
|---|---|---|
| VectorChord-BM25 | Apache-2.0 | None — fully compatible with GPL-2.0, permissive for all uses |
| pg_tokenizer.rs | Apache-2.0 | None |
| ParadeDB Community | AGPL-3.0 | Low for server-side PG extension (not linked into Python), but organizational AGPL policies may block adoption |

---

## 9. Decision Summary

### Recommendation

**Implement progressive enhancement with two levels:**

| Level | What | Dependencies | Search Quality |
|---|---|---|---|
| **Level 1** (default) | Weighted `ts_rank_cd` with `setweight(A/B/D)` on combined tsvector | Vanilla PostgreSQL | Good — field-weighted, proximity-aware |
| **Level 2** (optional) | True BM25 via VectorChord-BM25 | `vchord_bm25` + `pg_tokenizer` extensions | Excellent — IDF, saturation, normalization |

### Why This Approach

1. **Level 1 alone is a major improvement.** Most Plone sites will get dramatically better search just by upgrading pgcatalog — no extension installation needed. This covers the 80% case.

2. **Level 2 is for sites that need it.** Large multilingual sites, content-heavy portals, or sites where search is a primary UX feature can opt into BM25 by installing two PG extensions.

3. **VectorChord-BM25 is the right BM25 choice today:**
   - Apache-2.0 (no licensing friction)
   - Most languages (30 stemmers + CJK)
   - WAL replication confirmed (CNPG-ready without enterprise license)
   - Write-path maps to existing CatalogStateProcessor pattern
   - Custom model training for specialized sites

4. **The abstraction is minimal and future-proof.** If pg_textsearch ships WAL support and leaves preview, or if ParadeDB Community adds WAL replication, a third backend can be added without refactoring the rest of pgcatalog.

### What Not to Do

- **Do not support all three extensions.** The maintenance burden is not justified by the benefit. Pick one, optimize for it, make it excellent.
- **Do not make BM25 mandatory.** The vanilla PG path must always work. Extensions are opt-in enhancements.
- **Do not build a plugin framework.** Two concrete backend classes behind a thin interface is enough. No registry, no entry points, no dynamic loading.
- **Do not create separate tsvector columns for Title/Description.** Instead, prepend them with `setweight(A/B)` into the existing `searchable_text` column. Yes, tokens will be duplicated (Title/Description words appear both at weight A/B and inside SearchableText at weight D) — this is intentional and produces correct ranking behavior.

### Implementation Priority

1. **Phase 1: Level 1 — Weighted ts_rank_cd** (high value, low effort)
   - Change write path: weighted tsvector combining Title + Description + SearchableText
   - Change query path: auto-ranking when SearchableText is queried
   - Requires full reindex on upgrade
   - Works on every PostgreSQL installation

2. **Phase 2: Level 2 — VectorChord-BM25 backend** (high value, moderate effort)
   - Backend abstraction (thin interface)
   - BM25 write path + query path
   - Detection at startup
   - Tokenizer setup and configuration

3. **Phase 3: Configuration and polish** (medium value, moderate effort)
   - Admin-facing tokenizer configuration
   - Per-language analyzer setup
   - CJK analyzer support
   - Benchmarking and performance tuning

---

## Appendix: Key Reference Files

| File | Relevance |
|---|---|
| `sources/plone-pgcatalog/query.py` | `_handle_text()` — main modification point for search queries |
| `sources/plone-pgcatalog/config.py` | `CatalogStateProcessor` — write-path modification point |
| `sources/plone-pgcatalog/schema.py` | DDL for columns, indexes, functions |
| `sources/plone-pgcatalog/columns.py` | `IndexRegistry`, `language_to_regconfig()` |
| `sources/plone-pgcatalog/catalog.py` | `_extract_searchable_text()`, `_set_pg_annotation()` |
| `fulltext-search-postgres-report.md` | Background report on PG FTS, BM25, and extensions |
