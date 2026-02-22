<!-- diataxis: explanation -->

# Full-Text Search Deep Dive

plone.pgcatalog supports four tiers of text search, from basic word matching to
probabilistic relevance ranking. Each tier builds on the previous one, and the system
automatically selects the best available tier at startup.

This page explains how PostgreSQL full-text search works under the hood, how
plone.pgcatalog layers its search tiers on top, and how language-aware indexing and
relevance ranking fit together.

## How PostgreSQL full-text search works

PostgreSQL's full-text search is built on two data types and a matching operator:

- **`tsvector`**: A sorted list of normalized tokens (lexemes) with position
  information. Created from text via `to_tsvector()`, which applies language-specific
  rules: lowercasing, stop word removal, and stemming (reducing "running" to "run").

- **`tsquery`**: A boolean expression of tokens, created from user input via
  `plainto_tsquery()` (which inserts implicit AND between words) or `to_tsquery()`
  (which supports explicit boolean operators).

- **`@@`**: The match operator. `tsvector @@ tsquery` returns true if the document
  contains all required tokens.

A **GIN index** on a tsvector column makes the match operation fast: PostgreSQL
maintains an inverted index mapping each token to the set of rows containing it.
A search for "security policy" looks up both tokens in the GIN index and intersects
the row sets -- no table scan required.

**Language configurations** (called `regconfig` in PostgreSQL) control how text is
tokenized and stemmed. The `english` configuration knows that "running", "runs", and
"ran" are forms of "run". The `simple` configuration performs no stemming -- it
matches exact word forms only.

## Four tiers of text search

plone.pgcatalog implements a layered search architecture. Each tier adds
capabilities without removing the previous tier's functionality.

| Tier | Scope | Mechanism | Requires |
|---|---|---|---|
| **1** | SearchableText | Dedicated `tsvector` column, language-aware stemming, weighted A/B/D ranking | PostgreSQL (built-in) |
| **2** | SearchableText | BM25 probabilistic scoring via per-language columns | VectorChord-BM25 extension |
| **3** | Title / Description | GIN expression indexes on `idx` JSONB, `simple` config, word-level matching | PostgreSQL (built-in) |
| **4** | Addon ZCTextIndex | GIN expression indexes, `simple` config, auto-discovered at startup | PostgreSQL (built-in) |

Tiers 1 and 2 are alternatives for SearchableText -- tier 2 upgrades tier 1 when the
BM25 extension is available. Tiers 3 and 4 are independent and always active alongside
whichever SearchableText tier is in use.

### Tier 1: SearchableText (language-aware)

This is the primary search mechanism, always available on vanilla PostgreSQL.

**Storage:** A dedicated `searchable_text` column of type `tsvector` on the
`object_state` table. This column stores a pre-computed, language-aware token list
with weight labels.

**Indexing:** At write time, the `CatalogStateProcessor` builds a weighted tsvector
by combining three sources:

| Source | Weight | Purpose |
|---|---|---|
| Title | A (highest) | Title matches should dominate results |
| Description | B | Description matches are important but secondary |
| SearchableText body | D (lowest) | Body text provides broad coverage |

The tsvector expression uses `setweight()` to assign different weights:

```sql
setweight(to_tsvector('simple', COALESCE(idx->>'Title', '')), 'A') ||
setweight(to_tsvector('simple', COALESCE(idx->>'Description', '')), 'B') ||
setweight(to_tsvector(
    pgcatalog_lang_to_regconfig(idx->>'Language')::regconfig,
    searchable_text
), 'D')
```

Title and Description use the `simple` configuration (no stemming) so their exact
word forms are preserved. The body text uses the object's language for stemming --
so a German document's body is stemmed with the German stemmer, while its title
retains exact words.

**Querying:** When `SearchableText` appears in the query dict without an explicit
`sort_on`, plone.pgcatalog automatically ranks results by relevance using
`ts_rank_cd()`:

```sql
ts_rank_cd(
    '{0.1, 0.2, 0.4, 1.0}'::float4[],
    searchable_text,
    plainto_tsquery(regconfig, search_text)
)
```

The weight array `{0.1, 0.2, 0.4, 1.0}` maps to weights D, C, B, A. This means a
token match at weight A (Title) contributes 10x more to the score than a match at
weight D (body). The `ts_rank_cd` function uses cover density ranking, which
additionally rewards term proximity -- documents where the search terms appear close
together score higher.

**Index:** A GIN index on the `searchable_text` column provides fast boolean filtering.
The ranking computation runs only on the rows that pass the `@@` filter.

### Tier 2: SearchableText with BM25

When VectorChord-BM25 is installed, plone.pgcatalog upgrades from `ts_rank_cd` to
true BM25 scoring. The tsvector infrastructure is kept for GIN-indexed boolean
pre-filtering; BM25 provides the ranking.

**Storage:** Per-language `search_bm25_{lang}` columns of type `bm25vector`. Each
column uses a language-specific tokenizer (stemmer and optional pre-tokenizer for
CJK scripts). A fallback `search_bm25` column handles unconfigured languages.

**Indexing:** The `BM25Backend.process_search_data()` method combines title (repeated
3x for field boosting), description, and body text into a single string, then stores
it in the appropriate language column via `tokenize()`.

**Querying:** The query path uses a two-stage approach:

1. GIN pre-filter via `searchable_text @@ plainto_tsquery(...)` (same as Tier 1).
2. BM25 ranking via `search_bm25_{lang} <&> to_bm25query(index, tokenize(text, tokenizer))`.

BM25 scoring considers three factors that `ts_rank_cd` ignores:

- **IDF (Inverse Document Frequency):** Rare terms are weighted higher. In a corpus
  where "security" appears in 5,000 documents but "policy" appears in only 200,
  a match on "policy" contributes more to the score.

- **Term saturation:** A document mentioning "security" 50 times does not score 50x
  higher than one mentioning it once. BM25's k1 parameter controls the saturation
  curve -- after a few occurrences, additional matches have diminishing returns.

- **Length normalization:** Shorter documents are boosted relative to longer ones. A
  200-word page titled "Security Policy" scores higher than a 10,000-word report
  that mentions security once, even though both match the query.

### Tier 3: Title/Description (word-level)

Title and Description have their own search path, independent of SearchableText. This
is used when Plone searches specifically for Title or Description content (e.g., the
"Title" field in a collection criterion).

**Storage:** Values are stored in the `idx` JSONB column under the `Title` and
`Description` keys.

**Indexing:** GIN expression indexes are created at schema setup time:

```sql
CREATE INDEX idx_os_cat_title_tsv ON object_state
USING gin (to_tsvector('simple'::regconfig, COALESCE(idx->>'Title', '')))
WHERE idx IS NOT NULL;
```

These use the `simple` configuration (no stemming, no stop words). This is a
deliberate choice: Title and Description searches should match exact word forms.
Searching for "Run" should match documents titled "Run" but not "Running" -- the
user is looking for a specific word. SearchableText provides the stemmed search
when broader matching is desired.

**Querying:** The query builder generates:

```sql
to_tsvector('simple'::regconfig, COALESCE(idx->>'Title', ''))
    @@ plainto_tsquery('simple'::regconfig, %(search_text)s)
```

This expression matches the GIN expression index, so PostgreSQL uses an index scan.

### Tier 4: Addon ZCTextIndex fields

Plone add-ons can register additional ZCTextIndex indexes (e.g., for custom content
types that need field-specific search). plone.pgcatalog auto-discovers these at
startup.

**Discovery:** During `_sync_registry_from_db()`, the startup subscriber reads all
ZCatalog indexes. Those with `meta_type="ZCTextIndex"` and a JSONB key (not
SearchableText) are registered in the `IndexRegistry` as TEXT-type indexes.

**Index creation:** `_ensure_text_indexes()` creates GIN expression indexes for each
discovered field using the `simple` configuration, following the same pattern as
Title and Description.

**Querying:** The query builder handles addon text indexes identically to
Title/Description -- a `to_tsvector('simple', idx->>'{key}') @@ plainto_tsquery()`
clause that matches the GIN expression index.

## Language support

### Built-in PostgreSQL configurations

PostgreSQL ships with text search configurations for 30 languages. plone.pgcatalog
maps Plone's ISO 639-1 language codes to PostgreSQL configuration names via the
`pgcatalog_lang_to_regconfig()` SQL function and its Python mirror
`language_to_regconfig()`:

| Languages | PostgreSQL configs |
|---|---|
| Western European | danish, dutch, english, finnish, french, german, italian, norwegian, portuguese, spanish, swedish |
| Eastern European | estonian, hungarian, lithuanian, romanian, russian, serbian, turkish |
| Other | arabic, armenian, basque, catalan, greek, hindi, indonesian, irish, nepali, tamil, yiddish |

Languages not in the mapping (including all CJK languages at the tsvector level)
fall back to `simple` -- no stemming, basic whitespace tokenization.

### CJK support via pg_tokenizer

When VectorChord-BM25 is installed, CJK languages get proper word segmentation
through pg_tokenizer:

- **Chinese:** Jieba segmenter
- **Japanese:** Lindera segmenter
- **Korean:** Lindera segmenter

These are configured as per-language BM25 columns with dedicated tokenizers. The
tsvector tier still uses `simple` for CJK (since PostgreSQL's built-in tokenizers
do not support CJK segmentation), but the BM25 tier provides proper word-level
matching.

### Per-object language selection

Each Plone content object has a `Language` field (typically set by
`plone.app.multilingual`). plone.pgcatalog reads this field during indexing and
applies the corresponding language configuration:

- At tsvector write time: `to_tsvector(pgcatalog_lang_to_regconfig(Language), text)`
- At BM25 write time: routes to the correct `search_bm25_{lang}` column
- At query time: `plainto_tsquery(pgcatalog_lang_to_regconfig(Language), search_text)`

If the `Language` field in the query differs from the object's language, the stemmer
mismatch may produce suboptimal results. This is inherent to language-specific
stemming -- searching for German words with an English stemmer produces poor matches.
The BM25 fallback column (no stemmer, basic tokenization) provides a safety net for
cross-language searches.

## Relevance ranking

### Tsvector ranking (ts_rank_cd)

`ts_rank_cd` implements cover density ranking: it considers both the frequency of
matching terms and their proximity to each other within the document.

The weight multiplier array `{0.1, 0.2, 0.4, 1.0}` assigns scores:

| Weight | Score multiplier | Assigned to |
|---|---|---|
| D | 0.1 | Body text (SearchableText) |
| C | 0.2 | (unused) |
| B | 0.4 | Description |
| A | 1.0 | Title |

A match in the Title contributes 10x the score of the same match in the body.
Combined with cover density (proximity bonus), this produces good relevance ordering
for most searches without any extensions.

Relevance ranking is auto-applied when `SearchableText` is queried without an
explicit `sort_on`. If `sort_on` is set (e.g., `sort_on="modified"`), the explicit
sort takes priority and relevance ranking is not applied.

### BM25 ranking (optional)

When the BM25 backend is active, the ranking expression changes from `ts_rank_cd`
to the `<&>` operator, which computes a BM25 score. Lower scores indicate higher
relevance (the operator returns a distance metric).

BM25's parameters are controlled by the VectorChord-BM25 extension:

- **k1** (term saturation): Controls how quickly additional term occurrences
  saturate. Default is 1.2 -- after 3-4 occurrences, additional matches contribute
  very little.
- **b** (length normalization): Controls how much shorter documents are boosted.
  Default is 0.75 -- a document half the average length gets a meaningful boost.

Field-level boosting in BM25 is achieved by repeating the title text 3x in the
combined input string. This is crude but effective: BM25 sees the title terms as
more frequent relative to the document length, which increases their contribution
to the score. Future versions may adopt VectorChord-BM25's field boosting API if
one is added.
