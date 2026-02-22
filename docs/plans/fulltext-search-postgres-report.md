# Full-Text Search in PostgreSQL: From Basics to BM25

**A practical guide to PostgreSQL's built-in text search, the new BM25 extensions, and how they compare to Elasticsearch.**

*February 2026*

---

## Table of Contents

1. [How Full-Text Search Works](#1-how-full-text-search-works)
2. [PostgreSQL Built-In Full-Text Search](#2-postgresql-built-in-full-text-search)
3. [Field Boosting in PostgreSQL](#3-field-boosting-in-postgresql)
4. [BM25: The Gold Standard of Relevance Ranking](#4-bm25-the-gold-standard-of-relevance-ranking)
5. [BM25 Extensions for PostgreSQL](#5-bm25-extensions-for-postgresql)
6. [Multilingual Support Compared](#6-multilingual-support-compared)
7. [Cloud-Native Deployment Compared](#7-cloud-native-deployment-compared)
8. [Comparison with Elasticsearch](#8-comparison-with-elasticsearch)
9. [When to Use What](#9-when-to-use-what)

---

## 1. How Full-Text Search Works

Full-text search goes far beyond simple pattern matching like `LIKE '%word%'`.
It is a pipeline of steps that transform raw text into searchable, rankable tokens:

1. **Tokenization** — breaking text into individual words ("tokens")
2. **Normalization** — lowercasing, removing accents
3. **Stemming** — reducing words to their root form ("running" becomes "run", "foxes" becomes "fox")
4. **Stop word removal** — dropping common words that carry no search value ("the", "a", "is", "and")
5. **Indexing** — storing the processed tokens in a data structure optimized for fast lookup
6. **Ranking** — scoring how relevant each result is to the query

The key insight is that search operates on *meaning*, not characters.
A search for "running" should match documents containing "ran", "runs", and "runner" — and a match in a short title should rank higher than the same match buried in a long body of text.

---

## 2. PostgreSQL Built-In Full-Text Search

PostgreSQL ships with a mature, full-featured text search engine. No extensions required.

### Core Data Types

**tsvector** — the indexed document representation.
Each word is stemmed and stored with its position. Stop words are removed automatically:

```sql
SELECT to_tsvector('english', 'The quick brown foxes jumped over the lazy dogs');
-- Result: 'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2
```

**tsquery** — the search query:

```sql
SELECT to_tsquery('english', 'quick & fox');
-- Result: 'quick' & 'fox'
```

**The match operator `@@`** — connects queries to documents:

```sql
SELECT * FROM articles
WHERE to_tsvector('english', body) @@ to_tsquery('english', 'quick & fox');
```

### Language-Aware Configurations

PostgreSQL ships with 29+ language configurations that understand each language's stemming rules and stop words:

```sql
-- English: "running" stems to "run"
SELECT to_tsvector('english', 'running');  -- 'run':1

-- German: "Haeuser" stems to "haus"
SELECT to_tsvector('german', 'Haeuser');   -- 'haus':1

-- 'simple': no stemming, no stop words (good for proper nouns, identifiers)
SELECT to_tsvector('simple', 'The Running Man');  -- 'man':3 'running':2 'the':1
```

### Query Syntax

PostgreSQL supports a rich query language for text search:

```sql
-- AND: both terms must be present
to_tsquery('cat & dog')

-- OR: either term matches
to_tsquery('cat | dog')

-- NOT: exclude a term
to_tsquery('cat & !dog')

-- Phrase search: words adjacent and in order
to_tsquery('full <-> text <-> search')

-- Proximity: words within N positions of each other
to_tsquery('database <2> performance')

-- Prefix matching: matches "postgres", "posting", "post", etc.
to_tsquery('post:*')

-- Google-like syntax for user input (the most practical for end users)
websearch_to_tsquery('english', '"full text" search -spam')
-- Result: 'full' <-> 'text' & 'search' & !'spam'
```

### Indexing

For performance, create a GIN (Generalized Inverted Index) on the tsvector:

```sql
-- Option A: expression index (recomputes tsvector on each insert/update)
CREATE INDEX idx_articles_fts ON articles
USING GIN (to_tsvector('english', body));

-- Option B: stored generated column (precomputes, recommended for large tables)
ALTER TABLE articles ADD COLUMN body_tsv tsvector
  GENERATED ALWAYS AS (to_tsvector('english', body)) STORED;

CREATE INDEX idx_articles_fts ON articles USING GIN (body_tsv);
```

### Ranking Functions

PostgreSQL provides two built-in ranking functions:

**ts_rank** — based on term frequency (how often the search terms appear):

```sql
SELECT title, ts_rank(body_tsv, query) AS rank
FROM articles, to_tsquery('english', 'database') AS query
WHERE body_tsv @@ query
ORDER BY rank DESC;
```

**ts_rank_cd** — cover density ranking (also considers how close matching terms are to each other):

```sql
SELECT title, ts_rank_cd(body_tsv, query) AS rank
FROM articles, to_tsquery('english', 'full & text & search') AS query
WHERE body_tsv @@ query
ORDER BY rank DESC;
```

### Highlighting Search Results

PostgreSQL can generate highlighted snippets showing where matches occur:

```sql
SELECT ts_headline('english', body,
  to_tsquery('english', 'database'),
  'StartSel=<b>, StopSel=</b>, MaxFragments=3, FragmentDelimiter= ... '
) FROM articles
WHERE body_tsv @@ to_tsquery('english', 'database');

-- Result: "...the <b>database</b> was configured for high availability ... "
```

---

## 3. Field Boosting in PostgreSQL

A critical feature for good search relevance is **field boosting** — making matches in certain fields (like the title) count more than matches in others (like the body text).

### The Weight System

PostgreSQL supports four weight categories: **A, B, C, D** (from highest to lowest default weight).

**Step 1: Assign weights when building the tsvector:**

```sql
SELECT
  setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
  setweight(to_tsvector('english', coalesce(summary, '')), 'B') ||
  setweight(to_tsvector('english', coalesce(body, '')), 'D')
AS document_tsv
FROM articles;
```

**Step 2: Rank with custom weight multipliers.**
The `ts_rank` function accepts a weight array in the order `{D, C, B, A}`:

```sql
-- Default weights: D=0.1, C=0.2, B=0.4, A=1.0
SELECT title,
  ts_rank(
    '{0.1, 0.2, 0.4, 1.0}'::float4[],
    document_tsv,
    query
  ) AS rank
FROM articles, to_tsquery('english', 'postgres') AS query
WHERE document_tsv @@ query
ORDER BY rank DESC;
```

These weights are fully tunable. For example, to make title matches 10x more important than body matches:

```sql
ts_rank('{0.1, 0.2, 0.4, 10.0}'::float4[], document_tsv, query)
```

### Practical Example: Combined Stored Column

```sql
ALTER TABLE articles ADD COLUMN search_tsv tsvector
  GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(summary, '')), 'B') ||
    setweight(to_tsvector('english', coalesce(body, '')), 'D')
  ) STORED;

CREATE INDEX idx_articles_search ON articles USING GIN (search_tsv);
```

### Limitation

You only get **4 weight buckets** (A through D). For most applications — title, summary, body, tags — this is sufficient. If you need more granularity, you can combine weights arithmetically in the `ORDER BY` clause or use one of the BM25 extensions described below.

---

## 4. BM25: The Gold Standard of Relevance Ranking

### Why ts_rank Falls Short

PostgreSQL's built-in `ts_rank` is a simple term frequency counter. It has two significant blind spots:

1. **No IDF (Inverse Document Frequency)**: It does not know how rare a term is across the corpus. The word "PostgreSQL" appearing in 5 out of 10,000 documents should score much higher than "database" appearing in 8,000 out of 10,000 — but `ts_rank` treats them equally.

2. **No term frequency saturation**: A document where "database" appears 100 times scores 100x higher than one where it appears once. In reality, after a few occurrences the additional mentions add diminishing value.

### How BM25 Works

BM25 (Best Matching 25) is the ranking algorithm used by Elasticsearch, Solr, and most modern search engines. Its formula:

```
BM25(q, d) = SUM[ IDF(t) * (tf(t,d) * (k1 + 1)) / (tf(t,d) + k1 * (1 - b + b * |d|/avgdl)) ]
```

In practical terms, BM25 incorporates three improvements:

| Factor | What it does | Example |
|---|---|---|
| **IDF** | Rare terms score higher than common ones | "PostgreSQL" scores more than "the" |
| **TF saturation** | Diminishing returns for repeated terms | 50 occurrences is not 50x better than 1 |
| **Length normalization** | Short documents with a match rank higher | A match in a 5-word title beats the same match in a 5,000-word body |

The parameters `k1` (typically 1.2) and `b` (typically 0.75) control the saturation curve and length normalization strength. Most implementations use sensible defaults.

### BM25 vs ts_rank: A Concrete Example

Consider searching for "PostgreSQL performance" across these documents:

| Document | Title | Body (excerpt) |
|---|---|---|
| Doc A | "PostgreSQL Performance Tuning" | Short article, 200 words |
| Doc B | "Database Systems Overview" | Mentions "PostgreSQL performance" once in 10,000 words |
| Doc C | "PostgreSQL PostgreSQL PostgreSQL" | Repeats the word 100 times, 300 words |

- **ts_rank** would rank Doc C highest (most term occurrences), then Doc B or Doc A.
- **BM25** would rank Doc A highest (match in title-length text, both rare terms present, short document), then Doc B, then Doc C (term repetition saturates).

BM25's ranking aligns much better with what users actually consider relevant.

---

## 5. BM25 Extensions for PostgreSQL

As of early 2026, three extensions bring BM25 to PostgreSQL:

### 5.1 pg_textsearch (Tiger Data)

*Open-sourced October 2025. Apache-2.0 license. Preview release.*

The most "PostgreSQL-native" approach. It builds on top of PostgreSQL's existing `tsvector`/`tsquery` infrastructure, so it works with the language configurations and stemming you already know.

```sql
-- Create a BM25 index (uses native PG text search configs)
CREATE INDEX articles_content_idx ON articles
USING bm25(content)
WITH (text_config='english');

-- Query with BM25 scoring via the <@> operator
SELECT id, title,
       content <@> to_bm25query('database performance', 'articles_content_idx') AS score
FROM articles
ORDER BY score
LIMIT 10;
```

**Architecture:** Builds an inverted index in PostgreSQL's Dynamic Shared Areas (DSA) — shared memory accessible by all backend processes. Uses Block-Max WAND optimization for fast top-k retrieval.

**Performance claims:** Up to 4x faster top-k queries compared to naive BM25; parallel index builds; 41% smaller index size via delta encoding and bitpacking.

| Pros | Cons |
|---|---|
| Native PG text search integration | Preview release (not production-ready yet) |
| Apache-2.0 license | Field boosting not yet documented |
| No external index engine | In-memory index (shared memory) |
| Uses existing `text_config` | New project, small community |

#### Multilingual Support

Since pg_textsearch builds directly on PostgreSQL's `tsvector` infrastructure, it inherits all **29+ built-in language configurations**: Arabic, Armenian, Basque, Catalan, Danish, Dutch, English, Finnish, French, German, Greek, Hindi, Hungarian, Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Portuguese, Romanian, Russian, Serbian, Spanish, Swedish, Tamil, Turkish, Yiddish, plus `simple` (no stemming/no stop words).

The language is set via the `text_config` parameter at index creation time. This maps directly to PostgreSQL's `regconfig` system, so existing language-detection logic (like choosing the right config per document) works unchanged.

**CJK (Chinese/Japanese/Korean): Not supported.** PostgreSQL has no built-in configurations for CJK languages. External extensions like `zhparser` (Chinese) could theoretically be combined, but this is untested with pg_textsearch.

**Limitation:** One `text_config` per index — a multilingual site needs either separate indexes per language or the `simple` config (which works across all languages but provides no stemming).

#### Cloud-Native Deployment

**Docker:** No official image. You need to build a custom image by compiling from source (pure C, PGXS — straightforward) or downloading pre-built binaries from GitHub releases (Linux/macOS, amd64/arm64, PG 17-18).

**Installation:** No apt/deb/rpm packages. No `shared_preload_libraries` entry required — just `CREATE EXTENSION pg_textsearch;`. However, the extension uses PostgreSQL Dynamic Shared Areas (DSA) and requests ~256 MB of shared memory by default. Container environments must allocate sufficient `/dev/shm`.

**CNPG (CloudNativePG): Not suitable for production HA.** This is the critical limitation: pg_textsearch currently has **no WAL (Write-Ahead Log) support** and **no streaming replication** for its BM25 indexes. This means:

- Standby replicas will have **empty BM25 indexes** — search only works on the primary
- After failover, the new primary must **rebuild all BM25 indexes** from heap data (adds startup latency)
- Point-in-time recovery does not restore BM25 index state
- Single-writer serialization limits write concurrency

This is a fundamental architectural constraint, not a missing feature. Until WAL integration is added, pg_textsearch is best suited for single-node or development environments.

### 5.2 pg_search (ParadeDB)

*Most mature option. 210+ releases. Latest v0.21.8 (Feb 2026). AGPL / commercial license.*

Built on **Tantivy**, a Rust-based search engine library (similar to Apache Lucene). Brings its own index engine — does not use native tsvector. The most feature-complete option with arbitrary per-field boosting.

```sql
-- Create a BM25 index over multiple fields
CREATE INDEX search_idx ON articles
USING bm25 (id, title, description, body)
WITH (key_field='id');

-- Query with per-field boosting
SELECT title, pdb.score(id)
FROM articles
WHERE title ||| 'database'::pdb.boost(10.0)
   OR description ||| 'database'::pdb.boost(3.0)
   OR body ||| 'database'
ORDER BY pdb.score(id) DESC
LIMIT 10;
```

The `::pdb.boost(N)` syntax allows **arbitrary float weights per field** — no 4-bucket limitation. This is the closest to Elasticsearch's query-time boosting.

Additional features include fuzzy search, n-gram tokenizers, faceted search, and highlighting.

| Pros | Cons |
|---|---|
| Arbitrary per-field float boosts | AGPL license (commercial available) |
| Very mature (210+ releases) | Brings its own index engine (not native PG) |
| Fuzzy search, facets, highlighting | Heavier dependency |
| Closest to Elasticsearch feature set | Tantivy index separate from PG tables |

#### Multilingual Support

pg_search brings its own tokenization pipeline via Tantivy (independent of PostgreSQL's `tsvector`). It supports **12 tokenizer types** and **18 stemmer languages** via Snowball:

**Stemmers:** Arabic, Danish, Dutch, English, Finnish, French, German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil, Turkish.

This covers all major European languages but is missing some that PostgreSQL's built-in system has (Armenian, Basque, Catalan, Hindi, Indonesian, Irish, Lithuanian, Nepali, Serbian, Yiddish).

**CJK support is strong**, with multiple quality tiers:

| Tokenizer | Quality | Description |
|---|---|---|
| `chinese_compatible` | Basic | Character-level splitting (each CJK char = one token) |
| `icu` | Good | Unicode-standard word boundaries, handles mixed-language content |
| `lindera` (Chinese) | High | Dictionary-based (CC-CEDICT) |
| `lindera` (Japanese) | High | Dictionary-based (IPADIC) |
| `lindera` (Korean) | High | Dictionary-based (KoDic) |
| `jieba` | Best (Chinese) | Dictionary + statistical models |

Per-column tokenizer configuration is supported via the v2 API:

```sql
-- English with stemming
CREATE INDEX idx ON articles USING bm25 (
    id,
    (title::pdb.simple('stemmer=english', 'stopwords_language=english')),
    (body::pdb.simple('stemmer=english'))
) WITH (key_field='id');

-- Chinese with Lindera
CREATE INDEX idx ON articles USING bm25 (
    id,
    (content::pdb.chinese_lindera)
) WITH (key_field='id');

-- Mixed-language content with ICU
CREATE INDEX idx ON articles USING bm25 (
    id,
    (content::pdb.icu)
) WITH (key_field='id');
```

**Limitation:** No custom dictionary support beyond what the built-in tokenizers provide — you cannot add your own stemming rules or thesaurus entries.

#### Cloud-Native Deployment

**Docker:** Official image `paradedb/paradedb` with pg_search pre-installed on PG 17. Works out of the box:

```bash
docker run --name paradedb -e POSTGRES_PASSWORD=password -p 5432:5432 paradedb/paradedb
```

**Installation:** Pre-built `.deb` (Debian/Ubuntu) and `.rpm` (RHEL/Rocky) packages from GitHub releases. Also available via PGXN and Pigsty. Requires `shared_preload_libraries = 'pg_search'` on PG < 17 (not needed on PG 17+).

**Index storage:** Since v0.14.0, pg_search uses **PostgreSQL native block storage** (not Tantivy's filesystem). BM25 index data is stored as regular relation files within PG's data directory, integrated with the buffer cache and WAL.

**CNPG (CloudNativePG): Best supported of the three.** ParadeDB provides a dedicated Helm chart:

```bash
# Install CNPG operator
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm install cnpg --namespace cnpg-system --create-namespace cnpg/cloudnative-pg

# Install ParadeDB cluster
helm repo add paradedb https://paradedb.github.io/charts
helm install paradedb --namespace paradedb --create-namespace paradedb/paradedb
```

**Important HA distinction — Community vs Enterprise:**

| Capability | Community (AGPL) | Enterprise (commercial) |
|---|---|---|
| BM25 index on primary | Yes | Yes |
| WAL replication of BM25 indexes | **No** | Yes |
| Crash recovery of BM25 indexes | **No** | Yes |
| Seamless failover | **No** (rebuild needed) | Yes |

With the Community edition in a CNPG cluster, BM25 indexes exist only on the primary. After failover, indexes must be rebuilt on the new primary. The Enterprise edition replicates BM25 indexes via WAL to all standbys, enabling true high availability.

### 5.3 VectorChord-BM25

*Focused on multilingual BM25 with pluggable tokenizers. Apache-2.0 license.*

Implements the Block-WeakAnd algorithm for BM25 ranking. Designed to pair with vector search for hybrid semantic + keyword retrieval.

```sql
-- Tokenize and store as sparse BM25 vectors
UPDATE documents SET embedding = tokenize(passage, 'bert');

-- Create BM25 index
CREATE INDEX docs_bm25 ON documents USING bm25 (embedding bm25_ops);

-- Query with BM25 ranking
SELECT id, passage,
       embedding <&> to_bm25query('docs_bm25', tokenize('PostgreSQL', 'bert')) AS rank
FROM documents
ORDER BY rank
LIMIT 10;
```

| Pros | Cons |
|---|---|
| Pluggable tokenizers (BERT, Jieba, Lindera) | Requires explicit tokenization step |
| Strong multilingual support (CJK) | Field boosting not documented |
| Pairs well with vector/semantic search | Smaller community |
| Apache-2.0 license | Less mature than ParadeDB |

#### Multilingual Support

VectorChord-BM25 has the most flexible tokenization pipeline of the three, via its companion extension **pg_tokenizer.rs**. The pipeline is fully configurable:

```
Text → Character Filters → Pre-tokenizer → Token Filters → Model → bm25vector
```

**Stemmers: 30 languages** (the most comprehensive — matches PostgreSQL's full set plus Estonian): Arabic, Armenian, Basque, Catalan, Danish, Dutch, English (Porter), English (Porter2), Estonian, Finnish, French, German, Greek, Hindi, Hungarian, Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Portuguese, Romanian, Russian, Serbian, Spanish, Swedish, Tamil, Turkish, Yiddish.

**CJK support is strong**, with dedicated tokenizers:

- **Jieba** (Chinese): Dictionary + statistical word segmentation
- **Lindera** (Japanese): IPADIC, UniDic, NEologd dictionaries
- **Lindera** (Korean): KoDic dictionary
- **Lindera** (Chinese): CC-CEDICT dictionary
- **LLM-based tokenizers** (BERT, Gemma2, LLMLingua2): Trained on multilingual data, handle CJK without language-specific config

Custom analyzer pipelines are defined in SQL:

```sql
-- Custom English analyzer with stopwords and stemming
SELECT create_text_analyzer('english_analyzer', $$
pre_tokenizer = "unicode_segmentation"
[[character_filters]]
to_lowercase = {}
[[token_filters]]
stopwords = "nltk_english"
[[token_filters]]
stemmer = "english_porter2"
$$);

-- Chinese analyzer with Jieba
SELECT create_text_analyzer('chinese_analyzer', $$
[pre_tokenizer.jieba]
$$);
```

**Unique features:**

- **Custom model training:** You can train a tokenizer model on your own corpus — useful for domain-specific vocabulary (medical, legal, technical content):
  ```sql
  SELECT create_custom_model_tokenizer_and_trigger(
      tokenizer_name => 'my_tokenizer',
      model_name => 'my_model',
      text_analyzer_name => 'my_analyzer',
      table_name => 'documents',
      source_column => 'passage',
      target_column => 'embedding'
  );
  ```

- **Custom synonyms and stopwords:**
  ```sql
  SELECT create_synonym('domain_synonyms', $$
  postgres,postgresql,pgsql
  cms,content management system
  $$);

  SELECT create_stopwords('custom_stops', $$
  the
  a
  an
  $$);
  ```

- **PostgreSQL dictionary bridge:** The `pg_dict` token filter can use PostgreSQL's native text search dictionaries, bridging the two systems.

**Limitation:** Lindera CJK dictionaries require compilation with feature flags — not all pre-built packages include them. LLM-based tokenizers consume 100-200 MB RAM per model.

#### Cloud-Native Deployment

**Docker:** Official all-in-one image with both extensions pre-installed:

```bash
docker run --name vchord-suite \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d tensorchord/vchord-suite:pg17-latest
```

Also available at `ghcr.io/tensorchord/vchord-suite:pg17-latest` with version-pinned tags.

**Installation:** apt packages (`postgresql-17-vchord-bm25`) and rpm packages via Pigsty. Requires `shared_preload_libraries = 'vchord_bm25'`. Two extensions must be created:

```sql
CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE;
CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE;
```

**CNPG (CloudNativePG): Good foundations.** TensorChord maintains [cloudnative-vectorchord](https://github.com/tensorchord/cloudnative-vectorchord) with pre-built CNPG-compatible Docker images. A CNPG cluster manifest looks like:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: vectorchord-cluster
spec:
  instances: 3
  imageName: ghcr.io/tensorchord/cloudnative-vectorchord:17-v0.x.x
  postgresql:
    shared_preload_libraries:
      - "vchord_bm25"
  bootstrap:
    initdb:
      postInitSQL:
        - CREATE EXTENSION IF NOT EXISTS pg_tokenizer CASCADE;
        - CREATE EXTENSION IF NOT EXISTS vchord_bm25 CASCADE;
```

No dedicated Helm chart (unlike ParadeDB), but the CNPG images + standard CNPG manifests work. The BM25 index is implemented as a native PostgreSQL index access method, and it **replicates via WAL** to standbys — confirmed by source code analysis: all writes use PostgreSQL's `GenericXLogStart()`/`GenericXLogFinish()` API through the native buffer manager.

---

## 6. Multilingual Support Compared

| Feature | pg_textsearch | pg_search (ParadeDB) | VectorChord-BM25 |
|---|---|---|---|
| **Stemmer languages** | 29+ (via PG regconfig) | 18 (Tantivy Snowball) | 30 (Snowball, most comprehensive) |
| **Chinese** | No | Yes (jieba, lindera, character-level) | Yes (jieba, lindera, LLM models) |
| **Japanese** | No | Yes (lindera IPADIC) | Yes (lindera IPADIC/UniDic/NEologd) |
| **Korean** | No | Yes (lindera KoDic) | Yes (lindera KoDic) |
| **Mixed-language content** | Use `simple` config | ICU tokenizer (recommended) | LLM-based tokenizers |
| **Per-column language config** | No (one config per index) | Yes (v2 API cast syntax) | Yes (different tokenizers per column) |
| **Custom dictionaries** | Via PG text search dicts | No | Yes (stopwords, synonyms, pg_dict bridge) |
| **Custom model training** | No | No | Yes (train on your corpus) |
| **PG regconfig integration** | Native | None | Partial (pg_dict token filter) |

**For a European-language Plone site:** pg_textsearch is the simplest — it uses the same `regconfig` system and covers all European languages. VectorChord-BM25 matches this coverage and adds Estonian.

**For a multilingual Plone site with CJK:** ParadeDB or VectorChord-BM25. ParadeDB is more mature; VectorChord-BM25 offers more customization.

**Important for all three:** None provide automatic per-document language detection. The application layer must determine each document's language and route it to the appropriate tokenizer/config — which is what Plone already does via the `Language` field.

---

## 7. Cloud-Native Deployment Compared

| Aspect | pg_textsearch | pg_search (ParadeDB) | VectorChord-BM25 |
|---|---|---|---|
| **Language** | C (PGXS) | Rust (pgrx + Tantivy) | Rust (pgrx) |
| **License** | PostgreSQL (permissive) | AGPL-3.0 / commercial | Apache-2.0 |
| **PG versions** | 17, 18 | 15+ | 16, 17, 18 |
| **Official Docker image** | None (DIY) | `paradedb/paradedb` | `tensorchord/vchord-suite` |
| **apt/deb packages** | No | Yes | Yes (via Pigsty) |
| **rpm packages** | No | Yes | Yes (via Pigsty) |
| **shared_preload_libraries** | Not needed | Required (PG < 17 only) | Required |
| **CNPG Helm chart** | None | Yes (`paradedb/charts`) | None (use standard CNPG manifests) |
| **CNPG Docker images** | None (DIY) | Yes (official) | Yes (`cloudnative-vectorchord`) |
| **WAL replication of BM25 indexes** | **No** | Enterprise only | **Yes** (GenericXLog, confirmed) |
| **HA failover** | Rebuild indexes | Enterprise: seamless; Community: rebuild | Seamless (WAL-replicated) |
| **Shared memory overhead** | ~256 MB DSA | Standard PG buffers | Standard PG buffers |
| **External dependencies** | None | None | pg_tokenizer.rs |

### Deployment Path Summary

**Simplest path from Docker to CNPG:** ParadeDB. Official Docker image, official Helm chart, documented CNPG integration. The Community edition works for single-primary setups; Enterprise is needed for full HA with BM25 index replication.

**Best open-source path:** VectorChord-BM25 (Apache-2.0). Docker images available, CNPG-compatible images exist, and the BM25 index replicates via WAL (confirmed: all writes use `GenericXLogStart()`/`GenericXLogFinish()` through PG's native buffer manager). Requires managing two extensions (vchord_bm25 + pg_tokenizer.rs).

**Wait-and-see:** pg_textsearch. The most lightweight to install (pure C, no Rust toolchain) and permissively licensed, but the lack of WAL support makes it unsuitable for any CNPG deployment that requires HA. Worth revisiting once WAL integration is added.

---

## 8. Comparison with Elasticsearch

### Feature Matrix

| Feature | PG Built-in (ts_rank) | PG + BM25 Extension | Elasticsearch |
|---|---|---|---|
| **Setup** | Built-in, zero config | Extension install | Separate JVM cluster |
| **Ranking algorithm** | Term frequency | BM25 | BM25 |
| **Field boosting** | 4 weight buckets (A-D) | Arbitrary floats (ParadeDB) | Arbitrary floats per field |
| **Fuzzy search** | No | Yes (ParadeDB) | Yes, configurable |
| **Synonyms** | Custom dictionaries | Varies by extension | Simple config files |
| **Autocomplete** | Prefix matching only | N-grams (ParadeDB) | Edge n-grams, completion suggester |
| **Language support** | 29+ built-in configs | Same or pluggable tokenizers | Hundreds of analyzers |
| **Facets / aggregations** | Manual SQL GROUP BY | Yes (ParadeDB) | First-class framework |
| **Highlighting** | ts_headline | Yes | Yes, multiple strategies |
| **Consistency** | ACID, transactional | ACID, transactional | Eventually consistent |
| **Scaling** | Single node (+ replicas) | Single node | Distributed sharding |
| **Operational overhead** | None | Minimal (extension) | Significant (cluster mgmt) |
| **License** | PostgreSQL (permissive) | Apache-2.0 or AGPL | SSPL / Elastic License |

### Elasticsearch Field Boosting Example (for comparison)

In Elasticsearch, every field gets an arbitrary numeric boost:

```json
{
  "query": {
    "multi_match": {
      "query": "database performance",
      "fields": ["title^10", "summary^3", "body^1", "tags^5"],
      "type": "best_fields"
    }
  }
}
```

Per-clause boosting in bool queries:

```json
{
  "query": {
    "bool": {
      "should": [
        { "match": { "title": { "query": "database", "boost": 10 } } },
        { "match": { "body":  { "query": "database", "boost": 1 } } }
      ]
    }
  }
}
```

This flexibility is Elasticsearch's strongest suit for relevance tuning. ParadeDB's `::pdb.boost()` syntax now offers equivalent capability within PostgreSQL.

### The Consistency Advantage

A frequently underestimated benefit of PostgreSQL-based search: **transactional consistency**. When you update a row and commit, the search index reflects that change immediately and atomically. There is no sync lag, no eventual consistency window, and no separate indexing pipeline to monitor.

With Elasticsearch, there is always a delay between writing data and it becoming searchable (the "refresh interval", typically 1 second). For applications where search results must reflect the latest committed state — such as content management systems, e-commerce inventory, or financial data — this matters.

---

## 9. When to Use What

### PostgreSQL built-in FTS is the right choice when:

- Your data already lives in PostgreSQL
- You need ACID consistency (search reflects committed state)
- Your dataset is under ~5-10 million documents
- 4 relevance tiers (title/summary/body/tags) are sufficient
- You want zero operational overhead and no extra dependencies

### PostgreSQL + BM25 extension is the right choice when:

- You want better relevance ranking than `ts_rank` without leaving PostgreSQL
- You need arbitrary per-field boosting (ParadeDB)
- You want fuzzy search or faceted results (ParadeDB)
- You want to keep a single data store and avoid synchronization headaches
- Your dataset is moderate (up to tens of millions of documents)

### Elasticsearch is the right choice when:

- You need complex relevance tuning across many fields with fine-grained control
- Fuzzy matching, "did you mean" suggestions, and autocomplete are core features
- Your dataset is very large (hundreds of millions to billions of documents)
- You need rich aggregations and faceted navigation as primary features
- You can tolerate eventual consistency and the operational cost of running a cluster
- You have dedicated infrastructure/DevOps capacity

### Summary Decision Table

| Scenario | Recommendation |
|---|---|
| CMS with < 1M documents | PG built-in FTS |
| CMS needing better relevance | PG + pg_textsearch or ParadeDB |
| E-commerce product search (< 10M) | PG + ParadeDB (boosts, facets) |
| E-commerce product search (> 100M) | Elasticsearch |
| Log search / analytics | Elasticsearch (or OpenSearch) |
| Multilingual content (CJK) | PG + VectorChord-BM25 or Elasticsearch |
| Hybrid semantic + keyword search | PG + VectorChord-BM25 + pgvector |
| Must have transactional consistency | PostgreSQL (any variant) |

---

## References

- [PostgreSQL Documentation: Full Text Search](https://www.postgresql.org/docs/current/textsearch.html)
- [pg_textsearch — Tiger Data (GitHub)](https://github.com/timescale/pg_textsearch)
- [pg_textsearch Documentation — Tiger Data](https://www.tigerdata.com/docs/use-timescale/latest/extensions/pg-textsearch)
- [From ts_rank to BM25 — Tiger Data Blog](https://www.tigerdata.com/blog/introducing-pg_textsearch-true-bm25-ranking-hybrid-retrieval-postgres)
- [ParadeDB pg_search (GitHub)](https://github.com/paradedb/paradedb)
- [ParadeDB v2 API Deep Dive](https://www.paradedb.com/blog/v2api)
- [ParadeDB CNPG Helm Chart](https://paradedb.github.io/charts/charts/paradedb/)
- [ParadeDB Block Storage Architecture](https://www.paradedb.com/blog/block-storage-part-one)
- [VectorChord-BM25 (GitHub)](https://github.com/tensorchord/VectorChord-bm25)
- [pg_tokenizer.rs (GitHub)](https://github.com/tensorchord/pg_tokenizer.rs)
- [cloudnative-vectorchord — CNPG Images (GitHub)](https://github.com/tensorchord/cloudnative-vectorchord)
- [You Don't Need Elasticsearch: BM25 is Now in Postgres — Tiger Data](https://www.tigerdata.com/blog/you-dont-need-elasticsearch-bm25-is-now-in-postgres)
- [CloudNativePG Documentation](https://cloudnative-pg.io/)
