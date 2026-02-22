<!-- diataxis: explanation -->

# BM25 Design Decisions

plone.pgcatalog's BM25 integration is optional, auto-detected, and layered on top
of vanilla PostgreSQL's tsvector infrastructure. This page explains the goals and
constraints that shaped the design, why VectorChord-BM25 was chosen over
alternatives, and how the per-language column strategy works.

## Goals and constraints

The BM25 design was driven by two competing requirements: vanilla PostgreSQL must
work out of the box, and sites that want better relevance ranking should be able
to get it by installing extensions.

### Hard constraints

| Constraint | Rationale |
|---|---|
| No mandatory extensions | Plone must work with any standard PostgreSQL installation |
| Multilingual support | Plone serves content in 30+ languages; CJK is a real-world need |
| CloudNativePG / WAL replication | Production Plone often runs on Kubernetes with CloudNativePG; BM25 indexes must replicate to standbys |
| Open-source friendly licensing | Plone is GPL-2.0; the recommended extension stack should be freely distributable |
| Transactional consistency | BM25 data must be committed atomically with ZODB objects |

## Progressive enhancement architecture

Rather than requiring a specific extension, plone.pgcatalog implements a four-level
enhancement stack. Each level adds capabilities; no level removes previous
functionality.

```{mermaid}
flowchart TB
    L3["Level 3: Per-language BM25 columns<br/>Language-specific tokenizers<br/>CJK segmentation"]
    L2["Level 2: BM25 ranking<br/>IDF, saturation, length normalization<br/>VectorChord-BM25 extension"]
    L1["Level 1: Weighted ts_rank_cd<br/>Field boosting (A/B/D weights)<br/>Cover density ranking"]
    L0["Level 0: Boolean matching<br/>tsvector @@ tsquery<br/>GIN index scan"]

    L0 --> L1
    L1 --> L2
    L2 --> L3
```

**Level 0: Boolean matching.** The GIN index on `searchable_text` provides fast
yes/no matching. This is the filter stage -- it determines which documents contain
the search terms.

**Level 1: Weighted ts_rank_cd with field boosting.** Always available on vanilla
PostgreSQL. Title matches weighted 10x higher than body matches. Cover density
ranking rewards term proximity. This is the default ranking when no BM25 extension
is detected.

**Level 2: BM25 ranking.** When VectorChord-BM25 is detected at startup, ranking
switches from `ts_rank_cd` to true BM25 scoring. The tsvector GIN index is kept as
a pre-filter; BM25 provides the final score.

**Level 3: Per-language BM25 columns.** Each configured language gets its own
`bm25vector` column with a language-specific tokenizer. This enables proper stemming
and CJK word segmentation at the BM25 level.

Levels 2 and 3 are activated together by the `BM25Backend` class -- there is no
separate configuration step. If the extensions are present, they are used. If not,
the system falls back gracefully to Level 1.

## Why VectorChord-BM25?

Three PostgreSQL BM25 extensions were evaluated for plone.pgcatalog. The evaluation
considered license compatibility, language support, cloud-native deployment, maturity,
and feature completeness.

### pg_textsearch (Tiger Data)

The most natural fit architecturally -- it extends PostgreSQL's native tsvector/tsquery
with BM25 scoring, requiring minimal changes to the query and write paths.

However, at the time of evaluation it was a preview release with no WAL support.
BM25 indexes do not replicate to standby servers, which is a hard blocker for
CloudNativePG deployments where read replicas must have working search indexes.

### pg_search (ParadeDB)

The most mature option with production-ready BM25, arbitrary field boosting, and
strong CJK support. However, two factors weighed against it:

- **AGPL-3.0 license.** While server-side PostgreSQL extensions are arguably not
  "linked" into the application (and thus may not trigger AGPL's copyleft
  requirements), some organizations have blanket AGPL policies that prevent adoption.
  Plone should not create friction for these users.

- **WAL replication requires Enterprise license.** The Community edition's BM25
  indexes do not replicate to standbys. For CloudNativePG clusters, this means search
  only works on the primary, or indexes must be rebuilt after failover.

### VectorChord-BM25

VectorChord-BM25 scored highest in the evaluation (102/130 weighted points):

- **Apache-2.0 license.** No friction for distribution or organizational policies.
  Plone can freely recommend it in documentation without caveats.

- **WAL replication confirmed.** Source code analysis shows that all writes use
  PostgreSQL's `GenericXLogStart()`/`GenericXLogFinish()` API, and storage goes
  through the native buffer manager. BM25 indexes replicate to standbys without
  additional configuration. This was confirmed by the TensorChord team.

- **30+ stemmer languages.** The most comprehensive language coverage of any
  evaluated extension, matching PostgreSQL's full set of built-in text search
  configurations.

- **CJK support.** Chinese (Jieba), Japanese (Lindera), and Korean (Lindera)
  segmenters via pg_tokenizer.

- **Write-path alignment.** The explicit tokenization step (`text -> bm25vector`)
  maps directly to plone.pgcatalog's `CatalogStateProcessor` pattern. The processor
  already transforms text at write time; adding a `bm25vector` column is the same
  pattern with a different output type.

### Evaluation matrix

| Criterion (weight) | pg_textsearch | pg_search | VectorChord-BM25 |
|---|---|---|---|
| Search quality (5) | 4 | 5 | 4 |
| PG-native integration (4) | 5 | 2 | 3 |
| License (4) | 5 | 2 | 5 |
| CNPG / HA (4) | 1 | 3 | 5 |
| Multilingual (3) | 3 | 4 | 5 |
| Maturity (3) | 1 | 5 | 3 |
| Operational simplicity (3) | 4 | 3 | 2 |
| **Weighted total** | **82 / 130** | **89 / 130** | **102 / 130** |

## Per-language column strategy

### The problem with a single multilingual column

Different languages need different stemmers. The German stemmer reduces "Sicherheit"
to its root; the English stemmer reduces "security" to its root. Applying a German
stemmer to English text produces incorrect stems, and vice versa.

A single `bm25vector` column with one tokenizer cannot serve a multilingual site
well. The tokenizer must choose one stemmer, and every other language gets suboptimal
tokenization.

### One column per language

plone.pgcatalog creates a separate `search_bm25_{lang}` column for each configured
language. A site configured for English, German, and French gets three columns:

- `search_bm25_en` -- English Porter2 stemmer
- `search_bm25_de` -- German stemmer
- `search_bm25_fr` -- French stemmer

Plus a fallback column:

- `search_bm25` -- no stemmer, basic unicode tokenization

Each column has its own BM25 index and its own tokenizer (created via
`create_tokenizer()` at schema setup time).

### Write path

When an object is indexed, the `BM25Backend.process_search_data()` method reads the
object's `Language` field and populates the matching column. All other language
columns for that row are set to NULL.

An English document populates `search_bm25_en` and `search_bm25` (the fallback).
The `search_bm25_de` and `search_bm25_fr` columns remain NULL.

### Query path

At query time, the search backend checks the query's `Language` parameter:

- If a language is specified and has a configured column, the language-specific
  column and tokenizer are used for BM25 scoring.
- Otherwise, the fallback column is used.

### Storage efficiency

NULL columns consume zero storage in PostgreSQL thanks to TOAST optimization. A row
for an English document with three language columns only stores data in
`search_bm25_en` and `search_bm25` -- the German and French columns add zero bytes.

BM25 indexes are sparse: they only index non-NULL rows. The English BM25 index
contains only English documents; the German index contains only German documents.
This keeps each index small and fast.

### Language configuration

Languages are configured via the `PGCATALOG_BM25_LANGUAGES` environment variable:

- `PGCATALOG_BM25_LANGUAGES=en,de,fr` -- explicit language list
- `PGCATALOG_BM25_LANGUAGES=auto` -- auto-detect from `portal_languages` at startup
- Default (unset): `en` only

Each language code is validated against the `LANG_TOKENIZER_MAP` allowlist in
`backends.py`. Unknown language codes are rejected at startup with a clear error
message, preventing DDL injection via crafted language strings.

## SearchBackend abstraction

The `SearchBackend` abstract base class allows swapping ranking strategies without
changing query or indexing code. It has exactly five abstract methods plus two
optional hooks:

| Method | Purpose |
|---|---|
| `get_extra_columns()` | ExtraColumn definitions for `CatalogStateProcessor` |
| `get_schema_sql()` | DDL for backend-specific columns/indexes/functions |
| `process_search_data(pending)` | Extract backend-specific data during indexing |
| `build_search_clause(query, lang, pname)` | SQL clause for WHERE + ranking |
| `detect(dsn)` | Class method: check if backend is available |
| `uncatalog_extra()` | Column:None pairs for uncatalog (optional) |
| `install_schema(conn)` | Per-statement DDL execution (optional override) |

This is deliberately minimal. There is no plugin registry, no entry points, no
dynamic loading. Two concrete classes (`TsvectorBackend` and `BM25Backend`)
implement the interface, and a module-level singleton pattern (`get_backend()` /
`set_backend()`) provides the active instance.

### Auto-detection at startup

During the `IDatabaseOpenedWithRoot` subscriber, `detect_and_set_backend()` is called:

1. It tries `BM25Backend.detect(dsn)`, which queries `pg_available_extensions` for
   `vchord_bm25` and `pg_tokenizer`.
2. If both extensions are available, `BM25Backend` is activated with the configured
   languages.
3. If either extension is missing, `TsvectorBackend` is activated.

Detection checks `pg_available_extensions` (not `pg_extension`) so that it works
before `CREATE EXTENSION` has been executed. The BM25 backend's `get_schema_sql()`
includes the `CREATE EXTENSION` statements, which are applied when the state processor
is registered.

### Future extensibility

The thin abstraction makes it straightforward to add new backends if the PostgreSQL
BM25 landscape changes. If pg_textsearch ships WAL support and leaves preview, a
`PgTextsearchBackend` could be added without modifying any existing code outside the
backend module. The rest of plone.pgcatalog (security filters, path queries, field
indexes, keyword indexes, pagination, brain loading) is backend-agnostic.
