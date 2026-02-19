# I Cut Down a Forest

*Follow-up to [ZODB: Out of the Pickle Jar](https://community.plone.org/t/zodb-out-of-the-pickle-jar/22832)*

---

In the last post I showed how we got ZODB out of the pickle jar and into PostgreSQL JSONB.
Object state is now queryable JSON living in a real database. Nice.

But one thing kept bugging me: **portal_catalog**.

You know, that venerable piece of infrastructure that stores your catalog data in... a forest of BTrees.
Thousands of them. `FieldIndex`, `KeywordIndex`, `DateIndex` — each one its own BTree in ZODB.
Every time you publish a page, a small army of BTree buckets gets serialized, pickled, and written.
Multiply that by a dozen indexes and a few thousand content objects, and you've got yourself a proper forest.

So I cut it down.

## Goodbye BTrees, Hello PostgreSQL

**plone-pgcatalog** replaces ZCatalog's BTree storage with PostgreSQL columns — entirely.
Not a shim, not a proxy, not "let's sync BTrees to a side table."
The BTrees are simply gone.
All catalog data lives in the same `object_state` table that zodb-pgjsonb already uses for your ZODB objects:

| Column | What it does |
|---|---|
| `path` | Physical object path (for brain construction) |
| `idx` | **One JSONB column** — all your index values, queryable |
| `searchable_text` | Weighted tsvector for full-text search |

That's it.
Every `FieldIndex`, `KeywordIndex`, `DateIndex`, `BooleanIndex`, `PathIndex`, `UUIDIndex` — they all collapse into fields inside one JSONB column.
PostgreSQL is really good at querying JSONB.
GIN indexes, containment operators, range queries — it's all there natively.

**The result?** Content creation dropped from 175 ms/doc to 68.5 ms/doc.
That's **2.5x faster** writes compared to stock ZCatalog on RelStorage.
Turns out *not* serializing hundreds of BTree buckets per transaction saves some time.
Who knew.

## Full-Text Search — for Real

SearchableText uses a proper `tsvector` column with **language-aware stemming**.
German content gets the German stemmer, French gets French, and so on — 30 languages out of the box.
The `pgcatalog_lang_to_regconfig()` function maps Plone's language codes to PostgreSQL text search configurations automatically.

Title gets weight A, Description weight B, body text weight D.
Results come back ranked by relevance via `ts_rank_cd()`.
No configuration needed — it just works with a standard `postgres:17` image.

## But Wait — BM25!

Here's where it gets fun.

Drop in the [VectorChord-BM25](https://github.com/tensorchord/VectorChord-bm25) extension (one Docker image swap: `tensorchord/vchord-suite:pg17-latest`) and plone-pgcatalog **auto-detects** it at startup.
No config changes.
Suddenly your search ranking upgrades from tsvector's `ts_rank_cd` to proper **BM25 scoring** — the same algorithm that powers Lucene, Elasticsearch, and Solr under the hood:

- **IDF**: rare terms rank higher ("PostgreSQL" beats "the")
- **Term saturation**: repeating a word 50 times doesn't help
- **Length normalization**: short, focused pages aren't penalized against long ones
- **Title boosting**: title matches outrank body-only matches

Each configured language gets its **own BM25 column** with a language-specific tokenizer.
English gets Porter2 stemming, German gets Snowball, Chinese gets jieba segmentation, Japanese/Korean get lindera.
25+ languages supported.
Configure via one environment variable:

```
PGCATALOG_BM25_LANGUAGES=en,de,zh
```

Or set it to `auto` and let it read from `portal_languages`.

The tsvector column stays around for fast GIN pre-filtering.
BM25 handles the ranking.
Best of both worlds.

## Solr? Elasticsearch? What Are You Talking About?

Let's be honest about what a typical Plone site needs from search:

- Full-text search with stemming
- Relevance ranking
- Language-aware tokenization
- The usual catalog queries (portal_type, review_state, path, dates, keywords)

For this, you don't need a separate search cluster.
You don't need to run Solr alongside PostgreSQL alongside ZODB alongside Redis alongside... you get the idea.

- One PostgreSQL instance handles storage, catalog, full-text search, and BM25 ranking.
- One process.
- One backup strategy.
- One ops headcount.

If you're running a 10-million-document multilingual enterprise portal with faceted search, geo-queries, and real-time analytics — sure, maybe reach for the big guns.
For the rest of us?
PostgreSQL has quietly become ridiculously capable at search.
Let's use it.

## How It Works (the Short Version)

1. **Indexing**: When you create or edit content, `catalog_object()` stashes the index values in a thread-local pending store. During ZODB commit, a `CatalogStateProcessor` writes the catalog columns atomically alongside the object state. One transaction. Always consistent.

2. **Querying**: ZCatalog query dicts (`catalog(portal_type="Document", path="/plone/en/")`) get translated to parameterized SQL. JSONB containment for field queries, `?|` operator for keywords, `@@` for full-text. All through PostgreSQL's query planner with proper indexes.

3. **Addons just work**: The IndexRegistry auto-discovers all indexes from ZCatalog at startup. If your addon adds a `FieldIndex` via `catalog.xml`, plone-pgcatalog picks it up. Custom index types? Register an `IPGIndexTranslator` utility. Even `DateRecurringIndex` (Plone's recurring events) works — powered by pure PL/pgSQL recurrence expansion, no C extensions needed.

## Try It

The repo ships a turnkey multilingual example: a Plone site with ~800 Wikipedia articles in English, German, and Chinese, with translation linking via PAM.

Docker compose, a venv, install deps, generate a Zope instance, one zconsole script that creates the site + imports content, start Zope — done.
The [example README](https://github.com/bluedynamics/plone-pgcatalog/tree/main/example) walks you through it step by step.

Then search "Vulkan" in German or "火山" in Chinese and watch BM25 do its thing.

## Current Status

This is **beta software** (1.0.0b6). It's been through security hardening, conformance testing, and real-world testing with multilingual Plone sites. 600+ tests pass. But it's not battle-tested on production sites yet — that's where you come in.

The architecture is documented in [ARCHITECTURE.md](https://github.com/bluedynamics/plone-pgcatalog/blob/main/ARCHITECTURE.md) if you want to go deeper.

## The Stack

| Layer | What | |
|---|---|---|
| **zodb-json-codec** | Rust pickle-to-JSON transcoder | [repo](https://github.com/bluedynamics/zodb-json-codec) |
| **zodb-pgjsonb** | PostgreSQL JSONB storage for ZODB | [repo](https://github.com/bluedynamics/zodb-pgjsonb) |
| **plone-pgcatalog** | PostgreSQL catalog for Plone | [repo](https://github.com/bluedynamics/plone-pgcatalog) |

Feedback, testing, bug reports — all welcome. Let's see how far we can push PostgreSQL before we actually need that Elasticsearch cluster.
