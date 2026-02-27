# plone.pgcatalog

<!-- diataxis: landing -->

```{image} _static/logo-400.png
:alt: plone.pgcatalog logo
:width: 200px
:align: center
```

PostgreSQL-backed catalog for Plone, replacing ZCatalog BTrees with SQL queries on JSONB.

**Key capabilities:**

- Drop-in replacement for Plone's `portal_catalog`
- All standard ZCatalog index types supported
- Full-text search with language-aware stemming (30 languages)
- Optional BM25 ranking via VectorChord-BM25
- Optional async text extraction from PDFs, Office docs, and images via Apache Tika
- Zero ZODB cache pressure -- no BTree objects stored
- Transactional writes atomically alongside ZODB commit
- Catalog data queryable from any PostgreSQL client

**Requirements:** Python 3.12+, PostgreSQL 14+ (tested with 17), Plone 6, [zodb-pgjsonb](https://github.com/bluedynamics/zodb-pgjsonb)

## Documentation

::::{grid} 2
:gutter: 3

:::{grid-item-card} Tutorials
:link: tutorials/index
:link-type: doc

**Learning-oriented** -- Step-by-step lessons to build skills.

*Start here if you are new to plone.pgcatalog.*
:::

:::{grid-item-card} How-To Guides
:link: how-to/index
:link-type: doc

**Goal-oriented** -- Solutions to specific problems.

*Use these when you need to accomplish something.*
:::

:::{grid-item-card} Reference
:link: reference/index
:link-type: doc

**Information-oriented** -- Technical specifications and API details.

*Consult when you need detailed information.*
:::

:::{grid-item-card} Explanation
:link: explanation/index
:link-type: doc

**Understanding-oriented** -- Architecture and design decisions.

*Read to deepen your understanding of how it works.*
:::

::::

## Quick Start

1. {doc}`Install plone.pgcatalog <how-to/install>`
2. {doc}`Run the quickstart demo <tutorials/quickstart-demo>` (Docker + multilingual content in 5 minutes)
3. {doc}`Migrate an existing site <tutorials/migrate-from-zcatalog>`

```{toctree}
---
maxdepth: 3
caption: Documentation
titlesonly: true
hidden: true
---
tutorials/index
how-to/index
reference/index
explanation/index
```
