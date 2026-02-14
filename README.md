# plone.pgcatalog

PostgreSQL-backed catalog for Plone, replacing ZCatalog BTrees indexes with SQL queries on JSONB.

Requires [zodb-pgjsonb](https://github.com/bluedynamics/zodb-pgjsonb) as the ZODB storage backend.

## Features

- **All standard index types** supported: FieldIndex, KeywordIndex, DateIndex, BooleanIndex, DateRangeIndex, UUIDIndex, ZCTextIndex, ExtendedPathIndex, GopipIndex
- **DateRecurringIndex** for recurring events (Plone's `start`/`end` indexes) -- recurrence expansion at query time via [rrule_plpgsql](https://github.com/sirrodgepodge/rrule_plpgsql), no C extensions needed
- **Extensible** via `IPGIndexTranslator` named utilities for custom index types
- **Dynamic index discovery** from ZCatalog at startup -- addons adding indexes via `catalog.xml` just work
- **Transactional writes** -- catalog data written atomically alongside object state during ZODB commit
- **Full-text search** via PostgreSQL `tsvector`/`tsquery`
- **Zero ZODB cache pressure** -- no BTree/Bucket objects stored in ZODB
- **In-transaction catalog visibility** -- pending catalog data is automatically flushed to PG before queries, no `transaction.commit()` needed between writes and reads
- **Container-friendly** -- works on standard `postgres:17` Docker images, no extensions required

## Requirements

- Python 3.12+
- PostgreSQL 14+ (tested with 17)
- [zodb-pgjsonb](https://github.com/bluedynamics/zodb-pgjsonb)
- Plone 6

## Installation

```bash
pip install plone-pgcatalog
```

Add to your Zope configuration:

```xml
<!-- zope.conf -->
%import zodb_pgjsonb
<zodb_main>
  <pgjsonb>
    dsn dbname=mydb user=zodb password=zodb host=localhost port=5432
  </pgjsonb>
</zodb_main>
```

Install the `plone.pgcatalog:default` GenericSetup profile through Plone's Add-on installer or your policy package.

## Usage

Once installed, `portal_catalog` is replaced with `PlonePGCatalogTool`. All catalog queries use the same ZCatalog API:

```python
# Standard catalog queries -- same syntax as ZCatalog
results = catalog(portal_type="Document", review_state="published")
results = catalog(Subject={"query": ["Python", "Plone"], "operator": "or"})
results = catalog(SearchableText="my search term")
results = catalog(path={"query": "/plone/folder", "depth": 1})

# Recurring events (DateRecurringIndex)
results = catalog(start={
    "query": [DateTime("2025-03-01"), DateTime("2025-03-31")],
    "range": "min:max",
})
```

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) -- internal design, index registry, query translation, custom index types
- [BENCHMARKS.md](BENCHMARKS.md) -- performance comparison vs RelStorage+ZCatalog
- [CHANGES.md](CHANGES.md) -- changelog

## License

GPL-2.0
