<!-- diataxis: reference -->

# IPGIndexTranslator Interface Reference

This page documents the `IPGIndexTranslator` interface for extending
plone.pgcatalog with custom index types, its methods, security
contract, registration, wiring points, and built-in implementations.

## Interface Definition

Defined in `plone.pgcatalog.interfaces`:

```python
class IPGIndexTranslator(Interface):
    """Named utility that translates a custom index's data for PG storage + querying."""

    def extract(obj, index_name):
        """Extract value(s) from obj for this index.
        Returns dict to merge into idx JSONB."""

    def query(index_name, query_value, query_options):
        """Translate ZCatalog query to SQL fragment + params.
        Returns (sql_fragment, params_dict)."""

    def sort(index_name):
        """Return SQL expression for ORDER BY, or None."""
```

## Methods

`extract(obj, index_name) -> dict`
: Called during indexing (from `catalog.py`'s `_extract_from_translators()`).
  The `obj` argument is the IIndexableObject-wrapped Plone content object.
  Returns a dict of key-value pairs to merge into the `idx` JSONB column.
  Return `{}` if this translator stores no data (e.g., DateRangeInRangeIndex
  delegates to underlying indexes).

`query(index_name, raw, spec) -> tuple[str, dict]`
: Called during query execution (from `query.py`'s `_process_index()`).
  `raw` is the original query value as passed to the catalog. `spec` is the
  normalized query dict (simple values are wrapped as `{"query": value}`).
  Returns a tuple of `(sql_fragment, params_dict)` where the SQL fragment
  is inserted into the WHERE clause and params are bound via psycopg
  parameterized queries using `%(name)s` placeholders.

`sort(index_name) -> str | None`
: Called during sort processing (from `query.py`'s `_process_sort()`).
  Returns a SQL expression for ORDER BY, or `None` if the index does not
  support sorting.

## Security Contract

All `IPGIndexTranslator` implementations must follow these rules:

- All user-supplied values **must** use `%(name)s` parameter
  placeholders in the SQL fragment. Never string-format values into SQL.
- Index/column identifiers in the SQL fragment should be hardcoded
  constants or validated via `validate_identifier()`.
- `validate_identifier(name)` (from `plone.pgcatalog.columns`) rejects
  any name not matching `^[a-zA-Z_][a-zA-Z0-9_]*$`.
- The `query()` return value is appended directly to the WHERE clause.
  SQL injection through the fragment would bypass all parameterization.

## Registration

### Via ZCML (recommended for addons)

```xml
<utility
    provides="plone.pgcatalog.interfaces.IPGIndexTranslator"
    factory=".my_translator.MyTranslatorFactory"
    name="my_index_name"
/>
```

### Via Python (used at startup for auto-discovered indexes)

```python
from zope.component import provideUtility
from plone.pgcatalog.interfaces import IPGIndexTranslator

provideUtility(translator_instance, IPGIndexTranslator, name="my_index")
```

The utility name **must** match the ZCatalog index name.

## Wiring Points

| Module | Function | When Called |
|---|---|---|
| `catalog.py` | `_extract_from_translators()` | During indexing -- calls `extract()` for all registered translators |
| `query.py` | `_process_index()` | During query -- falls back to translator `query()` when index not in registry |
| `query.py` | `_process_sort()` | During sort -- falls back to translator `sort()` when index not in registry |

The fallback path in `_process_index()`: if an index name is not found
in the `IndexRegistry`, `query.py` calls `queryUtility(IPGIndexTranslator,
name=index_name)`. If a translator is found, its `query()` method is
called. If no translator is found either, the index is treated as a
simple JSONB field query.

## Built-in Implementations

### DateRecurringIndexTranslator

Defined in `plone.pgcatalog.dri`. Handles
`Products.DateRecurringIndex` instances.

**Constructor attributes:**

| Attribute | Description |
|---|---|
| `date_attr` | Object attribute for the base date (equals the index name) |
| `recurdef_attr` | Object attribute for the RRULE string |
| `until_attr` | Object attribute for the until date (rarely used) |

**Storage:**

Two keys in the `idx` JSONB column:

- `{index_name}`: ISO 8601 base date string
- `{index_name}_recurrence`: RFC 5545 RRULE string (if recurring)

RRULE strings are validated against the pattern
`^(RRULE:)?FREQ=(YEARLY|MONTHLY|...)` and capped at 1,000 characters.

**Query strategies:**

| Range | Non-recurring | Recurring |
|---|---|---|
| `min:max` | `date BETWEEN min AND max` | `EXISTS (rrule."between"(rrule, date, min, max))` |
| `min` | `date >= min` | `EXISTS (rrule."after"(rrule, date, min, 1))` |
| `max` | `date <= max` | `date <= max` (base date check sufficient) |
| exact | `date = value` | `EXISTS (rrule."between"(rrule, date, value, value))` |

**Sort:** `pgcatalog_to_timestamptz(idx->>'{index_name}')` (base date).

### DateRangeInRangeIndexTranslator

Defined in `plone.pgcatalog.addons_compat.driri`. Handles
`Products.DateRangeInRangeIndex` instances.

**Constructor attributes:**

| Attribute | Description |
|---|---|
| `startindex` | Name of the underlying start date index |
| `endindex` | Name of the underlying end date index |

**Storage:**

No-op. `extract()` returns `{}`. The underlying DateIndex or
DateRecurringIndex translators handle extraction into idx JSONB.

**Query:**

The query dict uses `start` and `end` keys (not `query`/`range`):

```python
catalog(event_dates={"start": DateTime("2025-03-01"), "end": DateTime("2025-03-31")})
```

| Case | SQL Logic |
|---|---|
| Non-recurring | `obj_start <= q_end AND obj_end >= q_start` (overlap test) |
| Recurring | `EXISTS (rrule."between"(rrule, base_start, q_start - duration, q_end))` where duration = `base_end - base_start` |

**Sort:** `pgcatalog_to_timestamptz(idx->>'{startindex}')` (start date
of the underlying start index).

### Auto-discovery

Both translators are auto-discovered and registered at startup by
`_register_dri_translators()` and `_register_driri_translators()` in
`startup.py`. These functions iterate the ZCatalog indexes, identify
`DateRecurringIndex` and `DateRangeInRangeIndex` instances by
`meta_type`, and register the corresponding translator utility with
the index name.
