<!-- diataxis: reference -->

# SQL Functions Reference

This page documents the PostgreSQL functions installed by plone.pgcatalog.
All functions are created automatically at startup as part of schema
initialization.

## pgcatalog_to_timestamptz

```sql
pgcatalog_to_timestamptz(text) -> timestamptz
```

An `IMMUTABLE` function that casts a text value to `timestamptz`.
Used in expression indexes for date sorting and filtering on ISO 8601
strings stored in the `idx` JSONB column.

- Handles ISO 8601 strings with timezone offsets.
- Returns `NULL` for `NULL` input.
- Marked `IMMUTABLE` to allow use in index expressions.

## pgcatalog_lang_to_regconfig

```sql
pgcatalog_lang_to_regconfig(text) -> text
```

Maps ISO 639-1 language codes to PostgreSQL text search configuration
names. Used in the `searchable_text` tsvector generation to select a
per-object language configuration.

Returns `'simple'` for unrecognized codes or `NULL` input.

### Language Mapping Table

| ISO Code | PG Regconfig |
|---|---|
| `ar` | `arabic` |
| `hy` | `armenian` |
| `eu` | `basque` |
| `ca` | `catalan` |
| `da` | `danish` |
| `nl` | `dutch` |
| `en` | `english` |
| `et` | `estonian` |
| `fi` | `finnish` |
| `fr` | `french` |
| `de` | `german` |
| `el` | `greek` |
| `hi` | `hindi` |
| `hu` | `hungarian` |
| `id` | `indonesian` |
| `ga` | `irish` |
| `it` | `italian` |
| `lt` | `lithuanian` |
| `ne` | `nepali` |
| `nb` | `norwegian` |
| `pt` | `portuguese` |
| `ro` | `romanian` |
| `ru` | `russian` |
| `sr` | `serbian` |
| `es` | `spanish` |
| `sv` | `swedish` |
| `ta` | `tamil` |
| `tr` | `turkish` |
| `yi` | `yiddish` |
| (other / `NULL`) | `simple` |

## rrule Functions

The `rrule` schema contains a pure PL/pgSQL implementation of RFC 5545
RRULE expansion. These functions are installed automatically at startup
and do not require any C extensions.

The schema is created idempotently using `CREATE SCHEMA IF NOT EXISTS`
with exception handling for type definitions.

### rrule."between"

```sql
rrule."between"(
    rrule text,
    dtstart timestamptz,
    range_start timestamptz,
    range_end timestamptz
) -> SETOF timestamptz
```

Returns all occurrences of the recurrence rule between `range_start` and
`range_end` (inclusive). The recurrence is computed from `dtstart` using
the RRULE string.

Used by `DateRecurringIndex` and `DateRangeInRangeIndex` translators for
range queries (the `min:max` query pattern).

### rrule."after"

```sql
rrule."after"(
    rrule text,
    dtstart timestamptz,
    after timestamptz,
    count integer
) -> SETOF timestamptz
```

Returns up to `count` occurrences of the recurrence rule that fall after
the given timestamp. The recurrence is computed from `dtstart` using the
RRULE string.

Used by `DateRecurringIndex` for `range="min"` queries (finding future
occurrences).
