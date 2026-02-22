<!-- diataxis: explanation -->

# Security Hardening

plone.pgcatalog constructs SQL queries from user-supplied search parameters. This
makes SQL injection prevention, denial-of-service limits, and access control
critical concerns. This page documents the security measures in place and the
reasoning behind each.

## SQL injection prevention

### Parameterized queries

All user-supplied values go through psycopg's parameterized query interface. The
query builder in `query.py` uses `%(name)s` placeholders exclusively -- values are
never string-formatted into SQL. psycopg handles escaping and type conversion at
the protocol level (PostgreSQL prepared statement parameters), which is immune to
SQL injection regardless of the value's content.

```python
# Correct: parameterized (used throughout plone.pgcatalog)
cur.execute(
    "SELECT zoid FROM object_state WHERE idx @> %(val)s::jsonb",
    {"val": Json({"portal_type": user_input})}
)

# Never done: string formatting
cur.execute(f"... WHERE idx->>'portal_type' = '{user_input}'")  # NEVER
```

### Safe DDL composition

DDL statements (CREATE INDEX, ALTER TABLE, CREATE FUNCTION) cannot use parameter
placeholders because PostgreSQL does not support parameterized DDL. plone.pgcatalog
uses psycopg's `sql` module for safe DDL composition:

- `psycopg.sql.Identifier()` for column names, index names, and table names.
- `psycopg.sql.Literal()` for string constants (e.g., tokenizer names in BM25 DDL).
- `psycopg.sql.SQL()` for static SQL fragments.

These composable types handle quoting correctly for all PostgreSQL identifier rules,
including names that contain special characters or are reserved words.

### Identifier validation

Index names from ZCatalog and language codes from configuration flow into SQL
expressions (e.g., `idx->>'portal_type'`). Even though these are not user-supplied
in the HTTP request sense, they originate from site configuration and could
theoretically be manipulated by a site administrator.

`validate_identifier(name)` in `columns.py` rejects any name that does not match
the pattern `^[a-zA-Z_][a-zA-Z0-9_]*$`. This is applied to:

- Index names during `IndexRegistry.register()` and `sync_from_catalog()`.
- Column names in DDL (BM25 column names, text index names).
- Language codes in BM25 backend DDL (validated against the `LANG_TOKENIZER_MAP`
  allowlist, then identifier-validated for the derived column/index/tokenizer names).

Any index name that fails validation is silently skipped during registry population,
with a warning logged.

## DoS prevention

Search queries can be influenced by web users via URL parameters (`b_start`,
`b_size`, `SearchableText`, `path`). Without limits, a malicious request could
trigger resource-exhausting queries.

| Limit | Value | Protects against |
|---|---|---|
| `_MAX_LIMIT` | 10,000 | Unbounded result sets. Even with `sort_limit` or `b_size` set by the user, the maximum number of rows returned is capped. |
| `_MAX_OFFSET` | 1,000,000 | Deep pagination. PostgreSQL must skip OFFSET rows before returning results; extremely large offsets cause full table scans. |
| `_MAX_SEARCH_LENGTH` | 1,000 chars | Full-text query explosion. `plainto_tsquery()` parses the input into a boolean expression; very long inputs produce expensive query plans. Search text is truncated before being passed to PostgreSQL. |
| `_MAX_RRULE_LENGTH` | 1,000 chars | RRULE parsing abuse. DateRecurringIndex queries use PL/pgSQL rrule expansion; extremely complex recurrence rules could consume significant CPU. |
| Max path list | 100 items | Large IN clauses. Path queries accept lists of paths; without a limit, a request could pass thousands of paths, producing an expensive `OR` expansion. |

These limits are enforced in `query.py` before SQL construction. They are deliberately
conservative -- legitimate Plone usage rarely approaches any of them.

Note that plone.pgcatalog does not implement application-level rate limiting on
search endpoints. For production deployments, a reverse proxy (nginx, HAProxy) should
rate-limit requests to `@@search` and `@@search-results` endpoints.

## Access control

### Management methods

Maintenance operations that modify catalog data or read diagnostic information are
protected with the `ManageZCatalogEntries` permission (accessible only to Managers
by default):

- `refreshCatalog()` -- rebuild catalog from content
- `reindexIndex()` -- re-apply a specific index
- `clearFindAndRebuild()` -- clear all catalog data and rebuild
- `manage_get_catalog_summary()` -- ZMI summary data
- `manage_get_catalog_objects()` -- ZMI object listing
- `manage_get_object_detail()` -- ZMI object detail
- `manage_get_indexes_and_metadata()` -- ZMI index listing

### Unrestricted search

`unrestrictedSearchResults()` bypasses security filtering (no `allowedRolesAndUsers`,
no `effectiveRange`). It is declared private via `ClassSecurityInfo`, making it
callable only from trusted Python code (not through-the-web, not from restricted
Python scripts, not from page templates).

### Security filters

Every call to `searchResults()` injects two security filters before the query
reaches `build_query()`:

- **`allowedRolesAndUsers`**: Plone's security index. The current user's roles and
  group memberships are passed as a keyword query, filtering results to objects the
  user is allowed to see. This is a KeywordIndex query using JSONB `?|` overlap,
  matching the same semantics as ZCatalog's `allowedRolesAndUsers` index.

- **`effectiveRange`**: Plone's publication date filtering. Content with a future
  `effective` date or a past `expires` date is excluded unless the user has the
  `AccessInactivePortalContent` permission. This is a DateRangeIndex query using
  the `pgcatalog_to_timestamptz()` expression index.

These filters are injected by `apply_security_filters()` in `query.py` and cannot
be bypassed through the public `searchResults()` API.

## IPGIndexTranslator security contract

Custom index types are supported via `IPGIndexTranslator` named utilities. These
utilities return raw SQL fragments that are inserted directly into WHERE clauses.
This is a deliberate design choice for flexibility, but it places security
responsibility on the translator implementation.

The security contract is documented in the `IPGIndexTranslator` interface:

- All user-supplied values MUST use `%(name)s` parameter placeholders with
  corresponding entries in the returned params dict.
- SQL fragments must never interpolate user input directly.
- Index and column identifiers in the fragment should be hardcoded constants or
  validated via `validate_identifier()`.

Translator authors who violate this contract introduce SQL injection
vulnerabilities. The interface docstring makes this explicit:

> The `query()` method returns a raw SQL fragment that is appended directly to the
> WHERE clause -- never interpolate user input into this fragment.

## Connection safety

### Closed connection checks

`release_request_connection()` checks `conn.closed` before returning a connection
to the pool. A connection that was closed by PostgreSQL (due to timeout, server
restart, or network failure) is not returned to the pool, preventing subsequent
queries from failing with a stale connection error.

### Autocommit mode

Pool connections operate in autocommit mode. This prevents long-running transactions
from holding locks or accumulating WAL overhead. Each query is its own transaction,
committed immediately.

The exception is the storage instance connection (used for read queries), which runs
inside a REPEATABLE READ transaction managed by zodb-pgjsonb. This transaction has a
well-defined lifecycle tied to the ZODB connection, and is cleaned up when the ZODB
connection is released.

### Advisory lock serialization

ZODB's `tpc_finish` uses PostgreSQL advisory locks to serialize transaction ID (TID)
generation. This prevents race conditions where two concurrent commits could generate
the same TID. The advisory lock is held only for the duration of TID assignment and
released immediately, minimizing contention.
