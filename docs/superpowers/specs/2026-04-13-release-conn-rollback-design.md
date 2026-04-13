# `release_request_connection`: rollback before pool return — Design Spec

**Issue:** https://github.com/bluedynamics/plone-pgcatalog/issues/118
**Date:** 2026-04-13
**Scope:** plone-pgcatalog (PR2 of 2 — companion to bluedynamics/zodb-pgjsonb PR #58)

## Problem

`release_request_connection()` calls `pool.putconn(conn)` without an
explicit `commit/rollback`.  For the pool fallback path (no ZODB
storage, e.g. tests, scripts, future non-zodb-pgjsonb consumers), pool
conns are not autocommit by default, and any prior `SELECT` leaves an
implicit transaction open until the next user reuses the conn.  This
keeps a `virtualxid` alive — the same root cause as #118 on the ZODB
side.

The leak is small in practice:
- pool conns rotate frequently — usually under load they get reused
  quickly and the next user opens a fresh implicit txn anyway,
- `psycopg_pool` reclaims idle conns after `max_idle` (default 10 min),

…but it's a real leak and trivial to close.

## Goal

Every connection returned to the pool by
`release_request_connection()` is in a clean state — no open
transaction.

## Design

Wrap an explicit `conn.rollback()` call before `pool.putconn(conn)`.
Use `contextlib.suppress(Exception)` so a connection in an unrecoverable
state (already closed, killed externally) doesn't break pool return.

```python
def release_request_connection(event=None):
    conn = getattr(_local, "pgcat_conn", None)
    pool = getattr(_local, "pgcat_pool", None)
    if conn is not None and pool is not None:
        try:
            if not conn.closed:
                with contextlib.suppress(Exception):
                    conn.rollback()
                pool.putconn(conn)
        except Exception:
            log.warning("Failed to return connection to pool", exc_info=True)
    _local.pgcat_conn = None
    _local.pgcat_pool = None
```

**Properties:**
- Cheap: `rollback()` on a conn with no open txn is a no-op
  round-trip (psycopg sends a `ROLLBACK` only if there's a txn).
- Safe on autocommit conns: psycopg's `rollback()` is a no-op when
  `autocommit=True`.
- Defense in depth: even if zodb-pgjsonb's PR #58 isn't yet
  installed, this prevents pgcatalog's own pool-fallback path from
  leaking.

## Tests

Three new unit tests in the existing `TestRequestConnection` class
(`tests/test_config.py`):

1. **`test_release_rolls_back_before_putconn`**: mock conn, call
   `release_request_connection()`, verify `conn.rollback()` was called
   exactly once **before** `pool.putconn(conn)`.
2. **`test_release_swallows_rollback_exception`**: `conn.rollback()`
   raises → `pool.putconn(conn)` still called, no exception
   propagates, thread-local cleared.
3. **`test_release_skips_rollback_for_closed_conn`**: `conn.closed =
   True` → neither `rollback()` nor `putconn()` called.

## Non-goals

- Changing `get_request_connection()` — connection acquisition is
  fine; it's the return path that needs the cleanup.
- Touching the storage-conn path — that's PR #58 in zodb-pgjsonb.
- Changing `_install_orjson_loader` or other module init.

## Rollout

- Single file change: `src/plone/pgcatalog/pool.py`.
- Test additions: `tests/test_config.py`.
- No schema, no API change.
- Independent of PR #58 — can land in any order.
