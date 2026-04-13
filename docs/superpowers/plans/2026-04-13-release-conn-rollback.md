# `release_request_connection` Rollback — Implementation Plan (PR2)

**Goal:** Add an explicit `conn.rollback()` before `pool.putconn(conn)` in `release_request_connection()` to close any open implicit transaction on the pool fallback path.

**Architecture:** 5-line change in `pool.py` plus 3 unit tests.

**Tech Stack:** Python 3.12+, psycopg 3 (mock only), pytest

**Spec:** `docs/superpowers/specs/2026-04-13-release-conn-rollback-design.md`

---

## File Map

- Modify: `src/plone/pgcatalog/pool.py` — `release_request_connection()`
- Modify: `tests/test_config.py` — `TestRequestConnection` class
- Modify: `CHANGES.md` — Unreleased entry

---

### Task 1: Failing tests for rollback behavior

**Files:**
- Modify: `tests/test_config.py` — append to `TestRequestConnection`

- [ ] **Step 1: Add three new tests at the end of `TestRequestConnection`**

Locate the last method in `TestRequestConnection` (`test_release_swallows_putconn_exception` at ~line 215).  Insert these methods *after* it but *before* the `_clean_pending` helper at line 230:

```python
    def test_release_rolls_back_before_putconn(self):
        """release_request_connection rolls back any open txn before pooling."""
        pool = mock.Mock()
        conn = mock.Mock()
        conn.closed = False
        pool.getconn.return_value = conn

        # Track call order
        call_order = []
        conn.rollback.side_effect = lambda: call_order.append("rollback")
        pool.putconn.side_effect = lambda c: call_order.append("putconn")

        get_request_connection(pool)
        release_request_connection()

        conn.rollback.assert_called_once()
        pool.putconn.assert_called_once_with(conn)
        # rollback must precede putconn
        assert call_order == ["rollback", "putconn"]

    def test_release_swallows_rollback_exception(self):
        """If rollback fails (e.g. dead conn), putconn still runs and no exception escapes."""
        pool = mock.Mock()
        conn = mock.Mock()
        conn.closed = False
        conn.rollback.side_effect = RuntimeError("connection dead")
        pool.getconn.return_value = conn

        get_request_connection(pool)
        # Must not raise
        release_request_connection()

        # putconn still called despite rollback failure
        pool.putconn.assert_called_once_with(conn)
        # Thread-local cleared
        assert getattr(pending_mod._local, "pgcat_conn", None) is None

    def test_release_skips_rollback_for_closed_conn(self):
        """A closed conn is not rolled back nor returned to pool."""
        pool = mock.Mock()
        conn = mock.Mock()
        conn.closed = True
        pool.getconn.return_value = conn

        # First obtain it (the closed-conn check happens later in this scenario);
        # set thread-local manually to avoid get_request_connection's closed check
        # opening a different conn.
        from plone.pgcatalog import pending as pending_mod_local

        pending_mod_local._local.pgcat_conn = conn
        pending_mod_local._local.pgcat_pool = pool

        release_request_connection()

        conn.rollback.assert_not_called()
        pool.putconn.assert_not_called()
        # Thread-local still cleared
        assert getattr(pending_mod._local, "pgcat_conn", None) is None
```

- [ ] **Step 2: Verify the first two fail (third should already pass)**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_config.py::TestRequestConnection -v -k "rolls_back_before_putconn or swallows_rollback_exception or skips_rollback_for_closed"`

Expected: 2 FAIL (`rollback.assert_called_once` — current code never calls rollback), 1 PASS (closed-conn skip already works).

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_config.py
git commit -m "test: failing regression for explicit rollback before pool return (#118)"
```

---

### Task 2: Implement rollback

**Files:**
- Modify: `src/plone/pgcatalog/pool.py` — `release_request_connection()` (~line 74)

- [ ] **Step 1: Add `contextlib` import at the top of `pool.py`**

Find the imports block (search for `import logging`).  Add `import contextlib` in the existing import block, alphabetically positioned (likely above `import logging` and `import os`):

```python
import contextlib
import logging
import os
```

- [ ] **Step 2: Update `release_request_connection`**

Replace the function body.  Locate:

```python
def release_request_connection(event=None):
    """Return the request-scoped connection to the pool.

    Called by the IPubEnd subscriber at the end of each Zope request.
    Safe to call when no request-scoped connection is active (no-op).
    """
    conn = getattr(_local, "pgcat_conn", None)
    pool = getattr(_local, "pgcat_pool", None)
    if conn is not None and pool is not None:
        try:
            if not conn.closed:
                pool.putconn(conn)
        except Exception:
            log.warning("Failed to return connection to pool", exc_info=True)
    _local.pgcat_conn = None
    _local.pgcat_pool = None
```

Replace with:

```python
def release_request_connection(event=None):
    """Return the request-scoped connection to the pool.

    Called by the IPubEnd subscriber at the end of each Zope request.
    Safe to call when no request-scoped connection is active (no-op).

    Issues an explicit ``conn.rollback()`` before pooling so the
    connection comes back idle (not 'idle in transaction').  This
    prevents the pool fallback path from leaving virtualxid locks
    that block ``CREATE INDEX CONCURRENTLY`` (#118).  ``rollback()``
    on a conn with no open transaction is a cheap no-op.
    """
    conn = getattr(_local, "pgcat_conn", None)
    pool = getattr(_local, "pgcat_pool", None)
    if conn is not None and pool is not None:
        try:
            if not conn.closed:
                # Close any implicit txn opened by prior SELECTs.  Suppress
                # exceptions — a dead conn shouldn't break pool return.
                with contextlib.suppress(Exception):
                    conn.rollback()
                pool.putconn(conn)
        except Exception:
            log.warning("Failed to return connection to pool", exc_info=True)
    _local.pgcat_conn = None
    _local.pgcat_pool = None
```

- [ ] **Step 3: Run the new tests**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_config.py::TestRequestConnection -v`

Expected: all (~10) tests pass.

- [ ] **Step 4: Run the full suite**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_config.py tests/test_suggestions.py tests/test_tika_enqueue.py -q`

Expected: all pass; no regressions in nearby modules.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/pool.py
git commit -m "fix(pool): rollback before pool return to release virtualxid (#118)"
```

---

### Task 3: Changelog + spec/plan + push + PR

**Files:**
- Modify: `CHANGES.md` (append to Unreleased section)

- [ ] **Step 1: Update CHANGES.md**

Read `CHANGES.md` to find the `## Unreleased` section.  Add an entry under `### Fixed`:

```markdown
- ``release_request_connection`` now issues an explicit
  ``conn.rollback()`` before returning the connection to the pool.
  Otherwise an implicit transaction opened by a prior ``SELECT`` on
  the pool fallback path stays alive, holding a ``virtualxid`` that
  blocks ``CREATE INDEX CONCURRENTLY``.  Companion fix to
  bluedynamics/zodb-pgjsonb#58 (the storage-conn path).  Closes #118.
```

- [ ] **Step 2: Commit changelog + spec + plan**

```bash
git add CHANGES.md docs/superpowers/
git commit -m "docs: changelog + spec + plan for #118 pool rollback fix"
```

- [ ] **Step 3: Push**

```bash
git push -u origin fix/release-conn-rollback
```

- [ ] **Step 4: Create PR**

```bash
gh pr create --repo bluedynamics/plone-pgcatalog --base main \
  --head fix/release-conn-rollback \
  --title "Pool: rollback before putconn to release virtualxid (#118)" \
  --body "$(cat <<'EOF'
## Summary

Companion fix to bluedynamics/zodb-pgjsonb#58 — closes the secondary leak in the **pool fallback path**.

\`release_request_connection()\` previously returned conns to the pool without an explicit \`commit/rollback\`.  Pool conns aren't autocommit by default, so any prior \`SELECT\` left an implicit transaction open, holding a \`virtualxid\` that blocks \`CREATE INDEX CONCURRENTLY\`.

Fix: \`with contextlib.suppress(Exception): conn.rollback()\` before \`pool.putconn(conn)\`.  Cheap (no-op when no open txn), safe (suppresses errors from dead conns), idempotent.

Closes #118.

## Tests

Three new unit tests in \`TestRequestConnection\`:
- rollback called before putconn (call-order assertion)
- rollback exception swallowed; putconn still runs; thread-local cleared
- closed conn: neither rollback nor putconn called

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Report PR URL.
