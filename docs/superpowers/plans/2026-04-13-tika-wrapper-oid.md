# Tika Enqueue: Resolve Wrapper OIDs — Implementation Plan

**Goal:** Fix `_enqueue_tika_jobs()` to resolve Dexterity NamedBlobFile/Image wrapper OIDs via a one-level second hop through `object_state`, so Tika jobs are enqueued for the dominant modern-Plone content case.

**Architecture:** In `_enqueue_tika_jobs()`, after the first `blob_state` lookup identifies refs without blob rows, fetch those wrapper OIDs' states from `object_state`, re-run `_collect_ref_oids()` over each wrapper state, and re-query `blob_state` with the derived inner OIDs. The queue row's `blob_zoid` becomes the inner Blob OID.

**Tech Stack:** Python 3.12+, psycopg 3, pytest

**Spec:** `docs/superpowers/specs/2026-04-13-tika-wrapper-oid-design.md`

---

## File Map

- Modify: `src/plone/pgcatalog/processor.py` — `_enqueue_tika_jobs()`
- Modify: `tests/test_tika_enqueue.py` — add regression + wrapper cases
- Modify: `CHANGES.md` — changelog entry

---

### Task 1: Write failing integration test for wrapper resolution

**Files:**
- Modify: `tests/test_tika_enqueue.py` (add method to `TestEnqueueLogic`)

- [ ] **Step 1: Add the test at the end of `TestEnqueueLogic`**

Locate the last method of `TestEnqueueLogic` (`test_process_skips_candidate_without_refs` at ~line 300). Insert this new method **right after** `test_idempotent_enqueue` (~line 271) and before `test_process_accumulates_candidates_with_blob_refs`:

```python
    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_enqueue_resolves_wrapper_oid(self, pg_conn_with_queue):
        """Dexterity NamedBlobFile wrapper: one-level hop to inner Blob.

        Content state @ref points at wrapper (object_state), wrapper
        state @ref points at Blob (blob_state).  Queue must store the
        inner Blob OID, not the wrapper.
        """
        conn = pg_conn_with_queue
        content_zoid, wrapper_zoid, blob_zoid, tid = 700, 701, 702, 1

        # Content object has no direct blob entry; its ref points at the
        # NamedBlobFile wrapper.
        insert_object(conn, content_zoid, tid)
        # The wrapper is a persistent object whose state carries the
        # real Blob @ref.
        wrapper_state = json.dumps(
            {
                "_blob": {
                    "@ref": [f"{blob_zoid:016x}", "ZODB.blob.Blob"],
                },
                "filename": "sample.pdf",
                "contentType": "application/pdf",
            }
        )
        insert_object(
            conn,
            wrapper_zoid,
            tid,
            class_mod="plone.namedfile.file",
            class_name="NamedBlobFile",
            state=wrapper_state,
        )
        self._insert_blob(conn, blob_zoid, tid)

        proc = CatalogStateProcessor()
        proc._tika_candidates = [
            {
                "zoid": content_zoid,
                "content_type": "application/pdf",
                "blob_refs": [wrapper_zoid],  # wrapper only, no direct blob
            }
        ]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        rows = self._get_queue(conn)
        assert len(rows) == 1
        assert rows[0]["zoid"] == content_zoid
        assert rows[0]["blob_zoid"] == blob_zoid  # inner, NOT wrapper_zoid
        assert rows[0]["tid"] == tid
        assert rows[0]["content_type"] == "application/pdf"
```

Also at the top of the file, add the `insert_object` import to the existing conftest import block:

```python
from tests.conftest import insert_object
```

(Already imported at line 10 — verify it's there; if not, add it.)

- [ ] **Step 2: Run test to verify it fails**

Run: `PGCATALOG_TIKA_URL=http://tika:9998 PYTHONPATH=src .venv/bin/pytest tests/test_tika_enqueue.py::TestEnqueueLogic::test_enqueue_resolves_wrapper_oid -v`

Expected: FAIL — assertion on `len(rows) == 1` fails (0 rows) because current code doesn't resolve the wrapper hop.

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/test_tika_enqueue.py
git commit -m "test: failing regression for NamedBlobFile wrapper OID resolution (#115)"
```

---

### Task 2: Implement the wrapper resolution in `_enqueue_tika_jobs`

**Files:**
- Modify: `src/plone/pgcatalog/processor.py:327-373` (the `_enqueue_tika_jobs` method)

- [ ] **Step 1: Replace `_enqueue_tika_jobs` body**

Replace the entire existing method with:

```python
    def _enqueue_tika_jobs(self, cursor):
        """Enqueue text extraction jobs for blobs committed in this txn.

        Content objects (File/Image) reference blobs either directly
        (legacy/Archetypes: content state carries a ``ZODB.blob.Blob``
        ``@ref``) or via a wrapper (Dexterity NamedBlobFile/Image:
        content state ``@ref`` points at the wrapper, whose own state
        carries the ``ZODB.blob.Blob`` ``@ref``).

        Resolution is two-step:
        1. Look up all candidate refs in ``blob_state``.
        2. For refs with no blob row, fetch their ``object_state`` row
           (the wrapper), extract inner ``@ref`` OIDs, look those up in
           ``blob_state``.

        The queue row stores the *inner* Blob OID as ``blob_zoid`` —
        the worker fetches blob data from ``blob_state`` using it.
        """
        candidates = self._tika_candidates
        self._tika_candidates = []

        # Collect all referenced oids across all candidates
        all_refs = set()
        for c in candidates:
            all_refs.update(c["blob_refs"])
        if not all_refs:
            return

        # Step 1: direct lookup in blob_state
        cursor.execute(
            "SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state "
            "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
            {"zoids": list(all_refs)},
        )
        blob_rows = {row[0]: row[1] for row in cursor.fetchall()}

        # Step 2: resolve wrapper refs via object_state second hop
        # (Dexterity NamedBlobFile/Image: content -> wrapper -> blob)
        wrapper_to_inner = {}  # wrapper_oid -> list[inner_oid]
        unresolved = all_refs - set(blob_rows)
        if unresolved:
            cursor.execute(
                "SELECT DISTINCT ON (zoid) zoid, state FROM object_state "
                "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
                {"zoids": list(unresolved)},
            )
            inner_refs = set()
            for row in cursor.fetchall():
                wrapper_oid, wrapper_state = row[0], row[1]
                inner = _collect_ref_oids(wrapper_state)
                if inner:
                    wrapper_to_inner[wrapper_oid] = inner
                    inner_refs.update(inner)

            if inner_refs:
                cursor.execute(
                    "SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state "
                    "WHERE zoid = ANY(%(zoids)s) ORDER BY zoid, tid DESC",
                    {"zoids": list(inner_refs)},
                )
                for row in cursor.fetchall():
                    blob_rows[row[0]] = row[1]

        if not blob_rows:
            return

        for c in candidates:
            content_zoid = c["zoid"]
            content_type = c.get("content_type")
            for ref_zoid in c["blob_refs"]:
                if ref_zoid in blob_rows:
                    # Direct hit: content ref is a Blob OID
                    self._insert_queue_row(
                        cursor, content_zoid, ref_zoid,
                        blob_rows[ref_zoid], content_type,
                    )
                elif ref_zoid in wrapper_to_inner:
                    # Wrapper hit: content ref is a NamedBlob* wrapper;
                    # enqueue for each resolvable inner Blob OID.
                    for inner_zoid in wrapper_to_inner[ref_zoid]:
                        if inner_zoid in blob_rows:
                            self._insert_queue_row(
                                cursor, content_zoid, inner_zoid,
                                blob_rows[inner_zoid], content_type,
                            )

    def _insert_queue_row(self, cursor, zoid, blob_zoid, tid, content_type):
        cursor.execute(
            "INSERT INTO text_extraction_queue "
            "  (zoid, blob_zoid, tid, content_type) "
            "VALUES (%(zoid)s, %(blob_zoid)s, %(tid)s, %(ct)s) "
            "ON CONFLICT (blob_zoid, tid) DO NOTHING",
            {
                "zoid": zoid,
                "blob_zoid": blob_zoid,
                "tid": tid,
                "ct": content_type,
            },
        )
```

Note: the INSERT was extracted to a helper to avoid duplication between the two enqueue branches. Same SQL as before, unchanged semantics.

- [ ] **Step 2: Run the previously-failing test**

Run: `PGCATALOG_TIKA_URL=http://tika:9998 PYTHONPATH=src .venv/bin/pytest tests/test_tika_enqueue.py::TestEnqueueLogic::test_enqueue_resolves_wrapper_oid -v`

Expected: PASS.

- [ ] **Step 3: Run the entire tika enqueue test module**

Run: `PGCATALOG_TIKA_URL=http://tika:9998 PYTHONPATH=src .venv/bin/pytest tests/test_tika_enqueue.py -v`

Expected: all previously-passing tests still pass (no regressions); new test passes.

- [ ] **Step 4: Commit**

```bash
git add src/plone/pgcatalog/processor.py
git commit -m "fix(tika): resolve NamedBlobFile/Image wrapper OIDs via object_state hop (#115)"
```

---

### Task 3: Add edge-case integration tests

**Files:**
- Modify: `tests/test_tika_enqueue.py` (add to `TestEnqueueLogic`)

- [ ] **Step 1: Add three more tests**

Append after `test_enqueue_resolves_wrapper_oid`:

```python
    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_enqueue_mixed_flat_and_wrapper(self, pg_conn_with_queue):
        """Mix: one candidate has direct blob ref, another has wrapper ref.

        Both should enqueue against the correct inner OID.
        """
        conn = pg_conn_with_queue

        # Flat: content 800 -> blob 801 (direct)
        flat_content, flat_blob, tid = 800, 801, 1
        insert_object(conn, flat_content, tid)
        self._insert_blob(conn, flat_blob, tid)

        # Wrapped: content 810 -> wrapper 811 -> blob 812
        wrap_content, wrap_oid, wrap_blob = 810, 811, 812
        insert_object(conn, wrap_content, tid)
        insert_object(
            conn,
            wrap_oid,
            tid,
            class_mod="plone.namedfile.file",
            class_name="NamedBlobImage",
            state=json.dumps(
                {"_blob": {"@ref": [f"{wrap_blob:016x}", "ZODB.blob.Blob"]}}
            ),
        )
        self._insert_blob(conn, wrap_blob, tid)

        proc = CatalogStateProcessor()
        proc._tika_candidates = [
            {
                "zoid": flat_content,
                "content_type": "application/pdf",
                "blob_refs": [flat_blob],
            },
            {
                "zoid": wrap_content,
                "content_type": "image/png",
                "blob_refs": [wrap_oid],
            },
        ]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        rows = {r["zoid"]: r for r in self._get_queue(conn)}
        assert rows[flat_content]["blob_zoid"] == flat_blob
        assert rows[wrap_content]["blob_zoid"] == wrap_blob
        assert rows[wrap_content]["content_type"] == "image/png"

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_enqueue_wrapper_with_missing_inner_blob(self, pg_conn_with_queue):
        """Wrapper exists in object_state but inner blob is missing."""
        conn = pg_conn_with_queue
        content_zoid, wrapper_zoid, tid = 820, 821, 1
        missing_blob = 99999

        insert_object(conn, content_zoid, tid)
        insert_object(
            conn,
            wrapper_zoid,
            tid,
            class_mod="plone.namedfile.file",
            class_name="NamedBlobFile",
            state=json.dumps(
                {"_blob": {"@ref": [f"{missing_blob:016x}", "ZODB.blob.Blob"]}}
            ),
        )
        # no blob_state row for missing_blob

        proc = CatalogStateProcessor()
        proc._tika_candidates = [
            {
                "zoid": content_zoid,
                "content_type": "application/pdf",
                "blob_refs": [wrapper_zoid],
            }
        ]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        assert self._get_queue(conn) == []

    @pytest.mark.skipif(not TIKA_URL, reason="PGCATALOG_TIKA_URL not set")
    def test_enqueue_ignores_unresolvable_refs(self, pg_conn_with_queue):
        """Ref with neither blob_state nor object_state entry is a noop."""
        conn = pg_conn_with_queue
        content_zoid, tid = 830, 1
        ghost_ref = 0xDEADBEEF

        insert_object(conn, content_zoid, tid)

        proc = CatalogStateProcessor()
        proc._tika_candidates = [
            {
                "zoid": content_zoid,
                "content_type": "application/pdf",
                "blob_refs": [ghost_ref],
            }
        ]

        with conn.cursor(row_factory=tuple_row) as cur:
            proc._enqueue_tika_jobs(cur)
        conn.commit()

        assert self._get_queue(conn) == []
```

- [ ] **Step 2: Run the new tests**

Run: `PGCATALOG_TIKA_URL=http://tika:9998 PYTHONPATH=src .venv/bin/pytest tests/test_tika_enqueue.py::TestEnqueueLogic -v`

Expected: all `TestEnqueueLogic` tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_tika_enqueue.py
git commit -m "test: edge cases for wrapper resolution (mixed, missing inner, unresolvable)"
```

---

### Task 4: Full suite regression check + changelog

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Run the full test suite**

Run: `PGCATALOG_TIKA_URL=http://tika:9998 PYTHONPATH=src .venv/bin/pytest -q`

Expected: all tests pass.

- [ ] **Step 2: Run again without TIKA_URL to verify skipped tests still skip, unaffected tests still pass**

Run: `PYTHONPATH=src .venv/bin/pytest -q`

Expected: Tika-gated tests skipped; all other tests pass.

- [ ] **Step 3: Read and update CHANGES.md**

Open `CHANGES.md`, identify the current unreleased/dev section (match existing format). Add an entry:

```markdown
- Tika enqueue: resolve Dexterity NamedBlobFile/NamedBlobImage wrapper
  OIDs via a second-hop lookup through `object_state`, so the queue
  receives jobs for modern Dexterity File/Image content. Previously
  the enqueue path assumed a flat state with a direct
  `ZODB.blob.Blob` `@ref`, which only held for Archetypes-style
  content. Closes #115.
```

- [ ] **Step 4: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog entry for wrapper OID resolution fix (#115)"
```

---

### Task 5: Push branch and open PR

- [ ] **Step 1: Push the branch**

Run: `git push -u origin fix/tika-wrapper-oid`

- [ ] **Step 2: Create the PR**

Run (adjusting the body if additional context is discovered during implementation):

```bash
gh pr create --repo bluedynamics/plone-pgcatalog --base main --head fix/tika-wrapper-oid \
  --title "Tika enqueue: resolve NamedBlobFile/Image wrapper OIDs (#115)" \
  --body "$(cat <<'EOF'
## Summary

- `_enqueue_tika_jobs()` now resolves Dexterity `NamedBlobFile` /
  `NamedBlobImage` wrapper OIDs via a one-hop lookup through
  `object_state` before querying `blob_state`. Wrappers are the
  standard Dexterity pattern, so previously the Tika queue stayed
  empty for PDF/Office/image File/Image content.
- Queue row's `blob_zoid` is the inner `ZODB.blob.Blob` OID
  (matches what the worker needs).
- Flat-state fast path unchanged: when the first `blob_state` lookup
  resolves all refs, no second hop is performed.
- 4 new integration tests: wrapper-resolve, mixed flat+wrapper,
  wrapper with missing inner blob, unresolvable ref.

Closes #115

## Test plan

- [x] Failing regression test (`test_enqueue_resolves_wrapper_oid`)
  added first, implementation flipped it green
- [x] Edge cases: mixed, missing inner blob, unresolvable ref
- [x] Full suite still passes (with and without `PGCATALOG_TIKA_URL`)
- [x] No schema change, no migration

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Report the PR URL.
