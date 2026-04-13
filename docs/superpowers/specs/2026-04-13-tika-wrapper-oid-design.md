# Tika Enqueue: Resolve Wrapper OIDs — Design Spec

**Issue:** https://github.com/bluedynamics/plone-pgcatalog/issues/115
**Date:** 2026-04-13

## Problem

With `PGCATALOG_TIKA_URL` configured, a `clearFindAndRebuild` over a
Plone 6 site with PDF/Office/image content produces **zero** rows in
`text_extraction_queue`. The cause: `_collect_ref_oids()` walks the
content object's JSON state one level deep, but Dexterity wraps blob
fields in `plone.namedfile.file.NamedBlobFile` /
`NamedBlobImage`. The `@ref` in the content state points at the
*wrapper*, not at the `ZODB.blob.Blob` sitting one persistent reference
deeper.

Reference chain for a Dexterity `File`:

```
Content File          (object_state)
   .file.@ref ─► NamedBlobFile   (object_state)   ← _collect_ref_oids
                  ._blob.@ref ─► ZODB.blob.Blob   (blob_state)        ← actual target
```

`_enqueue_tika_jobs()` queries `blob_state WHERE zoid = ANY(refs)` with
the wrapper OIDs, gets zero rows, and silently returns. This affects
every Dexterity File/Image — the dominant case in modern Plone.

## Goal

`_enqueue_tika_jobs()` must resolve wrapper OIDs to the actual blob
OIDs before querying `blob_state`, so Tika jobs are enqueued for
NamedBlobFile/Image content.

## Why SQL, not a ZODB object load

1. **Transaction context.** `_enqueue_tika_jobs` runs inside the active
   `tpc_vote` / `finalize`, using the commit cursor. A second
   `ZODB.DB.open()` would open a new PG connection with a new
   REPEATABLE READ snapshot that *cannot* see our own not-yet-committed
   `object_state` inserts. The cursor can.
2. **No re-entry into the current ZODB Connection.** The active
   Connection is mid-commit; re-entering it during commit is
   semantically invalid and a deadlock risk.
3. **Symmetry + speed.** The enqueue path already uses SQL to look up
   `blob_state`. One extra query per transaction beats N pickle
   decodes + persistent instantiations.
4. **No class-name hardcoding.** A generic second-hop lookup works for
   any wrapper type (NamedBlobFile, NamedBlobImage, future wrappers)
   without maintaining a class whitelist.

## Design

### Approach: one-level second hop

In `_enqueue_tika_jobs`, after the initial `blob_state` query returns
matches for a subset of refs, fetch the `state` of the *unresolved*
refs from `object_state`, run `_collect_ref_oids` over each wrapper
state, and re-query `blob_state` with the derived OIDs.

**One hop only** — no iteration. NamedBlobFile/Image embed the
`ZODB.blob.Blob` directly; a doubly nested wrapper is not a current
Plone pattern, and guessing at future ones is YAGNI. If it ever
matters, the loop structure is an obvious extension.

### Algorithm

```
all_refs = union of candidate.blob_refs
blob_rows = SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state WHERE zoid = ANY(all_refs)

unresolved = all_refs - blob_rows.keys
if unresolved:
    wrappers = SELECT DISTINCT ON (zoid) zoid, state FROM object_state
               WHERE zoid = ANY(unresolved)
    wrapper_to_inner = {}          # wrapper_oid -> list of inner_oids
    inner_refs = set()
    for wrapper_oid, state in wrappers:
        inner = _collect_ref_oids(state)
        wrapper_to_inner[wrapper_oid] = inner
        inner_refs.update(inner)
    if inner_refs:
        inner_blob_rows = SELECT DISTINCT ON (zoid) zoid, tid FROM blob_state
                          WHERE zoid = ANY(inner_refs)
        # Merge: candidate.ref → wrapper → inner blob → tid
        for candidate c:
            for ref_zoid in c.blob_refs:
                if ref_zoid in blob_rows:
                    enqueue(c.zoid, ref_zoid, blob_rows[ref_zoid])
                elif ref_zoid in wrapper_to_inner:
                    for inner_zoid in wrapper_to_inner[ref_zoid]:
                        if inner_zoid in inner_blob_rows:
                            enqueue(c.zoid, inner_zoid, inner_blob_rows[inner_zoid])
```

`DISTINCT ON (zoid) ... ORDER BY zoid, tid DESC` mirrors the existing
blob lookup — returns the most recent TID per OID, correct in both
history-free and history-preserving modes.

### What lands in the queue

The queue row stores `zoid` (content), `blob_zoid` (actual Blob OID),
and `tid`. When resolved via a wrapper, `blob_zoid` is the *inner*
Blob OID — which is exactly what the Tika worker needs to fetch from
`blob_state`. The wrapper OID is intentionally discarded; the queue
has no wrapper column and the worker has no need for it.

### Cost

- Fast path (flat-state content, Archetypes-style): unchanged — zero
  extra queries when the first `blob_state` lookup resolves everything.
- Wrapper path (Dexterity File/Image): +2 SQL round-trips per
  transaction (one `object_state` fetch, one extra `blob_state`
  fetch). Always batched: one query covers all unresolved refs across
  all candidates.

## Non-goals

- Recursion beyond one hop. YAGNI.
- Caching wrapper states across transactions. The unpickled JSON is
  already cheap; caching invites invalidation bugs.
- Detecting "this is a wrapper" by class name. The generic
  unresolved-ref check works for any structure.

## Testing

### Unit tests (no PG)

Extend `test_tika_enqueue.py`:

- **State shape fixture** that mimics a real Dexterity File: content
  state with `{"file": {"@ref": ["0000…wrapper", "...NamedBlobFile"]}}`,
  wrapper state with `{"_blob": {"@ref": ["0000…blob", "ZODB.blob.Blob"]}}`.
- Verify `_collect_ref_oids` on the content state returns only the
  wrapper OID (documents today's behavior — no regression).
- Verify `_collect_ref_oids` on the wrapper state returns the blob OID
  (already covered implicitly, but explicit is better).

### Integration tests (PG)

Extend `TestEnqueueLogic` in `test_tika_enqueue.py`:

- **`test_enqueue_resolves_wrapper_oid`**: seed `object_state` with a
  wrapper row whose state points to a `blob_state` entry. Run
  `_enqueue_tika_jobs` with a candidate whose `blob_refs` is the
  wrapper OID. Assert the queue row has `blob_zoid = inner_blob_oid`,
  not the wrapper OID.
- **`test_enqueue_mixed_flat_and_wrapper`**: one candidate with a
  direct blob ref (flat state), another with a wrapper ref. Both
  should enqueue against the correct inner OIDs.
- **`test_enqueue_wrapper_with_missing_inner_blob`**: wrapper exists
  but the inner blob is absent from `blob_state`. No row enqueued, no
  error.
- **`test_enqueue_ignores_unresolvable_refs`**: ref OID has neither a
  `blob_state` nor an `object_state` entry. Silent no-op, no error.

### Regression coverage

Existing `test_enqueue_blob_with_tika_url` stays green — its candidate
directly references a blob that exists in `blob_state`, so the first
query resolves it and the wrapper hop is skipped.

## Rollout

- Single file change: `src/plone/pgcatalog/processor.py`
- No schema change, no migration
- Backwards compatible: flat-state fast path is untouched
