# Detached-object reindex guard (#290) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A delete can no longer leave catalog rows for removed objects behind, and existing databases get the rows from past deletes cleaned up once.

**Architecture:** `PlonePGCatalogTool` overrides CMFCore's queue-aware `indexObject`, `reindexObject` and `unindexObject` and thereby lost CMFCore's `filterTemporaryItems` guard, which drops objects that are detached, request-parented or no longer contained in their parent. The fix mirrors that guard in the three methods, adds a second line of defence in `_set_pg_annotation` against non-rooted paths, and ships a one-shot repair as upgrade step 6 → 7 for rows that already exist.

**Tech Stack:** Python 3.12+, Plone 6.2, `Products.CMFCore.indexing.filterTemporaryItems`, psycopg, pytest with `zope.pytestlayer`, GenericSetup upgrade steps.

**Spec:** https://github.com/bluedynamics/plone-pgcatalog/issues/290 and the verified analysis in https://github.com/bluedynamics/plone-pgcatalog/issues/290#issuecomment-6923298855. Decisions taken after that comment: the defence-in-depth check in `_set_pg_annotation` is in; the verification pass for rooted stale rows is out of scope (one-off script against production later, follow-up issue only if it finds something).

## Global Constraints

- Work only in the worktree `sources/plone-pgcatalog-wt/fix-290-detached-reindex` (branch `fix-290-detached-reindex`, based on main `d38a145`). Never touch `sources/plone-pgcatalog/`.
- The shared `.venv` of the main checkout imports `plone.pgcatalog` from the main checkout, not from the worktree. Build a venv in the worktree first: `uv venv --python 3.13 .venv && uv pip install -e ".[test]"`.
- PostgreSQL for tests: container `zodb-pgjsonb-dev` must be up (`docker --context default start zodb-pgjsonb-dev`, port 5433). Run tests as `env -u ZODB_TEST_DSN .venv/bin/python -m pytest <target> -q -p no:cacheprovider`. Never run two pytest processes at the same time (shared `zodb_test` database). A full run exceeds the 10 minute foreground limit: run it in the background and wait.
- All imports at module top (`feedback_imports_at_top`), except where an existing inline import exists for a cycle.
- Code style: ruff 0.16.7 format and check run as pre-commit hooks; a commit that fails them is rejected. Fix and recommit.
- Every commit message ends with a blank line and `Assisted-by: Claude Sonnet 5.5` (the model that implements). No `Co-Authored-By`, no e-mail address.
- Docs under `docs/sources/` use one sentence per line and Plone documentation style; Vale runs on them in pre-commit.
- The CHANGES entry is written in Task 1 and only verified later; one entry for the whole fix.

## Review Focus

1. Renaming or moving content in one transaction unindexes and re-indexes the same zoid. The re-indexed object is wrapped and contained in its new parent, so the filter must let it through. Pinned by the existing rename and move tests in `tests/test_pg_integration.py` and `tests/test_move_integration.py`, which must stay green (Task 4 full run).
2. `reindexObject(obj, idxs=[...])` on a detached object must be dropped before `_partial_reindex` runs, otherwise a partial patch resurrects nothing but still joins the transaction. Pinned by `test_reindexObject_partial_skips_detached` (Task 1).
3. `unindexObject` for an object that is already gone from its parent (late unindex) must still uncatalog; CMFCore passes `checkId=False` for exactly this. Pinned by `test_unindexObject_not_in_parent_still_uncatalogs` (Task 1).
4. Objects without `getId` (non-CMF content cataloged by add-ons) skip the containment check and are indexed when they have a parent, as in CMFCore. Pinned by `test_indexObject_without_getId_is_indexed` (Task 1).
5. The repair must invalidate the query cache, or a cached UID query keeps returning the removed brain until the next catalog write. Pinned by the change-sequence assertion in `test_removes_only_non_rooted_rows` (Task 3).

---

### Task 1: Mirror CMFCore's `filterTemporaryItems` in the three public index methods

**Files:**
- Modify: `src/plone/pgcatalog/catalog.py` (imports; `indexObject` at line 601, `unindexObject` at line 621, `reindexObject` at line 633 on `d38a145`)
- Modify: `tests/conftest.py` (fake content helpers)
- Modify: `tests/test_catalog_plone.py:588-600` (`TestIndexObjectReindexObject`)
- Modify: `tests/test_clean_break.py:891-897` (`test_public_unindex_object`) and `tests/test_clean_break.py:1049-1061` (`test_reindex_object_uses_partial`)
- Modify: `tests/test_pg_integration.py` (class `TestUncatalogDeleteFlow(_PGLayerTest)` at line 787; imports)
- Delete: `tests/test_repro_290_pg.py` (throwaway reproduction, uncommitted, left in the worktree by the analysis)
- Modify: `CHANGES.md`

**Interfaces:**
- Produces in `tests/conftest.py`:
  - `fake_content(id="doc", *, contained=True, request_parent=False, with_get_id=True)` → an `Acquisition.Implicit` object wrapped in a fake container whose `getPhysicalPath()` is `("", "plone", id)`. `contained=False` makes the container's `__contains__` deny the id. `request_parent=True` makes the container provide `zope.publisher.interfaces.browser.IBrowserRequest`. `with_get_id=False` omits the `getId` method.
  - `detached_content(id="doc")` → the same content class, bare (no wrapper), so `getPhysicalPath()` is `(id,)` and `aq_parent` is `None`.
  - The content class must not have an `isTemporary` attribute.

- [ ] **Step 1: Write the PG regression test**

Add to `TestUncatalogDeleteFlow` in `tests/test_pg_integration.py` (imports at module top: `RelationValue` from `z3c.relationfield.relation`, `ICatalog` from `zc.relation.interfaces`, `IIntIds` from `zope.intid.interfaces`, `ObjectModifiedEvent` from `zope.lifecycleevent`, `getUtility`, `notify`):

```python
def test_delete_folder_with_relation_to_own_child_leaves_no_row(self, pg_functional):
    """#290: RelationBrokenEvent reindexes the detached source during the delete."""
    portal = pg_functional["portal"]
    setRoles(portal, TEST_USER_ID, ["Manager"])
    portal.invokeFactory("Folder", "f", title="F")
    folder = portal["f"]
    folder.invokeFactory("Image", "img", title="Img")
    intids = getUtility(IIntIds)
    folder.relatedItems = [RelationValue(intids.getId(folder["img"]))]
    notify(ObjectModifiedEvent(folder))
    transaction.commit()
    folder_uid = folder.UID()
    folder_zoid = int.from_bytes(folder._p_oid, "big")
    assert list(
        getUtility(ICatalog).findRelations({"to_id": intids.getId(folder["img"])})
    )

    portal.manage_delObjects(["f"])
    transaction.commit()

    assert (
        _query_pg(
            pg_functional,
            "SELECT zoid, path FROM object_state WHERE path IS NOT NULL AND path NOT LIKE '/%%'",
        )
        == []
    )
    assert _query_pg(
        pg_functional,
        "SELECT path, idx FROM object_state WHERE zoid = %s",
        (folder_zoid,),
    ) == [(None, None)]
    assert (
        list(portal["portal_catalog"].unrestrictedSearchResults(UID=folder_uid)) == []
    )
```

- [ ] **Step 2: Run it to verify it fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_pg_integration.py -k leaves_no_row -q -p no:cacheprovider`
Expected: FAIL, the first assertion shows one row `(<zoid>, 'f')`.

- [ ] **Step 3: Add the fake helpers to `tests/conftest.py`**

Implement `fake_content(...)` and `detached_content(...)` as specified in Interfaces, with the classes next to them. Use real `Acquisition.Implicit` wrapping (`content.__of__(container)`), not `__parent__` attributes.

- [ ] **Step 4: Write the unit tests**

Rewrite `TestIndexObjectReindexObject` in `tests/test_catalog_plone.py` (tool via `PlonePGCatalogTool.__new__`, `_set_pg_annotation` patched with `mock.patch.object`):

```python
def test_indexObject_calls_set_pg_annotation(self):          # fake_content() → ann_mock.assert_called_once_with(obj)
def test_reindexObject_calls_set_pg_annotation(self):        # fake_content(), uid="/plone/doc" → assert_called_once_with(obj, "/plone/doc")
def test_indexObject_skips_detached(self):                   # detached_content() → ann_mock.assert_not_called()
def test_reindexObject_skips_detached(self):                 # detached_content() → ann_mock.assert_not_called()
def test_reindexObject_partial_skips_detached(self):         # detached_content(), idxs=["portal_type"], _partial_reindex patched → neither mock called
def test_reindexObject_skips_object_not_in_parent(self):     # fake_content(contained=False) → ann_mock.assert_not_called()
def test_reindexObject_skips_request_parent(self):           # fake_content(request_parent=True) → ann_mock.assert_not_called()
def test_indexObject_without_getId_is_indexed(self):         # fake_content(with_get_id=False) → ann_mock.assert_called_once_with(obj)
def test_unindexObject_skips_detached(self):                 # detached_content(), uncatalog_object patched → assert_not_called()
def test_unindexObject_not_in_parent_still_uncatalogs(self): # fake_content(contained=False), uncatalog_object patched → assert_called_once_with("/plone/doc")
```

In `tests/test_clean_break.py`, replace the bare `mock.Mock()` objects in `test_public_unindex_object` and `test_reindex_object_uses_partial` with `fake_content("doc1")` and `fake_content()`; keep their assertions.

- [ ] **Step 5: Run the unit tests to verify the new ones fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_catalog_plone.py tests/test_clean_break.py -q -p no:cacheprovider`
Expected: the `skips_*` tests FAIL (`_set_pg_annotation` was called); the others PASS.

- [ ] **Step 6: Implement the guard in `src/plone/pgcatalog/catalog.py`**

Add `from Products.CMFCore.indexing import filterTemporaryItems` to the module imports. In `indexObject` and `reindexObject`, after the `is_move_in_progress()` early return where present and before any other work: `obj = filterTemporaryItems(object)`; when `None`, `log.debug("%s: skipping detached or temporary %s", <method name>, type(object).__name__)` and return. In `unindexObject`: the same with `filterTemporaryItems(object, checkId=False)`. Use the filtered object for the rest of the method. Extend each docstring with one sentence saying the filter mirrors CMFCore's queue-aware methods and why (#290).

- [ ] **Step 7: Run unit tests and the regression test**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_catalog_plone.py tests/test_clean_break.py tests/test_pg_integration.py -k "TestIndexObjectReindexObject or test_public_unindex_object or test_reindex_object_uses_partial or leaves_no_row" -q -p no:cacheprovider`
Expected: all PASS.

- [ ] **Step 8: Delete the throwaway reproduction and write the CHANGES entry**

`rm tests/test_repro_290_pg.py`. In `CHANGES.md`, insert above `## 1.0.0rc10 (2026-11-09)`:

```markdown
## 1.0.0rc11 (unreleased)

### Fixed

- Deleting a subtree no longer leaves catalog rows of removed objects
  behind. When a removed object was the target of a relation whose source
  was removed in the same delete (an item with a relation to an image
  inside itself), z3c.relationfield's `RelationBrokenEvent` reindexed the
  source after it had been detached, five.intid handed over the unwrapped
  object, and the catalog wrote a full row with the bare id as `path`,
  overwriting the uncatalog of the same transaction. The row kept the
  object findable by UID and type queries, so `uuidToObject()` raised
  instead of returning `None`. Stock Plone is protected by
  `filterTemporaryItems` in CMFCore's queue-aware `indexObject`,
  `reindexObject` and `unindexObject`; pgcatalog overrides exactly these
  methods and had dropped the filter. The three methods now apply it, so
  detached objects, objects below a request and objects no longer
  contained in their parent are skipped as in stock Plone. As a second
  line of defence, a catalog write with a path that is not rooted at the
  application is refused and logged. The upgrade step to profile version
  7 uncatalogs rows with such paths once; `maintenance.remove_detached_rows()`
  does the same from a script. #290
```

- [ ] **Step 9: Commit**

```bash
git add src/plone/pgcatalog/catalog.py tests/conftest.py tests/test_catalog_plone.py tests/test_clean_break.py tests/test_pg_integration.py CHANGES.md
git commit -m "fix: skip detached and temporary objects in indexObject/reindexObject/unindexObject (#290)" -m "Assisted-by: Claude Sonnet 5.5"
```

---

### Task 2: Defence in depth, `_set_pg_annotation` refuses non-rooted paths

**Files:**
- Modify: `src/plone/pgcatalog/catalog.py` (`_set_pg_annotation`, line 477 on `d38a145`)
- Test: `tests/test_catalog_plone.py` (new class `TestSetPgAnnotationRefusesNonRootedPath`)

**Interfaces:**
- Consumes: `detached_content()` from Task 1.
- Produces: `_set_pg_annotation(obj, uid=None)` returns `False` and registers nothing when the final `uid` does not start with `/`; logs at WARNING with the text `non-rooted path`.

- [ ] **Step 1: Write the failing tests**

```python
class TestSetPgAnnotationRefusesNonRootedPath:
    def test_explicit_uid_without_root_is_refused(self, caplog):
        # tool = PlonePGCatalogTool.__new__(PlonePGCatalogTool); patch plone.pgcatalog.catalog.set_pending
        # and tool._obj_to_zoid; caplog.set_level(logging.WARNING, "plone.pgcatalog.catalog")
        # assert tool._set_pg_annotation(fake_content(), uid="doc") is False
        # set_pending not called; _obj_to_zoid not called; "non-rooted path" in caplog.text

    def test_derived_path_of_detached_object_is_refused(self, caplog):
        # same with detached_content() and no uid → False, nothing registered, warning logged

    def test_rooted_path_passes_the_guard(self):
        # fake_content(), uid="/plone/doc", _obj_to_zoid patched to return None, obj gets the __dict__ fallback
        # assert result is True
```

- [ ] **Step 2: Run them to verify the first two fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_catalog_plone.py -k RefusesNonRootedPath -q -p no:cacheprovider`
Expected: two FAIL (guard missing), one PASS.

- [ ] **Step 3: Implement the guard**

In `_set_pg_annotation`, directly after the `if uid is None:` block that derives `uid` and before `self._obj_to_zoid(obj)`: when `not uid.startswith("/")`, `log.warning("_set_pg_annotation: refusing non-rooted path %r for %s; detached object reindexed? (#290)", uid, type(obj).__name__)` and `return False`.

- [ ] **Step 4: Run the tests**

Run: same command as Step 2.
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/catalog.py tests/test_catalog_plone.py
git commit -m "fix: refuse catalog writes with a non-rooted path (#290)" -m "Assisted-by: Claude Sonnet 5.5"
```

---

### Task 3: One-shot repair and upgrade step 6 → 7

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py` (new function next to `remove_acquired_uids`, line 91)
- Create: `src/plone/pgcatalog/upgrades/profile_7.py`, `src/plone/pgcatalog/upgrades/profile_7.zcml`
- Modify: `src/plone/pgcatalog/upgrades/configure.zcml` (include `profile_7.zcml`), `src/plone/pgcatalog/profiles/default/metadata.xml` (version 7)
- Modify: `docs/sources/how-to/rebuild-catalog.md` (new section after "Repair stale security data", before the `(undo-follow-up)=` label at line 145; one row in the "Choosing the right operation" table)
- Test: `tests/test_indexing.py` (next to `TestRemoveAcquiredUids`, line 436), `tests/test_pg_integration.py` (next to `TestRepairUpgradeStep`, line 2315)

**Interfaces:**
- Produces: `maintenance.remove_detached_rows(conn) -> int`: NULLs every catalog column (`uncatalog_assignments()` from `plone.pgcatalog.indexing`, imported at module top) of rows `WHERE path IS NOT NULL AND path NOT LIKE '/%'`, in one `UPDATE` without parameters; when `rowcount > 0` runs `SELECT nextval('pgcatalog_change_seq')` (as `undo.py` line 588 does); logs the count at INFO; returns the count. Does not commit; the caller does.
- Produces: `upgrades.profile_7.uncatalog_detached_rows(context)`: same context handling as `profile_4.repair_uncataloged_content` (resolve site, no-op unless `IPGCatalogTool.providedBy(catalog)`), then `with catalog._pg_connection() as conn: count = remove_detached_rows(conn); conn.commit()`, logs the count.

- [ ] **Step 1: Write the failing maintenance test in `tests/test_indexing.py`**

```python
class TestRemoveDetachedRows:
    """Data repair for #290 — uncatalog rows whose path is not rooted."""

    def test_removes_only_non_rooted_rows(self, pg_conn_with_catalog):
        conn = pg_conn_with_catalog
        insert_object(conn, zoid=80); catalog_object(conn, zoid=80, path="/plone/doc", idx={"UID": "keep"})
        insert_object(conn, zoid=81); catalog_object(conn, zoid=81, path="f", idx={"UID": "stale", "portal_type": "Folder"})
        conn.commit()
        seq_before = conn.execute("SELECT last_value FROM pgcatalog_change_seq").fetchone()["last_value"]

        assert remove_detached_rows(conn) == 1
        conn.commit()

        assert _get_row(conn, 80)["path"] == "/plone/doc"
        stale = _get_row(conn, 81)
        assert stale["path"] is None and stale["parent_path"] is None and stale["idx"] is None
        assert conn.execute("SELECT last_value FROM pgcatalog_change_seq").fetchone()["last_value"] > seq_before

    def test_nothing_to_do_returns_zero_and_keeps_sequence(self, pg_conn_with_catalog):
        # one rooted row only → returns 0, last_value unchanged
```

Import `remove_detached_rows` inside the test method, as the neighbouring `TestRemoveAcquiredUids` does with `remove_acquired_uids`.

- [ ] **Step 2: Run to verify it fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_indexing.py -k RemoveDetachedRows -q -p no:cacheprovider`
Expected: FAIL with ImportError.

- [ ] **Step 3: Implement `remove_detached_rows(conn)` in `maintenance.py`**

Docstring in the style of `remove_acquired_uids`: what the rows are, which release wrote them, that it is a one-shot repair and new writes are guarded (#290).

- [ ] **Step 4: Run the maintenance tests**

Run: same as Step 2. Expected: PASS.

- [ ] **Step 5: Write the failing upgrade step test in `tests/test_pg_integration.py`**

```python
class TestDetachedRowsUpgradeStep(_PGLayerTest):
    def test_upgrade_step_uncatalogs_non_rooted_rows(self, pg_functional):
        from plone.pgcatalog.upgrades.profile_7 import uncatalog_detached_rows

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        portal.invokeFactory("Document", "upg-detached", title="Detached")
        transaction.commit()
        doc_uid = portal["upg-detached"].UID()
        # simulate the #290 damage: bare id as path, as the bug wrote it
        with pg_functional["pgTestDB"].connection.cursor() as cur:
            cur.execute(
                "UPDATE object_state SET path = 'upg-detached', parent_path = '/', path_depth = 1 WHERE path = '/plone/upg-detached'"
            )
            assert cur.rowcount == 1
        pg_functional["pgTestDB"].connection.commit()

        uncatalog_detached_rows(portal["portal_setup"])
        transaction.commit()

        assert (
            _query_pg(
                pg_functional,
                "SELECT path, idx FROM object_state WHERE idx->>'UID' = %s OR path = 'upg-detached'",
                (doc_uid,),
            )
            == []
        )
        assert (
            list(portal["portal_catalog"].unrestrictedSearchResults(UID=doc_uid)) == []
        )
```

Check how `pg_functional["pgTestDB"].connection` is used by `_wipe_catalog_row` (line 2202) and follow that for the connection handling.

- [ ] **Step 6: Run to verify it fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_pg_integration.py -k DetachedRowsUpgradeStep -q -p no:cacheprovider`
Expected: FAIL with ImportError.

- [ ] **Step 7: Create the upgrade step**

`profile_7.py` with `uncatalog_detached_rows(context)` as specified; `profile_7.zcml` with `source="6" destination="7"`, title `Uncatalog rows left behind by deletes`, description naming the symptom (deleted content still found by UID and type queries, `path` holding only the id), that it is a single SQL statement, and `See #290.`; include it in `upgrades/configure.zcml`; set `<version>7</version>` in `profiles/default/metadata.xml`.

- [ ] **Step 8: Run the upgrade step test**

Run: same as Step 6. Expected: PASS.

- [ ] **Step 9: Document the repair**

In `docs/sources/how-to/rebuild-catalog.md`, add `## Repair rows of deleted content` after the "Repair stale security data" section, in the same shape as that section: what went wrong (link `[#290](https://github.com/bluedynamics/plone-pgcatalog/issues/290)`), that the upgrade step to profile version 7 repairs it once, how to run `remove_detached_rows` from a script (`with portal.portal_catalog._pg_connection() as conn:` then call, then `conn.commit()`), and that it is a single SQL statement that runs in well under a second. Add a row for `remove_detached_rows()` to the "Choosing the right operation" table following its columns. One sentence per line.

- [ ] **Step 10: Verify CHANGES and commit**

The CHANGES entry from Task 1 already names the upgrade step and the function; confirm the function name matches. Then:

```bash
git add src/plone/pgcatalog/maintenance.py src/plone/pgcatalog/upgrades/ src/plone/pgcatalog/profiles/default/metadata.xml docs/sources/how-to/rebuild-catalog.md tests/test_indexing.py tests/test_pg_integration.py
git commit -m "fix: upgrade step 6 to 7 uncatalogs rows with non-rooted paths (#290)" -m "Assisted-by: Claude Sonnet 5.5"
```

---

### Task 4: Full verification and pull request

**Files:** none new.

- [ ] **Step 1: Lint and format**

Run: `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`
Expected: no output, exit 0.

- [ ] **Step 2: Full test suite in the background**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest -q -p no:cacheprovider > /tmp/pgcatalog-290-full.log 2>&1` as a background command; wait for it; then `tail -5 /tmp/pgcatalog-290-full.log`.
Expected: `N passed` with zero failures. Report the exact summary line. Any failure in rename, move or undo tests means Task 1 broke Review Focus item 1: stop and report instead of patching tests.

- [ ] **Step 3: Push and open the PR**

```bash
git push -u origin fix-290-detached-reindex
gh pr create --title "fix: do not catalog detached objects reindexed during a delete (#290)" --body-file <body>
```

PR body (English): the symptom in two sentences, the root cause as found (CMFCore's `filterTemporaryItems` not carried over when the three methods were overridden; stock Plone verified clean in the same scenario), the three parts of the change (guard, defence in depth, upgrade step 6 → 7 plus `remove_detached_rows()`), the test summary line from Step 2, `Closes #290`, and as last line `Assisted-by: Claude Fable 5.1, Claude Sonnet 5.5`.

- [ ] **Step 4: Report**

Return the PR URL, the test summary line, and anything in the plan that did not match the code.
