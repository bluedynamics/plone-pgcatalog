# Wrapped pending brains for reindexObjectSecurity (#291) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `reindexObjectSecurity` recomputes `allowedRolesAndUsers` of children that are still pending in the same transaction (pasted or re-attached containers), and existing rows that the old behaviour left as `{Manager}` are recomputed once.

**Architecture:** `_pending_brains_for_path` currently hands `reindexObjectSecurity` the bare `jar.get(oid)` object; since #292 the filter in `reindexObject` drops it, so the children keep the roles computed from the source's permission state (too permissive for a copy of published content; before #292 they got `{Manager}`). The fix resolves each pending entry by traversing its stored absolute path and checks the oid, so the brain carries a wrapped object exactly like a committed brain does. A targeted variant of `maintenance.reindex_security` plus upgrade step 7 → 8 recomputes the rows that are `{Manager}`-only today.

**Tech Stack:** Python 3.12+, Plone 6.2, OFS traversal, psycopg, pytest with `zope.pytestlayer`, GenericSetup upgrade steps.

**Spec:** https://github.com/bluedynamics/plone-pgcatalog/issues/291 and the verified analysis with reproduction table in https://github.com/bluedynamics/plone-pgcatalog/issues/291#issuecomment-6927147298. The guard part of the issue's proposal is already done by #292 and is not part of this plan.

## Global Constraints

- Work only in the worktree `sources/plone-pgcatalog-wt/fix-291-pending-brain-wrapped` (branch `fix-291-pending-brain-wrapped`, based on main `29af20a`). Never touch `sources/plone-pgcatalog/`.
- Use the worktree's own `.venv` (built with `uv venv --python 3.13 .venv && uv pip install -e ".[test]"`); it imports `plone.pgcatalog` from the worktree. The main checkout's venv imports from the main checkout and must not be used for tests here.
- PostgreSQL for tests: container `zodb-pgjsonb-dev` on port 5433 must be up. Run tests as `env -u ZODB_TEST_DSN .venv/bin/python -m pytest <target> -q -p no:cacheprovider`. Never two pytest processes at once. The full suite exceeds the 10 minute foreground limit: run it in the background and wait.
- Imports at module top; tests may import inline where the surrounding file already does.
- ruff 0.16.7 format and check run as pre-commit hooks and reject the commit on failure; fix and recommit. Vale runs on docs changes.
- Commit messages end with a blank line and `Assisted-by: Claude Sonnet 5.5`. No `Co-Authored-By`, no e-mail address.
- Do not edit this plan or the #290 plan.
- Throwaway `tests/test_repro_291_pg.py` exists untracked; Task 1 turns it into the regression test and deletes it.

## Review Focus

1. Pasting a container whose children have their own workflow must stay unaffected: those children's `View` setting does not acquire, so their roles are the same before and after `notifyWorkflowCreated`. Pinned by `test_paste_keeps_workflow_child_roles` (Task 1).
2. A pending entry whose path no longer resolves (renamed in the same transaction) or resolves to a different object must yield no brain and no `Cannot get … from catalog` warning. Pinned by `test_unresolvable_path_skipped` and `test_foreign_object_at_path_skipped` (Task 1).
3. Pending entries whose path lies outside the queried subtree must not be traversed at all (traversal is the expensive part). Pinned by `test_only_matching_paths_are_traversed` (Task 1).
4. The repair must leave rows alone whose `allowed_roles` legitimately contain more than `Manager`, and must not touch `idx` or write the ZODB (partial reindex). Pinned by `test_paths_subset_only_touches_given_rows` and the `tid`/`idx` assertions it inherits from the existing security tests (Task 2).
5. A repair path that no longer resolves must be reported as failed, not raise and abort the whole upgrade. Pinned by `test_paths_unresolvable_counts_as_failed` (Task 2).

---

### Task 1: `_pending_brains_for_path` hands out wrapped objects

**Files:**
- Modify: `src/plone/pgcatalog/catalog.py` (`_pending_brains_for_path`, lines 890-934 on `29af20a`)
- Modify: `tests/test_clean_break.py:1086-1140` (`TestPendingBrainsForPath`)
- Modify: `tests/test_pg_integration.py` (new class next to `TestReindexSecurity`)
- Delete: `tests/test_repro_291_pg.py`
- Modify: `CHANGES.md`

**Interfaces:**
- Produces: `_pending_brains_for_path(path_query, found_paths)` keeps its signature and return type (`list[_PendingBrain]`). For every matching pending entry it resolves `obj = self.unrestrictedTraverse(obj_path, None)` (wrapped in `try/except Exception` → treated as `None`) and appends `_PendingBrain(obj_path, obj)` only when `obj is not None` and `getattr(aq_base(obj), "_p_oid", None) == p64(zoid)`. The `_p_jar` lookup and `jar.get()` go away. `_PendingBrain` is unchanged.

- [ ] **Step 1: Write the failing PG regression tests**

Add to `tests/test_pg_integration.py`, directly before `class TestReindexSecurity`:

```python
class TestPendingBrainSecurity(_PGLayerTest):
    """#291: reindexObjectSecurity must see pasted children as wrapped objects."""

    def _paste_published_folder(
        self, pg_functional, child_type="Image", document_workflow=False
    ):
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        wft = portal["portal_workflow"]
        wft.setChainForPortalTypes(("Folder",), "simple_publication_workflow")
        if document_workflow:
            wft.setChainForPortalTypes(("Document",), "simple_publication_workflow")
        portal.invokeFactory("Folder", "src", title="Src")
        wft.doActionFor(portal["src"], "publish")
        portal["src"].invokeFactory(child_type, "child", title="Child")
        portal.invokeFactory("Folder", "dest", title="Dest")
        transaction.commit()
        portal["dest"].manage_pasteObjects(portal.manage_copyObjects(["src"]))
        transaction.commit()
        return portal

    def test_paste_recomputes_roles_of_workflowless_child(self, pg_functional):
        self._paste_published_folder(pg_functional)
        folder = set(_allowed_roles(pg_functional, "/plone/dest/src"))
        child = set(_allowed_roles(pg_functional, "/plone/dest/src/child"))
        assert "Anonymous" not in folder  # the copy is private
        assert child == folder
        assert child != {"Manager"}

    def test_paste_keeps_workflow_child_roles(self, pg_functional):
        # A Document has its own workflow; the pasted copy starts private
        # on its own, so its roles equal the folder's without acquisition.
        self._paste_published_folder(
            pg_functional, child_type="Document", document_workflow=True
        )
        child = set(_allowed_roles(pg_functional, "/plone/dest/src/child"))
        assert child == set(_allowed_roles(pg_functional, "/plone/dest/src"))
        assert "Anonymous" not in child
```

Delete `tests/test_repro_291_pg.py` in this step.

- [ ] **Step 2: Run them to verify the first one fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_pg_integration.py -k TestPendingBrainSecurity -q -p no:cacheprovider`
Expected: `test_paste_recomputes_roles_of_workflowless_child` FAILS with `child == {'Anonymous'}`; the workflow-child test PASSES already.

- [ ] **Step 3: Rewrite the unit tests in `TestPendingBrainsForPath`**

Keep `test_no_pending_returns_empty`, `test_already_found_path_skipped`, `test_uncatalog_sentinel_skipped` (drop their `tool._p_jar = ...` lines). Replace `test_no_jar_returns_empty` and `test_matching_path_returns_brain`, and add two tests; all patch traversal with `mock.patch.object(tool, "unrestrictedTraverse", ...)` and import `p64` from `ZODB.utils`:

```python
def test_matching_path_returns_brain_with_traversed_object(self, tool):
    # pending {1: {"path": "/plone/doc1"}}; traversal returns fake with _p_oid = p64(1)
    # → one brain, getPath() == "/plone/doc1", _unrestrictedGetObject() is the fake;
    # unrestrictedTraverse.assert_called_once_with("/plone/doc1", None)

def test_unresolvable_path_skipped(self, tool):
    # traversal returns None → []

def test_foreign_object_at_path_skipped(self, tool):
    # pending zoid 1, traversal returns fake with _p_oid = p64(2) → []

def test_only_matching_paths_are_traversed(self, tool):
    # pending {1: {"path": "/plone/a/doc"}, 2: {"path": "/plone/b/doc"}}, query "/plone/a"
    # → unrestrictedTraverse called once, with "/plone/a/doc"
```

- [ ] **Step 4: Run the unit tests to verify the new ones fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_clean_break.py -k TestPendingBrainsForPath -q -p no:cacheprovider`
Expected: the four new or replaced tests FAIL (the code still reads `_p_jar`), the three kept ones PASS.

- [ ] **Step 5: Implement the change in `_pending_brains_for_path`**

As specified in Interfaces. Check that `aq_base` is imported at the top of `catalog.py` (add it if not). Replace the docstring's "Loads objects from the ZODB connection cache by OID." with two sentences: the object is resolved by traversing its stored path so that `reindexObjectSecurity` gets a wrapped object, as it does for committed brains, and entries that do not resolve to the pending zoid are skipped (#291).

- [ ] **Step 6: Run both test groups**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_clean_break.py tests/test_pg_integration.py -k "TestPendingBrainsForPath or TestPendingBrainSecurity or TestReindexSecurity" -q -p no:cacheprovider`
Expected: all PASS.

- [ ] **Step 7: CHANGES entry**

In `CHANGES.md`, under `## 1.0.0rc11 (unreleased)` → `### Fixed`, after the #290 bullet:

```markdown
- Children pasted or re-attached together with their container no longer
  end up with wrong `allowedRolesAndUsers`. `notifyWorkflowCreated` on the
  container runs `reindexObjectSecurity`, which finds the children in the
  pending store, and pgcatalog handed it the bare persistent objects
  without an acquisition wrapper. Children without a workflow of their own
  (images, files) then computed `{Manager}` from the Zope default. Since
  the filter from #290 skips unwrapped objects, the recomputation was
  silently dropped instead, which left a pasted copy of published content
  with children visible to `Anonymous` while the copy itself was private.
  Pending brains now resolve their object by traversing the stored path,
  as committed brains do, so the recomputation runs on the wrapped object.
  The upgrade step to profile version 8 recomputes the security indexes of
  all rows whose `allowed_roles` is exactly `{Manager}` once;
  `maintenance.reindex_security()` takes a `paths=` argument for the same
  from a script. #291
```

- [ ] **Step 8: Commit**

```bash
git add src/plone/pgcatalog/catalog.py tests/test_clean_break.py tests/test_pg_integration.py CHANGES.md
git commit -m "fix: resolve pending brains by traversal so reindexObjectSecurity gets wrapped objects (#291)" -m "Assisted-by: Claude Sonnet 5.5"
```

---

### Task 2: Targeted security repair and upgrade step 7 → 8

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py` (`reindex_security`, line 512 on `29af20a`; new `manager_only_paths` next to it)
- Create: `src/plone/pgcatalog/upgrades/profile_8.py`, `src/plone/pgcatalog/upgrades/profile_8.zcml`
- Modify: `src/plone/pgcatalog/upgrades/configure.zcml`, `src/plone/pgcatalog/profiles/default/metadata.xml` (version 8)
- Modify: `docs/sources/how-to/rebuild-catalog.md` (section "Repair stale security data", lines 126-145)
- Test: `tests/test_indexing.py` (new class next to `TestRemoveDetachedRows`), `tests/test_pg_integration.py` (`TestReindexSecurity`, and `test_profile_version_is_7` → `test_profile_version_is_8`)

**Interfaces:**
- Produces: `reindex_security(catalog, site, *, batch_size=_REBUILD_BATCH, paths=None) -> SecurityReindexResult`. With `paths=None` the behaviour is unchanged (site walk). With `paths` (an iterable of absolute path strings) it processes exactly those: each is resolved with `catalog.unrestrictedTraverse(path, None)` inside the existing `site_context(site)`; `None` is logged at WARNING (`reindex_security: not found %s`) and appended to `failed`; otherwise the same `_is_catalogable` check, partial reindex and batching as the walk. `checked` counts only reindexed objects.
- Produces: `manager_only_paths(conn) -> list[str]`: `SELECT path FROM object_state WHERE allowed_roles = ARRAY['Manager'] AND idx ? 'portal_type' ORDER BY path`, returns the paths.
- Produces: `upgrades.profile_8.recompute_manager_only_security(context)`: context handling as `profile_4.repair_uncataloged_content`; borrows a pool connection for `manager_only_paths`, returns it, skips with an INFO log when the list is empty, otherwise calls `reindex_security(catalog, site, paths=paths)` and logs `%d recomputed, %d failed`.

- [ ] **Step 1: Write the failing maintenance tests**

In `tests/test_pg_integration.py`, class `TestReindexSecurity`, reuse `_make_stale` and `_row`:

```python
def test_paths_subset_only_touches_given_rows(self, pg_functional):
    portal = self._make_stale(pg_functional)
    doc, folder = "/plone/sec-folder/sec-doc", "/plone/sec-folder"
    before_folder = self._row(pg_functional, folder)
    result = reindex_security(portal["portal_catalog"], portal, paths=[doc])
    transaction.commit()
    assert result.checked == 1 and result.failed == []
    assert self._row(pg_functional, doc)[0] == ["Anonymous"]
    assert (
        self._row(pg_functional, folder) == before_folder
    )  # still ["Stale"], untouched


def test_paths_unresolvable_counts_as_failed(self, pg_functional):
    portal = self._make_stale(pg_functional)
    result = reindex_security(portal["portal_catalog"], portal, paths=["/plone/nope"])
    assert result.checked == 0 and result.failed == ["/plone/nope"]
```

In `tests/test_indexing.py`, after `TestRemoveDetachedRows`:

```python
class TestManagerOnlyPaths:
    def test_returns_only_manager_only_content_rows(self, pg_conn_with_catalog):
        # zoid 90: path "/plone/a", idx {"allowedRolesAndUsers": ["Manager"], "portal_type": "Image"}
        # zoid 91: path "/plone/b", idx {"allowedRolesAndUsers": ["Manager", "Editor"], "portal_type": "Image"}
        # zoid 92: path "/plone/c", idx {"allowedRolesAndUsers": ["Manager"]}   (no portal_type)
        # sanity: _get_row(conn, 90)["allowed_roles"] == ["Manager"]  (catalog_object moves the key into the column)
        # assert manager_only_paths(conn) == ["/plone/a"]
```

If the sanity assertion fails because `catalog_object()` does not populate `allowed_roles` from `idx`, set the column with a direct `UPDATE object_state SET allowed_roles = %s WHERE zoid = %s` in the test instead; do not change `catalog_object`.

- [ ] **Step 2: Run to verify they fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_pg_integration.py tests/test_indexing.py -k "paths_subset or paths_unresolvable or ManagerOnlyPaths" -q -p no:cacheprovider`
Expected: TypeError (`paths` unexpected) and ImportError.

- [ ] **Step 3: Implement `paths=` in `reindex_security` and `manager_only_paths`**

Extend the docstring of `reindex_security` with one paragraph on `paths` and #291. Keep the loop body shared between the two sources (walk or paths); do not duplicate the batching.

- [ ] **Step 4: Run the maintenance tests**

Run: same as Step 2. Expected: PASS.

- [ ] **Step 5: Write the failing upgrade step test**

In `tests/test_pg_integration.py`, next to `TestDetachedRowsUpgradeStep`:

```python
class TestManagerOnlyUpgradeStep(_PGLayerTest):
    def test_upgrade_step_recomputes_manager_only_rows(self, pg_functional):
        from plone.pgcatalog.upgrades.profile_8 import recompute_manager_only_security

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        wft = portal["portal_workflow"]
        wft.setChainForPortalTypes(("Folder",), "simple_publication_workflow")
        portal.invokeFactory("Folder", "f", title="F")  # private initial state
        portal["f"].invokeFactory("Image", "img", title="Img")
        transaction.commit()
        # simulate rc8..rc10 damage
        with pg_functional["pgTestDB"].connection.cursor() as cur:
            cur.execute(
                "UPDATE object_state SET allowed_roles = ARRAY['Manager'] WHERE path = '/plone/f/img'"
            )
            assert cur.rowcount == 1
        pg_functional["pgTestDB"].connection.commit()

        recompute_manager_only_security(portal["portal_setup"])
        transaction.commit()

        img = set(_allowed_roles(pg_functional, "/plone/f/img"))
        assert img == set(_allowed_roles(pg_functional, "/plone/f"))
        assert img != {"Manager"}
```

Rename `test_profile_version_is_7` to `test_profile_version_is_8` and assert `<version>8</version>`.

- [ ] **Step 6: Run to verify it fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pytest tests/test_pg_integration.py -k "ManagerOnlyUpgradeStep or profile_version" -q -p no:cacheprovider`
Expected: ImportError and the version assertion failing.

- [ ] **Step 7: Create the upgrade step**

`profile_8.py` and `profile_8.zcml` (`source="7" destination="8"`, title `Recompute allowedRolesAndUsers of Manager-only rows`, description naming the symptom: children pasted or restored with their container were only visible to Managers, or, since 1.0.0rc11's first fix, kept the roles of the source; one partial reindex per affected row; `See #291.`). Include it in `upgrades/configure.zcml`; set `<version>8</version>`.

- [ ] **Step 8: Run the upgrade step tests**

Run: same as Step 6. Expected: PASS.

- [ ] **Step 9: Document the repair**

In the "Repair stale security data" section of `docs/sources/how-to/rebuild-catalog.md`, after the existing script block and its "It commits every 500 objects…" sentence, add a short paragraph: what #291 did to pasted children (link `[#291](https://github.com/bluedynamics/plone-pgcatalog/issues/291)`), that the upgrade step to profile version 8 recomputes the rows whose `allowed_roles` is exactly `Manager`, and how to run it from a script:

```python
from plone.pgcatalog.upgrades.profile_8 import recompute_manager_only_security

recompute_manager_only_security(portal.portal_setup)
```

plus one sentence that `reindex_security(catalog, site, paths=[...])` recomputes any given list of paths. One sentence per line.

- [ ] **Step 10: Commit**

```bash
git add src/plone/pgcatalog/maintenance.py src/plone/pgcatalog/upgrades/ src/plone/pgcatalog/profiles/default/metadata.xml docs/sources/how-to/rebuild-catalog.md tests/test_indexing.py tests/test_pg_integration.py
git commit -m "fix: upgrade step 7 to 8 recomputes Manager-only allowedRolesAndUsers rows (#291)" -m "Assisted-by: Claude Sonnet 5.5"
```

---

### Task 3: Full verification

- [ ] **Step 1: Lint and format**

Run: `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`
Expected: `… already formatted` and `All checks passed!`.

- [ ] **Step 2: Full test suite in the background**

Run `env -u ZODB_TEST_DSN .venv/bin/python -m pytest -q -p no:cacheprovider > /tmp/pgcatalog-291-full.log 2>&1` as a background command, wait for it, then `tail -3 /tmp/pgcatalog-291-full.log`.
Expected: `N passed` with zero failures. Report the exact line. A failure in any move, rename, undo or security test means Task 1 or 2 changed shared behaviour: stop and report, do not patch tests.

- [ ] **Step 3: Report**

Return the commits (hash, subject), the summary line, the ruff result, every deviation from the plan with its reason. Do not push and do not open a PR; the caller reviews first.
