# Repair uncataloged content Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recatalog content whose catalog columns were wiped by bluedynamics/zodb-pgjsonb#120, without clearing the catalog, via a maintenance function, an upgrade step (profile 3 → 4) and a documented command-line route; pin the known triggers with regression tests; and stop losing Tika-extracted text on a full reindex (stopgap "1a") plus restore text that is already lost (`requeue_lost_extractions()`).

**Architecture:** `maintenance.repair_uncataloged(catalog, site, ...)` walks the site with the catalog's existing `_walk_site_paths()`, keeps only CMF-catalog-aware objects, checks their rows in batches (`path IS NOT NULL AND idx IS NOT NULL`), and calls `catalog.catalog_object()` for the damaged ones, committing per batch. The upgrade step `profile_4.repair_uncataloged_content` calls it, following the pattern of `profile_3.resync_gopip_ranks`. The minimum `zodb-pgjsonb` version is raised to the fixed release, which enforces the rollout order; the repair also checks it at runtime. The repair survives `ConflictError` on a live site (one retry per batch), runs `ANALYZE` at the end, and the upgrade step skips the site walk when an SQL pre-count finds no candidate. For Tika, the processor's queue insert becomes an upsert that sets an existing row back to `pending` (1a), the worker only finishes jobs still in `processing` and merges plus marks `done` in one transaction, and `maintenance.requeue_lost_extractions()` re-pends files whose `searchable_text` has no weight-`'C'` lexeme left.

**Tech Stack:** Python 3.12+, Plone 6.2, Zope, GenericSetup upgrade steps, psycopg 3, PostgreSQL, pytest with `zope.pytestlayer`.

**Spec:** bluedynamics/plone-pgcatalog#244 (issue body, sections "Proposal", "Tika" and "Rollout", revised 2026-10-09). Background: bluedynamics/zodb-pgjsonb#120. Out of scope: #247 (extracted text in its own column, replaces 1a later).

## Global Constraints

- Work only in the worktree `sources/plone-pgcatalog-wt/repair-uncataloged-244` (branch `fix/244-repair-uncataloged`). Never in `sources/plone-pgcatalog/`.
- The repair must not clear catalog data and must not touch intact rows in the default mode (no ZODB write, unchanged `tid`).
- Only objects that Plone itself would catalog: callable `reindexObject`, and never the catalog tool. Do not copy `clearFindAndRebuild()`'s behaviour of cataloging every traversed object.
- Idempotent: running it twice repairs nothing the second time.
- New PG-layer tests go INTO `tests/test_pg_integration.py` and use its `pg_functional` fixture. A second module calling `fixture.create(PGCATALOG_PG_FIXTURE)` causes phantom "fixture not found" errors in full runs. `_query_pg` there returns tuple rows.
- Tests share the `zodb_test` DB on port 5433 (`docker --context default start zodb-pgjsonb-dev`) with zodb-pgjsonb's tests. Never run two pytest processes at the same time, in either repo.
- Worktree venv: `uv venv && uv pip install -c https://dist.plone.org/release/6.2-latest/constraints.txt -e ".[test]"` (same as CI). Run tests with `env -u ZODB_TEST_DSN uv run pytest ...`.
- ruff pinned: `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`. C901 max 13. Imports at module top; tests may import inline (existing style). `maintenance.py` must not import from `catalog.py` (catalog imports maintenance).
- Every commit message ends with `Assisted-by: Claude Opus 5.5` (or the model doing the work). Never `Co-Authored-By`, never a noreply address.
- `CHANGES.md` gets entries under a new `## Unreleased` heading at the top.
- zodb-pgjsonb 1.17.0 (with the #120 fix) is released; install it into the venv. Task 4 Step 2 additionally needs an older zodb-pgjsonb (e.g. `uv pip install "zodb-pgjsonb==1.16.*"`) for the "fails before the fix" check; reinstall 1.17.0 afterwards.
- Execution order: Tasks 1, 2, 5, 3, 4, 6, 7, 8, 9. Task 5 hardens Tasks 1 and 2; Task 9 documents Tasks 5 to 8.
- Lock order for the Tika race fix: zodb-pgjsonb writes `object_state` rows first and calls `CatalogStateProcessor.finalize()` (queue upsert) afterwards, in the same PG transaction (`zodb_pgjsonb/instance.py`, tpc_vote). The worker must take its locks in the same order (`object_state` merge, then queue row), or worker and editor deadlock.

## Review Focus

1. **Tools and the catalog itself are not cataloged.** `portal_setup`, `acl_users`, `portal_catalog` etc. never show up in the repaired paths. Pinned in Task 1 (exact path set).
2. **Intact content is not rewritten.** Default mode leaves the `tid` of healthy rows unchanged. Pinned in Task 1.
3. **One broken object does not abort the run.** An exception from `catalog_object()` is logged, the path goes to `failed`, the rest continues. Pinned in Task 1.
4. **Dry run writes nothing.** No catalog data and no ZODB commit. Pinned in Task 1.
5. **Upgrade step without the PG catalog.** No-op with a log line, no exception. Pinned in Task 2.
6. **Conflict on a live site does not abort the run.** A `ConflictError` at batch commit aborts that batch, retries it once, and on a second conflict reports its paths as `failed`. Pinned in Task 5.
7. **No repair against the buggy storage.** `repair_uncataloged()` raises before touching anything when zodb-pgjsonb < 1.17.0. Pinned in Task 5.
8. **Tika race.** A full reindex that lands while the worker holds a job in `processing` must end with the text present: either the reindex re-pends the job and the worker's `done` update matches nothing (worker rolls back its merge), or the worker commits first and the reindex re-pends a `done` row. Never `done` with the text missing. Pinned in Tasks 6 and 7.
9. **`skipped` and `pending` rows are not touched by the upsert.** `skipped` would be refused again (pure churn), `pending` is already queued. Pinned in Task 6.
10. **`requeue_lost_extractions()` only requeues current blob versions.** It recatalogs the affected objects and lets the processor (with 1a) re-pend the rows of the blobs the object references now. It never re-pends queue rows by SQL, because an object can have several blob fields and older rows point to replaced blobs that still sit in `blob_state` until the next pack. Pinned in Task 8.

---

## File Structure

- `src/plone/pgcatalog/maintenance.py`: gains `_REBUILD_BATCH`, `_commit_and_minimize()` (moved from `catalog.py`), `RepairResult`, `_is_catalogable()`, `_healthy_zoids()`, `_repair_batch()`, `repair_uncataloged()`.
- `src/plone/pgcatalog/catalog.py`: imports `_REBUILD_BATCH` and `_commit_and_minimize` from `maintenance` instead of defining them.
- `src/plone/pgcatalog/upgrades/profile_4.py`, `profile_4.zcml`; `upgrades/configure.zcml` includes it; `profiles/default/metadata.xml` → 4.
- `tests/test_pg_integration.py`: `TestRepairUncataloged`, `TestRepairUpgradeStep`, `TestPlainWritesKeepCatalogData`.
- `docs/sources/how-to/rebuild-catalog.md`: new section.
- `pyproject.toml`: `zodb-pgjsonb` minimum.
- `CHANGES.md`.
- `src/plone/pgcatalog/catalog.py` (`_walk_site_paths`: `collections.deque`).
- `src/plone/pgcatalog/processor.py` (`_insert_queue_row`: upsert).
- `src/plone/pgcatalog/tika_worker.py` (merge + `done` in one transaction, all status updates guarded by `status = 'processing'`).
- `tests/test_tika_enqueue.py`, `tests/test_tika_worker.py`.
- `docs/sources/how-to/enable-tika-extraction.md`, `docs/sources/explanation/tika-extraction.md`.

---

### Task 1: `repair_uncataloged()` in `maintenance.py`

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py`
- Modify: `src/plone/pgcatalog/catalog.py:72` (`_REBUILD_BATCH`) and `:90-99` (`_commit_and_minimize`)
- Test: `tests/test_pg_integration.py` (append a class)

**Interfaces:**
- Consumes: `PlonePGCatalogTool._walk_site_paths(site)` yielding `(obj, path)`; `PlonePGCatalogTool.catalog_object(obj, uid)`; `plone.pgcatalog.pool.get_pool(catalog)` (dict-row pool connections, as `reindexIndex()` uses). Do not use `catalog._pg_connection()` here: it may hand out the request-scoped connection, and the repair commits ZODB transactions while holding it.
- Produces: `maintenance.RepairResult(checked: int, paths: list[str], failed: list[str])` (NamedTuple).
- Produces: `maintenance.repair_uncataloged(catalog, site, *, dry_run=False, all_objects=False, batch_size=_REBUILD_BATCH) -> RepairResult`.
- Produces: `maintenance._REBUILD_BATCH`, `maintenance._commit_and_minimize(jar)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pg_integration.py`:

```python
# ---------------------------------------------------------------------------
# Repair of content uncataloged by zodb-pgjsonb#120 (#244)
# ---------------------------------------------------------------------------


def _wipe_catalog_row(pg_functional, path):
    """Simulate zodb-pgjsonb#120 damage: catalog columns NULL, row kept."""
    test_db = pg_functional["pgTestDB"]
    with test_db.connection.cursor() as cur:
        cur.execute(
            "UPDATE object_state SET path = NULL, parent_path = NULL, "
            "path_depth = NULL, idx = NULL, searchable_text = NULL "
            "WHERE path = %s",
            (path,),
        )
        assert cur.rowcount == 1


def _row_by_zoid(pg_functional, obj):
    from ZODB.utils import u64

    rows = _query_pg(
        pg_functional,
        "SELECT path, idx IS NOT NULL, tid FROM object_state WHERE zoid = %s",
        (u64(obj._p_oid),),
    )
    return rows[0]


class TestRepairUncataloged:
    def _setup(self, pg_functional):
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        portal.invokeFactory("Folder", "rep-folder", title="Folder")
        portal["rep-folder"].invokeFactory("Document", "broken", title="Broken")
        portal.invokeFactory("Document", "intact", title="Intact")
        transaction.commit()
        _wipe_catalog_row(pg_functional, "/plone/rep-folder/broken")
        return portal, portal["portal_catalog"]

    def test_repairs_only_damaged_content(self, pg_functional):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal, catalog = self._setup(pg_functional)
        intact_before = _row_by_zoid(pg_functional, portal["intact"])

        result = repair_uncataloged(catalog, portal)
        transaction.commit()

        assert result.paths == ["/plone/rep-folder/broken"]
        assert result.failed == []
        assert result.checked >= 3
        path, has_idx, _ = _row_by_zoid(pg_functional, portal["rep-folder"]["broken"])
        assert (path, has_idx) == ("/plone/rep-folder/broken", True)
        # intact content is not rewritten
        assert _row_by_zoid(pg_functional, portal["intact"]) == intact_before

    def test_second_run_repairs_nothing(self, pg_functional):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal, catalog = self._setup(pg_functional)
        repair_uncataloged(catalog, portal)
        transaction.commit()
        assert repair_uncataloged(catalog, portal).paths == []

    def test_dry_run_writes_nothing(self, pg_functional):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal, catalog = self._setup(pg_functional)
        result = repair_uncataloged(catalog, portal, dry_run=True)
        transaction.commit()

        assert result.paths == ["/plone/rep-folder/broken"]
        path, has_idx, _ = _row_by_zoid(pg_functional, portal["rep-folder"]["broken"])
        assert (path, has_idx) == (None, False)

    def test_all_objects_recatalogs_intact_content_too(self, pg_functional):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal, catalog = self._setup(pg_functional)
        result = repair_uncataloged(catalog, portal, all_objects=True)
        transaction.commit()

        assert "/plone/intact" in result.paths
        assert "/plone/rep-folder/broken" in result.paths

    def test_never_catalogs_tools_or_the_catalog(self, pg_functional):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal, catalog = self._setup(pg_functional)
        result = repair_uncataloged(catalog, portal, all_objects=True, dry_run=True)

        for tool in ("portal_catalog", "portal_setup", "acl_users"):
            assert not any(p.startswith(f"/plone/{tool}") for p in result.paths), tool

    def test_one_failing_object_does_not_abort(self, pg_functional, monkeypatch):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal, catalog = self._setup(pg_functional)
        _wipe_catalog_row(pg_functional, "/plone/intact")
        original = type(catalog).catalog_object

        def flaky(self, obj, uid=None, *args, **kw):
            if uid == "/plone/rep-folder/broken":
                raise RuntimeError("boom")
            return original(self, obj, uid, *args, **kw)

        monkeypatch.setattr(type(catalog), "catalog_object", flaky)
        result = repair_uncataloged(catalog, portal)
        transaction.commit()

        assert result.failed == ["/plone/rep-folder/broken"]
        assert result.paths == ["/plone/intact"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pg_integration.py -k TestRepairUncataloged -v`
Expected: FAIL with `ImportError: cannot import name 'repair_uncataloged'`.

- [ ] **Step 3: Move the batch helpers**

Cut `_REBUILD_BATCH = 500` (with its comment) and `_commit_and_minimize()` from `catalog.py` and paste them into `maintenance.py` below `_REINDEX_BATCH_SIZE`. Add `import transaction` to `maintenance.py`'s imports. In `catalog.py` add:

```python
from plone.pgcatalog.maintenance import _commit_and_minimize
from plone.pgcatalog.maintenance import _REBUILD_BATCH
```

next to the other `maintenance` imports (keep ruff's isort order). Check that `catalog.py` still uses `transaction` elsewhere before removing its import (`grep -n "transaction\." src/plone/pgcatalog/catalog.py`); leave it if used.

- [ ] **Step 4: Implement the repair**

Add to `maintenance.py` imports: `from plone.pgcatalog.pool import get_pool`, `from typing import NamedTuple`, `from ZODB.utils import u64`, `from zope.component.hooks import site as site_context`. Then, after `resync_gopip()`:

```python
class RepairResult(NamedTuple):
    """Outcome of :func:`repair_uncataloged`."""

    checked: int  # catalogable objects visited
    paths: list  # recataloged paths (in a dry run: would be recataloged)
    failed: list  # paths whose catalog_object() raised


def _is_catalogable(obj, catalog):
    """Plone's own rebuild criterion: CMF-catalog-aware, never the catalog."""
    base = aq_base(obj)
    if base is aq_base(catalog):
        return False
    return callable(getattr(base, "reindexObject", None))


def _healthy_zoids(conn, zoids):
    """Return the subset of *zoids* whose rows carry catalog data."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT zoid FROM object_state WHERE zoid = ANY(%s) "
            "AND path IS NOT NULL AND idx IS NOT NULL",
            (zoids,),
        )
        return {row["zoid"] for row in cur.fetchall()}


def _repair_batch(catalog, conn, batch, dry_run, all_objects):
    """Recatalog the damaged entries of *batch*; return (paths, failed)."""
    if all_objects:
        todo = batch
    else:
        healthy = _healthy_zoids(conn, [zoid for zoid, _, _ in batch])
        todo = [entry for entry in batch if entry[0] not in healthy]
    paths, failed = [], []
    for _, obj, path in todo:
        if dry_run:
            paths.append(path)
            continue
        try:
            catalog.catalog_object(obj, path)
        except Exception:
            log.warning("repair_uncataloged: failed to catalog %s", path, exc_info=True)
            failed.append(path)
        else:
            paths.append(path)
    return paths, failed


def repair_uncataloged(
    catalog, site, *, dry_run=False, all_objects=False, batch_size=_REBUILD_BATCH
):
    """Recatalog content whose catalog columns are missing, without clearing.

    Repairs rows wiped by zodb-pgjsonb#120: the object exists in the ZODB,
    but ``path`` or ``idx`` is NULL, so it is invisible to catalog queries
    and partial reindexes skip it.  Walks *site* like
    ``clearFindAndRebuild()`` but only touches catalog-aware objects whose
    row lacks catalog data (or every catalog-aware object with
    ``all_objects=True``).  Commits every *batch_size* objects; idempotent,
    so an interrupted run is simply repeated.  With ``dry_run=True`` it
    only reports and writes nothing.

    Returns:
        RepairResult(checked, paths, failed)
    """
    jar = catalog._p_jar
    checked = 0
    paths, failed = [], []
    batch = []

    def flush():
        nonlocal checked, batch
        checked += len(batch)
        done, broken = _repair_batch(catalog, conn, batch, dry_run, all_objects)
        paths.extend(done)
        failed.extend(broken)
        batch = []
        if dry_run:
            jar.cacheMinimize()
        else:
            _commit_and_minimize(jar)

    pool = get_pool(catalog)
    conn = pool.getconn()
    try:
        with site_context(site):
            for obj, path in catalog._walk_site_paths(site):
                oid = getattr(aq_base(obj), "_p_oid", None)
                if oid is None or not _is_catalogable(obj, catalog):
                    continue
                batch.append((u64(oid), obj, path))
                if len(batch) >= batch_size:
                    flush()
            if batch:
                flush()
    finally:
        pool.putconn(conn)

    log.info(
        "repair_uncataloged: %d checked, %d %s, %d failed",
        checked,
        len(paths),
        "would be recataloged" if dry_run else "recataloged",
        len(failed),
    )
    return RepairResult(checked, paths, failed)
```

If ruff flags `paths: list` style, use `list[str]`. If C901 complains, the nested `flush()` counts separately; keep it.

- [ ] **Step 5: Run the tests**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pg_integration.py -k "TestRepairUncataloged or TestMaintenanceOps" -v`
Expected: all PASS (`TestMaintenanceOps` guards the moved helpers). If `test_repairs_only_damaged_content` reports extra paths, print them: they are catalog-aware objects in the test site without catalog data; check whether Plone would catalog them before widening the filter or the assertion.

- [ ] **Step 6: Full suite, lint, commit**

Run: `env -u ZODB_TEST_DSN uv run pytest -q` then `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`

```bash
git add src/plone/pgcatalog/maintenance.py src/plone/pgcatalog/catalog.py tests/test_pg_integration.py
git commit -m "feat: repair_uncataloged() recatalogs content wiped by zodb-pgjsonb#120 (#244)

Walks the site without clearing the catalog and recatalogs only
catalog-aware objects whose row lacks path or idx. refreshCatalog()
cannot do this: it only visits rows that still have catalog data.

Assisted-by: Claude Opus 5.5"
```

---

### Task 2: Upgrade step 3 → 4

**Files:**
- Create: `src/plone/pgcatalog/upgrades/profile_4.py`
- Create: `src/plone/pgcatalog/upgrades/profile_4.zcml`
- Modify: `src/plone/pgcatalog/upgrades/configure.zcml`
- Modify: `src/plone/pgcatalog/profiles/default/metadata.xml`
- Test: `tests/test_pg_integration.py`

**Interfaces:**
- Consumes: `maintenance.repair_uncataloged(catalog, site, ...) -> RepairResult` (Task 1); `_wipe_catalog_row`, `_row_by_zoid` test helpers (Task 1).
- Produces: `upgrades.profile_4.repair_uncataloged_content(context) -> None`.

- [ ] **Step 1: Write the failing tests**

```python
class TestRepairUpgradeStep:
    def test_upgrade_step_repairs(self, pg_functional):
        from plone.pgcatalog.upgrades.profile_4 import repair_uncataloged_content

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        portal.invokeFactory("Document", "upg-doc", title="Upgrade")
        transaction.commit()
        _wipe_catalog_row(pg_functional, "/plone/upg-doc")

        repair_uncataloged_content(portal["portal_setup"])
        transaction.commit()

        path, has_idx, _ = _row_by_zoid(pg_functional, portal["upg-doc"])
        assert (path, has_idx) == ("/plone/upg-doc", True)

    def test_upgrade_step_noop_without_pg_catalog(self, caplog):
        from plone.pgcatalog.upgrades.profile_4 import repair_uncataloged_content

        class FakeSite:
            portal_catalog = object()

        class FakeContext:
            def getSite(self):
                return FakeSite()

        with caplog.at_level("INFO"):
            repair_uncataloged_content(FakeContext())
        assert "PG catalog not active" in caplog.text

    def test_profile_version_is_4(self):
        from pathlib import Path

        import plone.pgcatalog

        metadata = (
            Path(plone.pgcatalog.__file__).parent / "profiles/default/metadata.xml"
        ).read_text()
        assert "<version>4</version>" in metadata
```

- [ ] **Step 2: Run them to verify they fail**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pg_integration.py -k TestRepairUpgradeStep -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'plone.pgcatalog.upgrades.profile_4'`.

- [ ] **Step 3: Implement**

`src/plone/pgcatalog/upgrades/profile_4.py`:

```python
"""Profile v3 -> v4 upgrade: recatalog content wiped by zodb-pgjsonb#120.

Before zodb-pgjsonb's fix, any write of a cataloged object without a full
reindex (edit lock, sharing tab, write plus partial reindex) NULLed its
catalog columns.  Such objects are invisible to catalog queries, and
partial reindexes skip them.  This step runs
``maintenance.repair_uncataloged`` once.  The package requires the fixed
zodb-pgjsonb release, so the repaired rows cannot be wiped again.
"""

from Acquisition import aq_parent
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.maintenance import repair_uncataloged

import logging


log = logging.getLogger(__name__)


def repair_uncataloged_content(context):
    """Recatalog content whose catalog columns are missing.

    Accepts either shape GenericSetup hands to upgrade handlers (the
    portal_setup tool or an ImportContext).  No-ops when the active
    catalog is not the PG tool.
    """
    getSite = getattr(context, "getSite", None)
    site = getSite() if getSite is not None else aq_parent(context)
    if site is None:
        log.warning("repair_uncataloged_content: cannot resolve site; skipping")
        return

    catalog = getattr(site, "portal_catalog", None)
    if catalog is None or not IPGCatalogTool.providedBy(catalog):
        log.info("repair_uncataloged_content: PG catalog not active; skipping")
        return

    result = repair_uncataloged(catalog, site)
    log.info(
        "repair_uncataloged_content: %d recataloged, %d failed (of %d checked)",
        len(result.paths),
        len(result.failed),
        result.checked,
    )
```

`src/plone/pgcatalog/upgrades/profile_4.zcml`:

```xml
<configure
    xmlns="http://namespaces.zope.org/zope"
    xmlns:gs="http://namespaces.zope.org/genericsetup"
    i18n_domain="plone.pgcatalog"
    >

  <gs:upgradeStep
      profile="plone.pgcatalog:default"
      source="3"
      destination="4"
      title="Recatalog content uncataloged by zodb-pgjsonb#120"
      description="Writes without a full reindex (edit lock, sharing tab,
                   write plus partial reindex) used to NULL the catalog
                   columns, so the content vanished from listings and
                   search.  Recatalog every content object whose row
                   lacks catalog data.  Walks the whole site; on large
                   sites run it from the command line.  See #244."
      handler=".profile_4.repair_uncataloged_content"
      />

</configure>
```

Add `<include file="profile_4.zcml" />` to `upgrades/configure.zcml`; set `<version>4</version>` in `profiles/default/metadata.xml`.

- [ ] **Step 4: Run the tests**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pg_integration.py -k "TestRepairUpgradeStep or TestRepairUncataloged" tests/test_setuphandlers.py -v`
Expected: all PASS. If a setuphandlers test pins the profile version `3`, update it to `4`.

- [ ] **Step 5: Lint and commit**

```bash
uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .
git add src/plone/pgcatalog/upgrades/ src/plone/pgcatalog/profiles/default/metadata.xml tests/test_pg_integration.py
git commit -m "feat: upgrade step 3 -> 4 recatalogs content wiped by zodb-pgjsonb#120 (#244)

Assisted-by: Claude Opus 5.5"
```

---

### Task 3: Documentation and changelog

**Files:**
- Modify: `docs/sources/how-to/rebuild-catalog.md`
- Modify: `CHANGES.md`

- [ ] **Step 1: How-to section**

In `rebuild-catalog.md`, add a section after "Selective reindex (reindexIndex)":

````markdown
## Repair uncataloged content (repair_uncataloged)

Recatalogs content that exists in the ZODB but has no catalog data, without clearing the catalog.
Content got into this state through zodb-pgjsonb before the fix for
[zodb-pgjsonb#120](https://github.com/bluedynamics/zodb-pgjsonb/issues/120):
opening the edit form, the sharing tab, or any write without a full reindex.
The upgrade step to profile version 4 runs it once.
`refreshCatalog(clear=0)` cannot repair these objects, because it only visits rows that still have catalog data.

On large sites, run it from the command line instead of `@@plone-upgrade`, which can hit proxy timeouts.
Save this as `repair_uncataloged.py`:

```python
"""Run: zconsole run etc/zope.conf repair_uncataloged.py SITE_ID [--dry-run] [--all]"""

from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SpecialUsers import system
from plone.pgcatalog.maintenance import repair_uncataloged
from zope.component.hooks import setSite

import sys
import transaction

# zconsole does not reset sys.argv: [zconsole, run, zope.conf, script, *args]
args = sys.argv[4:]
site_id = next(a for a in args if not a.startswith("--"))
dry_run = "--dry-run" in args

site = app[site_id]  # noqa: F821  (app is provided by zconsole)
setSite(site)
newSecurityManager(None, system)

result = repair_uncataloged(
    site.portal_catalog, site, dry_run=dry_run, all_objects="--all" in args
)
for path in result.paths:
    print(path)
print(
    f"{result.checked} checked, {len(result.paths)} "
    f"{'would be recataloged' if dry_run else 'recataloged'}, "
    f"{len(result.failed)} failed"
)
if dry_run:
    transaction.abort()
else:
    transaction.commit()
```

Count first with `--dry-run` (on a copy of production if possible), then run without it.
The run commits every 500 objects and can be repeated after an interruption.
`--all` recatalogs every content object, not only the damaged ones.

When Tika extraction is configured, recataloged files are enqueued for extraction again, which restores their extracted text.
Expect a burst of extraction jobs.
````

Also add a row to the "Choosing the right operation" table:

```markdown
| `repair_uncataloged()` | No | Yes | ~15 ms per repaired object | Content missing from listings and search but present in the ZODB |
```

- [ ] **Step 2: Changelog**

Add at the top of `CHANGES.md` (above `## 1.0.0rc5`):

```markdown
## Unreleased

### Fixed

- Recatalog content that zodb-pgjsonb before the fix for
  bluedynamics/zodb-pgjsonb#120 left without catalog data. Any write of a
  cataloged object without a full reindex NULLed its catalog columns, for
  example opening the edit form (edit lock), the sharing tab, or a write
  followed by a partial reindex. The object stayed in the ZODB but vanished
  from listings and search, and partial reindexes skipped it. New
  `maintenance.repair_uncataloged()` walks the site without clearing the
  catalog and recatalogs only content whose row lacks catalog data. The
  upgrade step to profile version 4 runs it once; the how-to "Rebuild or
  reindex the catalog" shows how to run it from the command line with a
  dry run. Requires the zodb-pgjsonb release with the fix, so repaired
  content cannot be wiped again. #244
```

(Task 4 replaces "the zodb-pgjsonb release with the fix" with the concrete version.)

- [ ] **Step 3: Commit**

```bash
git add docs/sources/how-to/rebuild-catalog.md CHANGES.md
git commit -m "docs: how to repair content uncataloged by zodb-pgjsonb#120 (#244)

Assisted-by: Claude Opus 5.5"
```

---

### Task 4: Regression tests for the triggers and minimum version

zodb-pgjsonb 1.17.0 is released, so this task is no longer blocked.

**Files:**
- Modify: `pyproject.toml:29`
- Modify: `tests/test_pg_integration.py` (append a class; fix one stale comment in `TestMaintenanceOps.test_refresh_catalog_recatalogs_existing`)
- Modify: `CHANGES.md`

**Interfaces:**
- Consumes: `_row_by_zoid` (Task 1).

- [ ] **Step 1: Write the tests**

```python
class TestPlainWritesKeepCatalogData:
    """Writes without a full reindex must not uncatalog (zodb-pgjsonb#120)."""

    def _doc(self, pg_functional, doc_id):
        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        login(portal, TEST_USER_NAME)
        portal.invokeFactory("Document", doc_id, title="Doc")
        transaction.commit()
        doc = portal[doc_id]
        assert _row_by_zoid(pg_functional, doc)[:2] == (f"/plone/{doc_id}", True)
        return portal, doc

    def _assert_cataloged(self, pg_functional, doc, doc_id):
        assert _row_by_zoid(pg_functional, doc)[:2] == (f"/plone/{doc_id}", True)
        logout()

    def test_plain_attribute_write(self, pg_functional):
        portal, doc = self._doc(pg_functional, "pw-plain")
        doc.some_attr = 1
        transaction.commit()
        self._assert_cataloged(pg_functional, doc, "pw-plain")

    def test_write_plus_partial_reindex(self, pg_functional):
        portal, doc = self._doc(pg_functional, "pw-partial")
        doc.some_attr = 1
        doc.notifyModified()
        doc.reindexObject(idxs=["modified"])
        transaction.commit()
        self._assert_cataloged(pg_functional, doc, "pw-partial")

    def test_local_roles_plus_security_reindex(self, pg_functional):
        portal, doc = self._doc(pg_functional, "pw-sharing")
        doc.manage_setLocalRoles("someone", ["Reader"])
        doc.reindexObjectSecurity()
        transaction.commit()
        self._assert_cataloged(pg_functional, doc, "pw-sharing")

    def test_edit_lock(self, pg_functional):
        from plone.locking.interfaces import ILockable

        portal, doc = self._doc(pg_functional, "pw-lock")
        ILockable(doc).lock()
        transaction.commit()
        self._assert_cataloged(pg_functional, doc, "pw-lock")
```

Also add the extra triggers found in bluedynamics/zodb-pgjsonb#122 (remdub), one test each in the same class, same shape (write, commit, `_assert_cataloged`):

```python
def test_display_menu_layout(self, pg_functional):
    portal, doc = self._doc(pg_functional, "pw-layout")
    doc.setLayout("document_view")
    transaction.commit()
    self._assert_cataloged(pg_functional, doc, "pw-layout")


def test_image_scale_in_listing(self, pg_functional):
    """Live search / listings create scales: plone.scale annotation + safeWrite."""
    from plone.namedfile.file import NamedBlobImage
    from plone.pgcatalog.testing import TEST_IMAGE_PNG  # 1x1 PNG bytes; add if missing

    portal = pg_functional["portal"]
    setRoles(portal, TEST_USER_ID, ["Manager"])
    login(portal, TEST_USER_NAME)
    portal.invokeFactory("Image", "pw-img", title="Img")
    img = portal["pw-img"]
    img.image = NamedBlobImage(data=TEST_IMAGE_PNG, filename="x.png")
    img.reindexObject()
    transaction.commit()
    img.restrictedTraverse("@@images").scale("image", scale="thumb")
    transaction.commit()
    self._assert_cataloged(pg_functional, img, "pw-img")
```

If `plone.namedfile`'s scale storage lives in a separate persistent annotation object in this Plone version (so the image itself is not written), the test passes on the old storage too; keep it anyway as a guard, and note it in the report. #122 also names `api.content.transition()` and renaming or moving a folder. Check whether existing tests in `test_pg_integration.py` / `test_move_integration.py` already assert `path` and `idx` survive a transition and a folder rename/move (including the moved folder's own row). Add one test per case that is not covered, using `portal.portal_workflow.doActionFor(doc, "publish")` and `portal.manage_renameObject(...)` with the same assertion shape.

- [ ] **Step 2: Confirm they fail on the old storage, pass on the fixed one**

Run with the released (old) zodb-pgjsonb first: `env -u ZODB_TEST_DSN uv run pytest tests/test_pg_integration.py -k TestPlainWritesKeepCatalogData -v`
Expected: all four FAIL with `(None, False)`. If `test_edit_lock` passes here, the lock does not write the object in this setup: investigate (is `plone.locking` behaviour enabled on Document? does `ILockable(doc)` adapt?) and report back instead of deleting the test.

Then reinstall `zodb-pgjsonb>=1.17.0` and run again.
Expected: all four PASS.

- [ ] **Step 3: Fix the stale comment**

In `TestMaintenanceOps.test_refresh_catalog_recatalogs_existing`, the comment `# Change title in memory (do NOT commit — that would NULL idx)` describes the #120 bug. Change it to `# Change title in memory only; refreshCatalog must pick it up from ZODB`.

- [ ] **Step 4: Raise the minimum version**

`pyproject.toml`: `"zodb-pgjsonb>=1.17.0",`. In `CHANGES.md` replace "Requires the zodb-pgjsonb release with the fix" with "Requires zodb-pgjsonb >= 1.17.0".

- [ ] **Step 5: Full suite, lint, commit**

Run: `env -u ZODB_TEST_DSN uv run pytest -q` then `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`

```bash
git add pyproject.toml tests/test_pg_integration.py CHANGES.md
git commit -m "test: writes without full reindex keep catalog data; require fixed zodb-pgjsonb (#244)

Pins the four known triggers of zodb-pgjsonb#120: plain write, write
plus partial reindex, local roles plus reindexObjectSecurity, and the
edit lock.

Assisted-by: Claude Opus 5.5"
```

---

### Task 5: Harden the repair for production

Runs after Task 2.

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py`
- Modify: `src/plone/pgcatalog/catalog.py` (`_walk_site_paths`)
- Modify: `src/plone/pgcatalog/upgrades/profile_4.py`
- Test: `tests/test_pg_integration.py`

**Interfaces:**
- Produces: `maintenance._check_storage_fixed() -> None` (raises `RuntimeError`).
- Produces: `maintenance._commit_batch(jar, redo) -> tuple[list, list]`, shared with Task 8.
- Produces: `maintenance.count_uncataloged_candidates(conn) -> int`.

- [ ] **Step 1: Write the failing tests**

Append to the repair test classes in `tests/test_pg_integration.py`:

```python
class TestRepairHardening:
    def test_refuses_unfixed_storage(self, pg_functional, monkeypatch):
        from plone.pgcatalog import maintenance

        portal = pg_functional["portal"]
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: "1.16.2")
        with pytest.raises(RuntimeError, match="zodb-pgjsonb"):
            maintenance.repair_uncataloged(portal["portal_catalog"], portal)

    def test_conflict_retries_batch_once(self, pg_functional, monkeypatch):
        from plone.pgcatalog import maintenance
        from ZODB.POSException import ConflictError

        portal, catalog = TestRepairUncataloged()._setup(pg_functional)
        original = maintenance._commit_and_minimize
        calls = []

        def conflict_once(jar):
            calls.append(1)
            if len(calls) == 1:
                raise ConflictError("simulated")
            return original(jar)

        monkeypatch.setattr(maintenance, "_commit_and_minimize", conflict_once)
        result = maintenance.repair_uncataloged(catalog, portal)

        assert result.paths == ["/plone/rep-folder/broken"]
        assert result.failed == []
        path, has_idx, _ = _row_by_zoid(pg_functional, portal["rep-folder"]["broken"])
        assert (path, has_idx) == ("/plone/rep-folder/broken", True)

    def test_second_conflict_reports_batch_as_failed(self, pg_functional, monkeypatch):
        from plone.pgcatalog import maintenance
        from ZODB.POSException import ConflictError

        portal, catalog = TestRepairUncataloged()._setup(pg_functional)

        def always_conflict(jar):
            raise ConflictError("simulated")

        monkeypatch.setattr(maintenance, "_commit_and_minimize", always_conflict)
        result = maintenance.repair_uncataloged(catalog, portal)

        assert result.paths == []
        assert result.failed == ["/plone/rep-folder/broken"]

    def test_analyze_after_repair(self, pg_functional, caplog):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal, catalog = TestRepairUncataloged()._setup(pg_functional)
        with caplog.at_level("INFO"):
            repair_uncataloged(catalog, portal)
        assert "ANALYZE object_state" in caplog.text

    def test_candidate_count_sees_a_wiped_row(self, pg_functional):
        from plone.pgcatalog.maintenance import count_uncataloged_candidates

        portal = pg_functional["portal"]
        setRoles(portal, TEST_USER_ID, ["Manager"])
        portal.invokeFactory("Document", "cnt-doc", title="Count")
        transaction.commit()
        conn = pg_functional["pgTestDB"].connection
        before = count_uncataloged_candidates(conn)
        _wipe_catalog_row(pg_functional, "/plone/cnt-doc")
        # Delta, not absolute: other tests leave unpacked deleted objects behind.
        assert count_uncataloged_candidates(conn) == before + 1
```

And in `TestRepairUpgradeStep`:

```python
    def test_upgrade_step_skips_walk_without_candidates(self, pg_functional, monkeypatch):
        from plone.pgcatalog.upgrades import profile_4

        portal = pg_functional["portal"]
        monkeypatch.setattr(profile_4, "count_uncataloged_candidates", lambda conn: 0)
        monkeypatch.setattr(
            profile_4,
            "repair_uncataloged",
            lambda *a, **kw: pytest.fail("walk must be skipped"),
        )
        profile_4.repair_uncataloged_content(portal["portal_setup"])
```

Expected failure: `AttributeError`/`ImportError` on the new names.

- [ ] **Step 2: Implement**

In `maintenance.py` (module-level imports: `from importlib.metadata import version as _dist_version`, `from ZODB.POSException import ConflictError`, `import importlib`, `import re`):

```python
_PGJSONB_FIXED = (1, 17, 0)  # zodb-pgjsonb#120: plain writes no longer wipe columns


def _check_storage_fixed():
    """Refuse to repair against a zodb-pgjsonb that wipes catalog columns.

    The version pin in pyproject.toml only binds at install time; a build
    with --no-deps or overridden constraints can still run the old storage,
    which would uncatalog repaired objects again on their next plain write.
    """
    installed = _dist_version("zodb-pgjsonb")
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", installed)
    if match is None or tuple(map(int, match.groups())) < _PGJSONB_FIXED:
        raise RuntimeError(
            f"zodb-pgjsonb {installed} still wipes catalog columns on plain "
            "writes (zodb-pgjsonb#120); upgrade to >= 1.17.0 before repairing"
        )
```

`_commit_batch(jar, redo)`: commit; on `ConflictError` call `transaction.abort()`, log, call `redo()` (which re-runs `_repair_batch`, including the healthy check, because a concurrent editor may have recataloged meanwhile) and commit again; on a second `ConflictError` abort and return `([], done + broken)` of the retry. Return `(done, broken)` of whichever attempt committed. Rewrite `flush()` in `repair_uncataloged()` to use it for non-dry runs (`current, batch = batch, []` first, so `redo` closes over the right list).

At the top of `repair_uncataloged()`: `_check_storage_fixed()`. At the end of a non-dry run with `paths`: `ANALYZE object_state` on `conn` and `log.info("repair_uncataloged: ANALYZE object_state done")` (see #224). Check `pool.py` whether pool connections are autocommit; if not, `conn.commit()` after the ANALYZE.

`count_uncataloged_candidates(conn)`:

```python
def _catalogable_class(class_mod, class_name):
    """True when instances of the class could be cataloged (or unknown)."""
    try:
        cls = getattr(importlib.import_module(class_mod), class_name)
    except Exception:
        return True  # unknown or broken class: count it, the walk decides
    return callable(getattr(cls, "reindexObject", None))


def count_uncataloged_candidates(conn):
    """Upper bound for what repair_uncataloged() can find.

    Counts rows without catalog data whose class has ``reindexObject``.
    Deleted but unpacked objects and catalog-aware objects Plone never
    catalogs are included, so a positive count does not prove damage.
    Zero proves there is none, and the site walk can be skipped.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT class_mod, class_name, count(*) AS n FROM object_state "
            "WHERE idx IS NULL OR path IS NULL GROUP BY class_mod, class_name"
        )
        rows = cur.fetchall()
    return sum(
        row["n"]
        for row in rows
        if _catalogable_class(row["class_mod"], row["class_name"])
    )
```

(If the test connection returns tuple rows, adapt the test, not the function: production uses dict-row pool connections.)

In `profile_4.repair_uncataloged_content()`, after the PG catalog check: borrow a pool connection, call `count_uncataloged_candidates()`, and when it is 0 log `"repair_uncataloged_content: no candidates; skipping site walk"` and return. Import both names at module top so the test can monkeypatch them on `profile_4`.

In `catalog.py` `_walk_site_paths()`: `queue = deque([site_path])` and `queue.popleft()` (`from collections import deque` at module top). `list.pop(0)` is O(n) per call, so the walk was quadratic in the number of paths.

- [ ] **Step 3: Run, lint, commit**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pg_integration.py -k "TestRepair" -v`, then the full suite and ruff.

```bash
git commit -m "feat: harden repair_uncataloged() for live sites (#244)

Refuses zodb-pgjsonb < 1.17.0 at runtime, retries a conflicting batch
once, runs ANALYZE afterwards, and lets the upgrade step skip the site
walk when no row can be damaged. The site walk no longer pops from the
front of a list.

Assisted-by: Claude Opus 5.5"
```

---

### Task 6: A full reindex re-extracts (stopgap 1a)

**Files:**
- Modify: `src/plone/pgcatalog/processor.py` (`_insert_queue_row`)
- Test: `tests/test_tika_enqueue.py`

**Interfaces:**
- Changes: `CatalogStateProcessor._insert_queue_row(cursor, zoid, blob_zoid, tid, content_type)` now re-pends an existing row.

Background: the queue is unique on `(blob_zoid, tid)` and the blob's `tid` does not change on a metadata edit, so `ON CONFLICT DO NOTHING` meant a full reindex (which overwrites `searchable_text`) never got the extracted text back. #247 replaces this stopgap with a stored extraction column.

- [ ] **Step 1: Write the failing tests**

Next to `test_insert_queue_row_inserts_and_is_idempotent` (keep that test: a second insert on a `pending` row must still leave one untouched `pending` row):

```python
@pytest.mark.parametrize("status", ["done", "failed", "processing"])
def test_insert_queue_row_repends_finished_rows(self, pg_conn_with_queue, status):
    conn = pg_conn_with_queue
    proc = CatalogStateProcessor()
    with conn.cursor(row_factory=dict_row) as cur:
        proc._insert_queue_row(
            cur, zoid=42, blob_zoid=43, tid=7, content_type="application/pdf"
        )
        cur.execute(
            "UPDATE text_extraction_queue SET status = %s, attempts = 3, "
            "deferrals = 2, error = 'x', not_before = now() + interval '1 hour'",
            (status,),
        )
        proc._insert_queue_row(
            cur, zoid=42, blob_zoid=43, tid=7, content_type="application/pdf"
        )
    conn.commit()

    (row,) = self._get_queue(conn)
    assert row["status"] == "pending"
    assert (row["attempts"], row["deferrals"], row["error"]) == (0, 0, None)
    assert row["not_before"] <= datetime.now(timezone.utc)


@pytest.mark.parametrize("status", ["pending", "skipped"])
def test_insert_queue_row_leaves_pending_and_skipped_alone(
    self, pg_conn_with_queue, status
):
    conn = pg_conn_with_queue
    proc = CatalogStateProcessor()
    with conn.cursor(row_factory=dict_row) as cur:
        proc._insert_queue_row(
            cur, zoid=42, blob_zoid=43, tid=7, content_type="image/png"
        )
        cur.execute(
            "UPDATE text_extraction_queue SET status = %s, attempts = 2, error = 'kept'",
            (status,),
        )
        proc._insert_queue_row(
            cur, zoid=42, blob_zoid=43, tid=7, content_type="image/png"
        )
    conn.commit()

    (row,) = self._get_queue(conn)
    assert (row["status"], row["attempts"], row["error"]) == (status, 2, "kept")
```

(Module-level `from datetime import datetime, timezone` if not imported yet.)

- [ ] **Step 2: Implement**

```python
    def _insert_queue_row(self, cursor, zoid, blob_zoid, tid, content_type):
        # A full reindex rewrote searchable_text without the extracted text,
        # so a finished job for the same blob version has to run again (#244).
        # pending: already queued.  skipped: the allowlist would refuse it again.
        # Replaced by a stored extraction column in #247.
        cursor.execute(
            "INSERT INTO text_extraction_queue "
            "  (zoid, blob_zoid, tid, content_type) "
            "VALUES (%(zoid)s, %(blob_zoid)s, %(tid)s, %(ct)s) "
            "ON CONFLICT (blob_zoid, tid) DO UPDATE SET "
            "  zoid = EXCLUDED.zoid, content_type = EXCLUDED.content_type, "
            "  status = 'pending', attempts = 0, deferrals = 0, error = NULL, "
            "  not_before = now(), updated_at = now() "
            "WHERE text_extraction_queue.status NOT IN ('pending', 'skipped')",
            {"zoid": zoid, "blob_zoid": blob_zoid, "tid": tid, "ct": content_type},
        )
```

The NOTIFY trigger fires on INSERT only, so a re-pended row is picked up by the worker's poll (the poll is documented as load-bearing in `TikaWorker.run`). Do not widen the trigger: `_defer()` also writes `pending` and would then wake the worker for jobs that are not due yet.

- [ ] **Step 3: Run, lint, commit**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_tika_enqueue.py -v`, then the full suite and ruff.

```bash
git commit -m "fix: a full reindex re-extracts the file's text instead of dropping it (#244)

The queue is unique on (blob_zoid, tid) and the blob's tid does not
change on a metadata edit, so the old done row blocked a new job while
the reindex had already overwritten searchable_text. Upsert and re-pend
finished rows. Stopgap until #247 stores the extracted text.

Assisted-by: Claude Opus 5.5"
```

---

### Task 7: Worker finishes only jobs it still owns

**Files:**
- Modify: `src/plone/pgcatalog/tika_worker.py` (`_process_one`, `_update_searchable_text`, `_skip`, `_defer`, failure path)
- Test: `tests/test_tika_worker.py`

Race being closed: the worker merges the text and marks the job `done` in two transactions. A full reindex in between overwrites `searchable_text`; with Task 6 it re-pends the row, but the worker's unguarded `UPDATE ... WHERE id = %s` then sets it to `done` again and the text is lost for good.

- [ ] **Step 1: Write the failing tests**

```python
class TestRequeuedWhileProcessing:
    """A full reindex during extraction must not end as 'done without text' (#244)."""

    def _repend(self, conn, zoid):
        conn.execute(
            "UPDATE text_extraction_queue SET status = 'pending', attempts = 0 "
            "WHERE zoid = %s",
            (zoid,),
        )
        conn.execute(
            "UPDATE object_state SET searchable_text = to_tsvector('simple', 'reindexed') "
            "WHERE zoid = %s",
            (zoid,),
        )
        conn.commit()

    def test_result_is_dropped_when_job_was_repended(self, worker_db):
        zoid = 901
        _insert_object_with_blob(worker_db, zoid=zoid)
        _enqueue_job(worker_db, zoid=zoid)
        worker = TikaWorker(dsn=DSN, tika_url="http://tika:9998")

        def extract_then_reindex(conn, blob_zoid, tid, content_type):
            # Simulate a full reindex committed by Zope while Tika runs.
            other = psycopg.connect(DSN, row_factory=dict_row)
            try:
                self._repend(other, zoid)
            finally:
                other.close()
            return "late text"

        with patch.object(worker, "_extract", side_effect=extract_then_reindex):
            assert worker._process_one() is True

        status = _get_queue_status(worker_db, zoid)
        assert status["status"] == "pending"  # will run again
        text = worker_db.execute(
            "SELECT searchable_text::text AS t FROM object_state WHERE zoid = %s",
            (zoid,),
        ).fetchone()["t"]
        assert "late" not in text  # merge rolled back with the 'done' update

    def test_failure_update_does_not_overwrite_a_repended_job(self, worker_db):
        zoid = 902
        _insert_object_with_blob(worker_db, zoid=zoid)
        _enqueue_job(worker_db, zoid=zoid)
        worker = TikaWorker(dsn=DSN, tika_url="http://tika:9998")

        def reindex_then_fail(conn, blob_zoid, tid, content_type):
            other = psycopg.connect(DSN, row_factory=dict_row)
            try:
                self._repend(other, zoid)
            finally:
                other.close()
            raise ValueError("broken document")

        with patch.object(worker, "_extract", side_effect=reindex_then_fail):
            worker._process_one()

        status = _get_queue_status(worker_db, zoid)
        assert (status["status"], status["error"]) == ("pending", None)
```

Check the helper names (`_insert_object_with_blob`, `_enqueue_job`, `_get_queue_status`) and the `worker_db` row factory against the top of `tests/test_tika_worker.py` and adapt. If the object row needs `idx` for the merge function's `WHERE idx IS NOT NULL`, set it in the helper call.

- [ ] **Step 2: Implement**

- `_update_searchable_text()` stops committing; it only executes the merge.
- In `_process_one()`, the success path becomes one transaction, in this lock order (same as zodb-pgjsonb's tpc_vote: `object_state` first, queue second, so worker and editor cannot deadlock):
  1. merge the text into `object_state`;
  2. `UPDATE text_extraction_queue SET status = 'done', error = NULL, updated_at = now() WHERE id = %(id)s AND status = 'processing'`;
  3. `rowcount == 1`: commit. `rowcount == 0`: rollback (drops the merge too) and `log.info("Job %d was re-queued during extraction; result dropped", job_id)`.
- Add `AND status = 'processing'` to the failure-path update, `_skip()` and `_defer()`.

Why this is complete: if the reindex commits before the worker's transaction, the row is already `pending`, the guard matches nothing, the merge is rolled back, and the job runs again on the fresh column. If the worker commits first, the reindex's upsert finds `done` and re-pends it. Worst case is one extra extraction, never a `done` row without text.

- [ ] **Step 3: Run, lint, commit**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_tika_worker.py -v`, then the full suite and ruff.

```bash
git commit -m "fix(tika): worker finishes only jobs still in processing (#244)

Merge and 'done' now share one transaction, and every status update is
guarded by status = 'processing'. A full reindex during extraction
re-pends the job, so the late result is dropped and the job runs again
instead of ending as done without text.

Assisted-by: Claude Opus 5.5"
```

---

### Task 8: `requeue_lost_extractions()`

**Files:**
- Modify: `src/plone/pgcatalog/maintenance.py`
- Test: `tests/test_pg_integration.py`

**Interfaces:**
- Consumes: Task 6 (recataloging re-pends finished rows), `_commit_batch` (Task 5), `_check_storage_fixed` (Task 5).
- Produces: `maintenance.RequeueResult(checked: int, paths: list[str], failed: list[str])`.
- Produces: `maintenance.requeue_lost_extractions(catalog, *, dry_run=False, include_failed=False, batch_size=_REBUILD_BATCH) -> RequeueResult`.

Weight `'C'` in `searchable_text` is written only by the Tika merge (title A, description B, body D). A cataloged object with a `done` extraction job but no `'C'` lexeme lost its text, either through a full reindex before Task 6, through #120 followed by a normal edit, or through `clearFindAndRebuild()`.

The function does **not** re-pend queue rows by SQL. An object can have several blob fields, and after a re-upload the old blob still sits in `blob_state` until the next pack, so "newest row per object" and "newest row per blob" are both wrong. Instead it recatalogs the object: the processor resolves the blobs the object references now and, with Task 6, re-pends exactly their rows.

- [ ] **Step 1: Write the failing tests**

Needs `TIKA_URL` set in the processor for candidates to be collected; follow how existing PG-layer Tika tests (if any) enable it, otherwise monkeypatch `plone.pgcatalog.processor.TIKA_URL` and `_should_extract`. The test creates a File with a PDF blob, commits, marks its queue row `done`, sets `searchable_text` to a tsvector without `'C'` weight, then:

- `dry_run=True` lists the path and leaves the queue row `done`;
- a real run lists the path, and the queue row is `pending` with `attempts = 0`;
- a File whose `searchable_text` has a `'C'` lexeme (`setweight(to_tsvector('simple','pdf words'),'C')`) is not listed;
- with `include_failed=True`, a File whose row is `failed` is recataloged and its row is `pending`; without it, the row stays `failed`.

- [ ] **Step 2: Implement**

```python
_LOST_EXTRACTION_SQL = """
SELECT o.zoid, o.path FROM object_state o
WHERE o.idx IS NOT NULL AND o.path IS NOT NULL
  AND (
    (EXISTS (SELECT 1 FROM text_extraction_queue q
             WHERE q.zoid = o.zoid AND q.status = 'done')
     AND (o.searchable_text IS NULL
          OR length(ts_filter(o.searchable_text, '{c}')) = 0))
    OR (%(include_failed)s AND EXISTS (
          SELECT 1 FROM text_extraction_queue q
          WHERE q.zoid = o.zoid AND q.status = 'failed'))
  )
ORDER BY o.zoid
"""
```

Fetch all `(zoid, path)` rows on a pool connection, then traverse each path with `catalog.unrestrictedTraverse(path, None)` inside `site_context(site)` (site = `catalog._find_site_root()`), call `catalog.catalog_object(obj, path)` unless `dry_run`, and commit per `batch_size` via `_commit_batch`. Missing objects go to `failed` with a log line. Call `_check_storage_fixed()` first. Log the totals like `repair_uncataloged()`.

Document in the docstring: files whose extraction legitimately produced no text (scanned PDFs without OCR, images without OCR, empty files) look exactly like lost ones and are recataloged on every run; the dry run shows how many. Telling them apart needs #247.

Not called from any upgrade step: a bulk requeue against Tika is an operator decision, after the worker's allowlist and the Tika image pin are checked.

- [ ] **Step 3: Run, lint, commit**

```bash
git commit -m "feat: requeue_lost_extractions() restores extracted text dropped by reindexes (#244)

Finds files with a finished extraction job but no weight-C lexeme left
in searchable_text and recatalogs them, so the processor re-pends the
jobs of the blobs they reference now. Optionally retries failed jobs
the same way.

Assisted-by: Claude Opus 5.5"
```

---

### Task 9: Docs and changelog for Tasks 5 to 8

**Files:**
- Modify: `docs/sources/how-to/rebuild-catalog.md`
- Modify: `docs/sources/how-to/enable-tika-extraction.md`
- Modify: `docs/sources/explanation/tika-extraction.md`
- Modify: `CHANGES.md`

- [ ] **Step 1: rebuild-catalog how-to**

In the "Repair uncataloged content" section from Task 3:
- Add a warning before the script: with Tika enabled, the repair enqueues extraction for every repaired file, so the worker must already have the same `PGCATALOG_TIKA_CONTENT_TYPES` as Zope and the Tika image must be pinned. This applies to the upgrade step as well.
- One sentence each: the run retries a batch once on a write conflict and reports paths that conflict twice; it runs `ANALYZE object_state` at the end; the upgrade step skips the site walk when no row can be damaged; the repair refuses zodb-pgjsonb < 1.17.0.
- Fix the `clearFindAndRebuild()` description if it claims extracted text survives or is re-extracted: before this release it was lost, from this release on it is re-extracted.

- [ ] **Step 2: Tika how-to**

New section "Restore lost extracted text" after the requeue section: what got lost and why (full reindex before this release, #120), the precondition (allowlist, pinned Tika), and a zconsole script analogous to the repair one (`--dry-run`, `--include-failed`) calling `requeue_lost_extractions()`. Note the false positives (empty extraction) and point to #247. Mention that `--include-failed` replaces the manual `UPDATE ... WHERE status = 'failed'` and only retries current blob versions.

- [ ] **Step 3: Tika explanation**

In "Queue table": `UNIQUE(blob_zoid, tid)` now also means a full reindex re-pends the finished job for the same blob version (because the reindex rewrites `searchable_text`), and the worker only finishes jobs still in `processing`. One line pointing to #247 as the planned replacement.

- [ ] **Step 4: Changelog**

Under `## Unreleased`, `### Fixed`:

```markdown
- A full reindex of a file (title edit, workflow transition,
  `clearFindAndRebuild()`) no longer drops its Tika-extracted text: the
  finished extraction job is queued again. The worker only finishes jobs
  that are still in `processing`, so a reindex during extraction cannot
  leave a job marked done without its text. Stopgap until #247. #244
- New `maintenance.requeue_lost_extractions()` finds files whose
  extracted text was already lost and queues their extraction again.
  Run it after the upgrade, once the worker's allowlist and the Tika
  image pin are in place. #244
```

And extend the Task 3 entry: the repair refuses zodb-pgjsonb < 1.17.0, retries conflicting batches, runs `ANALYZE`, and with Tika enabled enqueues extraction for repaired files, so the worker allowlist must be in place before the upgrade.

- [ ] **Step 5: Build the docs and commit**

Build the docs the way CI does (see `.github/workflows/` and the `docs/` Makefile) and fix warnings.

```bash
git commit -m "docs: Tika re-extraction on reindex, lost-text requeue, repair hardening (#244)

Assisted-by: Claude Opus 5.5"
```

---

### After the tasks

- Push and open a PR against `main` titled `fix: repair content uncataloged by zodb-pgjsonb#120 (#244)`, body in English, ending with `Assisted-by: Claude Opus 5.5` and `Fixes #244`. Mention #247 as the follow-up that replaces the Task 6 stopgap.
- Rollout per site (same as the issue's "Rollout" section):
  1. Worker has the same `PGCATALOG_TIKA_CONTENT_TYPES` as Zope, Tika image pinned and settled. Precondition for the upgrade itself, because the repair enqueues extraction for repaired files.
  2. Deploy the new plone.pgcatalog (pulls zodb-pgjsonb >= 1.17.0).
  3. Repair dry run on a production copy; review the paths.
  4. Repair (command line on large sites) or the upgrade step.
  5. `requeue_lost_extractions` dry run, review the count, then run it (with `--include-failed` where old failed rows exist, e.g. aaf's 1708).
