# pgcontent IMetricProvider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a `plone.observability` `IMetricProvider` (`pgcontent`) in plone.pgcatalog that produces `plone_content_total` / `plone_content_by_state` content-count metrics via SQL, only for `IPGCatalogTool`-backed sites, registered only when `plone.observability` is installed.

**Architecture:** A new module `observability.py` with a pure `_aggregate(conn, site_ids)` SQL helper (two `GROUP BY` queries over `object_state`), a `_pg_site_ids(context)` discovery helper (filters Plone sites to those whose `portal_catalog` provides `IPGCatalogTool`), and a thin `PGContentMetricProvider` adapter that wires them together with a process-global TTL cache. Registered via a conditional ZCML `<adapter>` on `OFS.interfaces.IApplication`.

**Tech Stack:** Python 3, psycopg (dict_row), zope.component/zope.interface ZCML, pytest, the repo's `pg_conn_with_catalog` DB fixture and `pgcatalog_layer` integration layer.

## Global Constraints

- Soft dependency: **no** hard `install_requires` on plone.observability. Adapter registers only via `zcml:condition="installed plone.observability"`.
- Migration-safe: emit **only** for sites whose `portal_catalog` provides `IPGCatalogTool`; leave ZCatalog sites to plone.observability's generic provider.
- Metric parity with the generic provider: names `plone_content_total` / `plone_content_by_state`, `type="gauge"`, `scope="global"`, help strings `"Number of content objects by portal type"` / `"Number of content objects by workflow state"`, labels include `site=<site-id>`.
- TTL cache must be **process-global** (the `@@metrics` view builds a fresh provider instance per request via `getAdapters`). TTL from `PLONE_OBSERVABILITY_METRICS_CACHE_TTL` (default `60`).
- `collect()` must never raise; degrade silently (debug log) on missing pool / DB error / per-site traversal failure.
- Commit messages end with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- Tests run against the `zodb_test` DB (zodb-pgjsonb-dev container). Run command:
  `env -u ZODB_TEST_DSN .venv/bin/pytest <path> -v`

## Verified facts (from the codebase, b6)

- `Metric` is `@dataclass Metric(name, value, type, scope, help, labels={})` in `plone.observability.metric`.
- `IMetricProvider` in `plone.observability.interfaces` declares `name`, `scope`, `collect()`. The `@@metrics` view iterates `getAdapters((app,), IMetricProvider)`.
- Generic `ContentMetricProvider` emits `plone_content_total{portal_type,site}` and `plone_content_by_state{state,site}` and steps aside (`except Exception` → debug log) on non-ZCatalog backends.
- `PlonePGCatalogTool` `@implementer(IPGCatalogTool, IZCatalog)`; `IPGCatalogTool` is in `plone.pgcatalog.interfaces`.
- Connection pool: `plone.pgcatalog.pool.get_pool(context)` returns a `psycopg_pool.ConnectionPool` (has `.connection()` context manager); raises `RuntimeError` if none found.
- `object_state` columns include `idx JSONB` and `path TEXT`; paths look like `/<siteid>/...` so `split_part(path,'/',2)` is the site id.
- Test helpers: `plone.pgcatalog.indexing.catalog_object(conn, zoid, path, idx)` writes `path`+`idx`; fixture `pg_conn_with_catalog` yields a `dict_row` connection with the catalog schema installed. The `pgcatalog_layer` fixture loads the package `configure.zcml` (`testing.py` `PGCatalogLayer.setUpZope`).
- `ANY(%(param)s)` with a Python list is the established parametrization (e.g. `WHERE zoid = ANY(%(zoids)s)`).

---

### Task 1: SQL aggregation helper `_aggregate`

**Files:**
- Create: `src/plone/pgcatalog/observability.py`
- Modify: `pyproject.toml` (add `plone.observability` to the `test` extra)
- Test: `tests/test_observability.py`

**Interfaces:**
- Produces: `_aggregate(conn, site_ids: list[str]) -> list[Metric]` — runs two `GROUP BY` queries restricted to `site_ids`, returns `plone_content_total` and `plone_content_by_state` `Metric` objects. Consumed by `PGContentMetricProvider.collect()` (Task 2).

- [ ] **Step 1: Add plone.observability to the test extra**

In `pyproject.toml`, locate the `test` optional-dependencies list (contains `pytest`, `pytest-cov`, `zope.pytestlayer`) and add a line:

```toml
    "plone.observability",
```

- [ ] **Step 2: Install it into the test venv**

Run: `env -u ZODB_TEST_DSN .venv/bin/python -m pip install plone.observability`
Expected: `Successfully installed plone.observability-…` (b6 or newer).

- [ ] **Step 3: Write the failing test**

Create `tests/test_observability.py`:

```python
"""Tests for the pgcontent plone.observability metric provider."""

from plone.pgcatalog.indexing import catalog_object

import pytest


@pytest.fixture(autouse=True)
def _clear_metric_cache():
    """The provider caches process-globally; reset around each test."""
    from plone.pgcatalog import observability

    observability._cache.clear()
    yield
    observability._cache.clear()


class TestAggregate:
    def test_counts_by_type_and_state_for_given_site(self, pg_conn_with_catalog):
        from plone.pgcatalog.observability import _aggregate

        conn = pg_conn_with_catalog
        catalog_object(conn, zoid=1, path="/sitea/d1",
                       idx={"portal_type": "Document", "review_state": "published"})
        catalog_object(conn, zoid=2, path="/sitea/d2",
                       idx={"portal_type": "Document", "review_state": "private"})
        catalog_object(conn, zoid=3, path="/sitea/n1",
                       idx={"portal_type": "News Item", "review_state": "published"})
        catalog_object(conn, zoid=4, path="/siteb/d3",
                       idx={"portal_type": "Document", "review_state": "published"})
        conn.commit()

        metrics = _aggregate(conn, ["sitea"])

        totals = {
            (m.labels["portal_type"], m.labels["site"]): m.value
            for m in metrics if m.name == "plone_content_total"
        }
        states = {
            (m.labels["state"], m.labels["site"]): m.value
            for m in metrics if m.name == "plone_content_by_state"
        }
        assert totals == {("Document", "sitea"): 2, ("News Item", "sitea"): 1}
        assert states == {("published", "sitea"): 2, ("private", "sitea"): 1}
        assert all(m.type == "gauge" and m.scope == "global" for m in metrics)
        assert all(m.labels["site"] == "sitea" for m in metrics)

    def test_excludes_null_index_values_and_other_sites(self, pg_conn_with_catalog):
        from plone.pgcatalog.observability import _aggregate

        conn = pg_conn_with_catalog
        # no portal_type / review_state in idx → excluded
        catalog_object(conn, zoid=10, path="/sitea/x", idx={"Title": "no type"})
        # different site → excluded by the site filter
        catalog_object(conn, zoid=11, path="/siteb/y",
                       idx={"portal_type": "Document", "review_state": "published"})
        conn.commit()

        assert _aggregate(conn, ["sitea"]) == []
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_observability.py::TestAggregate -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'plone.pgcatalog.observability'`.

- [ ] **Step 5: Write the module with `_aggregate`**

Create `src/plone/pgcatalog/observability.py`:

```python
"""plone.observability IMetricProvider producing content counts via SQL.

This module is imported only when the conditional ZCML registration fires
(``zcml:condition="installed plone.observability"``), so importing
plone.observability at module top level is safe.
"""

from plone.observability.interfaces import IMetricProvider
from plone.observability.metric import Metric
from plone.pgcatalog.pool import get_pool
from zope.interface import implementer

import logging
import os
import threading
import time


logger = logging.getLogger(__name__)

# Process-global TTL cache. Key: tuple(sorted(site_ids)) -> (timestamp, [Metric]).
# Must be module-level: the @@metrics view builds a fresh provider instance per
# request via getAdapters(), so an instance cache would never hit across scrapes.
_cache = {}
_cache_lock = threading.Lock()


def _ttl():
    return int(os.environ.get("PLONE_OBSERVABILITY_METRICS_CACHE_TTL", "60"))


_TOTAL_SQL = (
    "SELECT split_part(path, '/', 2) AS site, idx->>'portal_type' AS pt, "
    "count(*) AS n FROM object_state "
    "WHERE idx->>'portal_type' IS NOT NULL "
    "AND split_part(path, '/', 2) = ANY(%(sites)s) "
    "GROUP BY site, pt"
)

_STATE_SQL = (
    "SELECT split_part(path, '/', 2) AS site, idx->>'review_state' AS rs, "
    "count(*) AS n FROM object_state "
    "WHERE idx->>'review_state' IS NOT NULL "
    "AND split_part(path, '/', 2) = ANY(%(sites)s) "
    "GROUP BY site, rs"
)


def _aggregate(conn, site_ids):
    """Run the two GROUP BY queries restricted to site_ids; return list[Metric]."""
    params = {"sites": list(site_ids)}
    metrics = []
    with conn.cursor() as cur:
        cur.execute(_TOTAL_SQL, params)
        for row in cur.fetchall():
            metrics.append(
                Metric(
                    name="plone_content_total",
                    value=row["n"],
                    type="gauge",
                    scope="global",
                    help="Number of content objects by portal type",
                    labels={"portal_type": row["pt"], "site": row["site"]},
                )
            )
        cur.execute(_STATE_SQL, params)
        for row in cur.fetchall():
            metrics.append(
                Metric(
                    name="plone_content_by_state",
                    value=row["n"],
                    type="gauge",
                    scope="global",
                    help="Number of content objects by workflow state",
                    labels={"state": row["rs"], "site": row["site"]},
                )
            )
    return metrics
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_observability.py::TestAggregate -v`
Expected: PASS (2 passed).

- [ ] **Step 7: Commit**

```bash
git add src/plone/pgcatalog/observability.py tests/test_observability.py pyproject.toml
git commit -m "feat(observability): SQL content-count aggregation helper (#174)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Site discovery, provider class, TTL cache

**Files:**
- Modify: `src/plone/pgcatalog/observability.py`
- Test: `tests/test_observability.py`

**Interfaces:**
- Consumes: `_aggregate(conn, site_ids)` (Task 1), `get_pool(context)`.
- Produces:
  - `_pg_site_ids(context) -> list[str]` — ids of Plone sites whose `portal_catalog` provides `IPGCatalogTool`.
  - `PGContentMetricProvider(context)` — `@implementer(IMetricProvider)`, `name="pgcontent"`, `scope="global"`, generator `collect()`. Registered as the named adapter in Task 3.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_observability.py`:

```python
class _FakeCatalog:
    pass


class _FakeSite:
    def __init__(self, site_id, catalog):
        self._id = site_id
        self._catalog = catalog

    def getId(self):
        return self._id

    def unrestrictedTraverse(self, name, default=None):
        if name == "portal_catalog":
            return self._catalog
        return default


class _FakeApp:
    def __init__(self, sites):
        self._sites = sites

    def objectValues(self):
        return self._sites


class TestSiteSelection:
    def test_only_ipgcatalogtool_sites(self):
        from plone.pgcatalog.interfaces import IPGCatalogTool
        from plone.pgcatalog.observability import _pg_site_ids
        from Products.CMFPlone.interfaces import IPloneSiteRoot
        from zope.interface import alsoProvides

        pg_catalog = _FakeCatalog()
        alsoProvides(pg_catalog, IPGCatalogTool)
        zcatalog = _FakeCatalog()  # not IPGCatalogTool

        pg_site = _FakeSite("sitea", pg_catalog)
        alsoProvides(pg_site, IPloneSiteRoot)
        z_site = _FakeSite("siteb", zcatalog)
        alsoProvides(z_site, IPloneSiteRoot)

        assert _pg_site_ids(_FakeApp([pg_site, z_site])) == ["sitea"]


class _SpyPool:
    def __init__(self, conn):
        self._conn = conn

    def connection(self):
        import contextlib

        @contextlib.contextmanager
        def _cm():
            yield self._conn

        return _cm()


class TestCollect:
    def test_yields_nothing_without_pg_sites(self, monkeypatch):
        from plone.pgcatalog import observability

        monkeypatch.setattr(observability, "_pg_site_ids", lambda ctx: [])
        provider = observability.PGContentMetricProvider(object())
        assert list(provider.collect()) == []

    def test_caches_within_ttl(self, monkeypatch):
        from plone.pgcatalog import observability
        from plone.observability.metric import Metric

        calls = {"n": 0}
        sample = [Metric(name="plone_content_total", value=1, type="gauge",
                         scope="global", help="h", labels={"portal_type": "Document",
                                                            "site": "sitea"})]

        def fake_aggregate(conn, site_ids):
            calls["n"] += 1
            return sample

        monkeypatch.setattr(observability, "_pg_site_ids", lambda ctx: ["sitea"])
        monkeypatch.setattr(observability, "get_pool",
                            lambda ctx: _SpyPool(conn=object()))
        monkeypatch.setattr(observability, "_aggregate", fake_aggregate)
        monkeypatch.setenv("PLONE_OBSERVABILITY_METRICS_CACHE_TTL", "300")

        provider = observability.PGContentMetricProvider(object())
        first = list(provider.collect())
        second = list(observability.PGContentMetricProvider(object()).collect())

        assert first == second == sample
        assert calls["n"] == 1  # second call served from the process-global cache

    def test_degrades_silently_without_pool(self, monkeypatch):
        from plone.pgcatalog import observability

        monkeypatch.setattr(observability, "_pg_site_ids", lambda ctx: ["sitea"])

        def boom(ctx):
            raise RuntimeError("no pool")

        monkeypatch.setattr(observability, "get_pool", boom)
        provider = observability.PGContentMetricProvider(object())
        assert list(provider.collect()) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_observability.py::TestSiteSelection tests/test_observability.py::TestCollect -v`
Expected: FAIL — `AttributeError: module 'plone.pgcatalog.observability' has no attribute '_pg_site_ids'` / `PGContentMetricProvider`.

- [ ] **Step 3: Add `_pg_site_ids` and `PGContentMetricProvider`**

Append to `src/plone/pgcatalog/observability.py`:

```python
def _pg_site_ids(context):
    """Return ids of Plone sites whose portal_catalog provides IPGCatalogTool.

    During a migration pgcatalog may be installed while a site still uses a
    ZCatalog; such sites are left to plone.observability's generic provider.
    """
    try:
        from Products.CMFPlone.interfaces import IPloneSiteRoot
    except ImportError:
        return []
    from plone.pgcatalog.interfaces import IPGCatalogTool

    site_ids = []
    for obj in context.objectValues():
        if not IPloneSiteRoot.providedBy(obj):
            continue
        try:
            catalog = obj.unrestrictedTraverse("portal_catalog", None)
        except Exception:
            catalog = None
        if catalog is not None and IPGCatalogTool.providedBy(catalog):
            site_ids.append(obj.getId())
    return site_ids


@implementer(IMetricProvider)
class PGContentMetricProvider:
    """Content-count metrics via SQL for IPGCatalogTool-backed sites."""

    name = "pgcontent"
    scope = "global"

    def __init__(self, context):
        self.context = context

    def collect(self):
        site_ids = _pg_site_ids(self.context)
        if not site_ids:
            return
        key = tuple(sorted(site_ids))
        now = time.time()
        with _cache_lock:
            entry = _cache.get(key)
            if entry is not None and (now - entry[0]) < _ttl():
                yield from entry[1]
                return
        try:
            pool = get_pool(self.context)
            with pool.connection() as conn:
                metrics = _aggregate(conn, list(key))
        except Exception:
            logger.debug("pgcontent metrics unavailable", exc_info=True)
            return
        with _cache_lock:
            _cache[key] = (now, metrics)
        yield from metrics
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_observability.py -v`
Expected: PASS (all of TestAggregate, TestSiteSelection, TestCollect).

- [ ] **Step 5: Commit**

```bash
git add src/plone/pgcatalog/observability.py tests/test_observability.py
git commit -m "feat(observability): pgcontent provider with site discovery + TTL cache (#174)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Conditional ZCML registration + CHANGES + registration test

**Files:**
- Modify: `src/plone/pgcatalog/configure.zcml`
- Modify: `CHANGES.md`
- Test: `tests/test_observability.py`

**Interfaces:**
- Consumes: `PGContentMetricProvider` (Task 2), `IMetricProvider`.
- Produces: a named adapter `pgcontent` providing `IMetricProvider` for `OFS.interfaces.IApplication`, registered only when plone.observability is installed.

- [ ] **Step 1: Add `xmlns:zcml` to configure.zcml root**

In `src/plone/pgcatalog/configure.zcml`, change the opening `<configure>` element to add the zcml namespace:

```xml
<configure
    xmlns="http://namespaces.zope.org/zope"
    xmlns:genericsetup="http://namespaces.zope.org/genericsetup"
    xmlns:zcml="http://namespaces.zope.org/zcml"
    >
```

- [ ] **Step 2: Register the conditional adapter**

In `src/plone/pgcatalog/configure.zcml`, before the closing `</configure>`, add:

```xml
  <!-- plone.observability content-count metric provider (soft dependency).
       Registered only when plone.observability is installed; the provider
       itself emits only for IPGCatalogTool-backed sites. -->
  <adapter
      factory=".observability.PGContentMetricProvider"
      provides="plone.observability.interfaces.IMetricProvider"
      for="OFS.interfaces.IApplication"
      name="pgcontent"
      zcml:condition="installed plone.observability"
      />
```

- [ ] **Step 3: Write the failing registration test**

Append to `tests/test_observability.py`:

```python
class TestRegistration:
    def test_pgcontent_adapter_registered(self, pgcatalog_layer):
        from plone.observability.interfaces import IMetricProvider
        from zope.component import getGlobalSiteManager

        gsm = getGlobalSiteManager()
        names = {
            reg.name
            for reg in gsm.registeredAdapters()
            if reg.provided is IMetricProvider
        }
        assert "pgcontent" in names
```

- [ ] **Step 4: Run the registration test to verify it passes**

(The `pgcatalog_layer` fixture loads `configure.zcml`; with plone.observability installed, the conditional adapter registers.)

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_observability.py::TestRegistration -v`
Expected: PASS (1 passed). If it fails with the adapter absent, confirm Step 1/Step 2 edits and that plone.observability is importable in the venv (Task 1 Step 2).

- [ ] **Step 5: Add the CHANGES entry**

In `CHANGES.md`, under `## 1.0.0b66 (unreleased)`, add an `### Added` section (create it above `### Fixed` if not present):

```markdown
### Added

- Ship a `plone.observability` `IMetricProvider` (`pgcontent`) that produces
  `plone_content_total` / `plone_content_by_state` content-count metrics via SQL
  for pg-catalog sites. Registered only when `plone.observability` is installed
  (`zcml:condition`) and emits only for sites backed by `IPGCatalogTool`
  (migration safe). #174
```

- [ ] **Step 6: Run the full observability test module**

Run: `env -u ZODB_TEST_DSN .venv/bin/pytest tests/test_observability.py -v`
Expected: PASS (TestAggregate, TestSiteSelection, TestCollect, TestRegistration).

- [ ] **Step 7: Commit**

```bash
git add src/plone/pgcatalog/configure.zcml tests/test_observability.py CHANGES.md
git commit -m "feat(observability): register pgcontent provider conditionally (#174)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Soft dependency / conditional ZCML → Task 3. ✓
- Migration-safe site selection (`IPGCatalogTool`) → Task 2 (`_pg_site_ids`) + test. ✓
- SQL aggregation, labels incl. `site`, NULL filtering → Task 1 (`_aggregate`) + tests. ✓
- Metric parity (names/type/scope/help/labels) → Task 1 code + assertions. ✓
- Process-global TTL cache → Task 2 (`_cache` + `collect`) + cache test. ✓
- Silent degradation → Task 2 (`except Exception`) + degradation test. ✓
- `plone.observability` in test extra → Task 1. ✓
- CHANGES entry → Task 3. ✓

**Placeholder scan:** none.

**Type consistency:** `_aggregate(conn, site_ids) -> list[Metric]`, `_pg_site_ids(context) -> list[str]`, `PGContentMetricProvider.name == "pgcontent"`, cache keyed by `tuple(sorted(site_ids))` — used consistently across Tasks 1–3 and the tests.
