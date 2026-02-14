"""Level 2 benchmark: PGCatalog vs ZCatalog — real Plone subprocess.

Runs in a subprocess per backend (PGJsonbStorage+PGCatalog or
FileStorage+ZCatalog).  Creates a real Plone site, creates content with
invokeFactory, runs realistic catalog queries through the full Plone stack.

This is the FAIR comparison — both backends go through the full Plone
indexing pipeline and ZODB storage layer.  ZCatalog uses FileStorage (the
standard Plone deployment, giving it maximum advantage — no network
overhead, just disk I/O cached by the OS).

Usage (called by run_benchmarks.py, not directly):
    python bench_plone_catalog.py --conf /path/to/zope.conf \
        --backend BackendName --docs 500 --iterations 50 --warmup 5
"""

from __future__ import annotations

import argparse
import io
import json
import os
import statistics
import sys
import time


# ---------------------------------------------------------------------------
# Stats helper
# ---------------------------------------------------------------------------


def _stats_dict(samples):
    """Convert timing samples list (ms) to a stats dictionary."""
    if not samples:
        return {}
    s = sorted(samples)
    n = len(s)
    return {
        "count": n,
        "mean_ms": round(statistics.mean(s), 3),
        "median_ms": round(statistics.median(s), 3),
        "p95_ms": round(s[int(n * 0.95)] if n > 1 else s[0], 3),
        "p99_ms": round(s[int(n * 0.99)] if n > 1 else s[0], 3),
        "min_ms": round(s[0], 3),
        "max_ms": round(s[-1], 3),
        "stddev_ms": round(statistics.stdev(s), 3) if n > 1 else 0.0,
        "total_ms": round(sum(s), 3),
        "ops_per_sec": round(1000.0 / statistics.mean(s), 1) if statistics.mean(s) > 0 else 0,
    }


# ---------------------------------------------------------------------------
# Zope setup
# ---------------------------------------------------------------------------


def setup_zope(conf_path, register_pgcatalog=False):
    """Configure Zope2 and return the application.

    Args:
        register_pgcatalog: If True, manually register the
            CatalogStateProcessor on the storage.  Needed because
            configure_wsgi() doesn't fire IDatabaseOpenedWithRoot,
            so the ZCML subscriber never fires.
    """
    saved_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        from Zope2.Startup.run import configure_wsgi

        configure_wsgi(conf_path)
    finally:
        sys.stdout = saved_stdout

    from Testing.makerequest import makerequest

    # Prevent Testing/custom_zodb.py from hijacking the database.
    # Zope2's startup() scans testinghome/instancehome for custom_zodb.py
    # and the Testing package ships one that creates a DemoStorage,
    # overriding our configured <pgjsonb>/<relstorage> storage.
    import Zope2.App.startup as _zas
    _zas._load_custom_zodb = lambda location: None

    import Zope2

    app = Zope2.app()

    if register_pgcatalog:
        # With the _load_custom_zodb fix above, startup() uses the real
        # PGJsonbStorage from dbtab.  load_zcml() registers our
        # IDatabaseOpenedWithRoot subscriber, and DatabaseOpenedWithRoot
        # fires at the end of startup() — so CatalogStateProcessor is
        # already registered automatically.
        db = app._p_jar.db()
        storage = getattr(db.storage, "_main", db.storage)
        n_proc = len(getattr(storage, '_state_processors', []))
        print(f"DEBUG: storage={type(storage).__name__}, "
              f"processors={n_proc}",
              file=sys.stderr)

    return makerequest(app)


def setup_admin(app):
    """Create admin user and set up security context."""
    import transaction
    from AccessControl.SecurityManagement import newSecurityManager

    uf = app.acl_users
    if not uf.getUserById("admin"):
        uf.userFolderAddUser("admin", "admin", ["Manager"], [])
    transaction.commit()

    admin = uf.getUserById("admin")
    newSecurityManager(None, admin.__of__(uf))


def create_site(app, site_id="bench_site", use_pgcatalog=False):
    """Create a Plone site. Returns (elapsed_ms, site)."""
    import transaction
    from plone.distribution.api.site import create
    from Products.CMFPlone.factory import _DEFAULT_PROFILE

    config = {
        "site_id": site_id,
        "title": "Benchmark Site",
        "setup_content": False,
        "default_language": "en",
        "portal_timezone": "UTC",
        "extension_ids": ["plone.volto:default"],
        "profile_id": _DEFAULT_PROFILE,
    }

    t0 = time.perf_counter()
    site = create(app, "volto", config)
    transaction.commit()

    # plone.distribution ignores extension_ids — replace portal_catalog
    # with PlonePGCatalogTool properly.
    if use_pgcatalog:
        from Acquisition import aq_base
        from zope.component.hooks import setSite
        from Products.CMFCore.interfaces import ICatalogTool
        from plone.pgcatalog.catalog import PlonePGCatalogTool

        setSite(site)
        sm = site.getSiteManager()

        # Unregister old utility (Five does NOT auto-unregister on deletion)
        sm.unregisterUtility(provided=ICatalogTool)

        # Remove old catalog with its subobjects (indexes, lexicons, etc.)
        site.manage_delObjects(["portal_catalog"])

        # Add new PGCatalog tool
        new_catalog = PlonePGCatalogTool()
        site._setObject("portal_catalog", new_catalog)
        sm.registerUtility(aq_base(site.portal_catalog), ICatalogTool)

        # Re-apply ZCatalog indexes from Plone core profiles.
        # MUST use run_dependencies=False to skip the cascade:
        #   catalog → componentregistry → toolset
        # which would purge IFactory registrations and replace our catalog.
        setup_tool = site.portal_setup
        for profile_id in [
            "profile-Products.CMFPlone:plone",
            "profile-plone.app.contenttypes:default",
            "profile-plone.app.event:default",
        ]:
            try:
                setup_tool.runImportStepFromProfile(
                    profile_id, "catalog", run_dependencies=False,
                )
            except Exception:
                pass

        # Sync IndexRegistry so build_query() knows about the indexes
        # (IDatabaseOpenedWithRoot fires before site exists, so no auto-sync)
        from plone.pgcatalog.columns import get_registry
        registry = get_registry()
        registry.sync_from_catalog(site.portal_catalog)

        transaction.commit()

    t1 = time.perf_counter()
    return (t1 - t0) * 1000.0, site


# ---------------------------------------------------------------------------
# Content creation
# ---------------------------------------------------------------------------


def create_content(site, n_docs):
    """Create N documents with realistic data.  Returns per-doc timing samples."""
    import transaction
    from zope.component.hooks import setSite

    setSite(site)

    # Generate deterministic content data
    import random

    rng = random.Random(42)

    subjects_pool = [
        "Python", "Zope", "Plone", "JavaScript", "CSS", "Docker",
        "PostgreSQL", "React", "Testing", "Security", "Performance",
        "API", "REST", "GraphQL", "Migration",
    ]
    states = ["published", "private", "pending"]
    state_weights = [70, 20, 10]

    samples = []
    for i in range(n_docs):
        title = f"Benchmark Document {i} — {rng.choice(subjects_pool)}"
        desc = f"This is benchmark document number {i} with some searchable text about {rng.choice(subjects_pool)} and {rng.choice(subjects_pool)}."
        n_tags = rng.randint(0, 4)
        tags = rng.sample(subjects_pool, k=min(n_tags, len(subjects_pool)))

        t0 = time.perf_counter()
        site.invokeFactory(
            "Document",
            f"doc-{i}",
            title=title,
            description=desc,
            subject=tags,
        )
        obj = site[f"doc-{i}"]

        # Set workflow state for some variety
        state = rng.choices(states, weights=state_weights, k=1)[0]
        if state != "private":
            from Products.CMFCore.utils import getToolByName
            wf = getToolByName(site, "portal_workflow")
            try:
                if state == "published":
                    wf.doActionFor(obj, "publish")
                elif state == "pending":
                    wf.doActionFor(obj, "submit")
            except Exception:
                pass  # Workflow may not support this transition

        transaction.commit()
        t1 = time.perf_counter()
        samples.append((t1 - t0) * 1000.0)

    return samples


# ---------------------------------------------------------------------------
# Content modification
# ---------------------------------------------------------------------------


def modify_content(site, n_docs, iterations):
    """Modify existing documents and reindex.  Returns timing samples."""
    import transaction

    samples = []
    for iteration in range(iterations):
        i = iteration % n_docs
        doc = site[f"doc-{i}"]
        t0 = time.perf_counter()
        doc.title = f"Modified Document {i} (iteration {iteration})"
        doc.reindexObject()
        transaction.commit()
        t1 = time.perf_counter()
        samples.append((t1 - t0) * 1000.0)

    return samples


# ---------------------------------------------------------------------------
# Query scenarios
# ---------------------------------------------------------------------------


def run_query_scenarios(site, iterations, warmup):
    """Run all query scenarios against portal_catalog.

    Returns dict of {scenario_name: {"stats": ..., "count": N}}.
    """
    catalog = site.portal_catalog

    scenarios = [
        (
            "P3_simple_query",
            "Simple: portal_type=Document",
            {"portal_type": "Document"},
        ),
        (
            "P4_complex_query",
            "Complex: type+state+subject+sort+limit",
            {
                "portal_type": "Document",
                "review_state": "published",
                "Subject": "Python",
                "sort_on": "modified",
                "sort_order": "descending",
                "b_size": 20,
            },
        ),
        (
            "P5_fulltext",
            "Full-text: SearchableText",
            {"SearchableText": "benchmark document"},
        ),
        (
            "P6_navigation",
            "Navigation: path depth=1 + sort",
            {
                "path": {"query": f"/{site.getId()}", "depth": 1},
                "sort_on": "getObjPositionInParent",
            },
        ),
        (
            "P8_security_filtered",
            "Security: published + anonymous access",
            {
                "portal_type": "Document",
                "review_state": "published",
                "allowedRolesAndUsers": ["Anonymous"],
            },
        ),
        (
            "P9_date_sorted",
            "Date-sorted: all content by creation date",
            {
                "sort_on": "created",
                "sort_order": "descending",
                "b_size": 50,
            },
        ),
    ]

    results = {}
    for name, description, query_dict in scenarios:
        # Warmup
        for _ in range(warmup):
            try:
                r = catalog.searchResults(**query_dict)
                len(r)  # Force evaluation
            except Exception:
                break

        # Timed iterations
        samples = []
        result_count = 0
        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                r = catalog.searchResults(**query_dict)
                result_count = len(r)
            except Exception:
                # Record as failure
                result_count = -1
                break
            t1 = time.perf_counter()
            samples.append((t1 - t0) * 1000.0)

        results[name] = {
            "description": description,
            "stats": _stats_dict(samples),
            "count": result_count,
        }

    return results


# ---------------------------------------------------------------------------
# Write-then-query benchmark (same transaction)
# ---------------------------------------------------------------------------


def bench_write_then_query(site, iterations, warmup):
    """Benchmark: create/modify content and query within the same transaction.

    Simulates the common Plone pattern where content is created or modified
    in a request handler, and the catalog is queried as part of building
    the response — all within the same transaction (no intermediate commit).

    PGCatalog uses the auto-flush mechanism (SAVEPOINT + write pending data)
    to make catalog entries visible.  ZCatalog updates BTree indexes in
    memory during catalog_object(), so queries work naturally.
    """
    import transaction

    catalog = site.portal_catalog
    total_iters = warmup + iterations

    # --- Create + Query (find by path) ---
    create_samples = []
    create_found = 0

    for i in range(total_iters):
        doc_id = f"wq-{i}"
        doc_path = f"/{site.getId()}/{doc_id}"

        t0 = time.perf_counter()
        site.invokeFactory(
            "Document", doc_id,
            title=f"WriteQuery Doc {i}",
            description=f"Write-then-query benchmark document {i}",
        )
        # Query within the same transaction — no commit yet
        results = catalog.searchResults(
            path={"query": doc_path, "depth": 0},
        )
        found = len(results) > 0
        t1 = time.perf_counter()

        if i >= warmup:
            create_samples.append((t1 - t0) * 1000.0)
            if found:
                create_found += 1

        transaction.commit()

    # --- Modify + Query (find by unique Subject tag) ---
    modify_samples = []
    modify_found = 0

    for i in range(total_iters):
        doc = site[f"wq-{i}"]
        unique_tag = f"wqmod{i}"

        t0 = time.perf_counter()
        doc.subject = (unique_tag,)
        doc.reindexObject()
        # Query within the same transaction — no commit yet
        results = catalog.searchResults(Subject=unique_tag)
        found = len(results) > 0
        t1 = time.perf_counter()

        if i >= warmup:
            modify_samples.append((t1 - t0) * 1000.0)
            if found:
                modify_found += 1

        transaction.commit()

    return {
        "create_then_query": {
            "description": "Create doc + find by path (same txn)",
            "stats": _stats_dict(create_samples),
            "found_rate": round(create_found / iterations, 3) if iterations else 0,
        },
        "modify_then_query": {
            "description": "Modify doc + find by Subject (same txn)",
            "stats": _stats_dict(modify_samples),
            "found_rate": round(modify_found / iterations, 3) if iterations else 0,
        },
    }


# ---------------------------------------------------------------------------
# Rebuild benchmark
# ---------------------------------------------------------------------------


def bench_rebuild(site, iterations):
    """Benchmark clearFindAndRebuild.  Returns timing samples."""
    catalog = site.portal_catalog
    import transaction

    samples = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        catalog.clearFindAndRebuild()
        transaction.commit()
        t1 = time.perf_counter()
        samples.append((t1 - t0) * 1000.0)

    return samples


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Plone catalog benchmark worker (subprocess)"
    )
    parser.add_argument("--conf", required=True, help="Path to zope.conf")
    parser.add_argument("--backend", required=True, help="Backend name")
    parser.add_argument("--docs", type=int, default=500, help="Number of documents")
    parser.add_argument("--iterations", type=int, default=50, help="Query iterations")
    parser.add_argument("--warmup", type=int, default=5, help="Warmup iterations")
    parser.add_argument("--rebuild-iterations", type=int, default=0,
                        help="Number of rebuild iterations (0 = skip)")
    parser.add_argument("--profile", action="store_true",
                        help="Run cProfile on query scenarios and dump stats to stderr")
    parser.add_argument("--pgcatalog", action="store_true",
                        help="Apply plone.pgcatalog:default profile (PGCatalog SQL engine)")
    args = parser.parse_args()

    app = setup_zope(args.conf, register_pgcatalog=args.pgcatalog)
    setup_admin(app)

    results = {"backend": args.backend}
    n_docs = args.docs

    # P0: Site creation
    site_time, site = create_site(app, use_pgcatalog=args.pgcatalog)
    results["site_creation_ms"] = round(site_time, 3)

    # Diagnostic: verify catalog class and IndexRegistry
    catalog = site.portal_catalog
    from Acquisition import aq_base
    base_catalog = aq_base(catalog)
    print(f"portal_catalog class: {type(base_catalog).__name__} "
          f"(module: {type(base_catalog).__module__})",
          file=sys.stderr)
    if args.pgcatalog:
        from plone.pgcatalog.columns import get_registry
        reg = get_registry()
        print(f"IndexRegistry: {len(reg)} indexes, {len(reg.metadata)} metadata",
              file=sys.stderr)

    # P1: Content creation (includes full indexing pipeline)
    create_samples = create_content(site, n_docs)
    results["content_creation"] = _stats_dict(create_samples)

    # Diagnostic: check if PG catalog columns are populated
    if args.pgcatalog:
        try:
            import psycopg
            dsn = os.environ.get("PGCATALOG_DSN", "")
            if dsn:
                with psycopg.connect(dsn) as dconn:
                    with dconn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM object_state WHERE path IS NOT NULL")
                        n = cur.fetchone()[0]
                        cur.execute("SELECT idx->>'portal_type' AS pt, COUNT(*) AS cnt "
                                    "FROM object_state WHERE idx IS NOT NULL "
                                    "GROUP BY idx->>'portal_type' ORDER BY cnt DESC LIMIT 5")
                        types = ", ".join(f"{r[0]}={r[1]}" for r in cur.fetchall())
                        print(f"PG diag: {n} rows with path, types: {types}", file=sys.stderr)
                        # SearchableText diagnostic
                        cur.execute(
                            "SELECT COUNT(*) FROM object_state "
                            "WHERE path LIKE %s AND searchable_text IS NOT NULL",
                            (f"/{site.getId()}/doc-%",)
                        )
                        st_ok = cur.fetchone()[0]
                        cur.execute(
                            "SELECT COUNT(*) FROM object_state "
                            "WHERE path LIKE %s AND searchable_text IS NULL",
                            (f"/{site.getId()}/doc-%",)
                        )
                        st_null = cur.fetchone()[0]
                        print(f"PG diag: doc-* searchable_text: "
                              f"{st_ok} OK, {st_null} NULL "
                              f"(total {st_ok + st_null})", file=sys.stderr)
        except Exception as e:
            print(f"PG diag failed: {e}", file=sys.stderr)

    # P2: Content modification + reindex
    modify_samples = modify_content(site, n_docs, min(args.iterations, n_docs))
    results["content_modification"] = _stats_dict(modify_samples)

    # P3-P9: Query scenarios
    if args.profile:
        import cProfile
        import pstats

        pr = cProfile.Profile()
        pr.enable()
        query_results = run_query_scenarios(site, args.iterations, args.warmup)
        pr.disable()

        # Dump profile to stderr (stdout is reserved for JSON)
        print(f"\n=== cProfile: {args.backend} query scenarios "
              f"({args.iterations} iterations) ===", file=sys.stderr)
        ps = pstats.Stats(pr, stream=sys.stderr)
        ps.sort_stats("cumulative")
        ps.print_stats(40)
        print(f"\n=== Top callers ===", file=sys.stderr)
        ps.sort_stats("tottime")
        ps.print_stats(40)
    else:
        query_results = run_query_scenarios(site, args.iterations, args.warmup)
    results["queries"] = query_results

    # P10-P11: Write-then-query (same transaction)
    wq_results = bench_write_then_query(site, args.iterations, args.warmup)
    results["write_then_query"] = wq_results

    # P7: clearFindAndRebuild (expensive — only if requested)
    if args.rebuild_iterations > 0:
        rebuild_samples = bench_rebuild(site, args.rebuild_iterations)
        results["rebuild"] = _stats_dict(rebuild_samples)

    # Output JSON on stdout (only output)
    print(json.dumps(results))


if __name__ == "__main__":
    main()
