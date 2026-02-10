"""Profile the write path for PGJsonbStorage + PGCatalog content creation.

Creates a real Plone site, then profiles document creation at multiple levels:
1. Per-phase timing via class-level monkey-patching
2. cProfile: full function-level call graph

Usage:
    python benchmarks/profile_writes.py --conf /path/to/zope.conf --docs 20
"""

from __future__ import annotations

import argparse
import cProfile
import io
import os
import pstats
import sys
import tempfile
import time


def setup_env():
    """Set up Zope/Plone environment."""
    bench_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, bench_dir)
    from bench_plone_catalog import setup_zope, setup_admin, create_site

    return setup_zope, setup_admin, create_site


def profile_content_creation(site, n_docs):
    """Profile content creation with cProfile."""
    import transaction
    from zope.component.hooks import setSite
    import random

    setSite(site)
    rng = random.Random(42)

    subjects_pool = [
        "Python", "Zope", "Plone", "JavaScript", "CSS", "Docker",
        "PostgreSQL", "React", "Testing", "Security", "Performance",
        "API", "REST", "GraphQL", "Migration",
    ]

    pr = cProfile.Profile()
    pr.enable()

    for i in range(n_docs):
        title = f"Profile Doc {i} -- {rng.choice(subjects_pool)}"
        desc = f"Profile doc {i} about {rng.choice(subjects_pool)}."
        tags = rng.sample(subjects_pool, k=rng.randint(0, 4))

        site.invokeFactory(
            "Document",
            f"prof-doc-{i}",
            title=title,
            description=desc,
            subject=tags,
        )
        obj = site[f"prof-doc-{i}"]

        from Products.CMFCore.utils import getToolByName

        wf = getToolByName(site, "portal_workflow")
        try:
            wf.doActionFor(obj, "publish")
        except Exception:
            pass

        transaction.commit()

    pr.disable()

    # Print results
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s)
    ps.sort_stats("cumulative")
    print("\n" + "=" * 80)
    print(f"  cProfile: Top 60 by cumulative time ({n_docs} documents)")
    print("=" * 80)
    ps.print_stats(60)
    print(s.getvalue())

    # Also print by tottime
    s2 = io.StringIO()
    ps2 = pstats.Stats(pr, stream=s2)
    ps2.sort_stats("tottime")
    print("\n" + "=" * 80)
    print(f"  cProfile: Top 40 by total (self) time ({n_docs} documents)")
    print("=" * 80)
    ps2.print_stats(40)
    print(s2.getvalue())


def profile_single_doc_phases(site):
    """Instrument a single document creation to measure per-phase timing.

    Uses class-level patching (not instance-level) to avoid breaking
    ZODB serialization when CMFEditions creates savepoints.
    """
    import transaction
    from zope.component.hooks import setSite
    import zodb_json_codec
    from zodb_pgjsonb.storage import PGJsonbStorageInstance

    setSite(site)

    # Accumulators (reset per sample)
    _acc = {
        "store_times": [],
        "decode_times": [],
        "vote_time": 0.0,
        "finish_time": 0.0,
        "begin_time": 0.0,
        "extract_idx_time": 0.0,
        "searchable_time": 0.0,
    }

    # --- Class-level patches (safe for pickling) ---
    _orig_store = PGJsonbStorageInstance.store
    _orig_vote = PGJsonbStorageInstance.tpc_vote
    _orig_finish = PGJsonbStorageInstance.tpc_finish
    _orig_begin = PGJsonbStorageInstance.tpc_begin

    def patched_store(self, *args, **kwargs):
        t0 = time.perf_counter()
        result = _orig_store(self, *args, **kwargs)
        _acc["store_times"].append((time.perf_counter() - t0) * 1000)
        return result

    def patched_vote(self, *args, **kwargs):
        t0 = time.perf_counter()
        result = _orig_vote(self, *args, **kwargs)
        _acc["vote_time"] = (time.perf_counter() - t0) * 1000
        return result

    def patched_finish(self, *args, **kwargs):
        t0 = time.perf_counter()
        result = _orig_finish(self, *args, **kwargs)
        _acc["finish_time"] = (time.perf_counter() - t0) * 1000
        return result

    def patched_begin(self, *args, **kwargs):
        t0 = time.perf_counter()
        result = _orig_begin(self, *args, **kwargs)
        _acc["begin_time"] = (time.perf_counter() - t0) * 1000
        return result

    # Module-level codec patch
    _orig_decode = zodb_json_codec.decode_zodb_record_for_pg

    def patched_decode(data):
        t0 = time.perf_counter()
        result = _orig_decode(data)
        _acc["decode_times"].append((time.perf_counter() - t0) * 1000)
        return result

    # Catalog patches (on the unwrapped tool class)
    from plone.pgcatalog.catalog import PlonePGCatalogTool

    _orig_extract = PlonePGCatalogTool._extract_idx
    _orig_searchable = PlonePGCatalogTool._extract_searchable_text

    def patched_extract(self, *args, **kwargs):
        t0 = time.perf_counter()
        result = _orig_extract(self, *args, **kwargs)
        _acc["extract_idx_time"] += (time.perf_counter() - t0) * 1000
        return result

    def patched_searchable(*args, **kwargs):
        t0 = time.perf_counter()
        result = _orig_searchable(*args, **kwargs)
        _acc["searchable_time"] += (time.perf_counter() - t0) * 1000
        return result

    # Apply class-level patches
    PGJsonbStorageInstance.store = patched_store
    PGJsonbStorageInstance.tpc_vote = patched_vote
    PGJsonbStorageInstance.tpc_finish = patched_finish
    PGJsonbStorageInstance.tpc_begin = patched_begin
    PlonePGCatalogTool._extract_idx = patched_extract
    PlonePGCatalogTool._extract_searchable_text = staticmethod(patched_searchable)
    zodb_json_codec.decode_zodb_record_for_pg = patched_decode

    # Run multiple docs and average
    n_samples = 10
    all_results = []

    for i in range(n_samples):
        # Reset accumulators
        _acc["store_times"] = []
        _acc["decode_times"] = []
        _acc["vote_time"] = 0.0
        _acc["finish_time"] = 0.0
        _acc["begin_time"] = 0.0
        _acc["extract_idx_time"] = 0.0
        _acc["searchable_time"] = 0.0

        t_total_start = time.perf_counter()

        # --- Application layer (Plone) ---
        t_app_start = time.perf_counter()
        site.invokeFactory(
            "Document",
            f"phase-doc-{i}",
            title=f"Phase Test Doc {i}",
            description=f"Description for phase test document {i}",
            subject=["Python", "Testing"],
        )
        obj = site[f"phase-doc-{i}"]
        from Products.CMFCore.utils import getToolByName
        wf = getToolByName(site, "portal_workflow")
        try:
            wf.doActionFor(obj, "publish")
        except Exception:
            pass
        t_app_end = time.perf_counter()
        app_time = (t_app_end - t_app_start) * 1000

        # --- Commit (triggers 2PC) ---
        t_commit_start = time.perf_counter()
        transaction.commit()
        t_commit_end = time.perf_counter()
        commit_time = (t_commit_end - t_commit_start) * 1000

        total_time = (t_commit_end - t_total_start) * 1000

        all_results.append({
            "total": total_time,
            "app_layer": app_time,
            "commit": commit_time,
            "tpc_begin": _acc["begin_time"],
            "store_calls": len(_acc["store_times"]),
            "store_total": sum(_acc["store_times"]),
            "store_per_obj": _acc["store_times"][:],
            "decode_total": sum(_acc["decode_times"]),
            "decode_per_obj": _acc["decode_times"][:],
            "tpc_vote": _acc["vote_time"],
            "tpc_finish": _acc["finish_time"],
            "extract_idx": _acc["extract_idx_time"],
            "searchable_text": _acc["searchable_time"],
        })

    # Restore originals
    PGJsonbStorageInstance.store = _orig_store
    PGJsonbStorageInstance.tpc_vote = _orig_vote
    PGJsonbStorageInstance.tpc_finish = _orig_finish
    PGJsonbStorageInstance.tpc_begin = _orig_begin
    PlonePGCatalogTool._extract_idx = _orig_extract
    PlonePGCatalogTool._extract_searchable_text = staticmethod(_orig_searchable)
    zodb_json_codec.decode_zodb_record_for_pg = _orig_decode

    # Print results
    print("\n" + "=" * 80)
    print(f"  Per-Phase Timing (mean of {n_samples} single-doc creates)")
    print("=" * 80)

    def mean(vals):
        return sum(vals) / len(vals) if vals else 0

    avg = {k: mean([r[k] for r in all_results])
           for k in ["total", "app_layer", "commit", "tpc_begin",
                      "store_calls", "store_total", "decode_total",
                      "tpc_vote", "tpc_finish", "extract_idx",
                      "searchable_text"]}

    store_minus_decode = avg["store_total"] - avg["decode_total"]
    commit_other = avg["commit"] - avg["tpc_begin"] - avg["store_total"] - avg["tpc_vote"] - avg["tpc_finish"]

    print(f"\n  Total per doc:           {avg['total']:8.2f} ms")
    print(f"  |-- Application layer:   {avg['app_layer']:8.2f} ms  "
          f"({avg['app_layer']/avg['total']*100:.0f}%)")
    print(f"  |   |-- _extract_idx:    {avg['extract_idx']:8.2f} ms")
    print(f"  |   '-- _extract_search: {avg['searchable_text']:8.2f} ms")
    print(f"  '-- transaction.commit:  {avg['commit']:8.2f} ms  "
          f"({avg['commit']/avg['total']*100:.0f}%)")
    print(f"      |-- tpc_begin:       {avg['tpc_begin']:8.2f} ms")
    print(f"      |-- store() x{avg['store_calls']:.0f}:     "
          f"{avg['store_total']:8.2f} ms")
    print(f"      |   |-- codec decode:{avg['decode_total']:8.2f} ms")
    print(f"      |   '-- other (proc):{store_minus_decode:8.2f} ms")
    print(f"      |-- tpc_vote:        {avg['tpc_vote']:8.2f} ms")
    print(f"      |-- tpc_finish:      {avg['tpc_finish']:8.2f} ms")
    print(f"      '-- commit overhead: {commit_other:8.2f} ms")

    # Show individual store() calls for last sample
    last = all_results[-1]
    if last["store_per_obj"]:
        print(f"\n  Last doc store() breakdown ({len(last['store_per_obj'])} objects):")
        for j, st in enumerate(last["store_per_obj"]):
            dec = last["decode_per_obj"][j] if j < len(last["decode_per_obj"]) else 0
            print(f"    obj {j}: store={st:.2f}ms  (decode={dec:.2f}ms, "
                  f"other={st-dec:.2f}ms)")


def main():
    parser = argparse.ArgumentParser(description="Profile write path")
    parser.add_argument("--docs", type=int, default=20,
                        help="Number of docs for cProfile run")
    args = parser.parse_args()

    bench_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, bench_dir)

    from run_benchmarks import _generate_zope_conf, _clean_pg_db

    _clean_pg_db()

    with tempfile.TemporaryDirectory(prefix="profile-writes-") as tmp_dir:
        conf_path = _generate_zope_conf("pgjsonb", tmp_dir)

        setup_zope, setup_admin, create_site = setup_env()
        app = setup_zope(conf_path, register_pgcatalog=True)
        setup_admin(app)

        print("Creating Plone site...", file=sys.stderr)
        _, site = create_site(app, "profile_site", use_pgcatalog=True)

        # Diagnostic
        cat = site.portal_catalog
        print(f"portal_catalog class: {type(cat.aq_base).__name__} "
              f"(module: {type(cat.aq_base).__module__})", file=sys.stderr)
        from plone.pgcatalog.columns import get_registry
        registry = get_registry()
        print(f"IndexRegistry: {len(registry)} indexes", file=sys.stderr)

        # Phase 1: Per-phase instrumented profiling
        print("\nPhase 1: Instrumented per-phase profiling...", file=sys.stderr)
        profile_single_doc_phases(site)

        # Phase 2: cProfile full call graph
        print(f"\nPhase 2: cProfile ({args.docs} docs)...", file=sys.stderr)
        profile_content_creation(site, args.docs)


if __name__ == "__main__":
    main()
