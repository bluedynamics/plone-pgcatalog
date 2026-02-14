"""Orchestrator: runs Level 1 and Level 2 PGCatalog vs ZCatalog benchmarks.

Usage:
    # Run Level 1 (standalone SQL vs BTree) at default scales
    python benchmarks/run_benchmarks.py level1

    # Run Level 1 at specific scales
    python benchmarks/run_benchmarks.py level1 --scales small,medium,large

    # Run Level 2 (real Plone subprocess)
    python benchmarks/run_benchmarks.py level2 --docs 500

    # Run both levels
    python benchmarks/run_benchmarks.py all

    # Export results to JSON
    python benchmarks/run_benchmarks.py all --output results.json

    # Adjust iterations
    python benchmarks/run_benchmarks.py level1 --iterations 50 --warmup 5
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# ANSI colors
# ---------------------------------------------------------------------------

HEADER = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BENCH_DIR = Path(__file__).resolve().parent
PGCATALOG_DIR = BENCH_DIR.parent
PROJECT_ROOT = PGCATALOG_DIR.parent.parent  # z3blobs/
INSTANCE_HOME = PROJECT_ROOT / "instance"
WORKER_SCRIPT = BENCH_DIR / "bench_plone_catalog.py"

# PostgreSQL DSN for Level 1 and Level 2 (PGJsonbStorage)
PGJSONB_DSN = os.environ.get(
    "PGCATALOG_BENCH_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)

# ---------------------------------------------------------------------------
# Level 1: import and run directly
# ---------------------------------------------------------------------------


def run_level1(scales, iterations, warmup):
    """Run Level 1 benchmark (standalone SQL vs BTree).

    Returns results dict.
    """
    # Add benchmark dir to path for imports
    sys.path.insert(0, str(BENCH_DIR))
    from bench_catalog import run_benchmark, SCALES

    results = {}
    for scale_name in scales:
        if scale_name not in SCALES:
            print(f"  {RED}Unknown scale: {scale_name}{RESET}")
            continue
        n = SCALES[scale_name]
        result = run_benchmark(scale_name, n, iterations, warmup)
        results[scale_name] = result

    return results


# ---------------------------------------------------------------------------
# Level 2: Plone subprocess
# ---------------------------------------------------------------------------

ZOPE_CONF_TEMPLATE = """\
%define INSTANCEHOME {instancehome}
instancehome $INSTANCEHOME
%define CLIENTHOME {clienthome}
clienthome $CLIENTHOME

debug-mode off
debug-exceptions off
security-policy-implementation C
verbose-security off
default-zpublisher-encoding utf-8

<environment>
    CHAMELEON_CACHE {clienthome}/cache
    zope_i18n_compile_mo_files true
</environment>
<dos_protection>
    form-memory-limit 1MB
    form-disk-limit 1GB
    form-memfile-limit 4KB
</dos_protection>

{db_section}
"""

PGJSONB_DB_SECTION = """\
%import zodb_pgjsonb

<zodb_db main>
    mount-point /
    cache-size 30000
    <pgjsonb>
        dsn {dsn}
    </pgjsonb>
</zodb_db>"""

FILESTORAGE_DB_SECTION = """\
<zodb_db main>
    mount-point /
    cache-size {cache_size}
    <filestorage>
        path {datafs_path}
    </filestorage>
</zodb_db>"""

RELSTORAGE_DB_SECTION = """\
%import relstorage

<zodb_db main>
    mount-point /
    cache-size {cache_size}
    <relstorage>
        keep-history false
        <postgresql>
            dsn {dsn}
        </postgresql>
    </relstorage>
</zodb_db>"""


def _generate_zope_conf(backend_type, tmp_dir, cache_size=400):
    """Generate a temporary zope.conf for a given backend.

    Args:
        cache_size: ZODB object cache size.  A small value (e.g. 400)
            simulates the cache pressure seen on large Plone sites where
            ZCatalog BTree nodes compete for limited cache slots.  PGCatalog
            is unaffected because it stores no BTree objects in ZODB.
    """
    clienthome = Path(tmp_dir) / f"clienthome-{backend_type}"
    clienthome.mkdir(exist_ok=True)
    (clienthome / "cache").mkdir(exist_ok=True)
    (clienthome / "log").mkdir(exist_ok=True)

    if backend_type == "pgjsonb":
        db_section = PGJSONB_DB_SECTION.format(dsn=PGJSONB_DSN)
    elif backend_type == "filestorage":
        datafs = Path(tmp_dir) / "Data.fs"
        db_section = FILESTORAGE_DB_SECTION.format(
            datafs_path=datafs, cache_size=cache_size,
        )
    elif backend_type == "relstorage":
        db_section = RELSTORAGE_DB_SECTION.format(
            dsn=PGJSONB_DSN, cache_size=cache_size,
        )
    else:
        raise ValueError(f"Unknown backend type: {backend_type}")

    conf_content = ZOPE_CONF_TEMPLATE.format(
        instancehome=INSTANCE_HOME,
        clienthome=clienthome,
        db_section=db_section,
    )

    conf_path = Path(tmp_dir) / f"zope-{backend_type}.conf"
    conf_path.write_text(conf_content)
    return str(conf_path)


def _clean_pg_db():
    """Drop ALL tables in the PG test database.

    Both PGJsonb and RelStorage use ``object_state`` as a table name,
    so we must nuke everything between runs to avoid conflicts.
    """
    try:
        import psycopg

        conn = psycopg.connect(PGJSONB_DSN, autocommit=True)
        # Terminate other connections first (REPEATABLE READ blocks DDL)
        conn.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        # Wait for backends to fully terminate (pg_terminate_backend is async)
        for _ in range(20):
            row = conn.execute(
                "SELECT COUNT(*) FROM pg_stat_activity "
                "WHERE datname = current_database() AND pid != pg_backend_pid()"
            ).fetchone()
            if row[0] == 0:
                break
            time.sleep(0.25)
        # Drop all tables in public schema
        conn.execute("""
            DO $$ DECLARE r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables
                          WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS '
                            || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """)
        # Also drop functions/types that RelStorage or PGJsonb may have created
        conn.execute("""
            DO $$ DECLARE r RECORD;
            BEGIN
                FOR r IN (SELECT routine_name FROM information_schema.routines
                          WHERE routine_schema = 'public') LOOP
                    EXECUTE 'DROP FUNCTION IF EXISTS '
                            || quote_ident(r.routine_name) || ' CASCADE';
                END LOOP;
            END $$;
        """)
        conn.close()
    except Exception as exc:
        print(f"  {YELLOW}Warning: could not clean PG database: {exc}{RESET}")


def _has_relstorage():
    """Check if RelStorage is importable."""
    try:
        import relstorage  # noqa: F401
        return True
    except ImportError:
        return False


def _run_plone_worker(conf_path, backend_name, n_docs, iterations, warmup,
                      rebuild_iterations=0, profile=False, pgcatalog=False):
    """Run bench_plone_catalog.py in a subprocess. Returns parsed JSON or None."""
    cmd = [
        sys.executable,
        str(WORKER_SCRIPT),
        "--conf", conf_path,
        "--backend", backend_name,
        "--docs", str(n_docs),
        "--iterations", str(iterations),
        "--warmup", str(warmup),
        "--rebuild-iterations", str(rebuild_iterations),
    ]
    if profile:
        cmd.append("--profile")
    if pgcatalog:
        cmd.append("--pgcatalog")

    env = os.environ.copy()
    if pgcatalog:
        env["PGCATALOG_DSN"] = PGJSONB_DSN

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=max(1200, n_docs * 2),  # scale with doc count (~2s/doc worst case)
        env=env,
    )

    if result.returncode != 0:
        print(f"    {RED}FAILED (exit code {result.returncode}){RESET}")
        stderr_lines = result.stderr.strip().split("\n")
        for line in stderr_lines[-15:]:
            print(f"      {DIM}{line}{RESET}")
        return None

    # Dump diagnostic/profile output (on stderr) to terminal
    if result.stderr.strip():
        # Show diagnostic lines (portal_catalog class, IndexRegistry, PG diag)
        for line in result.stderr.strip().split("\n"):
            if any(kw in line for kw in ("portal_catalog", "IndexRegistry",
                                          "PG diag", "State processor",
                                          "DEBUG", "WARNING")):
                print(f"      {DIM}{line}{RESET}")
        if profile:
            print(result.stderr, file=sys.stderr)

    # Parse JSON from stdout (last non-empty line)
    stdout_lines = [
        line for line in result.stdout.strip().split("\n") if line.strip()
    ]
    if not stdout_lines:
        print(f"    {RED}FAILED (no output){RESET}")
        return None

    try:
        return json.loads(stdout_lines[-1])
    except json.JSONDecodeError as exc:
        print(f"    {RED}FAILED (bad JSON: {exc}){RESET}")
        return None


def run_level2(n_docs, iterations, warmup, rebuild_iterations=0, cache_size=400,
               profile=False):
    """Run Level 2 benchmark (real Plone subprocess).

    Returns dict with results for each backend.
    """
    results = {}

    with tempfile.TemporaryDirectory(prefix="pgcatalog-bench-") as tmp_dir:
        backends = [
            ("PGJsonbStorage + PGCatalog", "pgjsonb"),
        ]

        # RelStorage is the real competition (same PG backend, BTree catalog)
        if _has_relstorage():
            backends.append(("RelStorage + ZCatalog", "relstorage"))
        else:
            print(f"  {YELLOW}RelStorage not installed — skipping.{RESET}")

        for backend_name, backend_type in backends:
            print(f"\n  {DIM}{backend_name} ({n_docs} docs, "
                  f"{iterations} iterations)...{RESET}", flush=True)

            # Clean database for PG-backed runs (both pgjsonb and relstorage
            # use object_state, so we must nuke all tables between runs)
            if backend_type in ("pgjsonb", "relstorage"):
                _clean_pg_db()

            # Generate temp zope.conf
            conf_path = _generate_zope_conf(backend_type, tmp_dir, cache_size)

            # Run worker subprocess
            t0 = time.perf_counter()
            worker_result = _run_plone_worker(
                conf_path, backend_name, n_docs, iterations, warmup,
                rebuild_iterations, profile=profile,
                pgcatalog=(backend_type == "pgjsonb"),
            )
            elapsed = (time.perf_counter() - t0) * 1000.0

            if worker_result:
                results[backend_type] = worker_result
                site_ms = worker_result.get("site_creation_ms", 0)
                create_mean = worker_result.get("content_creation", {}).get("mean_ms", 0)
                print(f"    {DIM}Done in {elapsed/1000:.1f}s "
                      f"(site: {site_ms:.0f}ms, create: {create_mean:.1f}ms/doc){RESET}")

    return results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _fmt_ms(val):
    """Format a millisecond value for display."""
    if val is None:
        return "N/A"
    if val < 1:
        return f"{val:.3f}ms"
    if val < 100:
        return f"{val:.1f}ms"
    if val < 10000:
        return f"{val:.0f}ms"
    return f"{val/1000:.1f}s"


def _ratio_str(a, b):
    """Format a ratio between two values (a = pgcatalog, b = zcatalog)."""
    if a is None or b is None or a == 0 or b == 0:
        return "N/A"
    ratio = b / a
    if ratio >= 1:
        return f"{GREEN}{ratio:.1f}x faster{RESET}"
    else:
        return f"{RED}{1/ratio:.1f}x slower{RESET}"


def print_level2_results(results):
    """Print Level 2 results as a formatted table.

    Dynamically adapts to whichever backends have data.
    """
    # Build list of (key, data, short_label) for backends that have data
    backend_defs = [
        ("pgjsonb", "PGCat"),
        ("filestorage", "ZCat"),
        ("relstorage", "RS+ZC"),
    ]
    backends = [(key, results[key], label)
                for key, label in backend_defs if key in results and results[key]]

    if not backends:
        print(f"\n  {YELLOW}No Level 2 results available.{RESET}")
        return

    title = " vs ".join(data.get("backend", label) for _, data, label in backends)

    print(f"\n{HEADER}{'=' * 80}")
    print(f"  Level 2: Real Plone — {title}")
    print(f"{'=' * 80}{RESET}")

    # -- Setup metrics -------------------------------------------------------

    site_parts = [f"{label} {_fmt_ms(data.get('site_creation_ms'))}"
                  for _, data, label in backends]
    print(f"\n  {DIM}Site creation: {' | '.join(site_parts)}{RESET}")

    create_parts = [
        f"{label} {_fmt_ms(data.get('content_creation', {}).get('mean_ms'))}/doc"
        for _, data, label in backends
    ]
    print(f"  {DIM}Content creation: {' | '.join(create_parts)}{RESET}")

    mod_parts = [
        f"{label} {_fmt_ms(data.get('content_modification', {}).get('mean_ms'))}/doc"
        for _, data, label in backends
    ]
    print(f"  {DIM}Content modify:   {' | '.join(mod_parts)}{RESET}")

    # -- Query scenarios -----------------------------------------------------

    # Collect all queries across backends
    all_queries = {}
    for _, data, _ in backends:
        all_queries.update(data.get("queries", {}))
    all_query_names = sorted(all_queries.keys())

    if all_query_names:
        # Header: scenario + one column per backend + ratio columns
        col_headers = [f"{label:>8}" for _, _, label in backends]
        # Ratio columns: compare each non-first backend against the first
        first_label = backends[0][2]
        ratio_headers = [f"{first_label}/{label:>8}" for _, _, label in backends[1:]]

        print(f"\n  {'Query Scenario':<40} {' '.join(col_headers)}"
              f"  {' '.join(f'{h:>12}' for h in ratio_headers)}")
        print(f"  {'─' * 40} {' '.join('─' * 8 for _ in backends)}"
              f"  {' '.join('─' * 12 for _ in backends[1:])}")

        for name in all_query_names:
            # Get median for each backend
            medians = []
            desc = name
            for _, data, _ in backends:
                q = data.get("queries", {}).get(name, {})
                medians.append(q.get("stats", {}).get("median_ms"))
                if q.get("description"):
                    desc = q["description"]

            # Count mismatch warning
            counts = {}
            for _, data, label in backends:
                q = data.get("queries", {}).get(name, {})
                c = q.get("count")
                if c is not None and c != "?":
                    counts[label] = c
            unique_counts = set(counts.values())
            count_note = ""
            if len(unique_counts) > 1:
                count_note = f" {YELLOW}({' '.join(f'{k}={v}' for k, v in counts.items())}){RESET}"

            cols = " ".join(f"{_fmt_ms(m):>8}" for m in medians)
            ratios = "  ".join(
                f"{_ratio_str(medians[0], m):>12}" for m in medians[1:]
            )
            print(f"  {desc:<40} {cols}  {ratios}{count_note}")

    # -- Write-then-query ---------------------------------------------------

    # Check if any backend has write_then_query data
    has_wq = any(data.get("write_then_query") for _, data, _ in backends)
    if has_wq:
        print(f"\n  {HEADER}Write-then-Query (same transaction){RESET}")
        for scenario_key, scenario_label in [
            ("create_then_query", "Create + query"),
            ("modify_then_query", "Modify + query"),
        ]:
            parts = []
            for _, data, label in backends:
                wq = data.get("write_then_query", {}).get(scenario_key, {})
                stats = wq.get("stats", {})
                found_rate = wq.get("found_rate", 0)
                median = stats.get("median_ms")
                if median is not None:
                    found_pct = f"{found_rate * 100:.0f}%"
                    parts.append(f"{label} {_fmt_ms(median)} "
                                 f"(found: {found_pct})")
            if parts:
                print(f"  {scenario_label:20s} {' | '.join(parts)}")

    # -- Rebuild -------------------------------------------------------------

    rebuild_parts = []
    for _, data, label in backends:
        r = data.get("rebuild", {}).get("mean_ms")
        if r:
            rebuild_parts.append(f"{label} {_fmt_ms(r)}")
    if rebuild_parts:
        print(f"\n  {DIM}Rebuild: {' | '.join(rebuild_parts)}{RESET}")


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def _collect_metadata(iterations, warmup):
    """Collect system metadata for reproducibility."""
    meta = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python": sys.version.split()[0],
        "platform": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "iterations": iterations,
        "warmup": warmup,
    }

    # PostgreSQL version
    try:
        import psycopg

        conn = psycopg.connect(PGJSONB_DSN)
        row = conn.execute("SHOW server_version").fetchone()
        if row:
            meta["postgresql"] = row[0]
        conn.close()
    except Exception:
        pass

    # Package versions
    for pkg in ["zodb_pgjsonb", "plone.pgcatalog", "ZODB", "Products.ZCatalog"]:
        try:
            from importlib.metadata import version
            meta[f"{pkg}_version"] = version(pkg)
        except Exception:
            pass

    return meta


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="PGCatalog vs ZCatalog benchmark orchestrator"
    )
    sub = parser.add_subparsers(dest="command")

    # Level 1
    l1 = sub.add_parser("level1", help="Standalone SQL vs BTree benchmark")
    l1.add_argument("--scales", default="small,medium",
                     help="Comma-separated: small,medium,large,xlarge,xxlarge")
    l1.add_argument("--iterations", type=int, default=100)
    l1.add_argument("--warmup", type=int, default=10)

    # Level 2
    l2 = sub.add_parser("level2", help="Real Plone subprocess benchmark")
    l2.add_argument("--docs", type=int, default=500)
    l2.add_argument("--iterations", type=int, default=50)
    l2.add_argument("--warmup", type=int, default=5)
    l2.add_argument("--rebuild", type=int, default=0,
                     help="Number of rebuild iterations (0=skip)")

    # All
    al = sub.add_parser("all", help="Run both Level 1 and Level 2")
    al.add_argument("--scales", default="small,medium",
                     help="Level 1 scales")
    al.add_argument("--docs", type=int, default=500,
                     help="Level 2 document count")
    al.add_argument("--iterations", type=int, default=100,
                     help="Level 1 iterations")
    al.add_argument("--l2-iterations", type=int, default=50,
                     help="Level 2 iterations")
    al.add_argument("--warmup", type=int, default=10)
    al.add_argument("--rebuild", type=int, default=0)

    # Common options
    for p in [l1, l2, al]:
        p.add_argument("--output", help="JSON output file")
    for p in [l2, al]:
        p.add_argument("--cache-size", type=int, default=400,
                         help="ZODB cache-size for ZCatalog backends (default 400, "
                              "simulates large-site cache pressure)")
        p.add_argument("--profile", action="store_true",
                         help="Run cProfile on query scenarios (output to stderr)")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    iterations = getattr(args, "iterations", 100)
    warmup = getattr(args, "warmup", 10)

    print(f"\n{HEADER}PGCatalog vs ZCatalog Benchmark Suite{RESET}")
    print(f"{DIM}Python {sys.version.split()[0]} | "
          f"{platform.system()} {platform.release()}{RESET}")

    all_results = {
        "metadata": _collect_metadata(iterations, warmup),
    }

    # Level 1
    if args.command in ("level1", "all"):
        scales = [s.strip() for s in args.scales.split(",")]
        print(f"\n{HEADER}Level 1: Standalone SQL vs BTree{RESET}")
        print(f"{DIM}Scales: {', '.join(scales)} | "
              f"{iterations} iterations | {warmup} warmup{RESET}")
        all_results["level1"] = run_level1(scales, iterations, warmup)

    # Level 2
    if args.command in ("level2", "all"):
        n_docs = getattr(args, "docs", 500)
        l2_iter = getattr(args, "l2_iterations", iterations)
        l2_warmup = min(warmup, 5)
        rebuild = getattr(args, "rebuild", 0)

        cache_size = getattr(args, "cache_size", 400)

        print(f"\n{HEADER}Level 2: Real Plone Subprocess{RESET}")
        print(f"{DIM}{n_docs} documents | {l2_iter} iterations | "
              f"{l2_warmup} warmup | cache-size {cache_size}{RESET}")
        do_profile = getattr(args, "profile", False)
        l2_results = run_level2(n_docs, l2_iter, l2_warmup, rebuild, cache_size,
                                profile=do_profile)
        all_results["level2"] = l2_results
        print_level2_results(l2_results)

    # JSON export
    output = getattr(args, "output", None)
    if output:
        with open(output, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\n{DIM}Results saved to {output}{RESET}")

    print(f"\n{HEADER}Done.{RESET}\n")


if __name__ == "__main__":
    main()
