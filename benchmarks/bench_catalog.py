"""Level 1 benchmark: PGCatalog (SQL) vs ZCatalog (BTrees).

Standalone benchmark — no Plone needed.  Inserts synthetic data into
both a PostgreSQL database (for pgcatalog) and an in-memory ZCatalog
(BTrees), then runs identical query scenarios against both.

Usage:
    python benchmarks/bench_catalog.py --scale small
    python benchmarks/bench_catalog.py --scale medium --iterations 50
    python benchmarks/bench_catalog.py --scale small,medium,large
    python benchmarks/bench_catalog.py --scale xlarge --output results.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

# Ensure the parent package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_generator import generate_objects
from data_generator import objects_to_idx


# ---------------------------------------------------------------------------
# ANSI colors
# ---------------------------------------------------------------------------

HEADER = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"

# ---------------------------------------------------------------------------
# Scale definitions
# ---------------------------------------------------------------------------

SCALES = {
    "small": 1_000,
    "medium": 10_000,
    "large": 50_000,
    "xlarge": 100_000,
    "xxlarge": 500_000,
}

# ---------------------------------------------------------------------------
# Timing infrastructure
# ---------------------------------------------------------------------------


@dataclass
class TimingStats:
    """Aggregated timing statistics (milliseconds)."""

    samples: list[float] = field(default_factory=list)

    @property
    def count(self):
        return len(self.samples)

    @property
    def mean(self):
        return statistics.mean(self.samples) if self.samples else 0.0

    @property
    def median(self):
        return statistics.median(self.samples) if self.samples else 0.0

    @property
    def p95(self):
        return _percentile(self.samples, 0.95)

    @property
    def p99(self):
        return _percentile(self.samples, 0.99)

    @property
    def stddev(self):
        return statistics.stdev(self.samples) if len(self.samples) > 1 else 0.0

    @property
    def ops_per_sec(self):
        return 1000.0 / self.mean if self.mean > 0 else 0.0

    def to_dict(self):
        return {
            "count": self.count,
            "mean_ms": round(self.mean, 4),
            "median_ms": round(self.median, 4),
            "p95_ms": round(self.p95, 4),
            "p99_ms": round(self.p99, 4),
            "stddev_ms": round(self.stddev, 4),
            "ops_per_sec": round(self.ops_per_sec, 1),
        }


def _percentile(data, pct):
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[-1]
    return s[f] + (k - f) * (s[c] - s[f])


def bench(fn, iterations, warmup):
    """Run fn() with warmup, return TimingStats."""
    for _ in range(warmup):
        fn()
    stats = TimingStats()
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)
    return stats


# ---------------------------------------------------------------------------
# PGCatalog setup
# ---------------------------------------------------------------------------

DSN = os.environ.get(
    "PGCATALOG_BENCH_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)


def setup_pgcatalog(objects):
    """Insert objects into PostgreSQL and return a connection.

    Uses a single batched INSERT with all columns (base + catalog) in one
    pass via psycopg3 executemany (pipeline mode).  Indexes are created
    AFTER bulk load for maximum throughput.
    """
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Json
    from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

    from plone.pgcatalog.columns import compute_path_info
    from plone.pgcatalog.schema import CATALOG_COLUMNS
    from plone.pgcatalog.schema import CATALOG_FUNCTIONS
    from plone.pgcatalog.schema import CATALOG_INDEXES

    conn = psycopg.connect(DSN, row_factory=dict_row)

    # Clean slate
    conn.execute(
        "DROP TABLE IF EXISTS blob_state, object_state, transaction_log CASCADE"
    )
    conn.commit()

    # Create base schema + catalog columns (but NOT indexes yet)
    conn.execute(HISTORY_FREE_SCHEMA)
    conn.execute(CATALOG_COLUMNS)
    conn.execute(CATALOG_FUNCTIONS)
    conn.commit()

    # Transaction log entry
    conn.execute(
        "INSERT INTO transaction_log (tid) VALUES (1) ON CONFLICT DO NOTHING"
    )

    # Prepare all rows with catalog data included
    params_list = []
    for obj in objects:
        idx = objects_to_idx(obj)
        parent_path, path_depth = compute_path_info(obj["path"])
        params_list.append({
            "zoid": obj["zoid"],
            "portal_type": obj["portal_type"],
            "path": obj["path"],
            "parent_path": parent_path,
            "path_depth": path_depth,
            "idx": Json(idx),
            "searchable_text": obj["SearchableText"],
        })

    # Single batched INSERT — psycopg3 executemany uses pipeline mode
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO object_state
                (zoid, tid, class_mod, class_name, state, state_size,
                 path, parent_path, path_depth, idx, searchable_text)
            VALUES (%(zoid)s, 1, 'plone.app.contenttypes.content',
                    %(portal_type)s, '{}'::jsonb, 2,
                    %(path)s, %(parent_path)s, %(path_depth)s, %(idx)s,
                    to_tsvector('simple'::regconfig, %(searchable_text)s))
            """,
            params_list,
        )
    conn.commit()

    # Create indexes AFTER bulk load (much faster than incremental)
    conn.execute(CATALOG_INDEXES)
    conn.commit()

    # Analyze for accurate query planning
    conn.execute("ANALYZE object_state")
    conn.commit()

    return conn


# ---------------------------------------------------------------------------
# ZCatalog setup
# ---------------------------------------------------------------------------


class _CatalogableObject:
    """Simple object that ZCatalog can index via getattr."""

    def __init__(self, data):
        for k, v in data.items():
            setattr(self, k, v)

    def allowedRolesAndUsers(self):
        return self._allowedRolesAndUsers

    def getObjPositionInParent(self):
        return self._getObjPositionInParent


def setup_zcatalog(objects):
    """Create an in-memory ZCatalog with standard indexes, catalog objects.

    Uses _catalog.addIndex() with index objects directly (not the
    high-level addIndex(type_name) which requires Zope's index registry).
    """
    from Products.ExtendedPathIndex.ExtendedPathIndex import ExtendedPathIndex
    from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
    from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
    from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
    from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
    from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
    from Products.ZCatalog.ZCatalog import ZCatalog
    from Products.ZCTextIndex.ZCTextIndex import PLexicon
    from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex

    catalog = ZCatalog("catalog")
    cat = catalog._catalog  # internal Catalog object

    # FieldIndexes
    for name in [
        "portal_type", "review_state", "sortable_title", "Creator",
        "getId", "Title", "UID", "getObjPositionInParent",
    ]:
        cat.addIndex(name, FieldIndex(name))

    # KeywordIndexes
    for name in ["Subject", "allowedRolesAndUsers"]:
        cat.addIndex(name, KeywordIndex(name))

    # DateIndexes
    for name in ["created", "modified", "effective", "expires"]:
        cat.addIndex(name, DateIndex(name))

    # BooleanIndex
    cat.addIndex("is_folderish", BooleanIndex("is_folderish"))

    # DateRangeIndex
    cat.addIndex(
        "effectiveRange",
        DateRangeIndex("effectiveRange", since_field="effective", until_field="expires"),
    )

    # ExtendedPathIndex
    cat.addIndex("path", ExtendedPathIndex("path"))

    # ZCTextIndex for SearchableText (needs a lexicon with pipeline)
    from Products.ZCTextIndex.Lexicon import CaseNormalizer
    from Products.ZCTextIndex.Lexicon import Splitter

    lexicon = PLexicon("plaintext_lexicon")
    lexicon._pipeline = (Splitter(), CaseNormalizer())
    catalog._setObject("plaintext_lexicon", lexicon)

    class _Extra:
        doc_attr = "SearchableText"
        index_type = "Okapi BM25 Rank"
        lexicon_id = "plaintext_lexicon"

    zctidx = ZCTextIndex("SearchableText", extra=_Extra(), caller=catalog)
    cat.addIndex("SearchableText", zctidx)

    # Metadata columns
    for col in [
        "portal_type", "review_state", "Title", "Description",
        "created", "modified", "UID", "getObjPositionInParent",
        "is_folderish",
    ]:
        cat.addColumn(col)

    # Catalog objects (use internal _catalog to avoid _p_jar access)
    for obj in objects:
        data = dict(obj)
        # ZCatalog needs callable methods for some indexes
        data["_allowedRolesAndUsers"] = data.pop("allowedRolesAndUsers")
        data["_getObjPositionInParent"] = data.pop("getObjPositionInParent")

        wrapper = _CatalogableObject(data)
        uid = obj["path"]  # ZCatalog uses uid=path
        cat.catalogObject(wrapper, uid)

    return catalog


# ---------------------------------------------------------------------------
# Query scenarios
# ---------------------------------------------------------------------------


def get_query_scenarios(objects):
    """Return list of (name, description, query_dict) tuples."""
    now = datetime.now(timezone.utc)

    scenarios = [
        (
            "Q01_field_match",
            "portal_type + review_state (field match)",
            {"portal_type": "Document", "review_state": "published"},
        ),
        (
            "Q02_multi_index",
            "4-index intersection (type+state+subject+security)",
            {
                "portal_type": "Document",
                "review_state": "published",
                "Subject": "Python",
                "allowedRolesAndUsers": ["Anonymous"],
            },
        ),
        (
            "Q03_date_range",
            "created in last 30 days (date range)",
            {
                "created": {
                    "query": [
                        (now - timedelta(days=30)).isoformat(),
                        now.isoformat(),
                    ],
                    "range": "min:max",
                },
            },
        ),
        (
            "Q05_fulltext",
            "SearchableText search",
            {"SearchableText": "python performance"},
        ),
        (
            "Q06_fulltext_filtered",
            "SearchableText + type + state + security",
            {
                "SearchableText": "python",
                "portal_type": "Document",
                "review_state": "published",
                "allowedRolesAndUsers": ["Anonymous"],
            },
        ),
        (
            "Q07_path_subtree",
            "path subtree query",
            {"path": "/plone/news"},
        ),
        (
            "Q08_sorted_paginated",
            "type filter + sort by modified + LIMIT 20",
            {
                "portal_type": "Document",
                "sort_on": "modified",
                "sort_order": "descending",
                "b_start": 0,
                "b_size": 20,
            },
        ),
        (
            "Q09_deep_page",
            "type filter + sort by title + OFFSET 5000 LIMIT 20",
            {
                "portal_type": "Document",
                "sort_on": "sortable_title",
                "b_start": 5000,
                "b_size": 20,
            },
        ),
        (
            "Q10_keyword_overlap",
            "keyword OR query (Subject)",
            {"Subject": ["Python", "Testing"]},
        ),
    ]

    return scenarios


# ---------------------------------------------------------------------------
# PGCatalog query runner
# ---------------------------------------------------------------------------


def run_pgcatalog_query(conn, query_dict):
    """Run a query via pgcatalog and return result count."""
    from plone.pgcatalog.search import _run_search

    results = _run_search(conn, query_dict)
    return len(results)


# ---------------------------------------------------------------------------
# ZCatalog query runner
# ---------------------------------------------------------------------------


def run_zcatalog_query(catalog, query_dict):
    """Run a query via ZCatalog and return result count."""
    results = catalog.searchResults(**query_dict)
    return len(results)


# ---------------------------------------------------------------------------
# Write scenario runners
# ---------------------------------------------------------------------------


def bench_pgcatalog_writes(conn, objects, n_writes):
    """Benchmark catalog_object writes to PG."""
    from plone.pgcatalog.indexing import catalog_object

    subset = objects[:n_writes]
    stats = TimingStats()
    for obj in subset:
        idx = objects_to_idx(obj)
        t0 = time.perf_counter()
        catalog_object(
            conn, obj["zoid"], obj["path"], idx,
            searchable_text=obj["SearchableText"],
        )
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)
    conn.commit()
    return stats


def bench_zcatalog_writes(catalog, objects, n_writes):
    """Benchmark catalog_object writes to ZCatalog.

    Uses the internal _catalog.catalogObject to avoid _p_jar access
    on non-persistent ZCatalog instances.
    """
    subset = objects[:n_writes]
    stats = TimingStats()
    cat = catalog._catalog
    for obj in subset:
        data = dict(obj)
        data["_allowedRolesAndUsers"] = data.pop("allowedRolesAndUsers")
        data["_getObjPositionInParent"] = data.pop("getObjPositionInParent")
        wrapper = _CatalogableObject(data)
        t0 = time.perf_counter()
        cat.catalogObject(wrapper, obj["path"])
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)
    return stats


# ---------------------------------------------------------------------------
# Main benchmark runner
# ---------------------------------------------------------------------------


def run_benchmark(scale_name, n_objects, iterations, warmup):
    """Run the full benchmark at a given scale. Returns results dict."""
    print(f"\n{HEADER}{'=' * 70}")
    print(f"  Scale: {scale_name} ({n_objects:,} objects) | "
          f"{iterations} iterations | {warmup} warmup")
    print(f"{'=' * 70}{RESET}\n")

    # Generate data
    print(f"  {DIM}Generating {n_objects:,} objects...{RESET}", end="", flush=True)
    t0 = time.perf_counter()
    objects = generate_objects(n_objects)
    t1 = time.perf_counter()
    print(f" {(t1-t0)*1000:.0f}ms")

    # Populate IndexRegistry for pgcatalog query builder
    from plone.pgcatalog.columns import IndexType, get_registry

    registry = get_registry()
    if "portal_type" not in registry:
        # Register indexes (same as conftest.py)
        for name, itype, key in [
            ("portal_type", IndexType.FIELD, "portal_type"),
            ("review_state", IndexType.FIELD, "review_state"),
            ("sortable_title", IndexType.FIELD, "sortable_title"),
            ("Creator", IndexType.FIELD, "Creator"),
            ("Title", IndexType.TEXT, "Title"),
            ("UID", IndexType.UUID, "UID"),
            ("Subject", IndexType.KEYWORD, "Subject"),
            ("allowedRolesAndUsers", IndexType.KEYWORD, "allowedRolesAndUsers"),
            ("created", IndexType.DATE, "created"),
            ("modified", IndexType.DATE, "modified"),
            ("effective", IndexType.DATE, "effective"),
            ("expires", IndexType.DATE, "expires"),
            ("is_folderish", IndexType.BOOLEAN, "is_folderish"),
            ("getObjPositionInParent", IndexType.GOPIP, "getObjPositionInParent"),
            ("effectiveRange", IndexType.DATE_RANGE, None),
            ("SearchableText", IndexType.TEXT, None),
            ("path", IndexType.PATH, None),
        ]:
            registry.register(name, itype, key)

    # Setup PGCatalog
    print(f"  {DIM}Setting up PGCatalog...{RESET}", end="", flush=True)
    t0 = time.perf_counter()
    pg_conn = setup_pgcatalog(objects)
    t1 = time.perf_counter()
    pg_setup_ms = (t1 - t0) * 1000.0
    print(f" {pg_setup_ms:.0f}ms")

    # Setup ZCatalog
    print(f"  {DIM}Setting up ZCatalog...{RESET}", end="", flush=True)
    t0 = time.perf_counter()
    zc = setup_zcatalog(objects)
    t1 = time.perf_counter()
    zc_setup_ms = (t1 - t0) * 1000.0
    print(f" {zc_setup_ms:.0f}ms")

    results = {
        "scale": scale_name,
        "n_objects": n_objects,
        "setup_pgcatalog_ms": round(pg_setup_ms, 1),
        "setup_zcatalog_ms": round(zc_setup_ms, 1),
        "queries": {},
        "writes": {},
    }

    # Run query scenarios
    scenarios = get_query_scenarios(objects)

    print(f"\n  {'Scenario':<45} {'PGCat (ms)':>10} {'ZCat (ms)':>10} {'Ratio':>12}")
    print(f"  {'─' * 45} {'─' * 10} {'─' * 10} {'─' * 12}")

    for name, description, query_dict in scenarios:
        # PGCatalog
        pg_result_count = [0]

        def pg_query(q=query_dict):
            pg_result_count[0] = run_pgcatalog_query(pg_conn, q)

        pg_stats = bench(pg_query, iterations, warmup)

        # ZCatalog
        zc_result_count = [0]

        def zc_query(q=query_dict):
            zc_result_count[0] = run_zcatalog_query(zc, q)

        try:
            zc_stats = bench(zc_query, iterations, warmup)
        except Exception as e:
            # Some query types may not be supported by bare ZCatalog
            zc_stats = None
            print(f"  {description:<45} {pg_stats.median:>10.3f} "
                  f"{'ERROR':>10} {'(skip)':>12}  {DIM}{e}{RESET}")
            results["queries"][name] = {
                "description": description,
                "pgcatalog": pg_stats.to_dict(),
                "zcatalog": None,
                "pgcatalog_count": pg_result_count[0],
                "zcatalog_count": None,
                "ratio": None,
            }
            continue

        # Calculate ratio
        if zc_stats and zc_stats.median > 0:
            ratio = zc_stats.median / pg_stats.median
            if ratio >= 1:
                ratio_str = f"{GREEN}{ratio:.1f}x faster{RESET}"
            else:
                ratio_str = f"{RED}{1/ratio:.1f}x slower{RESET}"
        else:
            ratio = None
            ratio_str = "N/A"

        # Result count verification
        count_match = pg_result_count[0] == zc_result_count[0]
        count_note = "" if count_match else f" {YELLOW}(count mismatch: pg={pg_result_count[0]} zc={zc_result_count[0]}){RESET}"

        print(f"  {description:<45} {pg_stats.median:>10.3f} "
              f"{zc_stats.median:>10.3f} {ratio_str}{count_note}")

        results["queries"][name] = {
            "description": description,
            "pgcatalog": pg_stats.to_dict(),
            "zcatalog": zc_stats.to_dict() if zc_stats else None,
            "pgcatalog_count": pg_result_count[0],
            "zcatalog_count": zc_result_count[0],
            "ratio": round(ratio, 2) if ratio else None,
        }

    # Write benchmarks
    n_writes = min(1000, n_objects)
    print(f"\n  {HEADER}Write benchmarks ({n_writes} objects){RESET}")
    print(f"  {'Operation':<45} {'PGCat (ms)':>10} {'ZCat (ms)':>10} {'Ratio':>12}")
    print(f"  {'─' * 45} {'─' * 10} {'─' * 10} {'─' * 12}")

    # W1: catalog_object (re-catalog existing objects)
    pg_write_stats = bench_pgcatalog_writes(pg_conn, objects, n_writes)
    zc_write_stats = bench_zcatalog_writes(zc, objects, n_writes)

    w_ratio = zc_write_stats.median / pg_write_stats.median if pg_write_stats.median > 0 else None
    if w_ratio and w_ratio >= 1:
        w_ratio_str = f"{GREEN}{w_ratio:.1f}x faster{RESET}"
    elif w_ratio:
        w_ratio_str = f"{RED}{1/w_ratio:.1f}x slower{RESET}"
    else:
        w_ratio_str = "N/A"

    print(f"  {'catalog_object (per-object)':<45} {pg_write_stats.median:>10.3f} "
          f"{zc_write_stats.median:>10.3f} {w_ratio_str}")

    results["writes"]["catalog_object"] = {
        "pgcatalog": pg_write_stats.to_dict(),
        "zcatalog": zc_write_stats.to_dict(),
        "ratio": round(w_ratio, 2) if w_ratio else None,
    }

    # Setup time comparison
    setup_ratio = zc_setup_ms / pg_setup_ms if pg_setup_ms > 0 else None
    print(f"\n  {DIM}Setup: PGCatalog {pg_setup_ms:.0f}ms vs ZCatalog {zc_setup_ms:.0f}ms{RESET}")

    results["setup_ratio"] = round(setup_ratio, 2) if setup_ratio else None

    pg_conn.close()

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="PGCatalog vs ZCatalog benchmark (Level 1: standalone)"
    )
    parser.add_argument(
        "--scale",
        default="small",
        help="Comma-separated scales: small,medium,large,xlarge,xxlarge (default: small)",
    )
    parser.add_argument(
        "--iterations", type=int, default=100,
        help="Query iterations per scenario (default: 100)",
    )
    parser.add_argument(
        "--warmup", type=int, default=10,
        help="Warmup iterations (default: 10)",
    )
    parser.add_argument(
        "--output", help="JSON output file path",
    )
    args = parser.parse_args()

    scales = [s.strip() for s in args.scale.split(",")]
    for s in scales:
        if s not in SCALES:
            print(f"Unknown scale: {s}. Available: {', '.join(SCALES.keys())}")
            sys.exit(1)

    print(f"\n{HEADER}PGCatalog vs ZCatalog Benchmark — Level 1 (SQL vs BTrees){RESET}")
    print(f"{DIM}Python {sys.version.split()[0]} | {platform.system()} {platform.release()}{RESET}")

    all_results = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "python": sys.version.split()[0],
            "platform": f"{platform.system()} {platform.release()}",
            "iterations": args.iterations,
            "warmup": args.warmup,
        },
        "level1": {},
    }

    for scale_name in scales:
        n = SCALES[scale_name]
        result = run_benchmark(scale_name, n, args.iterations, args.warmup)
        all_results["level1"][scale_name] = result

    if args.output:
        with open(args.output, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\n{DIM}Results saved to {args.output}{RESET}")

    print(f"\n{HEADER}Done.{RESET}\n")


if __name__ == "__main__":
    main()
