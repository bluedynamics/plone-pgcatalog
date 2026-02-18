#!/usr/bin/env python3
"""Fetch geography articles from Wikipedia for the seed dataset.

One-time script -- not needed by users.  Output is committed as
``seed_data.json.gz`` (tracked by git-lfs).

Usage::

    python example/fetch_wikipedia.py

Fetches ~1000 full articles from geography-related categories via the
MediaWiki API and writes ``example/seed_data.json.gz``.

License of the fetched content: CC BY-SA 4.0
(https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License)
"""

import gzip
import json
import time
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "plone-pgcatalog-seed-fetcher/1.0 (https://github.com/bluedynamics/plone-pgcatalog)"

# Categories to fetch.  (label, category_title, max_pages)
# max_pages=0 means: fetch pages from subcategories instead.
CATEGORIES = [
    ("Countries", "Category:Member states of the United Nations", 200),
    ("Countries", "Category:Observer states of the United Nations", 10),
    ("Islands", "Category:Islands by country", 0),
    ("Mountains", "Category:Mountains by country", 0),
    ("Rivers", "Category:Rivers by continent", 0),
    ("Lakes", "Category:Lakes by continent", 0),
    ("Deserts", "Category:Deserts by continent", 0),
    ("World Heritage Sites", "Category:World Heritage Sites by country", 0),
    ("National parks", "Category:National parks by country", 0),
    ("Volcanoes", "Category:Volcanoes by country", 0),
    ("Glaciers", "Category:Glaciers by country", 0),
    ("Seas", "Category:Seas", 50),
    ("Oceans", "Category:Oceans", 10),
    ("Peninsulas", "Category:Peninsulas by continent", 0),
    ("Capes", "Category:Headlands by country", 0),
]

# For parent categories with max_pages=0, fetch subcategory pages
# up to this total per parent.
SUBCAT_PAGE_LIMIT = 80

TARGET_TOTAL = 1050  # fetch a bit more, dedup will trim


def _api_get(params):
    """Make a MediaWiki API GET request."""
    params["format"] = "json"
    url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_category_members(category, cmtype="page", limit=500):
    """Fetch page titles from a category (with continuation)."""
    titles = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": category,
        "cmtype": cmtype,
        "cmlimit": "500",
    }
    while True:
        data = _api_get(params)
        for m in data.get("query", {}).get("categorymembers", []):
            titles.append(m["title"])
            if len(titles) >= limit:
                return titles
        cont = data.get("continue")
        if not cont:
            break
        params.update(cont)
    return titles


def fetch_subcategory_pages(parent_category, limit):
    """Fetch pages from subcategories of a parent category."""
    subcats = fetch_category_members(parent_category, cmtype="subcat", limit=50)
    pages = []
    for subcat in subcats:
        if len(pages) >= limit:
            break
        members = fetch_category_members(subcat, cmtype="page", limit=limit - len(pages))
        pages.extend(members)
        time.sleep(0.1)  # be nice to the API
    return pages[:limit]


def fetch_extract(title):
    """Fetch a full plain-text extract for a single title.

    Full articles can be very large; batching causes the API to
    silently drop results that exceed its response size limit.
    """
    params = {
        "action": "query",
        "prop": "extracts|info",
        "explaintext": "1",
        "exlimit": "1",
        "inprop": "url",
        "titles": title,
    }
    data = _api_get(params)
    for page in data.get("query", {}).get("pages", {}).values():
        if page.get("missing") or not page.get("extract"):
            return None
        return {
            "title": page["title"],
            "extract": page["extract"],
            "url": page.get("fullurl", ""),
            "pageid": page.get("pageid"),
        }
    return None


def split_extract(extract):
    """Split extract into description (first paragraph) and body (rest)."""
    parts = extract.split("\n", 1)
    description = parts[0].strip()
    body = parts[1].strip() if len(parts) > 1 else ""
    return description, body


def main():
    out_path = Path(__file__).parent / "seed_data.json.gz"

    print("Fetching geography articles from Wikipedia...")
    print(f"Target: ~{TARGET_TOTAL} articles\n")

    # Collect page titles by category
    all_pages = {}  # title -> label
    for label, category, max_pages in CATEGORIES:
        if len(all_pages) >= TARGET_TOTAL:
            break
        print(f"  [{label}] {category} ...", end=" ", flush=True)
        if max_pages == 0:
            titles = fetch_subcategory_pages(category, SUBCAT_PAGE_LIMIT)
        else:
            titles = fetch_category_members(category, limit=max_pages)
        new = 0
        for t in titles:
            if t not in all_pages and not t.startswith("Category:"):
                lower = t.lower()
                if any(
                    skip in lower
                    for skip in ("list of", "index of", "outline of", "disambiguation")
                ):
                    continue
                all_pages[t] = label
                new += 1
                if len(all_pages) >= TARGET_TOTAL:
                    break
        print(f"{new} new pages (total: {len(all_pages)})")
        time.sleep(0.2)

    print(f"\nFetching full extracts for {len(all_pages)} pages (one at a time)...")

    titles_list = list(all_pages.keys())
    articles = []
    for i, title in enumerate(titles_list):
        info = fetch_extract(title)
        if info is None:
            continue
        extract = info["extract"]
        if len(extract) < 100:
            continue  # skip stubs
        description, body = split_extract(extract)
        articles.append(
            {
                "title": info["title"],
                "description": description,
                "body": body,
                "category": all_pages[title],
                "url": info["url"],
            }
        )
        if (i + 1) % 50 == 0:
            print(f"  {len(articles)} articles fetched ({i + 1}/{len(titles_list)})...")
        time.sleep(0.1)

    print(f"\nTotal articles with sufficient content: {len(articles)}")

    # Build output
    output = {
        "source": "Wikipedia (en.wikipedia.org)",
        "license": "CC BY-SA 4.0",
        "license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
        "fetched": date.today().isoformat(),
        "article_count": len(articles),
        "articles": articles,
    }

    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=None)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"\nWrote {out_path} ({size_mb:.1f} MB, {len(articles)} articles)")


if __name__ == "__main__":
    main()
