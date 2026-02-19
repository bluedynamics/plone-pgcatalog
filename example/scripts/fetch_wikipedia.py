#!/usr/bin/env python3
"""Fetch multilingual geography articles from Wikipedia for the seed dataset.

One-time script -- not needed by users.  Output is committed as
``seed_data.json.gz`` (tracked by git-lfs).

Usage::

    python example/scripts/fetch_wikipedia.py

Fetches ~300 articles per language (English, German, Chinese) from
geography-related categories via the MediaWiki API, discovers cross-language
translations via ``langlinks``, and writes ``example/seed_data.json.gz``.

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

USER_AGENT = (
    "plone-pgcatalog-seed-fetcher/2.0 "
    "(https://github.com/bluedynamics/plone-pgcatalog)"
)

# Wikipedia API endpoints per language
WIKI_APIS = {
    "en": "https://en.wikipedia.org/w/api.php",
    "de": "https://de.wikipedia.org/w/api.php",
    "zh": "https://zh.wikipedia.org/w/api.php",
}

# Languages to fetch translations for
LANGUAGES = ["en", "de", "zh"]

# Categories to fetch (English Wikipedia, we'll find translations via langlinks)
CATEGORIES = [
    ("Countries", "Category:Member states of the United Nations", 200),
    ("Countries", "Category:Observer states of the United Nations", 10),
    ("Islands", "Category:Islands by country", 0),
    ("Mountains", "Category:Mountains by country", 0),
    ("Rivers", "Category:Rivers by continent", 0),
    ("Lakes", "Category:Lakes by continent", 0),
    ("Deserts", "Category:Deserts by continent", 0),
    ("World Heritage Sites", "Category:World Heritage Sites by country", 0),
    ("Volcanoes", "Category:Volcanoes by country", 0),
    ("Seas", "Category:Seas", 50),
    ("Oceans", "Category:Oceans", 10),
]

SUBCAT_PAGE_LIMIT = 60
TARGET_TOTAL = 400  # English articles to start from


def _api_get(api_url, params):
    """Make a MediaWiki API GET request."""
    params["format"] = "json"
    url = f"{api_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_category_members(api_url, category, cmtype="page", limit=500):
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
        data = _api_get(api_url, params)
        for m in data.get("query", {}).get("categorymembers", []):
            titles.append(m["title"])
            if len(titles) >= limit:
                return titles
        cont = data.get("continue")
        if not cont:
            break
        params.update(cont)
    return titles


def fetch_subcategory_pages(api_url, parent_category, limit):
    """Fetch pages from subcategories of a parent category."""
    subcats = fetch_category_members(
        api_url, parent_category, cmtype="subcat", limit=50
    )
    pages = []
    for subcat in subcats:
        if len(pages) >= limit:
            break
        members = fetch_category_members(
            api_url, subcat, cmtype="page", limit=limit - len(pages)
        )
        pages.extend(members)
        time.sleep(0.1)
    return pages[:limit]


def fetch_langlinks(api_url, title, target_langs):
    """Fetch interlanguage links for a title.

    Returns dict {lang_code: title_in_that_language}.
    """
    params = {
        "action": "query",
        "prop": "langlinks",
        "titles": title,
        "lllimit": "500",
    }
    result = {}
    data = _api_get(api_url, params)
    for page in data.get("query", {}).get("pages", {}).values():
        for ll in page.get("langlinks", []):
            lang = ll.get("lang", "")
            if lang in target_langs:
                result[lang] = ll["*"]
    return result


def fetch_extract(api_url, title):
    """Fetch a full plain-text extract for a single title."""
    params = {
        "action": "query",
        "prop": "extracts|info",
        "explaintext": "1",
        "exlimit": "1",
        "inprop": "url",
        "titles": title,
    }
    data = _api_get(api_url, params)
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
    out_path = Path(__file__).resolve().parent.parent / "seed_data.json.gz"

    print("Fetching multilingual geography articles from Wikipedia...", flush=True)
    print(f"Languages: {', '.join(LANGUAGES)}", flush=True)
    print(f"Target: ~{TARGET_TOTAL} English articles + translations\n", flush=True)

    en_api = WIKI_APIS["en"]

    # Step 1: Collect English page titles by category
    all_pages = {}  # title -> label
    for label, category, max_pages in CATEGORIES:
        if len(all_pages) >= TARGET_TOTAL:
            break
        print(f"  [{label}] {category} ...", end=" ", flush=True)
        if max_pages == 0:
            titles = fetch_subcategory_pages(en_api, category, SUBCAT_PAGE_LIMIT)
        else:
            titles = fetch_category_members(en_api, category, limit=max_pages)
        new = 0
        for t in titles:
            if t not in all_pages and not t.startswith("Category:"):
                lower = t.lower()
                if any(
                    skip in lower
                    for skip in (
                        "list of", "index of", "outline of", "disambiguation",
                    )
                ):
                    continue
                all_pages[t] = label
                new += 1
                if len(all_pages) >= TARGET_TOTAL:
                    break
        print(f"{new} new (total: {len(all_pages)})")
        time.sleep(0.2)

    # Step 2: For each English article, find langlinks + fetch extracts
    print(f"\nFetching extracts + translations for {len(all_pages)} articles...", flush=True)
    target_langs = set(LANGUAGES) - {"en"}

    # translation_groups: list of dicts with articles keyed by language
    translation_groups = []
    articles_by_lang = {lang: [] for lang in LANGUAGES}
    group_id = 0

    for i, (en_title, label) in enumerate(all_pages.items()):
        # Fetch English extract
        en_info = fetch_extract(en_api, en_title)
        if en_info is None or len(en_info["extract"]) < 100:
            continue

        en_desc, en_body = split_extract(en_info["extract"])
        group = {
            "group_id": group_id,
            "category": label,
            "articles": {
                "en": {
                    "title": en_info["title"],
                    "description": en_desc,
                    "body": en_body,
                    "url": en_info["url"],
                },
            },
        }

        # Fetch langlinks for de and zh
        try:
            langlinks = fetch_langlinks(en_api, en_title, target_langs)
        except Exception:
            langlinks = {}

        for lang, foreign_title in langlinks.items():
            try:
                foreign_api = WIKI_APIS[lang]
                info = fetch_extract(foreign_api, foreign_title)
                if info and len(info["extract"]) >= 80:
                    desc, body = split_extract(info["extract"])
                    group["articles"][lang] = {
                        "title": info["title"],
                        "description": desc,
                        "body": body,
                        "url": info["url"],
                    }
            except Exception:
                pass
            time.sleep(0.05)

        translation_groups.append(group)
        for lang, art in group["articles"].items():
            articles_by_lang[lang].append(art["title"])
        group_id += 1

        langs_got = list(group["articles"].keys())
        if (i + 1) % 5 == 0 or len(langs_got) > 1:
            counts = {lang: len(a) for lang, a in articles_by_lang.items()}
            print(
                f"  {i + 1}/{len(all_pages)} {en_title[:40]}"
                f" [{','.join(langs_got)}] — totals: {counts}",
                flush=True,
            )
        time.sleep(0.1)

    # Flatten to articles list with language + group_id
    articles = []
    for group in translation_groups:
        for lang, art in group["articles"].items():
            articles.append(
                {
                    "title": art["title"],
                    "description": art["description"],
                    "body": art["body"],
                    "url": art["url"],
                    "language": lang,
                    "category": group["category"],
                    "group_id": group["group_id"],
                }
            )

    counts = {l: len(a) for l, a in articles_by_lang.items()}
    print(f"\nArticles by language: {counts}")
    print(f"Translation groups: {len(translation_groups)}")
    print(f"Total articles: {len(articles)}")

    # Build output
    output = {
        "source": "Wikipedia (en/de/zh.wikipedia.org)",
        "license": "CC BY-SA 4.0",
        "license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
        "fetched": date.today().isoformat(),
        "languages": LANGUAGES,
        "article_count": len(articles),
        "translation_groups": len(translation_groups),
        "articles": articles,
    }

    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=None)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"\nWrote {out_path} ({size_mb:.1f} MB, {len(articles)} articles)")


if __name__ == "__main__":
    main()
