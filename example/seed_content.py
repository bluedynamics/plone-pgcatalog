"""Import seed content from Wikipedia articles into a multilingual Plone site.

Run via zconsole::

    .venv/bin/zconsole run instance/etc/zope.conf example/seed_content.py

Creates Documents in language folders (``/plone/en/library/``,
``/plone/de/library/``, ``/plone/zh/library/``) from
``example/seed_data.json.gz``.

Requires an existing multilingual Plone site with ``plone.pgcatalog`` and
``plone.app.multilingual`` profiles installed (see ``create_site.py``).

Content is CC BY-SA 4.0 licensed (Wikipedia).
"""

import gzip
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import transaction
from AccessControl.SecurityManagement import newSecurityManager
from plone import api
from plone.app.textfield.value import RichTextValue
from plone.i18n.normalizer.interfaces import IURLNormalizer
from zope.component import getUtility
from zope.component.hooks import setSite

# ── Configuration ────────────────────────────────────────────────────

DATA_FILE = Path(__file__).parent / "seed_data.json.gz"
SITE_ID = "Plone"
LANGUAGES = ["en", "de", "zh"]
DEFAULT_LANGUAGE = "en"
FOLDER_ID = "library"
FOLDER_TITLES = {
    "en": "Geography Library",
    "de": "Geographie-Bibliothek",
    "zh": "\u5730\u7406\u56fe\u4e66\u9986",
}
BATCH_SIZE = 50

# ── Helpers ──────────────────────────────────────────────────────────


def _text_to_html(text):
    """Convert plain text to minimal HTML paragraphs."""
    if not text:
        return ""
    paragraphs = text.split("\n\n")
    parts = []
    for p in paragraphs:
        p = p.strip()
        if not p:
            continue
        lines = p.split("\n")
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if (
                len(line) < 80
                and not line.endswith(".")
                and not line.endswith(",")
                and "\n" not in p
                and len(lines) == 1
            ):
                parts.append(f"<h2>{line}</h2>")
            else:
                parts.append(f"<p>{line}</p>")
    return "\n".join(parts)


def _make_id(title, normalizer):
    """Generate a Plone-friendly ID from a title."""
    return normalizer.normalize(title)[:80]


# ── Main ─────────────────────────────────────────────────────────────


def main(app):
    if SITE_ID not in app.objectIds():
        print(f"Error: Plone site '{SITE_ID}' not found.", file=sys.stderr)
        sys.exit(1)

    site = app[SITE_ID]
    setSite(site)

    # Set up admin security
    acl = site.acl_users
    admin = acl.getUserById("admin")
    if admin is None:
        admin = app.acl_users.getUserById("admin")
    if admin is None:
        print("Error: 'admin' user not found.", file=sys.stderr)
        sys.exit(1)
    newSecurityManager(None, admin.__of__(acl))

    # Load seed data
    if not DATA_FILE.exists():
        print(f"Error: {DATA_FILE} not found.", file=sys.stderr)
        print("Run 'python example/fetch_wikipedia.py' first.", file=sys.stderr)
        sys.exit(1)

    with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    articles = data["articles"]
    print(f"Loaded {len(articles)} articles from {DATA_FILE.name}")
    print(f"Source: {data['source']} ({data['license']})")
    print(f"Languages: {data.get('languages', ['en'])}")

    normalizer = getUtility(IURLNormalizer)

    # Group articles by translation_group
    groups = defaultdict(dict)
    for article in articles:
        gid = article.get("group_id", id(article))
        lang = article.get("language", "en")
        groups[gid][lang] = article

    # Ensure library folders exist in each language root
    lang_folders = {}
    for lang in LANGUAGES:
        lang_root = site.get(lang)
        if lang_root is None:
            lang_root = site
        if FOLDER_ID not in lang_root.objectIds():
            title = FOLDER_TITLES.get(lang, "Library")
            folder = api.content.create(
                container=lang_root,
                type="Folder",
                id=FOLDER_ID,
                title=title,
                language=lang,
            )
            api.content.transition(obj=folder, transition="publish")
            transaction.commit()
            print(f"Created /{SITE_ID}/{lang}/{FOLDER_ID}/")
        lang_folders[lang] = lang_root[FOLDER_ID]

    created = 0
    skipped = 0
    linked = 0
    t0 = time.time()

    for gid, lang_articles in groups.items():
        created_docs = {}

        for lang, article in lang_articles.items():
            folder = lang_folders.get(lang)
            if folder is None:
                continue

            doc_id = _make_id(article["title"], normalizer)
            if doc_id in set(folder.objectIds()):
                skipped += 1
                continue

            body_html = _text_to_html(article["body"])
            attribution = (
                f'<p><em>Source: <a href="{article["url"]}">'
                f"Wikipedia: {article['title']}</a> "
                f"({data['license']})</em></p>"
            )
            full_html = attribution + "\n" + body_html

            try:
                doc = api.content.create(
                    container=folder,
                    type="Document",
                    id=doc_id,
                    title=article["title"],
                    description=article["description"][:500]
                    if article["description"]
                    else "",
                    subject=(article.get("category", "Geography"),),
                    language=lang,
                )
                doc.text = RichTextValue(
                    full_html, "text/html", "text/x-html-safe"
                )
                api.content.transition(obj=doc, transition="publish")
                created += 1
                created_docs[lang] = doc
            except Exception as e:
                print(
                    f"  Error creating '{article['title']}' ({lang}): {e}",
                    file=sys.stderr,
                )
                transaction.abort()
                continue

        # Link translations
        if len(created_docs) > 1:
            try:
                from plone.app.multilingual.interfaces import (
                    ITranslationManager,
                )

                canonical_lang = (
                    DEFAULT_LANGUAGE
                    if DEFAULT_LANGUAGE in created_docs
                    else next(iter(created_docs))
                )
                canonical = created_docs[canonical_lang]
                manager = ITranslationManager(canonical)
                for lang, doc in created_docs.items():
                    if lang != canonical_lang:
                        manager.register_translation(lang, doc)
                        linked += 1
            except Exception as e:
                print(f"  Translation link error: {e}", file=sys.stderr)

        if created % BATCH_SIZE == 0 and created > 0:
            transaction.commit()
            elapsed = time.time() - t0
            rate = created / elapsed if elapsed > 0 else 0
            print(f"  {created} created, {linked} linked ({rate:.0f}/s) ...")

    transaction.commit()
    elapsed = time.time() - t0
    print(
        f"\nDone: {created} documents, {linked} translation links, "
        f"{skipped} skipped in {elapsed:.1f}s"
    )
    if created > 0:
        for lang in LANGUAGES:
            print(f"  Browse: http://localhost:8081/{SITE_ID}/{lang}/{FOLDER_ID}")


# zconsole provides `app` in the global namespace
main(app)  # noqa: F821 — `app` injected by zconsole
