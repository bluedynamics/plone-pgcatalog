"""Create a plain Plone Classic UI site WITHOUT plone.pgcatalog.

Run via zconsole::

    .venv/bin/zconsole run instance/etc/zope.conf scripts/create_plain_site.py

This creates a standard Plone site using ZCatalog (no PG catalog),
installs plone.app.multilingual, and imports ~800 Wikipedia articles.

Use this to test the migration scenario: create content first,
then install plone.pgcatalog later.
"""

import gzip
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import transaction
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.users import SimpleUser
from plone import api
from plone.app.textfield.value import RichTextValue
from plone.i18n.normalizer.interfaces import IURLNormalizer
from zope.component import getUtility
from zope.component.hooks import setSite

# ── Configuration ────────────────────────────────────────────────────

SITE_ID = "Plone"
SITE_TITLE = "Migration Test Site"
_script_dir = Path(sys.argv[-1]).resolve().parent
_example_dir = _script_dir.parent
DATA_FILE = _example_dir / "seed_data.json.gz"
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
    return normalizer.normalize(title)[:80]


# ── Step 1: Create plain Plone site ─────────────────────────────────


def create_plone_site(app):
    if SITE_ID in app.objectIds():
        print(f"Site '{SITE_ID}' already exists, skipping.")
        return app[SITE_ID]

    print(f"Creating plain Plone Classic UI site '{SITE_ID}' ...")
    from plone.distribution.api import site as site_api

    site = site_api.create(
        context=app,
        distribution_name="classic",
        answers={
            "site_id": SITE_ID,
            "title": SITE_TITLE,
            "description": "Plain Plone site for migration testing",
            "default_language": DEFAULT_LANGUAGE,
            "portal_timezone": "UTC",
            "setup_content": False,
        },
    )
    transaction.commit()
    print(f"  Created /{SITE_ID}/ (standard ZCatalog)")
    return site


# ── Step 2: Install plone.app.multilingual ──────────────────────────


def install_multilingual(site):
    setSite(site)
    print("Installing plone.app.multilingual ...")

    from plone.i18n.interfaces import ILanguageSchema
    from plone.registry.interfaces import IRegistry

    registry = getUtility(IRegistry)
    settings = registry.forInterface(ILanguageSchema, prefix="plone")
    settings.available_languages = LANGUAGES
    settings.default_language = DEFAULT_LANGUAGE
    settings.use_combined_language_codes = False
    transaction.commit()

    setup = api.portal.get_tool("portal_setup")
    setup.runAllImportStepsFromProfile("profile-plone.app.multilingual:default")
    transaction.commit()

    missing = [lang for lang in LANGUAGES if lang not in site.objectIds()]
    if missing:
        from plone.app.multilingual.setuphandlers import SetupMultilingualSite

        sms = SetupMultilingualSite()
        sms.setupSite(site)
        transaction.commit()

    for lang in LANGUAGES:
        if lang in site.objectIds():
            print(f"  Language folder /{SITE_ID}/{lang}/ ready")


# ── Step 3: Import seed content ─────────────────────────────────────


def import_seed_content(site):
    setSite(site)
    admin = SimpleUser("admin", "", ["Manager"], [])
    newSecurityManager(None, admin)

    if not DATA_FILE.exists():
        print(f"Seed data not found: {DATA_FILE}")
        print("Run 'python scripts/fetch_wikipedia.py' first.")
        return

    with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    articles = data["articles"]
    print(f"\nImporting {len(articles)} articles ...")

    normalizer = getUtility(IURLNormalizer)
    groups = defaultdict(dict)
    for article in articles:
        gid = article.get("group_id", id(article))
        lang = article.get("language", "en")
        groups[gid][lang] = article

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
            print(f"  Created /{SITE_ID}/{lang}/{FOLDER_ID}/")
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
                print(f"  Error: '{article['title']}' ({lang}): {e}")
                transaction.abort()
                continue

        if len(created_docs) > 1:
            try:
                from plone.app.multilingual.interfaces import ITranslationManager

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
                print(f"  Translation link error: {e}")

        if created % BATCH_SIZE == 0 and created > 0:
            transaction.commit()
            elapsed = time.time() - t0
            rate = created / elapsed if elapsed > 0 else 0
            print(f"  {created} created, {linked} linked ({rate:.0f}/s) ...")

    transaction.commit()
    elapsed = time.time() - t0
    print(
        f"  Done: {created} documents, {linked} translation links, "
        f"{skipped} skipped in {elapsed:.1f}s"
    )


# ── Step 4: Verify ZCatalog works ───────────────────────────────────


def verify_zcatalog(site):
    setSite(site)
    catalog = api.portal.get_tool("portal_catalog")
    print(f"\nCatalog class: {catalog.__class__.__name__}")
    total = len(catalog)
    docs = len(catalog(portal_type="Document"))
    print(f"Indexed objects: {total} total, {docs} Documents")
    if docs > 0:
        print("ZCatalog is working -- site ready for migration test.")
    else:
        print("WARNING: No documents found in catalog!")


# ── Main ─────────────────────────────────────────────────────────────


def main(app):
    create_plone_site(app)
    site = app[SITE_ID]
    install_multilingual(site)
    site = app[SITE_ID]
    import_seed_content(site)
    site = app[SITE_ID]
    verify_zcatalog(site)
    print(f"\nPlain site ready at /{SITE_ID}/ (no pgcatalog)")
    print("Next: install plone-pgcatalog and run the migration script.")


main(app)  # noqa: F821
