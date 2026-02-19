"""Create a multilingual Plone site, install pgcatalog, and import seed content.

Run via zconsole::

    zconsole run instance/etc/zope.conf scripts/create_site.py

This single script:

1. Creates a Plone Classic UI site (``/Plone``)
2. Installs ``plone.app.multilingual`` with EN, DE, ZH language folders
3. Installs the ``plone.pgcatalog`` add-on (catalog columns + indexes)
4. Imports ~800+ Wikipedia geography articles as published Documents
   across all three languages, with translations linked via PAM

Requires PostgreSQL running on port 5433 (see docker-compose.yml).
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
from AccessControl.users import SimpleUser
from plone import api
from plone.app.textfield.value import RichTextValue
from plone.i18n.normalizer.interfaces import IURLNormalizer
from zope.component import getUtility
from zope.component.hooks import setSite

# ── Configuration ────────────────────────────────────────────────────

SITE_ID = "Plone"
SITE_TITLE = "plone.pgcatalog Multilingual Example"
# __file__ is not defined in zconsole exec context — use script name from sys.argv
_script_dir = Path(sys.argv[-1]).resolve().parent
_example_dir = _script_dir.parent
DATA_FILE = _example_dir / "seed_data.json.gz"
LANGUAGES = ["en", "de", "zh"]
DEFAULT_LANGUAGE = "en"
FOLDER_ID = "library"
FOLDER_TITLES = {
    "en": "Geography Library",
    "de": "Geographie-Bibliothek",
    "zh": "\u5730\u7406\u56fe\u4e66\u9986",  # 地理图书馆
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


# ── Step 1: Create Plone site ───────────────────────────────────────


def create_plone_site(app):
    """Create a Plone Classic UI site if it doesn't exist."""
    if SITE_ID in app.objectIds():
        print(f"Plone site '{SITE_ID}' already exists, skipping creation.")
        return app[SITE_ID]

    print(f"Creating Plone Classic UI site '{SITE_ID}' ...")
    from plone.distribution.api import site as site_api

    site = site_api.create(
        context=app,
        distribution_name="classic",
        answers={
            "site_id": SITE_ID,
            "title": SITE_TITLE,
            "description": "Multilingual example site for plone.pgcatalog",
            "default_language": DEFAULT_LANGUAGE,
            "portal_timezone": "UTC",
            "setup_content": False,
        },
    )
    transaction.commit()
    print(f"  Created /{SITE_ID}/")
    return site


# ── Step 2: Install plone.app.multilingual ──────────────────────────


def install_multilingual(site):
    """Install PAM and set up language folders for EN, DE, ZH."""
    setSite(site)

    print("Installing plone.app.multilingual ...")

    # IMPORTANT: Configure languages BEFORE installing PAM profile.
    # PAM's post_handler (SetupMultilingualSite) checks the language
    # registry and returns early if only 1 language is configured.
    from plone.i18n.interfaces import ILanguageSchema
    from plone.registry.interfaces import IRegistry

    registry = getUtility(IRegistry)
    settings = registry.forInterface(ILanguageSchema, prefix="plone")
    settings.available_languages = LANGUAGES
    settings.default_language = DEFAULT_LANGUAGE
    settings.use_combined_language_codes = False
    transaction.commit()

    # Now install PAM profile — post_handler sees 3 languages, creates LRFs
    setup = api.portal.get_tool("portal_setup")
    setup.runAllImportStepsFromProfile("profile-plone.app.multilingual:default")
    transaction.commit()

    # Verify language root folders; create manually if post_handler missed them
    # (can happen in zconsole context where browser layers are absent)
    missing = [lang for lang in LANGUAGES if lang not in site.objectIds()]
    if missing:
        from plone.app.multilingual.setuphandlers import SetupMultilingualSite

        sms = SetupMultilingualSite()
        sms.setupSite(site)
        transaction.commit()

    # Verify language folders exist
    for lang in LANGUAGES:
        if lang in site.objectIds():
            print(f"  Language folder /{SITE_ID}/{lang}/ ready")
        else:
            print(f"  WARNING: /{SITE_ID}/{lang}/ not created!")

    print(f"  plone.app.multilingual installed ({len(LANGUAGES)} languages)")


# ── Step 3: Install pgcatalog ───────────────────────────────────────


def install_pgcatalog(site):
    """Install the plone.pgcatalog GenericSetup profile."""
    setSite(site)

    print("Installing plone.pgcatalog ...")
    setup = api.portal.get_tool("portal_setup")
    setup.runAllImportStepsFromProfile("profile-plone.pgcatalog:default")

    catalog = api.portal.get_tool("portal_catalog")
    if not list(catalog.indexes()):
        print("  Restoring ZCatalog indexes from Plone core profiles ...")
        for profile_id in [
            "profile-Products.CMFPlone:plone",
            "profile-Products.CMFEditions:CMFEditions",
            "profile-plone.app.contenttypes:default",
            "profile-plone.app.event:default",
        ]:
            setup.runImportStepFromProfile(
                profile_id, "catalog", run_dependencies=False
            )

    from plone.pgcatalog.columns import get_registry

    registry = get_registry()
    registry.sync_from_catalog(catalog)

    transaction.commit()
    print(
        f"  plone.pgcatalog installed ({len(list(catalog.indexes()))} catalog indexes)"
    )


# ── Step 4: Import multilingual seed content ────────────────────────


def import_seed_content(site):
    """Import Wikipedia geography articles into language folders with translations."""
    setSite(site)

    admin = SimpleUser("admin", "", ["Manager"], [])
    newSecurityManager(None, admin)

    if not DATA_FILE.exists():
        print(f"Seed data not found: {DATA_FILE}")
        print("Run 'python scripts/fetch_wikipedia.py' first to generate it.")
        print("Skipping seed content import.")
        return

    with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    articles = data["articles"]
    print(f"\nImporting {len(articles)} articles ({data['license']}) ...")
    print(f"Languages: {data.get('languages', ['en'])}")

    normalizer = getUtility(IURLNormalizer)

    # Group articles by translation_group
    groups = defaultdict(dict)
    for article in articles:
        gid = article.get("group_id", id(article))
        lang = article.get("language", "en")
        groups[gid][lang] = article

    # Create library folders in each language root
    lang_folders = {}
    for lang in LANGUAGES:
        lang_root = site.get(lang)
        if lang_root is None:
            # Fallback: create content in site root if no PAM
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

    # Import articles and link translations
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

        # Link translations via PAM if we have multiple languages
        if len(created_docs) > 1:
            try:
                from plone.app.multilingual.interfaces import (
                    ITranslationManager,
                )

                # Use first language as canonical
                canonical_lang = DEFAULT_LANGUAGE if DEFAULT_LANGUAGE in created_docs else next(iter(created_docs))
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
        f"  Done: {created} documents, {linked} translation links, "
        f"{skipped} skipped in {elapsed:.1f}s"
    )


# ── Main ─────────────────────────────────────────────────────────────


def main(app):
    create_plone_site(app)
    site = app[SITE_ID]
    install_multilingual(site)
    site = app[SITE_ID]
    install_pgcatalog(site)
    site = app[SITE_ID]
    import_seed_content(site)
    print(f"\nReady! Start Zope and browse http://localhost:8081/{SITE_ID}/")
    print("Try searching in different languages:")
    print("  EN: volcano, Amazon River, Mount Everest")
    print("  DE: Vulkan, Amazonas, Mount Everest")
    print("  ZH: \u706b\u5c71, \u4e9a\u9a6c\u900a\u6cb3, \u73e0\u7a46\u6717\u739b\u5cf0")


# zconsole provides `app` in the global namespace
main(app)  # noqa: F821 — `app` injected by zconsole
