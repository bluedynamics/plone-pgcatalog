"""Create a Plone Classic UI site, install pgcatalog, and import seed content.

Run via zconsole::

    zconsole run instance/etc/zope.conf create_site.py

This single script:

1. Creates a Plone Classic UI site (``/Plone``)
2. Installs the ``plone.pgcatalog`` add-on (catalog columns + indexes)
3. Imports ~800 Wikipedia geography articles as published Documents

Requires PostgreSQL running on port 5433 (see docker-compose.yml).
Content is CC BY-SA 4.0 licensed (Wikipedia).
"""

import gzip
import json
import sys
import time
from pathlib import Path

import transaction
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.users import SimpleUser
from plone import api
from plone.app.textfield.value import RichTextValue
from plone.distribution.api import site as site_api
from plone.i18n.normalizer.interfaces import IURLNormalizer
from zope.component import getUtility
from zope.component.hooks import setSite

# ── Configuration ────────────────────────────────────────────────────

SITE_ID = "Plone"
SITE_TITLE = "plone.pgcatalog Example"
# __file__ is not defined in zconsole exec context — use script name from sys.argv
_script_dir = Path(sys.argv[-1]).resolve().parent
DATA_FILE = _script_dir / "seed_data.json.gz"
FOLDER_ID = "library"
FOLDER_TITLE = "Geography Library"
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
            # Heuristic: standalone short line without punctuation = heading
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
    site = site_api.create(
        context=app,
        distribution_name="classic",
        answers={
            "site_id": SITE_ID,
            "title": SITE_TITLE,
            "description": "Example site for plone.pgcatalog",
            "default_language": "en",
            "portal_timezone": "UTC",
            "setup_content": False,
        },
    )
    transaction.commit()
    print(f"  Created /{SITE_ID}/")
    return site


# ── Step 2: Install pgcatalog ───────────────────────────────────────


def install_pgcatalog(site):
    """Install the plone.pgcatalog GenericSetup profile."""
    setSite(site)

    print("Installing plone.pgcatalog ...")
    setup = api.portal.get_tool("portal_setup")
    setup.runAllImportStepsFromProfile("profile-plone.pgcatalog:default")

    # toolset.xml replaces portal_catalog with a fresh PlonePGCatalogTool
    # that has no ZCatalog indexes. Re-apply Plone's catalog config to
    # restore UID, Title, allowedRolesAndUsers, etc.
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

    # Sync the IndexRegistry so _extract_idx() knows about all indexes
    # (at zconsole startup, the registry was empty because no Plone site existed)
    from plone.pgcatalog.columns import get_registry

    registry = get_registry()
    registry.sync_from_catalog(catalog)

    transaction.commit()
    print(f"  plone.pgcatalog installed ({len(list(catalog.indexes()))} catalog indexes)")


# ── Step 3: Import seed content ─────────────────────────────────────


def import_seed_content(site):
    """Import Wikipedia geography articles as Plone Documents."""
    setSite(site)

    # Set up security — use a Manager-role user for content creation
    admin = SimpleUser("admin", "", ["Manager"], [])
    newSecurityManager(None, admin)

    # Load seed data
    if not DATA_FILE.exists():
        print(f"Seed data not found: {DATA_FILE}")
        print("Run 'python fetch_wikipedia.py' first to generate it.")
        print("Skipping seed content import.")
        return

    with gzip.open(DATA_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    articles = data["articles"]
    print(f"\nImporting {len(articles)} articles ({data['license']}) ...")

    # Create or reuse library folder
    if FOLDER_ID not in site.objectIds():
        folder = api.content.create(
            container=site,
            type="Folder",
            id=FOLDER_ID,
            title=FOLDER_TITLE,
            description=f"Wikipedia geography articles ({data['license']}). "
            f"Source: {data['source']}.",
        )
        api.content.transition(obj=folder, transition="publish")
        transaction.commit()
        print(f"  Created /{SITE_ID}/{FOLDER_ID}/")
    else:
        folder = site[FOLDER_ID]
        print(f"  Using existing /{SITE_ID}/{FOLDER_ID}/")

    normalizer = getUtility(IURLNormalizer)
    existing_ids = set(folder.objectIds())
    created = 0
    skipped = 0
    t0 = time.time()

    for article in articles:
        doc_id = _make_id(article["title"], normalizer)
        if doc_id in existing_ids:
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
            )
            doc.text = RichTextValue(
                full_html, "text/html", "text/x-html-safe"
            )
            api.content.transition(obj=doc, transition="publish")
            created += 1
            existing_ids.add(doc_id)
        except Exception as e:
            print(f"  Error creating '{article['title']}': {e}", file=sys.stderr)
            transaction.abort()
            continue

        if created % BATCH_SIZE == 0:
            transaction.commit()
            elapsed = time.time() - t0
            rate = created / elapsed if elapsed > 0 else 0
            print(f"  {created} created ({rate:.0f}/s) ...")

    transaction.commit()
    elapsed = time.time() - t0
    print(
        f"  Done: {created} documents created, {skipped} skipped "
        f"in {elapsed:.1f}s"
    )


# ── Main ─────────────────────────────────────────────────────────────


def main(app):
    create_plone_site(app)
    # Re-fetch site after commit to avoid stale references
    site = app[SITE_ID]
    install_pgcatalog(site)
    # Re-fetch again after pgcatalog install commit
    site = app[SITE_ID]
    import_seed_content(site)
    print(f"\nReady! Start Zope and browse http://localhost:8081/{SITE_ID}/")


# zconsole provides `app` in the global namespace
main(app)  # noqa: F821 — `app` injected by zconsole
