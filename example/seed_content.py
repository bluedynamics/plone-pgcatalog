"""Import seed content from Wikipedia articles into a Plone site.

Run via zconsole::

    .venv/bin/zconsole run instance/etc/zope.conf example/seed_content.py

Creates Documents in ``/plone/library/`` from ``example/seed_data.json.gz``.
Requires an existing Plone site with the ``plone.pgcatalog`` profile installed.

Content is CC BY-SA 4.0 licensed (Wikipedia).
"""

import gzip
import json
import sys
import time
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
FOLDER_ID = "library"
FOLDER_TITLE = "Geography Library"
BATCH_SIZE = 50  # commit every N documents

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
        # Detect section headers (lines ending with no period, short)
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


# ── Main ─────────────────────────────────────────────────────────────


def main(app):
    # Find Plone site
    if SITE_ID not in app.objectIds():
        print(f"Error: Plone site '{SITE_ID}' not found.", file=sys.stderr)
        print(f"Available: {list(app.objectIds())}", file=sys.stderr)
        sys.exit(1)

    site = app[SITE_ID]
    setSite(site)

    # Set up admin security
    acl = site.acl_users
    admin = acl.getUserById("admin")
    if admin is None:
        # Try root acl_users
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
        print(f"Created /{SITE_ID}/{FOLDER_ID}/")
    else:
        folder = site[FOLDER_ID]
        print(f"Using existing /{SITE_ID}/{FOLDER_ID}/")

    normalizer = getUtility(IURLNormalizer)
    existing_ids = set(folder.objectIds())
    created = 0
    skipped = 0
    t0 = time.time()

    for i, article in enumerate(articles):
        doc_id = _make_id(article["title"], normalizer)
        if doc_id in existing_ids:
            skipped += 1
            continue

        body_html = _text_to_html(article["body"])
        # Prepend source attribution
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
                description=article["description"][:500] if article["description"] else "",
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

    # Final commit
    transaction.commit()
    elapsed = time.time() - t0
    print(
        f"\nDone: {created} documents created, {skipped} skipped "
        f"(already existed) in {elapsed:.1f}s"
    )
    if created > 0:
        print(f"Browse: http://localhost:8081/{SITE_ID}/{FOLDER_ID}")


# zconsole provides `app` in the global namespace
main(app)  # noqa: F821 — `app` injected by zconsole
