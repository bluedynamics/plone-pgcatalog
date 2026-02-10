"""Generate realistic Plone-like test data for catalog benchmarks.

Produces deterministic (seeded) datasets with distributions matching
real Plone sites: content types, review states, subjects, security
roles, dates, paths, and full-text content.

Usage:
    from benchmarks.data_generator import generate_objects
    objects = generate_objects(10_000, seed=42)
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone

# ---------------------------------------------------------------------------
# Distribution tables
# ---------------------------------------------------------------------------

CONTENT_TYPES = [
    ("Document", 0.40),
    ("News Item", 0.15),
    ("Event", 0.10),
    ("Image", 0.15),
    ("File", 0.10),
    ("Folder", 0.10),
]

REVIEW_STATES = [
    ("published", 0.70),
    ("private", 0.20),
    ("pending", 0.10),
]

SUBJECTS = [
    "Python",
    "Zope",
    "Plone",
    "JavaScript",
    "CSS",
    "Docker",
    "PostgreSQL",
    "React",
    "Testing",
    "Security",
    "Performance",
    "API",
    "REST",
    "GraphQL",
    "Migration",
]

ROLES_DISTRIBUTION = [
    (["Anonymous"], 0.70),
    (["Anonymous", "Member"], 0.15),
    (["Member", "Editor"], 0.10),
    (["Manager"], 0.05),
]

# Simple word corpus for SearchableText generation.
# Mix of technical + general words for realistic text search.
WORD_CORPUS = (
    "the and for with that this from but have been are was were will can "
    "one all would there their what about which when make like just over "
    "such great its may after also did many before must through back well "
    "system software application framework development programming code "
    "server database web page content management portal site user admin "
    "security access control permission role workflow state publish review "
    "search index catalog query filter sort order result list view template "
    "component interface adapter utility event subscriber handler hook "
    "configuration settings plugin addon extension module package library "
    "python zope plone django flask react angular javascript typescript "
    "html css json xml rest api graphql http request response status error "
    "test unit integration functional performance benchmark speed fast slow "
    "memory cache storage file blob image document folder collection news "
    "migration upgrade import export data model schema field type value "
    "string integer boolean date time datetime timestamp range period "
    "create read update delete crud operation transaction commit rollback "
    "connection pool thread process async concurrent parallel distributed "
    "deploy container docker kubernetes cloud service microservice monitor "
    "log debug trace profile optimize scale load balance proxy reverse "
    "certificate ssl tls encryption hash token session cookie header auth "
    "postgresql mysql sqlite redis elasticsearch rabbitmq kafka celery "
    "numpy pandas matplotlib scikit tensorflow pytorch machine learning "
    "artificial intelligence natural language processing computer vision"
).split()

# Top-level sections for path generation
SECTIONS = [
    "news",
    "events",
    "about",
    "products",
    "services",
    "support",
    "community",
    "documentation",
    "downloads",
    "blog",
    "resources",
    "team",
    "projects",
    "gallery",
    "contact",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _weighted_choice(rng, distribution):
    """Pick from a list of (value, weight) pairs."""
    values, weights = zip(*distribution)
    return rng.choices(values, weights=weights, k=1)[0]


def _generate_path(rng, section_counts):
    """Generate a realistic path in a tree structure.

    Returns (path, parent_path, depth).
    Builds a hierarchical tree with sections → subsections → items.
    """
    section = rng.choice(SECTIONS)
    section_counts[section] = section_counts.get(section, 0) + 1

    depth = rng.choices([2, 3, 4, 5, 6], weights=[10, 35, 30, 15, 10], k=1)[0]

    parts = ["plone", section]
    for level in range(2, depth):
        # Create subsections with limited fan-out
        sub_id = f"item-{rng.randint(1, max(3, 20 // level))}"
        parts.append(sub_id)

    # Leaf node
    slug = f"content-{section_counts[section]}"
    parts.append(slug)

    path = "/" + "/".join(parts)
    parent_path = "/" + "/".join(parts[:-1])
    return path, parent_path, len(parts)


def _generate_text(rng, min_words=50, max_words=200):
    """Generate realistic SearchableText content."""
    n_words = rng.randint(min_words, max_words)
    words = rng.choices(WORD_CORPUS, k=n_words)
    # Capitalize first word, add some sentence structure
    sentences = []
    i = 0
    while i < len(words):
        sent_len = rng.randint(5, 15)
        sent_words = words[i : i + sent_len]
        if sent_words:
            sent_words[0] = sent_words[0].capitalize()
            sentences.append(" ".join(sent_words) + ".")
        i += sent_len
    return " ".join(sentences)


def _generate_title(rng, index):
    """Generate a realistic title for a content item."""
    adjectives = [
        "New",
        "Updated",
        "Important",
        "Featured",
        "Latest",
        "Special",
        "Annual",
        "Monthly",
        "Weekly",
        "Quick",
        "Advanced",
        "Basic",
        "Complete",
        "Essential",
        "Modern",
    ]
    nouns = [
        "Guide",
        "Report",
        "Update",
        "Article",
        "Tutorial",
        "Overview",
        "Analysis",
        "Review",
        "Summary",
        "Announcement",
        "Release",
        "Document",
        "Policy",
        "Handbook",
        "Reference",
    ]
    adj = rng.choice(adjectives)
    noun = rng.choice(nouns)
    topic = rng.choice(SUBJECTS)
    return f"{adj} {topic} {noun} #{index}"


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------


def generate_objects(n, seed=42):
    """Generate n realistic Plone-like catalog objects.

    Args:
        n: Number of objects to generate
        seed: Random seed for reproducibility

    Returns:
        List of dicts, each with keys:
            zoid, path, parent_path, path_depth,
            portal_type, review_state, Title, sortable_title,
            Description, Subject, allowedRolesAndUsers,
            created, modified, effective, expires,
            is_folderish, UID, getObjPositionInParent,
            SearchableText
    """
    rng = random.Random(seed)
    now = datetime(2026, 2, 1, tzinfo=timezone.utc)
    section_counts = {}

    objects = []
    for i in range(n):
        portal_type = _weighted_choice(rng, CONTENT_TYPES)
        review_state = _weighted_choice(rng, REVIEW_STATES)
        roles = _weighted_choice(rng, ROLES_DISTRIBUTION)

        title = _generate_title(rng, i)
        path, parent_path, path_depth = _generate_path(rng, section_counts)

        # Dates: created 0-730 days ago, modified 0-60 days after creation
        created_offset = rng.randint(0, 730)
        created = now - timedelta(days=created_offset)
        modified = created + timedelta(days=rng.randint(0, min(60, created_offset)))

        # Effective: 90% have one (= created or slightly after)
        if rng.random() < 0.9:
            effective = created + timedelta(hours=rng.randint(0, 48))
        else:
            effective = None

        # Expires: 5% have an expiry date
        if rng.random() < 0.05:
            expires = now + timedelta(days=rng.randint(1, 365))
        else:
            expires = None

        # Subjects: 0-4 tags
        n_subjects = rng.choices([0, 1, 2, 3, 4], weights=[20, 30, 25, 15, 10], k=1)[
            0
        ]
        subjects = rng.sample(SUBJECTS, k=min(n_subjects, len(SUBJECTS)))

        text = _generate_text(rng)

        obj = {
            "zoid": i + 1,
            "path": path,
            "parent_path": parent_path,
            "path_depth": path_depth,
            "portal_type": portal_type,
            "review_state": review_state,
            "Title": title,
            "sortable_title": title.lower(),
            "Description": f"Description for {title}",
            "Subject": subjects,
            "allowedRolesAndUsers": roles,
            "created": created.isoformat(),
            "modified": modified.isoformat(),
            "effective": effective.isoformat() if effective else None,
            "expires": expires.isoformat() if expires else None,
            "is_folderish": portal_type == "Folder",
            "UID": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "getObjPositionInParent": rng.randint(0, 99),
            "SearchableText": f"{title} {text}",
        }
        objects.append(obj)

    return objects


def objects_to_idx(obj):
    """Convert a generated object dict to the idx JSONB format.

    Separates out path/searchable_text (dedicated columns) from
    the idx dict (JSONB column).
    """
    idx = {
        "portal_type": obj["portal_type"],
        "review_state": obj["review_state"],
        "Title": obj["Title"],
        "sortable_title": obj["sortable_title"],
        "Description": obj["Description"],
        "Subject": obj["Subject"],
        "allowedRolesAndUsers": obj["allowedRolesAndUsers"],
        "created": obj["created"],
        "modified": obj["modified"],
        "effective": obj["effective"],
        "expires": obj["expires"],
        "is_folderish": obj["is_folderish"],
        "UID": obj["UID"],
        "getObjPositionInParent": obj["getObjPositionInParent"],
    }
    return idx


if __name__ == "__main__":
    # Quick sanity check
    objs = generate_objects(100)
    print(f"Generated {len(objs)} objects")

    # Distribution check
    from collections import Counter

    types = Counter(o["portal_type"] for o in objs)
    states = Counter(o["review_state"] for o in objs)
    print(f"Types: {dict(types)}")
    print(f"States: {dict(states)}")
    print(f"Sample path: {objs[0]['path']}")
    print(f"Sample title: {objs[0]['Title']}")
    print(f"SearchableText length: {len(objs[0]['SearchableText'])} chars")
