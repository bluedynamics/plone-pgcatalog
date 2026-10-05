"""Tests for /rmeta/text response parsing.

The fixtures are real Tika output for two majors, because the majors
differ in ways that break code: see tests/fixtures/tika/README.md.
"""

from plone.pgcatalog.tika_rmeta import extract_text
from plone.pgcatalog.tika_rmeta import METADATA_FIELDS_DEFAULT
from plone.pgcatalog.tika_rmeta import metadata_fields_from_env

import json
import pytest


def load(name):
    with open(f"tests/fixtures/tika/{name}") as fh:
        return json.load(fh)


def test_compound_document_text_appears_exactly_once():
    """Measured: the container entry holds only the file names, the child
    entries hold the text, so concatenation does not double-count."""
    text = extract_text(load("rmeta_4_1_0_compound_zip.json"), METADATA_FIELDS_DEFAULT)
    assert text.count("ALPHA") == 1
    assert text.count("BRAVO") == 1
    assert "vertrag.txt" in text


def test_image_caption_is_harvested_without_ocr():
    text = extract_text(load("rmeta_4_1_0_image_exif.json"), METADATA_FIELDS_DEFAULT)
    assert "Sonnenuntergang am Attersee" in text


def test_only_whitelisted_metadata_is_included():
    payload = [
        {
            "tk:content": "body",
            "dc:title": "Kept",
            "tk:parse-time-millis": "42",
            "tiff:Model": "ILCE-7RM5",
        }
    ]
    text = extract_text(payload, ("dc:title",))
    assert "Kept" in text
    assert "42" not in text
    assert "ILCE-7RM5" not in text


def test_metadata_comes_from_the_container_entry_only():
    payload = [
        {"tk:content": "outer", "dc:title": "Container"},
        {"tk:content": "inner", "dc:title": "Embedded"},
    ]
    text = extract_text(payload, ("dc:title",))
    assert "Container" in text
    assert "Embedded" not in text
    assert "inner" in text, "embedded *content* is still kept"


def test_tika_4_content_key_is_read():
    """Production runs 4.1.0, where the key is tk:content. Reading only the
    3.x name returned empty text for every document."""
    text = extract_text(
        [{"tk:content": "Kaufvertrag Attersee", "dc:title": "T"}],
        ("dc:title",),
    )
    assert "Kaufvertrag Attersee" in text


def test_tika_3_content_key_still_works():
    text = extract_text([{"X-TIKA:content": "Altbestand"}], ())
    assert "Altbestand" in text


def test_both_majors_from_their_real_fixtures_agree():
    """The same ZIP through 3.2.3 and 4.1.0 must yield the same words."""
    old = extract_text(load("rmeta_3_2_3_compound_zip.json"), ())
    new = extract_text(load("rmeta_4_1_0_compound_zip.json"), ())
    for token in ("ALPHA", "BRAVO", "vertrag.txt", "anhang.txt"):
        assert token in old, f"{token} missing from the 3.2.3 fixture"
        assert token in new, f"{token} missing from the 4.1.0 fixture"


def test_the_4_1_0_key_wins_when_both_are_present():
    """Defensive: a proxy or a mixed payload must not double-count."""
    text = extract_text([{"tk:content": "NEW", "X-TIKA:content": "OLD"}], ())
    assert "NEW" in text
    assert "OLD" not in text


def test_rmeta_content_is_not_markdown():
    """Why this change is also a fix. Tika 4's PUT /tika returns Markdown,
    so a link's target lands in searchable_text as junk lexemes;
    /rmeta/text's tk:content does not. Measured against the production
    digest: five lexemes against two for the same sentence."""
    text = extract_text([{"tk:content": "Siehe die Akte."}], ())
    assert "[" not in text
    assert "](" not in text
    assert "example.org" not in text


@pytest.mark.parametrize("payload", [[], {}, None, "not a list"])
def test_degenerate_payloads_yield_empty_text(payload):
    """Review Focus 3: a malformed body must not raise out of the parser."""
    assert extract_text(payload, METADATA_FIELDS_DEFAULT) == ""


def test_list_valued_metadata_is_joined():
    payload = [{"tk:content": "", "dc:subject": ["Vertrag", "Attersee"]}]
    text = extract_text(payload, ("dc:subject",))
    assert "Vertrag" in text
    assert "Attersee" in text


# ── The whitelist must reach both worker modes ──────────────────────


def test_unset_environment_gives_the_default_fields():
    assert metadata_fields_from_env({}) == METADATA_FIELDS_DEFAULT


def test_configured_fields_are_parsed_and_blanks_dropped():
    fields = metadata_fields_from_env(
        {"PGCATALOG_TIKA_METADATA_FIELDS": " dc:title ,, meta:keyword "}
    )
    assert fields == ("dc:title", "meta:keyword")


def test_the_in_process_worker_honours_the_setting(monkeypatch):
    """PGCATALOG_* settings apply to Zope and the in-process worker, which
    startup.py builds without passing metadata_fields. Reading the variable
    only in main() left the in-process worker on the default."""
    from plone.pgcatalog.tika_worker import TikaWorker

    monkeypatch.setenv("PGCATALOG_TIKA_METADATA_FIELDS", "dc:title")
    assert TikaWorker(dsn="x", tika_url="y").metadata_fields == ("dc:title",)
