"""Tests for MIME type normalisation used by the Tika enqueue gate."""

from plone.pgcatalog.mimetypes import content_types_from_env
from plone.pgcatalog.mimetypes import DEFAULT_CONTENT_TYPES
from plone.pgcatalog.mimetypes import matches
from plone.pgcatalog.mimetypes import normalise


def test_lowercases_and_strips_whitespace():
    assert normalise("  APPLICATION/PDF ") == "application/pdf"


def test_collapses_parameter_whitespace_but_keeps_the_parameter():
    assert normalise("audio/ogg;codecs=opus") == "audio/ogg; codecs=opus"
    assert normalise("audio/ogg ;  codecs=opus") == "audio/ogg; codecs=opus"


def test_none_and_empty_are_none():
    assert normalise(None) is None
    assert normalise("   ") is None


def test_parameterised_value_matches_a_bare_configured_type():
    """The production shape that never matched before."""
    assert matches("text/plain; charset=utf-8", {"text/plain"})


def test_full_string_wins_over_the_bare_type():
    """Tika's own type set contains parameterised entries, so an explicit
    parameterised configuration entry must stay meaningful."""
    allowed = {"audio/ogg; codecs=opus"}
    assert matches("audio/ogg; codecs=opus", allowed)
    assert not matches("audio/ogg", allowed)


def test_bare_type_still_matches_exactly():
    assert matches("application/pdf", {"application/pdf"})
    assert not matches("application/zip", {"application/pdf"})


def test_no_content_type_never_matches():
    assert not matches(None, {"application/pdf"})


# ── The allowlist, shared by the enqueue side and the worker (#235) ──


def test_unset_environment_gives_the_default_allowlist():
    types = content_types_from_env({})
    assert "application/pdf" in types
    assert "image/jpeg" in types, "the default deliberately includes images"


def test_configured_allowlist_is_normalised_and_drops_blanks():
    types = content_types_from_env(
        {"PGCATALOG_TIKA_CONTENT_TYPES": " Application/PDF , application/msword,, "}
    )
    assert types == {"application/pdf", "application/msword"}


def test_default_constant_and_the_unset_case_agree():
    """The enqueue side and the worker must mean the same thing by 'unset'."""
    expected = {normalise(t) for t in DEFAULT_CONTENT_TYPES.split(",") if t.strip()}
    assert content_types_from_env({}) == expected
