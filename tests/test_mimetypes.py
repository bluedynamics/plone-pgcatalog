"""Tests for MIME type normalisation used by the Tika enqueue gate."""

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
