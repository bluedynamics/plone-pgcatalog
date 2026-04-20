"""Tests for SanitizeRowsModifier.

Registered as an IQueryModifier so that collection edit previews
(``@@querybuilder_html_results``) survive rows missing the ``i``
(index) field.  Without this filter, ``queryparser.parseFormquery``
populates ``parsedquery[None] = ...`` and the subsequent
``catalog(**parsedquery)`` call in ``plone.app.querystring`` fails at
the Python level with ``TypeError: keywords must be strings`` — which
tools like the Collection edit widget cannot recover from, blocking
editorial fixes.  See README #142.
"""

from plone.pgcatalog.querymodifier import SanitizeRowsModifier


class TestSanitizeRowsModifier:
    def test_drops_row_without_i(self):
        mod = SanitizeRowsModifier()
        query = [
            {"i": "portal_type", "o": "...selection.any", "v": ["Event"]},
            {"o": "...selection.any", "v": ["AT26"]},  # missing i
            {"i": "review_state", "o": "...selection.any", "v": ["published"]},
        ]
        assert mod(query) == [
            {"i": "portal_type", "o": "...selection.any", "v": ["Event"]},
            {"i": "review_state", "o": "...selection.any", "v": ["published"]},
        ]

    def test_drops_row_with_i_none(self):
        mod = SanitizeRowsModifier()
        query = [
            {"i": None, "o": "op", "v": "x"},
            {"i": "portal_type", "o": "op", "v": ["Event"]},
        ]
        assert mod(query) == [
            {"i": "portal_type", "o": "op", "v": ["Event"]},
        ]

    def test_drops_row_with_i_empty_string(self):
        mod = SanitizeRowsModifier()
        query = [
            {"i": "", "o": "op", "v": "x"},
            {"i": "portal_type", "o": "op", "v": ["Event"]},
        ]
        assert mod(query) == [
            {"i": "portal_type", "o": "op", "v": ["Event"]},
        ]

    def test_drops_row_with_non_string_i(self):
        mod = SanitizeRowsModifier()
        query = [
            {"i": 1, "o": "op", "v": "x"},
            {"i": b"portal_type", "o": "op", "v": ["Event"]},
        ]
        assert mod(query) == []

    def test_passes_through_clean_query(self):
        mod = SanitizeRowsModifier()
        query = [
            {"i": "portal_type", "o": "op", "v": ["Event"]},
            {"i": "path", "o": "op", "v": "/Plone/"},
        ]
        assert mod(query) == query

    def test_handles_none_query(self):
        mod = SanitizeRowsModifier()
        assert mod(None) is None

    def test_handles_empty_query(self):
        mod = SanitizeRowsModifier()
        assert mod([]) == []

    def test_handles_non_list_query_unchanged(self):
        """Some callers pass dicts or tuples — pass them through unchanged."""
        mod = SanitizeRowsModifier()
        query = {"not_a_list": True}
        assert mod(query) == query

    def test_records_are_wrapped_by_publisher_as_dict_like(self):
        """Zope's ``:records`` marshalling returns ``HTTPRequest.record``
        instances, not plain dicts.  They behave like dicts (supports
        ``.get()``) but aren't ``dict`` instances.
        """

        class FakeRecord:
            def __init__(self, data):
                self._data = data

            def get(self, key, default=None):
                return self._data.get(key, default)

            def __eq__(self, other):
                return isinstance(other, FakeRecord) and self._data == other._data

        good = FakeRecord({"i": "portal_type", "v": ["Event"]})
        bad = FakeRecord({"v": ["AT26"]})

        mod = SanitizeRowsModifier()
        result = mod([good, bad])
        assert result == [good]


class TestRegistration:
    def test_modifier_is_registered_as_utility(self, pgcatalog_layer):
        from plone.app.querystring.interfaces import IQueryModifier
        from zope.component import getUtilitiesFor

        utilities = dict(getUtilitiesFor(IQueryModifier))
        assert "plone.pgcatalog.sanitize_rows" in utilities
        assert isinstance(
            utilities["plone.pgcatalog.sanitize_rows"], SanitizeRowsModifier
        )
