"""Tests for _collect_ref_oids — pure unit tests, no PG needed."""

from plone.pgcatalog.processor import _collect_ref_oids

import json


class TestCollectRefOids:
    """Extract @ref oid integers from JSON state dicts."""

    def test_empty_state(self):
        assert _collect_ref_oids({}) == []

    def test_simple_ref(self):
        state = {"blob": {"@ref": "0000000000000042"}}
        assert _collect_ref_oids(state) == [0x42]

    def test_ref_with_class(self):
        state = {"blob": {"@ref": ["00000000000000ff", "ZODB.blob.Blob"]}}
        assert _collect_ref_oids(state) == [0xFF]

    def test_nested_refs(self):
        state = {
            "file": {
                "_blob": {"@ref": "0000000000000001"},
                "contentType": "application/pdf",
            },
            "image": {
                "_blob": {"@ref": "0000000000000002"},
            },
        }
        result = _collect_ref_oids(state)
        assert sorted(result) == [1, 2]

    def test_refs_in_list(self):
        state = {
            "items": [
                {"@ref": "0000000000000003"},
                {"@ref": "0000000000000004"},
            ]
        }
        assert sorted(_collect_ref_oids(state)) == [3, 4]

    def test_non_ref_dicts_ignored(self):
        state = {"title": "Hello", "count": 42, "tags": ["a", "b"]}
        assert _collect_ref_oids(state) == []

    def test_invalid_hex_ignored(self):
        state = {"bad": {"@ref": "not_a_valid_hex!"}}
        assert _collect_ref_oids(state) == []

    def test_wrong_length_ignored(self):
        state = {"short": {"@ref": "0042"}}
        assert _collect_ref_oids(state) == []

    def test_deeply_nested(self):
        state = {"a": {"b": {"c": {"d": {"@ref": "000000000000000a"}}}}}
        assert _collect_ref_oids(state) == [10]

    def test_ref_not_descended_into(self):
        """@ref dicts are leaf nodes — don't recurse into the ref value."""
        state = {"@ref": "0000000000000001"}
        assert _collect_ref_oids(state) == [1]

    def test_real_world_file_state(self):
        """Simulates a Plone File object's JSON state."""
        state = {
            "title": "Report Q4",
            "description": "",
            "file": {
                "@cls": ["plone.namedfile.file", "NamedBlobFile"],
                "_blob": {"@ref": ["0000000000bc614e", "ZODB.blob.Blob"]},
                "contentType": "application/pdf",
                "filename": "report.pdf",
            },
            "creators": ["admin"],
        }
        assert _collect_ref_oids(state) == [0xBC614E]

    # ── JSON string input (decode_zodb_record_for_pg_json path) ────

    def test_json_string_input(self):
        """state may be a JSON string from the fast codec path."""
        state_dict = {"blob": {"@ref": "0000000000000042"}}
        state_str = json.dumps(state_dict)
        assert _collect_ref_oids(state_str) == [0x42]

    def test_json_string_real_world(self):
        """Full File state as JSON string — the actual bug scenario."""
        state_dict = {
            "title": "Report Q4",
            "file": {
                "@cls": ["plone.namedfile.file", "NamedBlobFile"],
                "_blob": {"@ref": ["0000000000bc614e", "ZODB.blob.Blob"]},
                "contentType": "application/pdf",
            },
        }
        state_str = json.dumps(state_dict)
        assert _collect_ref_oids(state_str) == [0xBC614E]

    def test_json_string_empty(self):
        assert _collect_ref_oids("{}") == []

    def test_invalid_json_string(self):
        assert _collect_ref_oids("not json") == []
