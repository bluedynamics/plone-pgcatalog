"""Version checks against the installed zodb-pgjsonb (#256)."""

from plone.pgcatalog import maintenance
from plone.pgcatalog import undo

import logging
import pytest


@pytest.mark.parametrize(
    ("installed", "expected"),
    [
        ("1.17.0", True),
        ("1.17.1", True),
        ("1.18.0rc1", True),
        ("1.18.0.dev3", True),
        ("2.0", True),
        ("1.16.2", False),
        ("1.17.0rc1", False),
        ("0.1.dev1+g1a2b3c4", None),
        ("0.0.0", None),
        ("not a version", None),
    ],
)
def test_zodb_pgjsonb_at_least(monkeypatch, installed, expected):
    monkeypatch.setattr(maintenance, "_dist_version", lambda name: installed)
    assert maintenance.zodb_pgjsonb_at_least("1.17.0") is expected


def test_not_installed(monkeypatch):
    def missing(name):
        raise maintenance.PackageNotFoundError(name)

    monkeypatch.setattr(maintenance, "_dist_version", missing)
    assert maintenance.zodb_pgjsonb_at_least("1.17.0") is None


class TestCheckStorageFixed:
    def test_refuses_old(self, monkeypatch):
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: "1.16.2")
        with pytest.raises(RuntimeError, match="still wipes catalog columns"):
            maintenance._check_storage_fixed()

    @pytest.mark.parametrize("installed", ["1.17.0", "2.0", "1.18.0.dev3"])
    def test_accepts_fixed(self, monkeypatch, installed):
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: installed)
        maintenance._check_storage_fixed()

    @pytest.mark.parametrize("installed", ["0.1.dev1+g1a2b3c4", "0.0.0"])
    def test_unknown_version_warns_and_continues(self, monkeypatch, caplog, installed):
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: installed)
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.maintenance"):
            maintenance._check_storage_fixed()
        assert "cannot verify" in caplog.text
        assert installed in caplog.text


class _Storage:
    def supportsUndo(self):
        return True


class TestCheckHookSupportVersions:
    @pytest.mark.parametrize("installed", ["0.1.dev1+g1a2b3c4", "0.0.0"])
    def test_unknown_version_warns_cannot_verify(self, monkeypatch, caplog, installed):
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: installed)
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.undo"):
            assert undo.check_hook_support(_Storage()) is False
        assert "cannot verify" in caplog.text
        assert "has no undone() hook" not in caplog.text

    def test_dev_release_of_a_new_enough_version(self, monkeypatch, caplog):
        monkeypatch.setattr(maintenance, "_dist_version", lambda name: "1.18.1.dev2")
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.undo"):
            assert undo.check_hook_support(_Storage()) is True
        assert caplog.text == ""
