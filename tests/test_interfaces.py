"""Tests for plone.pgcatalog.interfaces — ensure importability and structure."""

from plone.pgcatalog.interfaces import IPGCatalogTool, IPGIndexTranslator
from zope.interface import Interface


def test_ipgcatalogtool_is_interface():
    assert IPGCatalogTool.isOrExtends(Interface)


def test_ipgindextranslator_is_interface():
    assert IPGIndexTranslator.isOrExtends(Interface)


def test_ipgindextranslator_has_methods():
    names = list(IPGIndexTranslator.names())
    assert "extract" in names
    assert "query" in names
    assert "sort" in names
