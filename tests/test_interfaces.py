"""Tests for plone.pgcatalog.interfaces — ensure importability and structure."""

from plone.pgcatalog.dri import DateRecurringIndexTranslator
from plone.pgcatalog.interfaces import IPGCatalogTool
from plone.pgcatalog.interfaces import IPGIndexTranslator
from Products.CMFCore.interfaces import ICatalogTool
from zope.interface import Interface
from zope.interface.verify import verifyClass
from zope.interface.verify import verifyObject


def test_ipgcatalogtool_is_interface():
    assert IPGCatalogTool.isOrExtends(Interface)


def test_ipgcatalogtool_extends_icatalogtool():
    assert IPGCatalogTool.isOrExtends(ICatalogTool)


def test_ipgindextranslator_is_interface():
    assert IPGIndexTranslator.isOrExtends(Interface)


def test_ipgindextranslator_has_methods():
    names = list(IPGIndexTranslator.names())
    assert "extract" in names
    assert "query" in names
    assert "sort" in names


def test_dri_translator_verifyclass():
    assert verifyClass(IPGIndexTranslator, DateRecurringIndexTranslator)


def test_dri_translator_verifyobject():
    translator = DateRecurringIndexTranslator(
        date_attr="start",
        recurdef_attr="recurrence",
        until_attr="until",
    )
    assert verifyObject(IPGIndexTranslator, translator)
