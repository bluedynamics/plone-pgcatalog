"""Vocabularies that need PostgreSQL to be fast."""

from plone.app.vocabularies.catalog import KeywordsVocabulary
from plone.app.vocabularies.terms import safe_simplevocabulary_from_values
from plone.base.utils import safe_text
from plone.pgcatalog.pgindex import PGIndex
from Products.CMFCore.utils import getToolByName


class PGKeywordsVocabulary(KeywordsVocabulary):
    """``plone.app.vocabularies.Keywords`` with one query per section (#234).

    With ``plone.subjects_of_navigation_root`` set, Plone's implementation
    intersects the path index with one keyword lookup per keyword, i.e.
    one database query per keyword.  This asks PostgreSQL for the distinct
    keywords below the section in a single query.  Falls back to the
    inherited implementation when the keyword index is not PG-backed.

    Project vocabularies for other keyword indexes can subclass this and
    set ``keyword_index``, like with Plone's class.
    """

    def keywords_of_section(self, section, kwfilter):
        catalog = getToolByName(section, "portal_catalog")
        index = catalog._catalog.getIndex(self.keyword_index)
        if not isinstance(index, PGIndex):
            return super().keywords_of_section(section, kwfilter)
        kwfilter = safe_text(kwfilter)
        path = "/".join(section.getPhysicalPath())
        values = sorted(index.uniqueValues(path=path))
        if kwfilter:
            values = [value for value in values if kwfilter in safe_text(value)]
        return safe_simplevocabulary_from_values(values)


PGKeywordsVocabularyFactory = PGKeywordsVocabulary()
