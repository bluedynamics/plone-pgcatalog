# Implementation taken from plone.api.portal to avoid
# having plone.api as a dependency

from Acquisition import aq_inner
from plone.registry.interfaces import IRegistry
from zope.component import getUtility
from zope.globalrequest import getRequest


def get_default_language():
    """Return the default language.

    :returns: language identifier
    :rtype: string
    :Example: :ref:`portal-get-default-language-example`
    """
    from plone.i18n.interfaces import ILanguageSchema

    registry = getUtility(IRegistry)
    settings = registry.forInterface(ILanguageSchema, prefix="plone")
    return settings.default_language


def get_current_language(context=None):
    """Return the current negotiated language.

    :param context: context object
    :type context: object
    :returns: language identifier
    :rtype: string
    :Example: :ref:`portal-get-current-language-example`
    """
    request = getRequest()
    return (
        request.get("LANGUAGE", None)
        or (context and aq_inner(context).Language())
        or get_default_language()
    )
