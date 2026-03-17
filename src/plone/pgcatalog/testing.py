"""Test layers for plone.pgcatalog.

Provides Plone integration and functional testing layers with the
move optimization handlers installed and IPGCatalogTool registered.
"""

from plone.app.testing import FunctionalTesting
from plone.app.testing import IntegrationTesting
from plone.app.testing import PLONE_FIXTURE
from plone.app.testing import PloneSandboxLayer
from zope.configuration import xmlconfig


class PGCatalogLayer(PloneSandboxLayer):
    """Plone sandbox layer with plone.pgcatalog ZCML loaded.

    Loads the configure.zcml which registers:
    - IPGCatalogTool utility
    - IDatabaseOpenedWithRoot subscriber
    - IPubEnd subscriber

    Also installs the move optimization handlers.
    """

    defaultBases = (PLONE_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        import plone.pgcatalog

        xmlconfig.file("configure.zcml", plone.pgcatalog, context=configurationContext)

        # Install move optimization handlers (replaces OFS dispatch handlers)
        from plone.pgcatalog.move import install_move_handlers

        install_move_handlers()


PGCATALOG_FIXTURE = PGCatalogLayer()

PGCATALOG_INTEGRATION_TESTING = IntegrationTesting(
    bases=(PGCATALOG_FIXTURE,),
    name="plone.pgcatalog:Integration",
)

PGCATALOG_FUNCTIONAL_TESTING = FunctionalTesting(
    bases=(PGCATALOG_FIXTURE,),
    name="plone.pgcatalog:Functional",
)
