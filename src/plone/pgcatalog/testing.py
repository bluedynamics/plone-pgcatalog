"""Test layers for plone.pgcatalog.

Provides Plone integration and functional testing layers with the
move optimization handlers installed and IPGCatalogTool registered.

Two flavours:

1. **DemoStorage layers** (``PGCATALOG_INTEGRATION_TESTING``,
   ``PGCATALOG_FUNCTIONAL_TESTING``): Standard Plone test layers
   backed by in-memory DemoStorage.  Fast, no PostgreSQL needed.

2. **PG-backed layer** (``PGCATALOG_PG_FUNCTIONAL_TESTING``):
   Writes to a real PostgreSQL database via ``PGJsonbStorage``.
   Enables end-to-end testing of the full catalog pipeline
   (content → ZODB → PG catalog columns → SQL queries).
   Requires ``ZODB_TEST_DSN`` or local Docker PG on port 5433.
"""

from plone.app.testing import FunctionalTesting
from plone.app.testing import IntegrationTesting
from plone.app.testing import PLONE_FIXTURE
from plone.app.testing import PloneSandboxLayer
from plone.testing import Layer
from zope.configuration import xmlconfig

import logging


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DemoStorage layers (no PostgreSQL needed)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# PG-backed layer (requires PostgreSQL)
# ---------------------------------------------------------------------------


class PGCatalogPGFixture(Layer):
    """Plone fixture backed by PGJsonbStorage + PostgreSQL.

    Unlike the DemoStorage layers above, this writes to a real
    PostgreSQL database, enabling end-to-end testing of the full
    catalog pipeline (content creation → ZODB commit → PG catalog
    columns → SQL catalog queries).

    Supports stacking: child layers can ``push()``/``pop()`` their own
    snapshot levels on the exposed ``pgTestDB`` resource.

    Layer hierarchy::

        PLONE_FIXTURE (loads ZCML, installs products)
          └── PGCatalogPGFixture (replaces zodbDB with PGJsonbStorage)
                └── CustomTestLayer (optional — adds fixtures, pushes snapshot)
    """

    defaultBases = (PLONE_FIXTURE,)

    def setUp(self):
        from plone.pgcatalog.schema import install_catalog_schema
        from zodb_pgjsonb.storage import PGJsonbStorage
        from zodb_pgjsonb.testing import get_test_dsn
        from zodb_pgjsonb.testing import PGTestDB

        import ZODB

        dsn = get_test_dsn()

        # 1. Set up PG database with base + catalog schema
        self._test_db = PGTestDB(dsn)
        self._test_db.setup()
        install_catalog_schema(self._test_db.connection)

        # 2. Create PGJsonbStorage + ZODB.DB
        self._storage = PGJsonbStorage(dsn=dsn)
        self._db = ZODB.DB(self._storage)
        self["zodbDB"] = self._db

        # 3. Initialize Zope Application root in PG database
        self._init_zope_app()

        # 4. Load pgcatalog components + install move handlers
        self._setup_zcml()

        # 5. Register CatalogStateProcessor on the storage.
        #    PGCatalogLayer loads configure.zcml which registers an
        #    IDatabaseOpenedWithRoot subscriber for this, but PGCatalogPGFixture
        #    doesn't load ZCML — call the handler directly.
        from plone.pgcatalog.startup import register_catalog_processor
        from zope.processlifetime import DatabaseOpenedWithRoot

        register_catalog_processor(DatabaseOpenedWithRoot(self._db))

        # 6. Create Plone site in PG
        self._setup_plone_site()

        # 7. Push snapshot — base state with Plone site
        self._test_db.push()

        # Expose PGTestDB so child layers can push/pop their own snapshots
        self["pgTestDB"] = self._test_db
        log.info("PGCatalogPGFixture: setUp complete (PG-backed)")

    def _init_zope_app(self):
        """Initialize the Zope Application root in the PG database.

        A fresh PG database has an empty ZODB root.  Zope expects the
        root to contain an ``Application`` object (normally created by
        ``Zope2.startup_wsgi``).  We create it and register this DB as
        ``Zope2.DB`` so ``zopeApp()`` uses it.
        """
        from OFS.Application import Application

        import transaction
        import Zope2

        conn = self._db.open()
        root = conn.root()

        if "Application" not in root:
            app = Application()
            app._setId("Application")
            root["Application"] = app
            transaction.commit()

        conn.close()

        # Register this DB in Zope2 so zopeApp() can find it
        self._old_zope2_db = getattr(Zope2, "DB", None)
        Zope2.DB = self._db

    def _setup_zcml(self):
        """Register pgcatalog components and install move handlers.

        PLONE_FIXTURE already loaded core ZCML.  We register pgcatalog
        components directly in Python to avoid genericsetup namespace
        issues with a fresh configuration context.
        """
        from plone.pgcatalog.catalog import PlonePGCatalogTool
        from plone.pgcatalog.interfaces import IPGCatalogTool
        from plone.pgcatalog.move import install_move_handlers
        from zope.component import provideUtility

        # Register IPGCatalogTool utility (normally via ZCML <utility>)
        provideUtility(PlonePGCatalogTool(), IPGCatalogTool)

        # Install move optimization handlers
        install_move_handlers()

    def _setup_plone_site(self):
        """Create a Plone site in the PG-backed database."""
        from plone.app.testing import TEST_USER_ID
        from plone.app.testing import TEST_USER_NAME
        from plone.app.testing import TEST_USER_PASSWORD
        from plone.app.testing.interfaces import DEFAULT_LANGUAGE
        from plone.app.testing.interfaces import PLONE_SITE_ID
        from plone.app.testing.interfaces import PLONE_SITE_TITLE
        from plone.app.testing.interfaces import SITE_OWNER_NAME
        from plone.app.testing.interfaces import SITE_OWNER_PASSWORD
        from plone.app.testing.interfaces import TEST_USER_ROLES
        from plone.testing import zope as zope_testing
        from Products.CMFPlone.factory import addPloneSite

        with zope_testing.zopeApp() as app:
            # Create owner user
            app["acl_users"].userFolderAddUser(
                SITE_OWNER_NAME, SITE_OWNER_PASSWORD, ["Manager"], []
            )
            zope_testing.login(app["acl_users"], SITE_OWNER_NAME)

            # Create Plone site with extension profiles (must match
            # PloneFixture.extensionProfiles to get content types)
            addPloneSite(
                app,
                PLONE_SITE_ID,
                title=PLONE_SITE_TITLE,
                setup_content=False,
                default_language=DEFAULT_LANGUAGE,
                extension_ids=(
                    "plone.app.contenttypes:default",
                    "plonetheme.barceloneta:default",
                ),
            )

            # Disable default workflow
            app[PLONE_SITE_ID]["portal_workflow"].setDefaultChain("")

            # Create test user
            pas = app[PLONE_SITE_ID]["acl_users"]
            pas.source_users.addUser(TEST_USER_ID, TEST_USER_NAME, TEST_USER_PASSWORD)
            for role in TEST_USER_ROLES:
                pas.portal_role_manager.doAssignRoleToPrincipal(TEST_USER_ID, role)

            # Replace standard CatalogTool with PlonePGCatalogTool so that
            # catalog_object() routes through PG annotation pipeline.
            # Must happen AFTER addPloneSite (portal_catalog must exist).
            from plone.pgcatalog.setuphandlers import _replace_catalog

            portal = app[PLONE_SITE_ID]

            from zope.component.hooks import setSite

            setSite(portal)
            _replace_catalog(portal)

            # Re-import catalog indexes on the fresh PlonePGCatalogTool
            # (UID, portal_type, etc. — needed for ZCatalog compatibility)
            from plone.pgcatalog.setuphandlers import _ensure_catalog_indexes

            _ensure_catalog_indexes(portal)
            setSite(None)

            zope_testing.logout()

    def testSetUp(self):
        """Restore PG tables to snapshot + prepare ZODB connection."""
        from plone.app.testing import setRoles
        from plone.app.testing import TEST_USER_ID
        from plone.app.testing.interfaces import PLONE_SITE_ID
        from plone.app.testing.interfaces import SITE_OWNER_NAME
        from plone.testing import zope as zope_testing

        self._test_db.restore()
        # After PG restore, the transaction_log went backwards so
        # poll_invalidations on pooled connections won't detect changes.
        # Clear the pool to force fresh connections + REPEATABLE READ.
        self._db.cacheMinimize()
        self._db.pool.clear()

        # Open app + portal like IntegrationTesting does
        self["app"] = app = zope_testing.addRequestContainer(
            self._db.open().root()["Application"]
        )
        self["portal"] = app[PLONE_SITE_ID]
        self["request"] = self["portal"].REQUEST

        # Login as site owner (Manager) + set up local site manager
        zope_testing.login(app["acl_users"], SITE_OWNER_NAME)

        from zope.component.hooks import setSite

        setSite(self["portal"])
        setRoles(self["portal"], TEST_USER_ID, ["Manager"])

    def testTearDown(self):
        """Clean up ZODB connection and request."""
        from zope.component.hooks import setSite
        from zope.security.management import endInteraction

        import transaction

        endInteraction()
        setSite(None)

        # Abort any pending transaction so the connection can be closed
        transaction.abort()

        # Close the ZODB connection opened in testSetUp
        if "app" in self:
            self["app"]._p_jar.close()
            del self["app"]
        if "portal" in self:
            del self["portal"]
        if "request" in self:
            del self["request"]

    def tearDown(self):
        import Zope2

        self._test_db.pop()
        self._db.close()
        self._storage.close()
        self._test_db.teardown()

        # Restore original Zope2.DB
        Zope2.DB = self._old_zope2_db

        del self["zodbDB"]
        del self["pgTestDB"]
        log.info("PGCatalogPGFixture: tearDown complete")


PGCATALOG_PG_FIXTURE = PGCatalogPGFixture()

# Only FunctionalTesting — IntegrationTesting blocks commits,
# so ZODB writes never reach PG (defeats the purpose of this layer).
PGCATALOG_PG_FUNCTIONAL_TESTING = FunctionalTesting(
    bases=(PGCATALOG_PG_FIXTURE,),
    name="PGCatalogPGFixture:FunctionalTesting",
)
