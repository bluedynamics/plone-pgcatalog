from persistent.mapping import PersistentMapping
from plone.pgcatalog.maintenance import _CatalogIndexMapping

import logging


logger = logging.getLogger(__name__)


def fix_catalog_indexes(context):
    catalog = context.portal_catalog

    raw_indexes = catalog._catalog.indexes

    if isinstance(raw_indexes, _CatalogIndexMapping):
        logger.info("No catalog._catalog.indexes migration necessary")
        return

    indexes = _CatalogIndexMapping()
    indexes.update(raw_indexes)
    catalog._catalog.indexes = indexes

    logger.info("Migrated catalog._catalog.indexes to use a pgatalog index wrapper")
