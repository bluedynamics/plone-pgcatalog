# plone.pgcatalog

PostgreSQL-backed catalog for Plone, replacing ZCatalog BTrees indexes with SQL queries on JSONB.

Extends the `object_state` table from [zodb-pgjsonb](https://github.com/bluedynamics/zodb-pgjsonb) with catalog columns (`idx` JSONB, `path`, `searchable_text` TSVECTOR).
