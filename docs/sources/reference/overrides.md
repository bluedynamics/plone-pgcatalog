<!-- diataxis: reference -->

# Overrides and replaced behaviour

Everything plone.pgcatalog replaces in Plone, Zope or add-ons, and where.
plone.pgcatalog does not monkeypatch: it does not assign attributes of other packages' modules or classes.
It replaces behaviour through the Zope component registry, ZCML overrides and its own persistent objects.

## Replaced event handlers

Installed by `plone.pgcatalog.move.install_move_handlers()` at Zope startup (`IDatabaseOpenedWithRoot`), in the global component registry.

| Original handler | Replaced by | For | Why |
|---|---|---|---|
| `OFS.subscribers.dispatchObjectWillBeMovedEvent` | `move._wrapped_dispatchObjectWillBeMovedEvent` | `(OFS.interfaces.IItem, IObjectWillBeMovedEvent)` | Flags a move in progress, so the catalog skips the per-object uncatalog of every descendant. |
| `OFS.subscribers.dispatchObjectMovedEvent` | `move._wrapped_dispatchObjectMovedEvent` | `(OFS.interfaces.IItem, IObjectMovedEvent)` | Registers one bulk SQL update of the subtree's paths instead of a reindex per descendant. |

Both wrappers call the original dispatcher.
The effect applies to every move and rename in the process, also on sites that do not use plone.pgcatalog: the wrappers only change behaviour while a PG catalog is active.

## ZCML overrides

Registered in `overrides.zcml`, which Plone loads together with the package's `configure.zcml`.
To undo one, override it again in your own package's `overrides.zcml`.

| What | Replaced component | Condition | Why |
|---|---|---|---|
| GenericSetup import step `toolset` | `plone.pgcatalog.setuphandlers.importToolset` | always | Keeps CMFPlone's `toolset.xml` from replacing `portal_catalog` with a plain `CatalogTool` on later profile imports. |
| Vocabulary `plone.app.vocabularies.Keywords` | `vocabularies.PGKeywordsVocabularyFactory` | always | Lists the keywords of a section (`plone.subjects_of_navigation_root`) in one query instead of one per keyword. Subclass `PGKeywordsVocabulary` for vocabularies over other keyword indexes. See #234. |
| Indexer `SearchableText` for `IFile` | `indexers.indexer_SearchableText_file_override` | always; acts only when `PGCATALOG_TIKA_URL` is set | Skips `portal_transforms` for file bodies, which the Tika worker extracts. Delegates to `plone.app.contenttypes` otherwise. |
| `NamedfileFieldConverter` of `plone.app.dexterity.textindexer` | `indexers.TikaAwareNamedfileFieldConverter` | `plone.app.dexterity` installed; acts only when `PGCATALOG_TIKA_URL` is set | The same for blob fields marked *searchable*. |
| `IFacetedCatalog` utility of eea.facetednavigation | `addons_compat.eeafacetednavigation.PGFacetedCatalog` | `eea.facetednavigation` installed | Facet counts from PostgreSQL. See {doc}`../explanation/addon-integrations`. |

## Replaced persistent objects

Done by the `plone.pgcatalog:default` profile on install.
The profile cannot be uninstalled (see {doc}`../how-to/install`).

| Object | Replaced by | Notes |
|---|---|---|
| `portal_catalog` | `plone.pgcatalog.catalog.PlonePGCatalogTool` | Index definitions and metadata columns, including add-on ones, are carried over; data lives in PostgreSQL. |
| `portal_catalog._catalog` | `maintenance._CatalogCompat` | Holds the index definitions only (no BTrees). |
| `portal_catalog.Indexes` | `pgindex.PGCatalogIndexes` | Hands out the proxies below. |

## Index proxies

`portal_catalog._catalog.indexes[name]`, `_catalog.getIndex(name)` and `portal_catalog.Indexes[name]` return proxies instead of the raw ZCatalog index objects.
Attributes that the proxy does not implement are delegated to the raw index; its BTrees are empty.

| Indexes | Proxy | Answered from PostgreSQL |
|---|---|---|
| Indexes stored in the `idx` JSONB column (field, keyword, date, boolean, UUID, GopipIndex, …) | `pgindex.PGIndex` | `_index` (read-only mapping), `uniqueValues()` (with an optional `path` argument), `_apply_index()` |
| `path`, `effectiveRange`, `SearchableText` (dedicated columns) | `pgindex.PGSpecialIndex` | `_apply_index()` only |

`isinstance(index, ExtendedPathIndex)` and similar checks see the proxy, not the original class; `index.meta_type` still reports the original type.

## Additional hooks

These add behaviour without replacing anything.

| Hook | Registered for | Purpose |
|---|---|---|
| `startup.register_catalog_processor` | `IDatabaseOpenedWithRoot` | Registers the state processor on zodb-pgjsonb, applies the schema, checks the database locale. |
| `pool.release_request_connection` | `IPubEnd` | Returns the request's PostgreSQL connection to the pool. |
| `gopip.container_modified` | `(IOrderableFolder, IContainerModifiedEvent)` | Keeps `getObjPositionInParent` ranks in folder order. |
| `querymodifier.SanitizeRowsModifier` | `IQueryModifier`, named `plone.pgcatalog.sanitize_rows` | Drops malformed Collection query rows before `parseFormquery`. |
| `observability.PGContentMetricProvider` | `IMetricProvider`, named `pgcontent` | Content count metric, when `plone.observability` is installed. |
