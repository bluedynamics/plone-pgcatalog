# plone.pgcatalog

PostgreSQL-backed catalog for Plone, replacing ZCatalog BTrees indexes with SQL queries on JSONB.

Extends the `object_state` table from [zodb-pgjsonb](https://github.com/bluedynamics/zodb-pgjsonb) with catalog columns (`idx` JSONB, `path`, `searchable_text` TSVECTOR).

## Dynamic Index Registration

Indexes are discovered dynamically from ZCatalog's registered index objects at startup. When GenericSetup imports `catalog.xml`, it calls `ZCatalog.addIndex()` which stores index objects in the catalog's internal registry. At Zope startup, `plone.pgcatalog` syncs its `IndexRegistry` from these objects via `sync_from_catalog()`.

This means addons that add indexes via `catalog.xml` profiles are automatically supported — no code changes needed in plone.pgcatalog.

### Supported index meta_types

| ZCatalog meta_type | IndexType | Notes |
|---|---|---|
| FieldIndex | FIELD | Exact match, range, NOT |
| KeywordIndex | KEYWORD | Contains any/all (OR/AND) |
| DateIndex | DATE | Timestamp comparison |
| BooleanIndex | BOOLEAN | True/false |
| DateRangeIndex | DATE_RANGE | Composite (effective + expires) |
| UUIDIndex | UUID | Exact match |
| ZCTextIndex | TEXT | Full-text via tsvector |
| ExtendedPathIndex / PathIndex | PATH | Path containment |
| GopipIndex | GOPIP | Integer position |

### Custom index types (IPGIndexTranslator)

For index types not in the table above, addons can register an `IPGIndexTranslator` named utility:

```python
from plone.pgcatalog.interfaces import IPGIndexTranslator
from zope.interface import implementer

@implementer(IPGIndexTranslator)
class MyCustomTranslator:
    def extract(self, obj, index_name):
        """Return dict to merge into idx JSONB."""
        return {"my_key": getattr(obj, "my_attr", None)}

    def query(self, index_name, query_value, query_options):
        """Return (sql_fragment, params_dict)."""
        return ("idx->>'my_key' = %(my_val)s", {"my_val": query_value})

    def sort(self, index_name):
        """Return SQL expression for ORDER BY, or None."""
        return "idx->>'my_key'"
```

Register via ZCML:
```xml
<utility
    factory=".translators.MyCustomTranslator"
    provides="plone.pgcatalog.interfaces.IPGIndexTranslator"
    name="MyCustomIndex" />
```
