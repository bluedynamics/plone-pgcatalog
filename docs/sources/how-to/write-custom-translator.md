<!-- diataxis: how-to -->

# Write a Custom Index Translator

## When You Need This

Use `IPGIndexTranslator` when your ZCatalog index type is not in the standard `META_TYPE_MAP`:

- Custom composite indexes
- Third-party addon indexes
- Non-standard query formats

Standard types (FieldIndex, KeywordIndex, DateIndex, BooleanIndex, DateRangeIndex, UUIDIndex, ZCTextIndex, ExtendedPathIndex, GopipIndex) are handled automatically.

## The IPGIndexTranslator Interface

Three methods:

```python
def extract(obj, index_name) -> dict:
    """Return key-value pairs to merge into idx JSONB."""

def query(index_name, raw, spec) -> tuple[str, dict]:
    """Return (sql_fragment, params_dict) for WHERE clause."""

def sort(index_name) -> str | None:
    """Return SQL expression for ORDER BY, or None."""
```

## Example: A Priority Score Index

Suppose you have a custom `PriorityIndex` that stores a numeric score:

```python
from plone.pgcatalog.columns import validate_identifier
from plone.pgcatalog.interfaces import IPGIndexTranslator
from zope.interface import implementer


@implementer(IPGIndexTranslator)
class PriorityIndexTranslator:

    def extract(self, obj, index_name):
        score = getattr(obj, "priority_score", None)
        if score is not None:
            return {index_name: int(score)}
        return {}

    def query(self, index_name, raw, spec):
        validate_identifier(index_name)
        query_val = spec.get("query")
        range_spec = spec.get("range")
        if query_val is None:
            return ("TRUE", {})

        p = f"priority_{index_name}"
        if range_spec == "min":
            sql = f"(idx->>'{index_name}')::int >= %({p})s"
            return (sql, {p: int(query_val)})
        elif range_spec == "max":
            sql = f"(idx->>'{index_name}')::int <= %({p})s"
            return (sql, {p: int(query_val)})
        else:
            sql = f"(idx->>'{index_name}')::int = %({p})s"
            return (sql, {p: int(query_val)})

    def sort(self, index_name):
        return f"(idx->>'{index_name}')::int"
```

## Register via ZCML

```xml
<utility
    provides="plone.pgcatalog.interfaces.IPGIndexTranslator"
    factory=".translators.PriorityIndexTranslator"
    name="priority_score"
/>
```

The utility **name** must match the ZCatalog index name.

## Register Programmatically (for auto-discovery)

```python
from zope.component import provideUtility
from plone.pgcatalog.interfaces import IPGIndexTranslator

translator = PriorityIndexTranslator()
provideUtility(translator, IPGIndexTranslator, name="priority_score")
```

## Security Requirements

- **Always** use `%(name)s` parameter placeholders for user-supplied values.
- **Never** interpolate query values into SQL strings.
- **Always** call `validate_identifier()` on `index_name` in `query()` and `sort()`.
- Index column references (like `idx->>'{name}'`) are safe because `index_name` comes from the component architecture (validated at registration time) -- but the belt-and-suspenders `validate_identifier()` check prevents any edge cases.

## Testing

Unit test the translator without PostgreSQL:

```python
def test_query_exact():
    t = PriorityIndexTranslator()
    sql, params = t.query("priority_score", {"query": 5}, {"query": 5})
    assert "priority_score" in sql
    assert params["priority_priority_score"] == 5
```

Integration test with PostgreSQL (see `tests/test_dri.py` for patterns).

## Built-in Examples

Study these for more complex patterns:

- `dri.py` (`DateRecurringIndexTranslator`) -- RRULE expansion, multiple query strategies (min, max, min:max, exact), rrule_plpgsql functions for recurrence at query time.
- `addons_compat/driri.py` (`DateRangeInRangeIndexTranslator`) -- proxy over two indexes, overlap queries, no-op `extract()` (underlying indexes handle storage).
