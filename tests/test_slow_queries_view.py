"""The Slow Queries empty-state shows the configured threshold, not the default.

The "no slow queries yet" message used to hard-code the env var name and its
`default 10ms`; it now surfaces the actually-configured threshold via
`manage_get_slow_query_threshold()`.
"""

import pathlib


_DTML = (
    pathlib.Path(__file__).parent.parent
    / "src"
    / "plone"
    / "pgcatalog"
    / "www"
    / "catalogSlowQueries.dtml"
)


def test_empty_state_uses_configured_threshold():
    text = _DTML.read_text()
    empty_state = text.split("No slow queries recorded yet.", 1)[1]
    # Shows the actual value via the view method ...
    assert "manage_get_slow_query_threshold()" in empty_state
    # ... and no longer hard-codes the default.
    assert "default 10ms" not in empty_state
