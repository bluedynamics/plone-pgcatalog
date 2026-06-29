"""#188: state-changing Advanced-tab forms must POST, not GET.

The `Update Catalog` and `Clear and Rebuild` buttons mutate the catalog
(Clear and Rebuild is destructive). A GET form leaves the action in the
URL bar, so a reload / Back / prefetch silently re-runs it. They must use
`method="post"` for a clean Post/Redirect/Get flow.
"""

import pathlib
import re


_DTML = (
    pathlib.Path(__file__).parent.parent
    / "src"
    / "plone"
    / "pgcatalog"
    / "www"
    / "catalogAdvanced.dtml"
)


def _form_block_for(action_name):
    """Return the `<form ...>` tag that wraps the button for *action_name*."""
    text = _DTML.read_text()
    forms = re.findall(r"<form\b[^>]*>.*?</form>", text, re.DOTALL)
    for form in forms:
        if action_name in form:
            # The opening <form ...> tag only.
            return re.match(r"<form\b[^>]*>", form, re.DOTALL).group(0)
    raise AssertionError(f"no <form> wrapping {action_name!r} found")


def test_update_catalog_form_uses_post():
    tag = _form_block_for("manage_catalogReindex:method")
    assert re.search(r'method\s*=\s*["\']post["\']', tag, re.IGNORECASE), tag


def test_clear_and_rebuild_form_uses_post():
    tag = _form_block_for("manage_catalogRebuild:method")
    assert re.search(r'method\s*=\s*["\']post["\']', tag, re.IGNORECASE), tag


def test_no_advanced_form_defaults_to_get():
    """No form in the template may omit an explicit method (GET default)."""
    text = _DTML.read_text()
    for tag in re.findall(r"<form\b[^>]*>", text):
        assert re.search(r'method\s*=\s*["\']post["\']', tag, re.IGNORECASE), tag
