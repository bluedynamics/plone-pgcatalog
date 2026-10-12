"""Undo on a history-preserving storage keeps the catalog consistent (#265).

Runs on its own layer, ``PGCATALOG_PG_HP_FIXTURE``: a history-preserving
PGJsonbStorage whose snapshot includes ``object_history``, ``pack_state``
and the undo queue, so an undo never picks a revision of another test.

Content is created by the site owner.  Restricted searches run as ``bob``,
who owns nothing and has no global role besides Member, so the Owner role
cannot make content visible for the wrong reason.
"""

from App.ApplicationManager import AltDatabaseManager
from App.ApplicationManager import FakeConnection
from plone.app.testing import login
from plone.app.testing import logout
from plone.app.testing.interfaces import SITE_OWNER_NAME
from plone.pgcatalog import undo
from plone.pgcatalog.testing import PGCATALOG_PG_HP_FIXTURE
from plone.testing import zope as zope_testing
from ZODB.utils import u64
from zope.pytestlayer import fixture

import logging
import pytest
import transaction


globals().update(
    fixture.create(
        PGCATALOG_PG_HP_FIXTURE,
        session_fixture_name="pg_hp_session",
        class_fixture_name="pg_hp_class",
        function_fixture_name="pg_hp",
    )
)


class _HPLayerTest:
    """Base for every test class in this module (one layer setup for all)."""

    layer = PGCATALOG_PG_HP_FIXTURE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

READER_ONLY = ["Manager", "Reader"]


def _sql(pg_hp, sql, params=None):
    with pg_hp["pgTestDB"].connection.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def _row(pg_hp, obj):
    """(path, idx IS NOT NULL, allowed_roles) of *obj*'s row."""
    rows = _sql(
        pg_hp,
        "SELECT path, idx IS NOT NULL, allowed_roles FROM object_state WHERE zoid = %s",
        (u64(obj._p_oid),),
    )
    return rows[0] if rows else None


def _idx(pg_hp, obj):
    return _sql(
        pg_hp, "SELECT idx FROM object_state WHERE zoid = %s", (u64(obj._p_oid),)
    )[0][0]


def _queue(pg_hp):
    return _sql(pg_hp, "SELECT kind, zoid, path FROM pgcatalog_undo_queue ORDER BY id")


def _paths(results):
    return sorted(b.getPath() for b in results)


def _undo_manager(app):
    """The ZMI's undo form object (Control_Panel/Database/main).

    Built like ``DatabaseChooser.__getitem__`` does, which needs a Zope
    configuration the test layer does not have.
    """
    manager = AltDatabaseManager()
    manager.id = "main"
    manager._p_jar = FakeConnection(app._p_jar.db(), app._p_jar)
    return manager.__of__(app)


def _undo(pg_hp, *positions):
    """Undo the transactions at *positions* of the undo log (0 = last).

    Goes through ``manage_undo_transactions``, which commits, like the ZMI
    form and the CMF Undo tool do.
    """
    transaction.commit()
    manager = _undo_manager(pg_hp["app"])
    info = manager.undoable_transactions(0, max(positions) + 1)
    manager.manage_undo_transactions([info[pos]["id"] for pos in positions])


def _follow_up(portal, limit=None):
    result = undo.process_queue(portal.portal_catalog, limit)
    transaction.commit()
    return result


def _as_owner(pg_hp):
    zope_testing.login(pg_hp["app"]["acl_users"], SITE_OWNER_NAME)


def _search_as(pg_hp, user, **query):
    portal = pg_hp["portal"]
    if user is None:
        logout()
    else:
        login(portal, user)
    try:
        return _paths(portal.portal_catalog.searchResults(**query))
    finally:
        _as_owner(pg_hp)


def _add_bob(portal):
    portal.acl_users.source_users.addUser("bob", "bob", "secret-bob")


def _restricted_tree(portal, folder_id, child_ids=("child",)):
    """Folder only Manager and Reader may view, with documents inside."""
    _add_bob(portal)
    portal.invokeFactory("Folder", folder_id, title="Folder")
    folder = portal[folder_id]
    for child_id in child_ids:
        folder.invokeFactory("Document", child_id, title=f"Doc {child_id}")
    # Separate transaction: a security reindex does not see children that
    # are not committed yet.
    transaction.commit()
    for permission in ("View", "Access contents information"):
        folder.manage_permission(permission, READER_ONLY, acquire=False)
    folder.reindexObjectSecurity()
    transaction.commit()
    return folder


def _share_with_bob(folder):
    folder.manage_setLocalRoles("bob", ["Reader"])
    folder.reindexObjectSecurity()
    transaction.commit()


@pytest.fixture
def pgcatalog_moves(monkeypatch):
    """The move handlers only act when they see the PG catalog tool."""
    import plone.pgcatalog.move as move_mod

    monkeypatch.setattr(move_mod, "_is_pgcatalog_active", lambda: True)


# ---------------------------------------------------------------------------
# 1. Sharing
# ---------------------------------------------------------------------------


class TestUndoSharing(_HPLayerTest):
    def test_sharing_fails_closed_then_restores(self, pg_hp):
        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, "sh")
        child = folder["child"]
        before = {obj.getId(): _row(pg_hp, obj) for obj in (folder, child)}

        _share_with_bob(folder)
        assert "user:bob" in _row(pg_hp, child)[2]
        assert _search_as(pg_hp, "bob", path="/plone/sh") == [
            "/plone/sh",
            "/plone/sh/child",
        ]

        _undo(pg_hp, 0)

        assert "bob" not in dict(portal["sh"].get_local_roles())
        assert _row(pg_hp, folder) == (None, False, None)
        assert _row(pg_hp, child)[1:] == (True, None)
        assert _search_as(pg_hp, "bob", path="/plone/sh") == []
        catalog = portal.portal_catalog
        assert _paths(catalog.unrestrictedSearchResults(path="/plone/sh")) == [
            "/plone/sh/child"
        ]

        result = _follow_up(portal)

        assert result.processed == 2
        assert result.remaining == 0
        assert _queue(pg_hp) == []
        assert {obj.getId(): _row(pg_hp, obj) for obj in (folder, child)} == before
        assert _search_as(pg_hp, "bob", path="/plone/sh") == []
        assert _paths(catalog.searchResults(path="/plone/sh")) == [
            "/plone/sh",
            "/plone/sh/child",
        ]

    def test_queue_order_recatalog_first(self, pg_hp):
        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, "sq", ("a", "b"))
        _share_with_bob(folder)

        _undo(pg_hp, 0)

        assert [(kind, path) for kind, _, path in _queue(pg_hp)] == [
            ("recatalog", "/plone/sq"),
            ("security", "/plone/sq/a"),
            ("security", "/plone/sq/b"),
        ]


# ---------------------------------------------------------------------------
# 2. Workflow transition
# ---------------------------------------------------------------------------


class TestUndoWorkflowTransition(_HPLayerTest):
    def test_publish_of_a_folder(self, pg_hp):
        portal = pg_hp["portal"]
        wf = portal.portal_workflow
        wf.setChainForPortalTypes(
            ("Folder", "Document"), ("simple_publication_workflow",)
        )
        portal.invokeFactory("Folder", "wf", title="Folder")
        portal["wf"].invokeFactory("Document", "child", title="Child")
        transaction.commit()
        folder, child = portal["wf"], portal["wf"]["child"]
        before = {obj.getId(): _row(pg_hp, obj) for obj in (folder, child)}
        assert _idx(pg_hp, folder)["review_state"] == "private"

        wf.doActionFor(folder, "publish")
        transaction.commit()
        assert _idx(pg_hp, folder)["review_state"] == "published"
        assert _search_as(pg_hp, None, path="/plone/wf") == ["/plone/wf"]

        _undo(pg_hp, 0)

        assert _row(pg_hp, folder) == (None, False, None)
        assert _row(pg_hp, child)[1:] == (True, None)
        assert _search_as(pg_hp, None, path="/plone/wf") == []

        _follow_up(portal)

        assert wf.getInfoFor(portal["wf"], "review_state") == "private"
        assert _idx(pg_hp, folder)["review_state"] == "private"
        assert {obj.getId(): _row(pg_hp, obj) for obj in (folder, child)} == before
        assert _search_as(pg_hp, None, path="/plone/wf") == []


# ---------------------------------------------------------------------------
# 3. Rename and cross-container move
# ---------------------------------------------------------------------------


def _assert_cataloged_at(pg_hp, *objs):
    for obj in objs:
        assert _row(pg_hp, obj)[:2] == ("/".join(obj.getPhysicalPath()), True), obj


def _rows_below(pg_hp, prefix):
    return _sql(
        pg_hp, "SELECT path FROM object_state WHERE path LIKE %s", (prefix + "%",)
    )


class TestUndoMove(_HPLayerTest):
    def _tree(self, portal, folder_id):
        portal.invokeFactory("Folder", folder_id, title="Folder")
        folder = portal[folder_id]
        folder.invokeFactory("Folder", "sub", title="Sub")
        folder["sub"].invokeFactory("Document", "doc", title="Doc")
        transaction.commit()
        return folder

    def test_rename(self, pg_hp, pgcatalog_moves):
        portal = pg_hp["portal"]
        self._tree(portal, "mv-old")

        portal.manage_renameObject("mv-old", "mv-new")
        transaction.commit()
        assert _rows_below(pg_hp, "/plone/mv-new")

        _undo(pg_hp, 0)
        _follow_up(portal)

        folder = portal["mv-old"]
        assert "mv-new" not in portal.objectIds()
        _assert_cataloged_at(pg_hp, folder, folder["sub"], folder["sub"]["doc"])
        assert _rows_below(pg_hp, "/plone/mv-new") == []
        assert _queue(pg_hp) == []

    def test_cross_container_move(self, pg_hp, pgcatalog_moves):
        portal = pg_hp["portal"]
        self._tree(portal, "mv-src")
        portal.invokeFactory("Folder", "mv-target", title="Target")
        transaction.commit()
        target = portal["mv-target"]
        for permission in ("View", "Access contents information"):
            target.manage_permission(permission, ["Manager"], acquire=False)
        target.reindexObjectSecurity()
        transaction.commit()
        doc = portal["mv-src"]["sub"]["doc"]
        roles_before = _row(pg_hp, doc)[2]

        clipboard = portal.manage_cutObjects(["mv-src"])
        target.manage_pasteObjects(clipboard)
        transaction.commit()
        assert _row(pg_hp, doc) == (
            "/plone/mv-target/mv-src/sub/doc",
            True,
            ["Manager"],
        )

        _undo(pg_hp, 0)
        _follow_up(portal)

        folder = portal["mv-src"]
        assert "mv-src" not in portal["mv-target"].objectIds()
        _assert_cataloged_at(pg_hp, folder, folder["sub"], folder["sub"]["doc"])
        assert _rows_below(pg_hp, "/plone/mv-target/") == []
        assert _row(pg_hp, folder["sub"]["doc"])[2] == roles_before
        assert _queue(pg_hp) == []


# ---------------------------------------------------------------------------
# 4. and 5. Edits
# ---------------------------------------------------------------------------


class TestUndoEdit(_HPLayerTest):
    def test_edit_of_a_document(self, pg_hp):
        from plone.app.textfield.value import RichTextValue

        portal = pg_hp["portal"]
        portal.invokeFactory("Document", "ed", title="Old title")
        doc = portal["ed"]
        doc.text = RichTextValue("<p>oldbody</p>", "text/html", "text/html")
        doc.reindexObject()
        transaction.commit()

        doc.title = "New title"
        doc.text = RichTextValue("<p>newbody</p>", "text/html", "text/html")
        doc.reindexObject()
        transaction.commit()
        catalog = portal.portal_catalog
        assert _paths(catalog(SearchableText="newbody")) == ["/plone/ed"]

        _undo(pg_hp, 0)
        assert _row(pg_hp, doc) == (None, False, None)
        assert _paths(catalog(SearchableText="newbody")) == []

        _follow_up(portal)

        assert _row(pg_hp, doc)[:2] == ("/plone/ed", True)
        assert _idx(pg_hp, doc)["Title"] == "Old title"
        assert _paths(catalog(SearchableText="oldbody")) == ["/plone/ed"]
        assert _paths(catalog(SearchableText="newbody")) == []

    def test_edit_of_a_container_hides_its_subtree(self, pg_hp):
        """The accepted cost of the subtree rule: an edit that did not touch
        security still hides the children's security data until the
        follow-up has run."""
        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, "ce", ("a", "b"))
        _share_with_bob(folder)
        before = {obj.getId(): _row(pg_hp, obj) for obj in folder.objectValues()}

        folder.title = "Renamed title"
        folder.reindexObject()
        transaction.commit()

        _undo(pg_hp, 0)

        children = folder.objectValues()
        assert [_row(pg_hp, obj)[1:] for obj in children] == [(True, None)] * 2
        assert _search_as(pg_hp, "bob", path="/plone/ce") == []

        _follow_up(portal)

        assert {
            obj.getId(): _row(pg_hp, obj) for obj in folder.objectValues()
        } == before
        assert _idx(pg_hp, portal["ce"])["Title"] == "Folder"
        assert _search_as(pg_hp, "bob", path="/plone/ce") == [
            "/plone/ce",
            "/plone/ce/a",
            "/plone/ce/b",
        ]


# ---------------------------------------------------------------------------
# 6. and 7. Delete and add
# ---------------------------------------------------------------------------


class TestUndoDeleteAndAdd(_HPLayerTest):
    def test_undo_of_a_delete(self, pg_hp):
        portal = pg_hp["portal"]
        portal.invokeFactory("Folder", "del", title="Deleted folder")
        portal["del"].invokeFactory("Folder", "sub", title="Sub")
        portal["del"]["sub"].invokeFactory("Document", "doc", title="Doc")
        transaction.commit()
        folder = portal["del"]
        doc = folder["sub"]["doc"]

        portal.manage_delObjects(["del"])
        transaction.commit()
        assert _row(pg_hp, doc)[:2] == (None, False)

        _undo(pg_hp, 0)
        assert _row(pg_hp, doc)[:2] == (None, False)

        _follow_up(portal)

        folder = portal["del"]
        _assert_cataloged_at(pg_hp, folder, folder["sub"], folder["sub"]["doc"])
        assert _queue(pg_hp) == []

    def test_undo_of_an_add(self, pg_hp):
        portal = pg_hp["portal"]
        portal.invokeFactory("Document", "added", title="Added")
        transaction.commit()
        zoid = u64(portal["added"]._p_oid)

        _undo(pg_hp, 0)
        _follow_up(portal)

        assert "added" not in portal.objectIds()
        assert _sql(pg_hp, "SELECT 1 FROM object_state WHERE zoid = %s", (zoid,)) == []
        assert _queue(pg_hp) == []

    def test_undo_of_the_undo_of_an_add(self, pg_hp):
        """The re-created row has no catalog data; its class comes from
        object_history.  Both undos before any follow-up: a follow-up writes
        the container and would block the second undo."""
        portal = pg_hp["portal"]
        portal.invokeFactory("Document", "again", title="Again")
        transaction.commit()
        zoid = u64(portal["again"]._p_oid)
        _undo(pg_hp, 0)
        assert "again" not in portal.objectIds()

        _undo(pg_hp, 0)  # undo the undo
        assert ("recatalog", zoid, None) in _queue(pg_hp)
        _follow_up(portal)

        _assert_cataloged_at(pg_hp, portal, portal["again"])
        assert _queue(pg_hp) == []

    def test_undo_of_an_add_with_a_later_child_is_refused(self, pg_hp):
        """No dangling child row: the folder's BTree was created by the add
        and changed by the later one, so ZODB refuses the undo, and the
        hook never runs."""
        from ZODB.POSException import UndoError

        portal = pg_hp["portal"]
        portal.invokeFactory("Folder", "new", title="New folder")
        transaction.commit()
        portal["new"].invokeFactory("Document", "later", title="Later")
        transaction.commit()
        later = portal["new"]["later"]
        before = _row(pg_hp, later)

        with pytest.raises(UndoError):
            _undo(pg_hp, 1)
        transaction.abort()

        assert _row(pg_hp, later) == before
        assert _queue(pg_hp) == []

    def test_object_gone_before_the_follow_up_is_dropped(self, pg_hp):
        portal = pg_hp["portal"]
        portal.invokeFactory("Folder", "gone", title="Gone")
        portal["gone"].invokeFactory("Document", "doc", title="Doc")
        transaction.commit()
        doc = portal["gone"]["doc"]
        portal.manage_delObjects(["gone"])
        transaction.commit()
        _undo(pg_hp, 0)
        # deleted again before the follow-up ran
        portal.manage_delObjects(["gone"])
        transaction.commit()

        result = _follow_up(portal)

        assert result.dropped == 2
        assert result.failed == 0
        assert _row(pg_hp, doc)[:2] == (None, False)
        assert _queue(pg_hp) == []

    def test_undo_of_a_delete_at_the_root_hides_the_site(self, pg_hp):
        """Adding or deleting content writes the container (its modification
        date), so undoing a delete in the portal root applies the subtree
        rule to the portal: every row of the site loses its security data
        until the follow-up has run."""
        portal = pg_hp["portal"]
        portal.invokeFactory("Document", "bystander", title="Bystander")
        portal.invokeFactory("Document", "victim", title="Victim")
        transaction.commit()
        bystander = portal["bystander"]
        before = _row(pg_hp, bystander)
        portal.manage_delObjects(["victim"])
        transaction.commit()

        _undo(pg_hp, 0)

        assert _row(pg_hp, portal) == (None, False, None)
        assert _row(pg_hp, bystander)[1:] == (True, None)

        _follow_up(portal)

        assert _row(pg_hp, bystander) == before
        _assert_cataloged_at(pg_hp, portal, portal["victim"])


# ---------------------------------------------------------------------------
# 8. Several tids in one undoMultiple
# ---------------------------------------------------------------------------


class TestUndoMultiple(_HPLayerTest):
    def _sharing_then_edit(self, pg_hp, folder_id):
        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, folder_id)
        before = {obj.getId(): _row(pg_hp, obj) for obj in (folder, folder["child"])}
        _share_with_bob(folder)
        folder.title = "Edited"
        folder.reindexObject()
        transaction.commit()
        return portal, folder, before

    def test_newest_first(self, pg_hp):
        portal, folder, before = self._sharing_then_edit(pg_hp, "um")

        _undo(pg_hp, 0, 1)
        assert _search_as(pg_hp, "bob", path="/plone/um") == []
        _follow_up(portal)

        folder = portal["um"]
        assert "bob" not in dict(folder.get_local_roles())
        assert folder.title == "Folder"
        assert {
            obj.getId(): _row(pg_hp, obj) for obj in (folder, folder["child"])
        } == before
        assert _idx(pg_hp, folder)["Title"] == "Folder"
        assert _search_as(pg_hp, "bob", path="/plone/um") == []
        assert _queue(pg_hp) == []

    def test_oldest_first_is_refused(self, pg_hp):
        """Undoing the sharing change before the edit that followed it:
        content has no conflict resolution, so ZODB refuses."""
        from ZODB.POSException import UndoError

        portal, folder, _ = self._sharing_then_edit(pg_hp, "uo")
        rows = {obj.getId(): _row(pg_hp, obj) for obj in (folder, folder["child"])}

        with pytest.raises(UndoError):
            _undo(pg_hp, 1, 0)
        transaction.abort()

        assert {
            obj.getId(): _row(pg_hp, obj) for obj in (folder, folder["child"])
        } == rows
        assert _queue(pg_hp) == []

    def test_a_refused_undo_rolls_back_the_hook(self, pg_hp, monkeypatch):
        """The hook has hidden and queued the rows of the first tid when the
        second one fails: nothing of it survives."""
        from ZODB.POSException import UndoError

        calls = []
        real_hide = undo.hide_undone

        def spy(cursor, tid, restored, deleted):
            calls.append(tid)
            return real_hide(cursor, tid, restored, deleted)

        monkeypatch.setattr(undo, "hide_undone", spy)

        portal, folder, _ = self._sharing_then_edit(pg_hp, "ua")
        portal.invokeFactory("Document", "other", title="Other")
        transaction.commit()
        other = portal["other"]
        other.title = "Other edited"
        other.reindexObject()
        transaction.commit()
        rows = {obj.getId(): _row(pg_hp, obj) for obj in (folder, other)}

        # newest first: the edit of "other" (hook runs), then the sharing
        # change, which the later edit of the folder blocks.
        with pytest.raises(UndoError):
            _undo(pg_hp, 0, 3)
        transaction.abort()

        assert len(calls) == 1
        assert {obj.getId(): _row(pg_hp, obj) for obj in (folder, other)} == rows
        assert _queue(pg_hp) == []
        assert portal["other"].title == "Other edited"


# ---------------------------------------------------------------------------
# 9. Budget
# ---------------------------------------------------------------------------


class TestUndoBudget(_HPLayerTest):
    def test_leftovers_stay_hidden(self, pg_hp):
        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, "bu", ("a", "b"))
        _share_with_bob(folder)
        folder.title = "Edited"
        folder.reindexObject()
        transaction.commit()

        _undo(pg_hp, 0)
        result = _follow_up(portal, limit=1)

        assert (result.processed, result.remaining) == (1, 2)
        assert [kind for kind, _, _ in _queue(pg_hp)] == ["security", "security"]
        assert _search_as(pg_hp, "bob", path="/plone/bu") == ["/plone/bu"]

        result = _follow_up(portal)

        assert (result.processed, result.remaining) == (2, 0)
        assert _search_as(pg_hp, "bob", path="/plone/bu") == [
            "/plone/bu",
            "/plone/bu/a",
            "/plone/bu/b",
        ]

    def test_limit_from_environment(self, monkeypatch):
        monkeypatch.delenv("PGCATALOG_UNDO_FOLLOWUP_LIMIT", raising=False)
        assert undo.followup_limit() == 2000
        monkeypatch.setenv("PGCATALOG_UNDO_FOLLOWUP_LIMIT", "50")
        assert undo.followup_limit() == 50
        for bad in ("0", "-3", "many"):
            monkeypatch.setenv("PGCATALOG_UNDO_FOLLOWUP_LIMIT", bad)
            assert undo.followup_limit() == 2000


# ---------------------------------------------------------------------------
# 10. No follow-up ran: repair_uncataloged
# ---------------------------------------------------------------------------


class TestUndoRepair(_HPLayerTest):
    def test_repair_uncataloged_drains_the_queue(self, pg_hp):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, "rp")
        child = folder["child"]
        before = {obj.getId(): _row(pg_hp, obj) for obj in (folder, child)}
        _share_with_bob(folder)
        _undo(pg_hp, 0)  # the process "crashed": no follow-up
        assert _row(pg_hp, folder) == (None, False, None)
        assert _row(pg_hp, child)[1:] == (True, None)

        repair_uncataloged(portal.portal_catalog, portal)
        transaction.commit()

        assert {obj.getId(): _row(pg_hp, obj) for obj in (folder, child)} == before
        assert _queue(pg_hp) == []

    def test_dry_run_does_not_drain(self, pg_hp):
        from plone.pgcatalog.maintenance import repair_uncataloged

        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, "rd")
        _share_with_bob(folder)
        _undo(pg_hp, 0)

        repair_uncataloged(portal.portal_catalog, portal, dry_run=True)
        transaction.abort()

        assert len(_queue(pg_hp)) == 2


# ---------------------------------------------------------------------------
# 11. Request end: IPubSuccess
# ---------------------------------------------------------------------------


class TestUndoAtRequestEnd(_HPLayerTest):
    def _undone_sharing(self, pg_hp, folder_id):
        portal = pg_hp["portal"]
        folder = _restricted_tree(portal, folder_id)
        before = {obj.getId(): _row(pg_hp, obj) for obj in (folder, folder["child"])}
        _share_with_bob(folder)
        _undo(pg_hp, 0)
        return folder, before

    def test_subscriber_runs_the_follow_up(self, pg_hp):
        """The ZMI's undo form lives below the Zope root, outside any site:
        every Plone site of the root is drained."""
        from zope.component.hooks import setSite
        from zope.event import notify
        from ZPublisher.pubevents import PubSuccess

        folder, before = self._undone_sharing(pg_hp, "ps")
        request = pg_hp["request"]
        request["PARENTS"] = [_undo_manager(pg_hp["app"]), pg_hp["app"]]
        setSite(None)
        try:
            notify(PubSuccess(request))
        finally:
            setSite(pg_hp["portal"])

        assert {
            obj.getId(): _row(pg_hp, obj) for obj in (folder, folder["child"])
        } == before
        assert _queue(pg_hp) == []

    def test_site_from_the_request(self, pg_hp):
        from ZPublisher.pubevents import PubSuccess

        folder, before = self._undone_sharing(pg_hp, "pr")
        request = pg_hp["request"]
        request["PARENTS"] = [pg_hp["portal"], pg_hp["app"]]

        undo.drain_after_undo(PubSuccess(request))

        assert {
            obj.getId(): _row(pg_hp, obj) for obj in (folder, folder["child"])
        } == before

    def test_nothing_undone_nothing_done(self, pg_hp, monkeypatch):
        from ZPublisher.pubevents import PubSuccess

        undo._local.followup_pending = False
        calls = []
        monkeypatch.setattr(undo, "process_queue", lambda *a: calls.append(a))

        undo.drain_after_undo(PubSuccess(pg_hp["request"]))

        assert calls == []

    def test_subscriber_never_raises(self, pg_hp, monkeypatch, caplog):
        from ZPublisher.pubevents import PubSuccess

        self._undone_sharing(pg_hp, "pe")

        def boom(*args):
            raise RuntimeError("boom")

        monkeypatch.setattr(undo, "process_queue", boom)
        request = pg_hp["request"]
        request["PARENTS"] = [pg_hp["portal"], pg_hp["app"]]
        with caplog.at_level(logging.ERROR, logger="plone.pgcatalog.undo"):
            undo.drain_after_undo(PubSuccess(request))

        assert "undo follow-up failed" in caplog.text
        assert len(_queue(pg_hp)) == 2  # still queued, still hidden

    def test_remaining_rows_are_logged(self, pg_hp, monkeypatch, caplog):
        from ZPublisher.pubevents import PubSuccess

        self._undone_sharing(pg_hp, "pl")
        monkeypatch.setenv("PGCATALOG_UNDO_FOLLOWUP_LIMIT", "1")
        request = pg_hp["request"]
        request["PARENTS"] = [pg_hp["portal"], pg_hp["app"]]
        with caplog.at_level(logging.WARNING, logger="plone.pgcatalog.undo"):
            undo.drain_after_undo(PubSuccess(request))

        assert "1 catalog rows are still hidden" in caplog.text
        assert len(_queue(pg_hp)) == 1


# ---------------------------------------------------------------------------
# 12. Query cache
# ---------------------------------------------------------------------------


class TestUndoQueryCache(_HPLayerTest):
    def test_cached_results_are_not_served_after_the_undo(self, pg_hp):
        portal = pg_hp["portal"]
        catalog = portal.portal_catalog
        folder = _restricted_tree(portal, "qc")
        _share_with_bob(folder)
        assert _paths(catalog(path="/plone/qc")) == ["/plone/qc", "/plone/qc/child"]
        assert _paths(catalog(path="/plone/qc")) == ["/plone/qc", "/plone/qc/child"]
        (seq_before,) = _sql(pg_hp, "SELECT last_value FROM pgcatalog_change_seq")[0]

        _undo(pg_hp, 0)

        (seq_after,) = _sql(pg_hp, "SELECT last_value FROM pgcatalog_change_seq")[0]
        assert seq_after > seq_before
        assert _paths(catalog(path="/plone/qc")) == []


# ---------------------------------------------------------------------------
# ZMI
# ---------------------------------------------------------------------------


class TestUndoZMI(_HPLayerTest):
    def test_summary_and_button(self, pg_hp):
        portal = pg_hp["portal"]
        catalog = portal.portal_catalog
        assert catalog.manage_get_catalog_summary()["undo_queue"] == 0
        folder = _restricted_tree(portal, "zm")
        _share_with_bob(folder)
        _undo(pg_hp, 0)
        transaction.begin()  # new snapshot for the read connection
        assert catalog.manage_get_catalog_summary()["undo_queue"] == 2

        result = catalog.manage_processUndoQueue()
        transaction.commit()

        assert (result.processed, result.remaining) == (2, 0)
        assert _queue(pg_hp) == []
