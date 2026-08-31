"""Unit tests for the sparse-rank assignment (plone.pgcatalog.gopip).

assign_ranks() is the write-cost engine of the gopip resync (#216): it must
rewrite only rows whose stored rank violates the desired order.  These
tests pin the minimal-write guarantees the design relies on — prepend and
single drag & drop cost one write, append and delete cost zero.
"""

from itertools import pairwise
from plone.pgcatalog.gopip import _INT4_MAX
from plone.pgcatalog.gopip import _INT4_MIN
from plone.pgcatalog.gopip import assign_ranks
from plone.pgcatalog.gopip import RANK_STEP


def apply_updates(current, updates):
    merged = {**current, **updates}
    return sorted(merged, key=lambda cid: merged[cid])


def assert_order(current, desired, updates):
    """Updates applied to current must sort exactly into desired order."""
    assert apply_updates(current, updates) == list(desired)
    ranks = [{**current, **updates}[cid] for cid in desired]
    assert all(a < b for a, b in pairwise(ranks)), ranks
    assert all(_INT4_MIN < r < _INT4_MAX for r in ranks), ranks


class TestAssignRanksMinimalWrites:
    def test_already_ordered_is_noop(self):
        current = {"a": 0, "b": 1024, "c": 2048}
        assert assign_ranks(current, ["a", "b", "c"]) == {}

    def test_legacy_dense_ordered_is_noop(self):
        current = {"a": 0, "b": 1, "c": 2}
        assert assign_ranks(current, ["a", "b", "c"]) == {}

    def test_delete_costs_nothing(self):
        # 'b' deleted: remaining ranks keep their gaps
        current = {"a": 0, "c": 2048}
        assert assign_ranks(current, ["a", "c"]) == {}

    def test_move_to_top_costs_one_write(self):
        current = {"a": 0, "b": 1, "c": 2}
        desired = ["c", "a", "b"]
        updates = assign_ranks(current, desired)
        assert set(updates) == {"c"}
        assert_order(current, desired, updates)

    def test_prepend_costs_one_write_and_goes_negative(self):
        # indexer stored dense 0 for the new object; 'a' also holds 0
        current = {"new": 0, "a": 0, "b": 1, "c": 2}
        desired = ["new", "a", "b", "c"]
        updates = assign_ranks(current, desired)
        assert set(updates) == {"new"}
        assert updates["new"] < 0
        assert_order(current, desired, updates)

    def test_move_down_one_costs_one_write(self):
        current = {"a": 0, "b": 1024, "c": 2048, "d": 3072}
        desired = ["b", "a", "c", "d"]
        updates = assign_ranks(current, desired)
        assert len(updates) == 1
        assert_order(current, desired, updates)

    def test_insert_into_gap_uses_midpoint(self):
        current = {"a": 0, "b": 2048, "x": 5000}
        desired = ["a", "x", "b"]
        updates = assign_ranks(current, desired)
        assert set(updates) == {"x"}
        assert 0 < updates["x"] < 2048
        assert_order(current, desired, updates)


class TestAssignRanksCorrectness:
    def test_full_reversal(self):
        current = {"a": 0, "b": 1, "c": 2, "d": 3}
        desired = ["d", "c", "b", "a"]
        updates = assign_ranks(current, desired)
        assert_order(current, desired, updates)

    def test_missing_rank_is_always_assigned(self):
        # row exists but idx lacks the key -> SQL NULL -> None
        current = {"a": 0, "b": None, "c": 2048}
        desired = ["a", "b", "c"]
        updates = assign_ranks(current, desired)
        assert "b" in updates
        assert_order(current, desired, updates)

    def test_duplicate_legacy_ranks_are_healed(self):
        current = {"a": 5, "b": 5, "c": 5}
        desired = ["a", "b", "c"]
        updates = assign_ranks(current, desired)
        assert_order(current, desired, updates)

    def test_all_missing_ranks(self):
        current = {"a": None, "b": None}
        updates = assign_ranks(current, ["a", "b"])
        assert_order(current, ["a", "b"], updates)

    def test_empty(self):
        assert assign_ranks({}, []) == {}

    def test_single(self):
        assert assign_ranks({"a": 0}, ["a"]) == {}


class TestPendingGopipStore:
    """The pending_gopip store must behave like the move/partial stores."""

    def setup_method(self):
        import transaction

        transaction.abort()

    teardown_method = setup_method

    def test_last_registration_wins_and_pop_clears(self):
        from plone.pgcatalog.pending import add_pending_gopip
        from plone.pgcatalog.pending import pop_all_pending_gopip

        add_pending_gopip("/plone/f", ["a", "b"])
        add_pending_gopip("/plone/f", ["b", "a"])
        add_pending_gopip("/plone/g", ["x"])
        assert pop_all_pending_gopip() == {"/plone/f": ["b", "a"], "/plone/g": ["x"]}
        assert pop_all_pending_gopip() == {}

    def test_savepoint_rollback_restores_snapshot(self):
        from plone.pgcatalog.pending import add_pending_gopip
        from plone.pgcatalog.pending import pop_all_pending_gopip

        import transaction

        add_pending_gopip("/plone/f", ["a"])
        sp = transaction.savepoint()
        add_pending_gopip("/plone/g", ["x"])
        sp.rollback()
        assert pop_all_pending_gopip() == {"/plone/f": ["a"]}

    def test_abort_clears(self):
        from plone.pgcatalog.pending import add_pending_gopip
        from plone.pgcatalog.pending import pop_all_pending_gopip

        import transaction

        add_pending_gopip("/plone/f", ["a"])
        transaction.abort()
        assert pop_all_pending_gopip() == {}


class TestAssignRanksGapExhaustion:
    def test_no_gap_triggers_renumber(self):
        # insert between two adjacent ranks: no room, full renumber
        current = {"a": 0, "b": 1, "x": 99}
        desired = ["a", "x", "b"]
        updates = assign_ranks(current, desired)
        assert_order(current, desired, updates)

    def test_repeated_same_spot_inserts_stay_ordered(self):
        # halving gaps eventually exhausts; renumber must kick in silently
        current = {"a": 0, "b": RANK_STEP}
        order = ["a", "b"]
        for i in range(25):
            new_id = f"n{i}"
            current[new_id] = 0  # indexer snapshot: dense position 1
            order = [order[0], new_id, *order[1:]]
            updates = assign_ranks(current, order)
            current.update(updates)
            ranks = [current[cid] for cid in order]
            assert all(x < y for x, y in pairwise(ranks)), (i, ranks)

    def test_prepend_near_int4_min_renumbers(self):
        current = {"a": _INT4_MIN + 10, "b": 0, "new": 0}
        desired = ["new", "a", "b"]
        updates = assign_ranks(current, desired)
        assert_order(current, desired, updates)

    def test_append_near_int4_max_renumbers(self):
        current = {"a": 0, "b": _INT4_MAX - 10, "new": None}
        desired = ["a", "b", "new"]
        updates = assign_ranks(current, desired)
        assert_order(current, desired, updates)
