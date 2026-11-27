"""The state store conformance suite (public testing API): run it against every store.

    from statecraft.testing.store_contract import *  # noqa: F403

    @pytest.fixture
    def store(tmp_path):
        yield MyStore(...)


What must hold: create is create-only; one valid lease at a time, with a new epoch each time;
a step or fault commits only with the current, unexpired lease and the expected step, and
releases the lease; renewals never make a commit fail; a stale worker's renew, release and fault
are refused; control commits need the current revision and no running step; kill always wins and
fences the running step; inbox appends are deduplicated; every change writes its history entry
and one durable event in the same transaction; ended records with intents left are still work.
History reads back every revision's full record (checkpoints and deltas); compacting a run keeps
its final records, last events, audit events and message ids, and changes nothing on a dry run;
an active run only loses the bodies of applied messages; only an ended run nobody reads (attached
or inheriting) is forgotten, and then entirely.
"""

from __future__ import annotations

import pytest

from statecraft.kernel.process import Fault, Record, Transition
from statecraft.kernel.store import Busy, CallRefused, Conflict, Fenced
from statecraft.kernel.transitions import apply_signal


def rec(agent_id: str = "root", **kw) -> Record:
    return Record(**{"id": agent_id, "agent": "t:a", "state": "s0", **kw})


async def test_create_is_create_only(store) -> None:
    assert await store.create(rec())
    assert not await store.create(rec(state="other"))
    assert (await store.load("root")).state == "s0"
    assert (await store.load("root")).revision == 1


async def test_one_lease_at_a_time_with_new_epochs(store) -> None:
    await store.create(rec())
    a = await store.try_acquire("root", "a", 10, now=0)
    assert a is not None and a.epoch == 1
    assert await store.try_acquire("root", "b", 10, now=5) is None
    b = await store.try_acquire("root", "b", 10, now=11)  # a's lease expired
    assert b is not None and b.epoch == 2


async def test_a_step_commits_with_the_current_lease_and_releases_it(store) -> None:
    await store.create(rec())
    lease = await store.try_acquire("root", "a", 10, now=0)
    after = await store.commit_step("root", lease, 0, Transition(state="s1"), now=1)
    assert (after.step, after.state) == (1, "s1")
    assert await store.lease("root") is None
    with pytest.raises(Fenced):
        await store.commit_step("root", lease, 1, Transition(state="s2"), now=2)


async def test_an_expired_or_stale_lease_cannot_commit_a_step_or_a_fault(store) -> None:
    await store.create(rec())
    old = await store.try_acquire("root", "a", 10, now=0)
    with pytest.raises(Fenced):
        await store.commit_step("root", old, 0, Transition(state="s1"), now=10)
    new = await store.try_acquire("root", "b", 10, now=10)
    for outcome in (Transition(state="s1"), Fault("late")):
        with pytest.raises(Fenced):
            await store.commit_step("root", old, 0, outcome, now=11)
    assert await store.renew("root", old, 10, now=11) is None
    await store.release("root", old)
    assert await store.lease("root") == new
    assert (await store.load("root")).reasons == []


async def test_renewals_keep_a_long_step_committable(store) -> None:
    await store.create(rec())
    lease = await store.try_acquire("root", "a", 10, now=0)
    for t in (8, 16, 24):
        lease = await store.renew("root", lease, 10, now=t)
        assert lease is not None
    after = await store.commit_step("root", lease, 0, Transition(state="s1"), now=30)
    assert after.step == 1


async def test_the_expected_step_is_checked(store) -> None:
    await store.create(rec())
    lease = await store.try_acquire("root", "a", 10, now=0)
    with pytest.raises(Conflict):
        await store.commit_step("root", lease, 3, Transition(state="s1"), now=1)


async def test_a_fault_is_committed_with_fencing_and_keeps_the_step(store) -> None:
    await store.create(rec())
    lease = await store.try_acquire("root", "a", 10, now=0)
    after = await store.commit_step("root", lease, 0, Fault("boom"), now=1)
    assert (after.step, after.state, after.reason("fault")["error"]) == (0, "s0", "boom")


async def test_control_needs_the_revision_and_no_running_step(store) -> None:
    await store.create(rec())
    r = await store.load("root")
    stopped = await store.commit_control(
        "root", r.revision, lambda x: apply_signal(x, "STOP"), now=0
    )
    assert stopped.reason("stopped")
    with pytest.raises(Conflict):
        await store.commit_control("root", r.revision, lambda x: apply_signal(x, "CONT"), now=0)
    await store.commit_control("root", stopped.revision, lambda x: apply_signal(x, "CONT"), now=0)
    current = await store.load("root")
    await store.try_acquire("root", "a", 10, now=0)
    with pytest.raises(Busy):
        await store.commit_control(
            "root", current.revision, lambda x: apply_signal(x, "STOP"), now=1
        )


async def test_kill_wins_and_fences_the_running_step(store) -> None:
    await store.create(rec())
    lease = await store.try_acquire("root", "a", 100, now=0)
    killed = await store.kill("root")
    assert killed.status == "terminated"
    with pytest.raises(Fenced):
        await store.commit_step("root", lease, 0, Transition(state="s1"), now=1)


async def test_inbox_appends_are_deduplicated(store) -> None:
    await store.create(rec())
    assert await store.append_inbox("root", "m1", {"kind": "signal", "signal": "STOP"})
    assert not await store.append_inbox("root", "m1", {"kind": "signal", "signal": "CONT"})
    assert await store.append_inbox("root", "m2", {"kind": "signal", "signal": "CONT"})
    box = await store.read_inbox("root", 0)
    assert [(m["n"], m["message"]["signal"]) for m in box] == [(1, "STOP"), (2, "CONT")]
    assert [m["n"] for m in await store.read_inbox("root", 1)] == [2]


async def test_every_change_writes_history_and_one_event(store) -> None:
    await store.create(rec())
    await store.create(rec("root/c"))
    lease = await store.try_acquire("root", "a", 10, now=0)
    await store.commit_step("root", lease, 0, Transition(state="s1"), now=1)
    history = await store.history("root")
    assert [h["revision"] for h in history] == [1, 2]
    assert history[-1]["record"]["state"] == "s1"
    events = await store.read_events("root")
    assert [e["seq"] for e in events] == [1, 2, 3]
    assert [e["kind"] for e in events] == ["created", "created", "step"]
    assert [e["seq"] for e in await store.read_events("root", 2)] == [3]


async def test_ended_records_with_intents_left_are_still_work(store) -> None:
    await store.create(rec())
    lease = await store.try_acquire("root", "a", 10, now=0)
    await store.commit_step(
        "root", lease, 0, Transition(state="s1", spawns={"c": ("x", {})}), now=1
    )
    killed = await store.kill("root")
    assert killed.status == "terminated"
    assert [r.id for r in await store.scan_work()] == ["root"]


async def test_the_tree_is_the_root_and_its_descendants(store) -> None:
    for agent_id in ("a_b", "a_b/c", "a_b/c/d", "a_bx", "zz"):
        await store.create(rec(agent_id))
    assert [r.id for r in await store.tree("a_b")] == ["a_b", "a_b/c", "a_b/c/d"]


async def test_roots_are_the_records_without_a_parent(store) -> None:
    for agent_id in ("b", "a", "a/child", "a/child/leaf", "b/x"):
        await store.create(rec(agent_id))
    assert [r.id for r in await store.roots()] == ["a", "b"]


async def test_both_stores_keep_the_same_json(store) -> None:
    from statecraft.kernel.store import NotJSON

    await store.create(
        rec(data={"z": 1, "a": (1, 2), 3: "x"}, children={"zeta": "z", "alpha": "a"})
    )
    loaded = await store.load("root")
    assert loaded.data == {"z": 1, "a": [1, 2], "3": "x"}  # tuples are lists, keys strings
    assert list(loaded.data) == ["z", "a", "3"] and list(loaded.children) == ["zeta", "alpha"]
    lease = await store.try_acquire("root", "a", 10, now=0)
    with pytest.raises(NotJSON):  # not JSON: refused before anything is written
        await store.commit_step("root", lease, 0, Transition(state="s1", data={"s": {1}}), now=1)
    assert (await store.load("root")).step == 0
    with pytest.raises(NotJSON):
        await store.append_inbox("root", "m", {"kind": "signal", "signal": ["USR", b"x"]})


async def test_an_expired_lease_cannot_be_renewed(store) -> None:
    await store.create(rec())
    lease = await store.try_acquire("root", "a", 10, now=0)
    assert await store.renew("root", lease, 10, now=11) is None  # expired, even unclaimed
    assert await store.renew("root", lease, 10, now=5) is not None


async def test_the_call_journal_keeps_identities_and_unknown_outcomes(store) -> None:
    await store.create(Record(id="a", agent="x", state="go"))
    await store.create(Record(id="b", agent="x", state="go"))
    entry, created = await store.begin_call("a", "a/0/go/call/c1", "pay.charge", "d1", 1.0)
    assert created and entry["status"] == "sent"
    again, created = await store.begin_call("a", "a/0/go/call/c1", "pay.charge", "d1", 2.0)
    assert not created and again["since"] == 1.0
    with pytest.raises(CallRefused, match="different request"):
        await store.begin_call("a", "a/0/go/call/c1", "pay.charge", "d2", 3.0)
    with pytest.raises(CallRefused, match="belongs to a"):
        await store.begin_call("b", "a/0/go/call/c1", "pay.charge", "d1", 3.0)
    marked = await store.end_call("a/0/go/call/c1", "indeterminate", clear_by="resend")
    assert marked["clear_by"] == "resend"
    with pytest.raises(CallRefused, match="unknown outcome"):  # no new key around it
        await store.begin_call("a", "a/1/go/call/c2", "pay.charge", "d3", 4.0)
    await store.begin_call("a", "a/1/go/call/c3", "mail.send", "d4", 4.0)  # another effect
    await store.begin_call("b", "b/0/go/call/c1", "pay.charge", "d5", 4.0)  # another agent
    settled = await store.end_call("a/0/go/call/c1", "completed", result={"ok": 1})
    assert "clear_by" not in settled and settled["result"] == {"ok": 1}
    await store.begin_call("a", "a/1/go/call/c2", "pay.charge", "d3", 5.0)  # settled: free
    assert [c["key"] for c in await store.calls("a")] == [
        "a/0/go/call/c1",
        "a/1/go/call/c3",
        "a/1/go/call/c2",
    ]
    with pytest.raises(ValueError):
        await store.end_call("a/0/go/call/c1", "maybe")


__all__ = [
    "rec",
    "test_a_fault_is_committed_with_fencing_and_keeps_the_step",
    "test_a_step_commits_with_the_current_lease_and_releases_it",
    "test_an_expired_lease_cannot_be_renewed",
    "test_an_expired_or_stale_lease_cannot_commit_a_step_or_a_fault",
    "test_both_stores_keep_the_same_json",
    "test_control_needs_the_revision_and_no_running_step",
    "test_create_is_create_only",
    "test_ended_records_with_intents_left_are_still_work",
    "test_every_change_writes_history_and_one_event",
    "test_inbox_appends_are_deduplicated",
    "test_kill_wins_and_fences_the_running_step",
    "test_one_lease_at_a_time_with_new_epochs",
    "test_renewals_keep_a_long_step_committable",
    "test_roots_are_the_records_without_a_parent",
    "test_the_call_journal_keeps_identities_and_unknown_outcomes",
    "test_the_expected_step_is_checked",
    "test_the_tree_is_the_root_and_its_descendants",
]


async def _ended_run(store, root: str = "root", steps: int = 40) -> None:
    """A root that took `steps` steps (its data growing like a conversation), got a message,
    and ended."""
    await store.create(rec(root, data={"log": []}))
    for n in range(steps):
        lease = await store.try_acquire(root, "a", 10, now=n)
        log = [*(await store.load(root)).data["log"], f"turn {n} " + "x" * 200]
        await store.commit_step(root, lease, n, Transition(state="s0", data={"log": log}), now=n)
    await store.append_inbox(root, "m1", {"kind": "signal", "signal": "STOP"})
    record = await store.load(root)
    await store.commit_control(root, record.revision, lambda r: r.but(inbox_applied=1), now=steps)
    await store.kill(root)


async def test_history_reads_back_every_record_from_checkpoints_and_deltas(store) -> None:
    await _ended_run(store, steps=40)
    history = await store.history("root")
    assert [h["revision"] for h in history] == list(range(1, len(history) + 1))
    assert [len(h["record"]["data"]["log"]) for h in history[:42]] == [0, *range(1, 41), 40]
    assert history[-1]["record"]["status"] == "terminated"
    assert history[-1]["record"] == (await store.load("root")).to_dict()


async def test_compacting_an_ended_run_keeps_what_it_is(store) -> None:
    await _ended_run(store)
    final = (await store.load("root")).to_dict()
    events_before = await store.read_events("root")
    await store.audit("root", {"op": "signal", "by": "ops"})
    dry = await store.compact("root", dry_run=True)
    assert dry["history"] > 0 and dry["events"] > 0 and dry["inbox"] == 1
    assert len(await store.history("root")) > 1  # a dry run changes nothing
    assert await store.compact("root") == dry
    [only] = await store.history("root")
    assert only["record"] == final and only["revision"] == final["revision"]
    kinds = [e["kind"] for e in await store.read_events("root")]
    assert kinds == ["signal", "audit"] and len(events_before) > 2
    [message] = await store.read_inbox("root", 0)
    assert message["message"] == {"kind": "compacted"}
    assert await store.compact("root") == {"inbox": 0, "history": 0, "events": 0}


async def test_an_active_run_only_loses_applied_message_bodies(store) -> None:
    await store.create(rec())
    await store.append_inbox("root", "m1", {"kind": "signal", "signal": "STOP"})
    await store.append_inbox("root", "m2", {"kind": "signal", "signal": "CONT"})
    record = await store.load("root")
    await store.commit_control("root", record.revision, lambda r: r.but(inbox_applied=1), now=0)
    assert await store.compact("root") == {"inbox": 1, "history": 0, "events": 0}
    box = await store.read_inbox("root", 0)
    assert [m["message"] for m in box] == [
        {"kind": "compacted"},
        {"kind": "signal", "signal": "CONT"},
    ]
    assert not await store.append_inbox("root", "m1", {"kind": "signal", "signal": "STOP"})


async def test_forgetting_needs_an_ended_run_nobody_reads(store) -> None:
    await store.create(rec("live"))
    with pytest.raises(Busy):
        await store.forget("live")
    await _ended_run(store, "old", steps=3)
    await store.create(rec("fork", inherits={"root": "old"}))
    with pytest.raises(Busy, match="fork"):
        await store.forget("old")
    await store.kill("fork")
    await store.forget("fork")
    assert await store.forget("old", dry_run=True) == {"agents": 1}
    assert await store.find("old") is not None
    await store.forget("old")
    assert await store.find("old") is None and await store.history("old") == []
    assert await store.read_events("old") == [] and await store.read_inbox("old", 0) == []
    with pytest.raises(KeyError):
        await store.forget("old")


__all__ += [
    "test_an_active_run_only_loses_applied_message_bodies",
    "test_compacting_an_ended_run_keeps_what_it_is",
    "test_forgetting_needs_an_ended_run_nobody_reads",
    "test_history_reads_back_every_record_from_checkpoints_and_deltas",
]


async def test_event_numbers_keep_growing_after_compaction(store) -> None:
    await _ended_run(store, steps=3)
    last = (await store.read_events("root"))[-1]["seq"]
    await store.compact("root")
    await store.audit("root", {"op": "a"})
    await store.audit("root", {"op": "b"})
    seqs = [e["seq"] for e in await store.read_events("root")]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs) and seqs[-2] > last
    assert [e["op"] for e in await store.read_events("root", last)] == ["a", "b"]


async def test_a_run_a_live_child_works_in_is_not_forgotten(store) -> None:
    await _ended_run(store, "old", steps=2)
    await store.create(rec("other"))
    await store.create(rec("other/c", parent="other", workspace="old", workspace_mode="rw"))
    with pytest.raises(Busy, match="other/c"):
        await store.forget("old")
    assert await store.find("old") is not None


__all__ += [
    "test_a_run_a_live_child_works_in_is_not_forgotten",
    "test_event_numbers_keep_growing_after_compaction",
]
