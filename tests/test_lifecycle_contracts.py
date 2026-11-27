"""Lifecycle contracts: finishing with children, and messages around an agent's end.

What must hold
  * an agent may not finish while a child is live: the step faults, naming the child; ending the
    child (TERM or KILL) in the same step, or joining it first, finishes normally;
  * a signal to an agent that has ended is refused (`Ended`), never silently dropped; a parent's
    signal to an ended child stays a no-op;
  * a message that arrives while an agent's last step runs is read before it ends: that step is
    not committed, and the state runs again with the message;
  * USR messages left unread when an agent is ended (TERM, KILL) are kept in `undelivered`.
The tests coordinate with events, not sleeps.
"""

from __future__ import annotations

import asyncio

import pytest

from statecraft import Agent, Done, Spawn, Wait, state
from statecraft.kernel.store import Ended
from statecraft.testing import memory_runtime


class Waiter(Agent):
    """Waits for an answer it never gets (stays live)."""

    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "q", "question": "?"}, then="done")

    @state
    async def done(self, ctx, data):
        return Done


class Parent(Agent):
    """Spawns a waiter, then finishes: as is, after TERM in the same step, or after a join."""

    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        return Spawn({"w": ("waiter", {})}, then="finish")

    @state
    async def finish(self, ctx, data):
        how = ctx.input.get("how")
        if how == "term":
            ctx.signal("w", "TERM")
            return Done
        if how == "join" and not data.get("joined"):
            data["joined"] = True
            ctx.signal("w", "KILL")
            return Wait(join=["w"], then="finish")
        return Done


GATE: dict[str, asyncio.Event] = {}
RUNS: list[int] = []


class Last(Agent):
    """Its only step pauses at a gate (the test sends a message meanwhile), then finishes with
    what its mailbox holds. `RUNS` counts how often the step ran (its data is not committed when
    the step is refused)."""

    start = "go"

    @state
    async def go(self, ctx, data):
        RUNS.append(len(ctx.signals))
        if len(RUNS) == 1:
            GATE["entered"].set()
            await GATE["release"].wait()
        await ctx.write("seen", {"messages": ctx.signals})
        return Done


def runtime(**options):
    rt = memory_runtime(**options)
    rt.registrar.agents.register("parent", Parent, spawns=["waiter"])
    rt.registrar.agents.register("waiter", Waiter)
    rt.registrar.agents.register("last", Last, writes=["seen"])
    return rt


@pytest.mark.parametrize(
    ("how", "view", "child"),
    [
        (None, "suspended", "suspended"),
        ("term", "succeeded", "terminated"),
        ("join", "succeeded", "terminated"),
    ],
)
async def test_an_agent_finishes_only_without_live_children(how, view, child) -> None:
    async with runtime() as rt:
        root = await rt.spawn("parent", {"how": how}, root_id="p")
        await rt.run(root, timeout=10)
        record = await rt.get(root)
        assert record["view"] == view, record
        assert (await rt.get("p/w"))["view"] == child
    if how is None:
        assert "finished while children are live: w" in record["error"]


async def test_signals_to_an_ended_agent_are_refused() -> None:
    async with runtime() as rt:
        root = await rt.spawn("parent", {"how": "term"}, root_id="p")
        await rt.run(root, timeout=10)
        for signal in ("STOP", "CONT", "TERM", "KILL", ("USR", {"type": "steer", "text": "x"})):
            with pytest.raises(Ended):
                await rt.signal(root, signal)
        with pytest.raises(Ended):
            await rt.store.append_inbox("p/w", "late", {"kind": "signal", "signal": "STOP"})


async def test_a_message_during_the_last_step_is_read_before_the_end() -> None:
    GATE.update(entered=asyncio.Event(), release=asyncio.Event())
    RUNS.clear()
    async with runtime(executors=1) as rt:
        root = await rt.spawn("last", {}, root_id="l")
        await GATE["entered"].wait()  # its step runs and has read its (empty) mailbox
        await rt.signal(root, ("USR", {"type": "follow_up", "text": "one more"}))
        GATE["release"].set()  # the step now tries to end the agent
        ended = asyncio.Event()

        def listener(_event):
            ended.set()

        rt.listeners.append(listener)
        while (await rt.get(root))["status"] == "active":
            ended.clear()
            await asyncio.wait_for(ended.wait(), 10)
        seen = (await rt.artifact(root, "seen"))["payload"]
        record = await rt.get(root)
    assert seen == {"messages": [{"type": "follow_up", "text": "one more"}]}
    assert RUNS == [0, 1]  # the first run ended without the message: refused, run again
    assert record["view"] == "succeeded"


async def test_unread_messages_of_an_ended_agent_are_kept() -> None:
    async with runtime() as rt:
        root = await rt.spawn("waiter", {}, root_id="w")
        await rt.run(root, timeout=10)
        await rt.signal(root, ("USR", {"type": "steer", "text": "never read"}))
        await rt.run(root, timeout=10)  # applied to its mailbox; it waits on, unread
        await rt.signal(root, "KILL")
        record = await rt.store.load(root)
    assert record.status == "terminated"
    assert record.undelivered == [
        {"message": {"type": "steer", "text": "never read"}, "reason": "unread when it ended"}
    ]


async def test_messages_not_applied_when_an_agent_is_killed_are_kept() -> None:
    async with runtime() as rt:
        root = await rt.spawn("waiter", {}, root_id="k")
        await rt.run(root, timeout=10)
        await rt.signal(root, ("USR", {"n": 1}))  # accepted, not applied yet
        await rt.signal(root, "KILL")
        record = await rt.store.load(root)
    assert record.undelivered == [{"message": {"n": 1}, "reason": "arrived as it was killed"}]
    assert await rt.store.read_inbox(root, record.inbox_applied) == []


async def test_a_message_after_a_term_in_the_same_batch_is_kept() -> None:
    async with runtime() as rt:
        root = await rt.spawn("waiter", {}, root_id="t")
        await rt.run(root, timeout=10)
        await rt.signal(root, "TERM")  # no on_term: ends at once when applied
        await rt.signal(root, ("USR", {"n": 2}))
        await rt.run(root, timeout=10)
        record = await rt.store.load(root)
    assert record.status == "terminated"
    assert record.undelivered == [{"message": {"n": 2}, "reason": "arrived as it ended"}]
