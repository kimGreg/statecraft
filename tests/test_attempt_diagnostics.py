"""Attempts that did not commit, and CHLD (roadmap E1, audit R11).

What must hold
  * an attempt that did not commit leaves an `attempt` event in the run's log, once, with why
    and the effect calls it made: a commit refused by fencing, and a step cancelled by KILL (its
    calls are taken from the running attempt);
  * when a step's outcome cannot be stored and a fault is committed instead, the fault keeps the
    step's calls;
  * a child that faults or ends sends its parent a CHLD event (child, status, whether it
    faulted); the parent's wait is still decided from the child's record.
"""

from __future__ import annotations

import asyncio

from statecraft import Agent, Done, Spawn, Wait, state
from statecraft.kernel.store import Fenced
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Adds(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        data["n"] = await ctx.call("math.add", a=1, b=2)
        if ctx.input.get("hang"):
            await asyncio.sleep(30)
        if ctx.input.get("unstorable"):
            data["bad"] = float("nan")  # not JSON: the outcome cannot be stored
        return Done


class Fails(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        raise RuntimeError("child broke")


class Parent(Agent):
    start = "spread"

    @state
    async def spread(self, ctx, data):
        return Spawn({"ok": ("adds", {}), "bad": ("fails", {})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["ok", "bad"], then="done", on_fault="done")

    @state
    async def done(self, ctx, data):
        return Done


def runtime(**options):
    rt = memory_runtime(**options)
    rt.registrar.tools.register(add)
    rt.registrar.agents.register("adds", Adds, tools=["math.add"])
    rt.registrar.agents.register("fails", Fails)
    rt.registrar.agents.register("parent", Parent, spawns=["adds", "fails"])
    return rt


async def test_a_fenced_commit_keeps_its_attempt() -> None:
    async with runtime() as rt:
        original = rt.store.commit_step
        refused = {"n": 0}

        async def fenced_once(*args, **kwargs):
            if refused["n"] == 0:
                refused["n"] += 1
                raise Fenced("the lease was lost")
            return await original(*args, **kwargs)

        rt.store.commit_step = fenced_once
        await rt.spawn("adds", root_id="f")
        await rt.run("f", timeout=5)
        assert (await rt.get("f"))["view"] == "succeeded"  # the state ran again and committed
        [attempt] = await rt.attempts("f")
        assert "the lease was lost" in attempt["why"] and attempt["state"] == "go"
        assert [c["effect"] for c in attempt["crossings"]] == ["math.add"]


async def test_a_killed_step_keeps_the_calls_it_made() -> None:
    async with runtime(executors=1) as rt:
        await rt.spawn("adds", {"hang": True}, root_id="k")
        for _ in range(200):
            if rt._attempts.get(("k", 1)):
                break
            await asyncio.sleep(0.01)
        await rt.signal("k", "KILL")
        for _ in range(200):
            if await rt.attempts("k"):
                break
            await asyncio.sleep(0.01)
        [attempt] = await rt.attempts("k")
        assert attempt["why"].startswith("cancelled")
        assert [c["effect"] for c in attempt["crossings"]] == ["math.add"]
        assert rt._attempts == {}


async def test_an_unstorable_outcome_faults_with_its_calls() -> None:
    async with runtime() as rt:
        await rt.spawn("adds", {"unstorable": True}, root_id="u")
        await rt.run("u", timeout=5)
        [fault] = await rt.faults()
        assert "could not be stored" in fault["error"] or "not JSON" in fault["error"]
        assert [c["effect"] for c in fault["crossings"]] == ["math.add"]


async def test_children_send_chld_to_their_parent() -> None:
    async with runtime() as rt:
        await rt.spawn("parent", root_id="p")
        await rt.run("p", timeout=5)
        news = [e for e in await rt.store.read_events("p") if e["kind"] == "CHLD"]
        found = {(e["agent"], e["child"], e["child_status"], e["fault"]) for e in news}
        assert ("p", "p/ok", "succeeded", False) in found
        assert ("p", "p/bad", "active", True) in found  # faulted: suspended until decided
        assert all(e["agent"] == "p" for e in news)


async def test_every_event_tells_its_agent_and_that_agents_status() -> None:
    """A reader waiting for `agent == root and status == succeeded` is never misled by a child's
    news, a diagnostic or an audit: every event's `status` is its own agent's status then."""
    async with runtime() as rt:
        await rt.spawn("parent", root_id="q")
        await rt.store.audit("q", {"op": "look", "agent": "q", "by": "ops"})
        await rt.run("q", timeout=5)
        events = await rt.store.read_events("q")
    assert all("agent" in e and "status" in e for e in events)
    ok = next(e for e in events if e["kind"] == "CHLD" and e["child"] == "q/ok")
    assert (ok["agent"], ok["status"], ok["child_status"]) == ("q", "active", "succeeded")
    assert not [e for e in events if e["agent"] == "q" and e["status"] == "succeeded"]
