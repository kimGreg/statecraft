"""Failures stay where they happen: one agent's failure never stops the others.

What must hold
  * a value that is not a signal is refused by the API (ValueError; HTTP 400 is tested with the
    server) and never stored;
  * a message already in an inbox that cannot be applied (a poisoned store, an old client, an
    answer to a question that closed) is rejected: recorded in the agent's history, skipped,
    and the executor and every other agent go on;
  * a spawn whose agent cannot be created faults the parent with the reason; after the registry
    is fixed, CONT carries the spawn out;
  * any exception from preparing or running a step is the state's fault, committed once, with
    the lease released: an agent constructor that raises, a CancelledError raised inside the
    state, a state result that cannot be stored;
  * a state that bounds a tool call itself (`asyncio.wait_for`) and handles the timeout succeeds;
    a state that faults keeps its own error first;
  * after `stop`, no step holds a lease, even one that outlived the first cancellation.
"""

from __future__ import annotations

import asyncio

import pytest

from statecraft import Agent, Done, EffectDefinition, Runtime, Spawn, Wait, state
from statecraft.testing import FunctionProvider, memory_runtime


class Sleeper(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(sleep=0.05, then="done")

    @state
    async def done(self, ctx, data):
        return Done


class BadInit(Agent):
    start = "go"
    built = 0

    def __init__(self) -> None:
        BadInit.built += 1
        raise RuntimeError("constructor broke")

    @state
    async def go(self, ctx, data):
        return Done


class CancelsItself(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        raise asyncio.CancelledError()


class Unstorable(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        data["value"] = {1, 2}  # a set is not JSON
        return Done


class Bounded(Agent):
    """Gives a slow tool 50 ms, then goes on without it."""

    start = "go"

    @state
    async def go(self, ctx, data):
        try:
            await asyncio.wait_for(ctx.call("svc.slow"), 0.05)
        except TimeoutError:
            data["gave_up"] = True
        return Done


class GatherFails(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await asyncio.gather(ctx.call("svc.slow"), ctx.call("svc.bad"))
        return Done


class Parent(Agent):
    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        return Spawn({"c": (ctx.input["child"], {})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["c"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


async def slow(arguments):
    await asyncio.sleep(5)
    return True


async def bad(arguments):
    raise ValueError("the real cause")


def runtime_with(**options) -> Runtime:
    rt = memory_runtime(**options)
    r = rt.registrar
    for name, cls in (
        ("sleeper", Sleeper),
        ("bad_init", BadInit),
        ("cancels", CancelsItself),
        ("unstorable", Unstorable),
        ("bounded", Bounded),
        ("gather", GatherFails),
    ):
        r.agents.register(name, cls, tools=["svc.*"])
    r.agents.register("parent", Parent, spawns=["*"])
    r.providers.register("svc", FunctionProvider({"svc.slow": slow, "svc.bad": bad}))
    for op in ("svc.slow", "svc.bad"):
        r.tools.register(
            EffectDefinition(name=op, policy="read_only", provider="svc", operation=op)
        )
    return rt


async def test_a_value_that_is_not_a_signal_is_refused() -> None:
    async with runtime_with() as rt:
        root = await rt.spawn("sleeper", root_id="s")
        for bad_signal in ("FOO", "stop", ["USR"], ("USR",), 3, "CHLD"):
            with pytest.raises(ValueError):
                await rt.signal(root, bad_signal)
        assert await rt.store.read_inbox(root, 0) == []


async def test_a_poisoned_inbox_message_is_rejected_and_the_rest_go_on() -> None:
    async with runtime_with(executors=1) as rt:
        poisoned = await rt.spawn("sleeper", root_id="p")
        await rt.store.append_inbox(poisoned, "old-client", {"kind": "signal", "signal": "FOO"})
        await rt.store.append_inbox(poisoned, "junk", {"what": "ever"})
        other = await rt.spawn("sleeper", root_id="o")
        await rt.run(other, timeout=10)
        await rt.run(poisoned, timeout=10)
        assert (await rt.get(other))["view"] == "succeeded"
        assert (await rt.get(poisoned))["view"] == "succeeded"
        rejected = [h for h in await rt.history(poisoned) if h["kind"] == "rejected"]
        assert [h["detail"]["message_id"] for h in rejected] == ["old-client", "junk"]
        assert "unknown signal 'FOO'" in rejected[0]["detail"]["reason"]
        assert all(not t.done() for t in rt._tasks)  # the executor is still running


async def test_a_spawn_that_cannot_be_created_faults_the_parent_until_fixed(tmp_path) -> None:
    from statecraft.providers.stores import SqliteStore
    from statecraft.providers.workspaces import LocalWorkspace

    def make(with_ghost: bool) -> Runtime:
        rt = Runtime(
            store=SqliteStore(tmp_path / "db"),
            workspace=LocalWorkspace(tmp_path / "ws"),
            executors=0,
            scan_every=0.01,
        )
        rt.registrar.agents.register("parent", Parent, spawns=["*"])
        rt.registrar.agents.register("sleeper", Sleeper)
        if with_ghost:
            rt.registrar.agents.register("ghost", Sleeper)
        return rt

    async with make(with_ghost=True) as first:  # the spawn is committed, not carried out
        root = await first.spawn("parent", {"child": "ghost"}, root_id="p")
        record = await first.store.load(root)
        # run its first state by hand, stopping before the intent is carried out
        from statecraft.kernel.process import Transition

        lease = await first.store.try_acquire(root, "x", 30, first.clock())
        await first.store.commit_step(
            root, lease, 0, Transition(state="wait", spawns={"c": ("ghost", {})}), first.clock()
        )
        assert record.step == 0
    async with make(with_ghost=False) as second:  # restarted without the agent
        other = await second.spawn("sleeper", root_id="o")
        await second.run(timeout=10)
        parent = await second.get(root)
        assert parent["view"] == "suspended"
        assert "cannot spawn c (ghost)" in parent["error"]
        assert (await second.get(other))["view"] == "succeeded"
    async with make(with_ghost=True) as third:  # fixed: CONT carries the spawn out
        await third.signal(root, "CONT")
        await third.run(root, timeout=10)
        assert (await third.get(root))["view"] == "succeeded"
        assert (await third.get(f"{root}/c"))["view"] == "succeeded"


@pytest.mark.parametrize(
    ("agent", "error"),
    [
        ("bad_init", "constructor broke"),
        ("cancels", "cancelled from inside"),
        ("unstorable", "could not be stored"),
    ],
)
async def test_a_step_failure_is_one_committed_fault(agent, error) -> None:
    BadInit.built = 0
    async with runtime_with(lease_ttl=0.2, beat=0.05) as rt:
        root = await rt.spawn(agent, root_id="a")
        await rt.run(root, timeout=5)
        record = await rt.get(root)
        await asyncio.sleep(0.4)  # past the lease: nothing runs it again on its own
        await rt.run(root, timeout=5)
        faults = [h for h in await rt.history(root) if h["kind"] == "fault"]
        lease = await rt.store.lease(root)
    assert record["view"] == "suspended" and error in record["error"]
    assert len(faults) == 1 and lease is None
    if agent == "bad_init":
        assert BadInit.built == 1


async def test_a_state_may_bound_its_own_calls() -> None:
    async with runtime_with() as rt:
        root = await rt.spawn("bounded", root_id="b")
        await rt.run(root, timeout=5)
        record = await rt.get(root)
    assert record["view"] == "succeeded" and record["data"]["gave_up"] is True


async def test_a_faulting_state_keeps_its_own_error_first() -> None:
    async with runtime_with() as rt:
        root = await rt.spawn("gather", root_id="g")
        await rt.run(root, timeout=10)
        error = (await rt.get(root))["error"]
    assert error.index("the real cause") < error.index("still running")


class Stubborn(Agent):
    """Its cleanup outlives the first cancellation and honours the second."""

    start = "go"
    entered: asyncio.Event | None = None

    @state
    async def go(self, ctx, data):
        Stubborn.entered.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            await asyncio.sleep(30)
        return Done


async def test_stop_releases_the_lease_of_a_step_that_resists_cancellation() -> None:
    Stubborn.entered = asyncio.Event()
    rt = memory_runtime(executors=1)
    rt.registrar.agents.register("stubborn", Stubborn)
    await rt.start()
    root = await rt.spawn("stubborn", root_id="s")
    await asyncio.wait_for(Stubborn.entered.wait(), 5)
    await rt.stop(drain=0.1)
    assert await rt.store.lease(root) is None
    assert (await rt.store.load(root)).status == "active"
    await rt.close()
