"""The concurrency contract (architecture §5; acceptance checklist 6 to 12).

What must hold
  * agents run concurrently, never more steps at once than `concurrency` slots;
  * a parent waiting on a join holds no slot (one slot is enough for a parent and its children);
  * effects run in parallel inside a state and all end before the commit; a call still running
    when the state ends is cancelled and the step faults;
  * one agent's fault does not stop another;
  * a provider's concurrency limit holds;
  * a CPU-heavy tool runs in a process: heartbeats and other agents keep moving;
  * KILL and a lost lease cancel the running step and its in-flight calls; nothing is committed.
"""

from __future__ import annotations

import asyncio
import time

from statecraft import Agent, Done, EffectPolicy, Spawn, Wait, state
from statecraft.kernel.process import Lease
from statecraft.sdk.effect import EffectDefinition
from statecraft.testing import FunctionProvider, memory_runtime
from statecraft.testing.tools import burn, sleep


class Sleeper(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.call("work.sleep", seconds=ctx.input.get("seconds", 0.2))
        if ctx.input.get("fail"):
            raise RuntimeError("this one fails")
        await ctx.write("slept", {"ok": True})
        return Done


class Fan(Agent):
    """Waits on two children: a parent in a join must not hold the only slot."""

    start = "spread"

    @state
    async def spread(self, ctx, data):
        return Spawn(
            {"a": ("sleeper", {"seconds": 0.05}), "b": ("sleeper", {"seconds": 0.05})}, then="join"
        )

    @state
    async def join(self, ctx, data):
        return Wait(join=["a", "b"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


class Gather(Agent):
    """Three calls in parallel inside one state."""

    start = "go"

    @state
    async def go(self, ctx, data):
        started = time.monotonic()
        await asyncio.gather(*(ctx.call("work.sleep", seconds=0.3) for _ in range(3)))
        await ctx.write("took", {"seconds": time.monotonic() - started})
        return Done


class Leaky(Agent):
    """Starts a call and returns without waiting for it."""

    start = "go"

    @state
    async def go(self, ctx, data):
        data["_"] = id(asyncio.ensure_future(ctx.call("work.sleep", seconds=5)))  # left running
        await asyncio.sleep(0.05)
        return Done


class Burner(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("burnt", {"digest": await ctx.call("work.burn", seconds=1.5)})
        return Done


def runtime_with(**options):
    runtime = memory_runtime(**options)
    runtime.registrar.tools.register(sleep)
    runtime.registrar.tools.register(burn)
    for name, cls in {
        "sleeper": Sleeper,
        "fan": Fan,
        "gather": Gather,
        "leaky": Leaky,
        "burner": Burner,
    }.items():
        runtime.registrar.agents.register(name, cls, tools=["work.*"], spawns=["sleeper"])
    return runtime


async def test_agents_run_concurrently_within_the_slots() -> None:
    for slots, at_least in ((3, 3), (10, 6)):
        runtime = runtime_with(executors=1, concurrency=slots, scan_every=0.01)
        async with runtime:
            roots = [await runtime.spawn("sleeper", {"seconds": 0.3}) for _ in range(10)]
            for root in roots:
                await until(lambda r=root, rt=runtime: status(rt, r, "succeeded"))
            peak = runtime.executors[0].peak
            assert at_least <= peak <= slots, (slots, peak)


async def test_a_parent_in_a_join_holds_no_slot() -> None:
    runtime = runtime_with(executors=1, concurrency=1, scan_every=0.01)
    async with runtime:
        root = await runtime.spawn("fan")
        await until(lambda: status(runtime, root, "succeeded"), seconds=10)


async def test_effects_run_in_parallel_inside_a_state() -> None:
    runtime = runtime_with()
    async with runtime:
        root = await runtime.spawn("gather")
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "took"))["payload"]["seconds"] < 0.8
        crossings = [c for e in await runtime.history(root) for c in e.get("crossings", [])]
        assert len(crossings) == 3 and all(c["ok"] for c in crossings)


async def test_a_call_left_running_faults_the_state() -> None:
    runtime = runtime_with()
    async with runtime:
        root = await runtime.spawn("leaky")
        [row] = await runtime.run(root, timeout=10)
        assert row["view"] == "suspended" and "still running" in row["error"]


async def test_one_agents_fault_does_not_stop_another() -> None:
    runtime = runtime_with(executors=1, scan_every=0.01)
    async with runtime:
        bad = await runtime.spawn("sleeper", {"seconds": 0.05, "fail": True})
        good = await runtime.spawn("sleeper", {"seconds": 0.2})
        await until(lambda: status(runtime, good, "succeeded"))
        assert "this one fails" in (await runtime.get(bad))["error"]


async def test_a_providers_concurrency_limit_holds() -> None:
    running = {"now": 0, "most": 0}

    async def slow(arguments):
        running["now"] += 1
        running["most"] = max(running["most"], running["now"])
        await asyncio.sleep(0.1)
        running["now"] -= 1
        return True

    runtime = memory_runtime(executors=1, concurrency=10, scan_every=0.01)
    runtime.registrar.providers.register("svc", FunctionProvider({"svc.slow": slow}, concurrency=2))
    runtime.registrar.tools.register(
        EffectDefinition(name="svc.slow", policy="read_only", provider="svc", operation="svc.slow")
    )

    class Caller(Agent):
        start = "go"

        @state
        async def go(self, ctx, data):
            await asyncio.gather(*(ctx.call("svc.slow") for _ in range(3)))
            return Done

    runtime.registrar.agents.register("caller", Caller, tools=["svc.slow"])
    async with runtime:
        roots = [await runtime.spawn("caller") for _ in range(3)]
        for root in roots:
            await until(lambda r=root: status(runtime, r, "succeeded"))
    assert running["most"] == 2


async def test_cpu_work_in_a_process_keeps_heartbeats_and_others_moving() -> None:
    runtime = runtime_with(executors=1, concurrency=4, scan_every=0.01, lease_ttl=0.6, beat=0.1)
    async with runtime:
        burner = await runtime.spawn("burner")
        ticks = [await runtime.spawn("sleeper", {"seconds": 0.05}) for _ in range(5)]
        for root in ticks:
            await until(lambda r=root: status(runtime, r, "succeeded"), seconds=10)
        assert (await runtime.get(burner))["status"] == "active"  # still burning, not blocking
        await until(lambda: status(runtime, burner, "succeeded"), seconds=60)
        attempts = [
            c["attempts"] for e in await runtime.history(burner) for c in e.get("crossings", [])
        ]
        assert attempts == [1]  # the lease held: the step was never cancelled and rerun


class Waiter(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.call("svc.hang")
        return Done


def hanging_runtime(cancelled: list[str], **options):
    async def hang(arguments):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append("x")
            raise

    runtime = memory_runtime(executors=1, scan_every=0.01, lease_ttl=60, **options)
    runtime.registrar.providers.register("svc", FunctionProvider({"svc.hang": hang}))
    runtime.registrar.tools.register(
        EffectDefinition(
            name="svc.hang",
            provider="svc",
            operation="svc.hang",
            policy=EffectPolicy(kind="read_only", timeout=None),
        )
    )
    runtime.registrar.agents.register("waiter", Waiter, tools=["svc.hang"])
    return runtime


async def test_kill_cancels_in_flight_calls_at_once() -> None:
    cancelled: list[str] = []
    async with hanging_runtime(cancelled, beat=30) as runtime:  # no heartbeat to rely on
        killed = await runtime.spawn("waiter")
        await until(lambda: leased(runtime, killed))
        started = time.monotonic()
        await runtime.signal(killed, "KILL")
        await until(lambda: asyncio.sleep(0, len(cancelled) == 1))
        assert time.monotonic() - started < 1.0
        assert (await runtime.get(killed))["status"] == "terminated"
        kinds = [e["kind"] for e in await runtime.history(killed)]
        assert kinds == ["created", "intent", "signal"]  # created, initialized, killed


async def test_a_lost_lease_cancels_in_flight_calls() -> None:
    cancelled: list[str] = []
    async with hanging_runtime(cancelled, beat=0.05) as runtime:
        lost = await runtime.spawn("waiter")
        await until(lambda: leased(runtime, lost))
        with runtime.store.transaction() as t:  # another executor takes the lease over
            _lease, epoch = t.get_lease(lost)
            t.put_lease(lost, Lease("someone-else", epoch + 1, time.time() + 0.2), epoch + 1)
        await until(lambda: asyncio.sleep(0, len(cancelled) == 1))
        assert (await runtime.get(lost))["step"] == 0  # nothing committed by the lost step


# ---- helpers ----------------------------------------------------------------------------------


async def status(runtime, root: str, wanted: str) -> bool:
    return (await runtime.get(root))["status"] == wanted


async def leased(runtime, root: str) -> bool:
    return await runtime.store.lease(root) is not None


async def until(check, seconds: float = 20.0) -> None:
    deadline = asyncio.get_running_loop().time() + seconds
    while not await check():
        assert asyncio.get_running_loop().time() < deadline, "the condition never held"
        await asyncio.sleep(0.01)
