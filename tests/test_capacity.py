"""Capacity and cancellation of work that cannot be stopped (threads, processes).

What must hold
  * a thread tool that times out keeps its provider's capacity until the thread ends: with
    `concurrency=1`, never two runs at once;
  * no other attempt of the call starts while the timed-out attempt still runs;
  * a step cancelled while its thread runs (KILL) ends at once; the capacity stays taken until
    the thread ends;
  * a KILL carried out by one executor cancels the step on every executor of the runtime.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import ClassVar

from statecraft import Agent, Done, EffectError, Spawn, Wait, state, tool
from statecraft.testing import memory_runtime

LIVE = {"now": 0, "peak": 0, "runs": 0}
LOCK = threading.Lock()


@tool(name="work.block", policy="idempotent", execution="thread", timeout=0.1, max_attempts=3)
def block(seconds: float) -> bool:
    with LOCK:
        LIVE["now"] += 1
        LIVE["runs"] += 1
        LIVE["peak"] = max(LIVE["peak"], LIVE["now"])
    time.sleep(seconds)
    with LOCK:
        LIVE["now"] -= 1
    return True


class Blocker(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        try:
            await ctx.call("work.block", seconds=ctx.input["seconds"])
        except EffectError as exc:
            data["failure"] = exc.cls
        return Done


def runtime(**options):
    rt = memory_runtime(**options)
    rt.registrar.tools.register(block)
    rt.registrar.providers._r.providers["python"].concurrency = 1
    rt.registrar.agents.register("blocker", Blocker, tools=["work.*"])
    return rt


async def test_a_timed_out_thread_keeps_its_capacity_and_attempts_never_overlap() -> None:
    LIVE.update(now=0, peak=0, runs=0)
    async with runtime() as rt:
        rt.effects.backoff = 0.001
        a = await rt.spawn("blocker", {"seconds": 0.3}, root_id="a")
        b = await rt.spawn("blocker", {"seconds": 0.3}, root_id="b")
        await asyncio.gather(rt.run(a, timeout=20), rt.run(b, timeout=20))
        crossing = next(
            c
            for h in await rt.history(a)
            for c in h.get("crossings", [])
            if c["effect"] == "work.block"
        )
    assert LIVE["peak"] == 1, LIVE  # concurrency 1 holds through timeouts and retries
    assert LIVE["runs"] == 6  # two calls, three attempts each, one after another
    assert crossing["attempts"] == 3 and crossing["failure"] == "timeout" and crossing["lingered"]


class Slow(Agent):
    start = "go"
    ticks: ClassVar[list[float]] = []
    started: ClassVar[asyncio.Event | None] = None

    @state
    async def go(self, ctx, data):
        Slow.started.set()
        for _ in range(200):
            Slow.ticks.append(time.monotonic())
            await asyncio.sleep(0.02)
        return Done


class Killer(Agent):
    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        return Spawn({"c": ("slow", {})}, then="kill")

    @state
    async def kill(self, ctx, data):
        await Slow.started.wait()
        ctx.signal("c", "KILL")
        return Wait(join=["c"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


async def test_a_kill_cancels_the_step_on_every_executor() -> None:
    Slow.ticks, Slow.started = [], asyncio.Event()
    rt = memory_runtime(executors=3, concurrency=4, beat=30, lease_ttl=60)
    rt.registrar.agents.register("killer", Killer, spawns=["slow"])
    rt.registrar.agents.register("slow", Slow)
    async with rt:
        root = await rt.spawn("killer", root_id="k")
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["view"] == "succeeded"
        assert (await rt.get(f"{root}/c"))["view"] == "terminated"
        ended = len(Slow.ticks)
        await asyncio.sleep(0.3)
    assert len(Slow.ticks) - ended <= 1  # the killed step stopped (no 30 s heartbeat wait)
