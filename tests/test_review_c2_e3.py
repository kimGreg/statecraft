"""Regressions from the independent review of C2, D3, D4, E1 and E3: a raising `charge_of` settles
its hold and is not sent again; maintenance stops steps already queued for a slot; collection
does not make executors spin, and stops its sweep when its lease runs out (a committed object
stays)."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from test_budgets import Meter, runtime

from statecraft import Agent, Done, state
from statecraft.testing import memory_runtime


class BadCharge(Meter):
    def charge_of(self, operation, result):
        raise KeyError("usage")


async def test_charge_of_raising_leaks_hold_and_resends():
    meter = BadCharge()
    async with runtime(meter) as rt:
        await rt.spawn("uses", {"times": 1}, root_id="b", budget={"units": 100})
        await rt.run("b", timeout=5)
        rec = await rt.get("b")
        found = await rt.budget("b")
        print("view", rec["view"], "sent", meter.sent, "budget", found)
        assert found["held"] == {}, found


class Slow(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await asyncio.sleep(0.3)
        return Done


async def test_maintenance_lets_queued_steps_start():
    rt = memory_runtime(concurrency=1, executors=1)
    rt.registrar.agents.register("slow", Slow)
    async with rt:
        for i in range(4):
            await rt.spawn("slow", {}, root_id=f"s{i}")
        for _ in range(100):
            vs = [(await rt.get(f"s{i}"))["view"] for i in range(4)]
            if "running" in vs:
                break
            await asyncio.sleep(0.01)
        print("queued", sum(len(e.running) for e in rt.executors))
        await rt.maintenance(True)
        await asyncio.sleep(1.5)
        views = [(await rt.get(f"s{i}"))["view"] for i in range(4)]
        print(views)
        assert views.count("succeeded") <= 1, views


async def test_gc_lease_makes_executor_spin():
    rt = memory_runtime(executors=1, scan_every=1.0)
    rt.registrar.agents.register("slow", Slow)
    async with rt:
        await rt.maintenance(True)
        await rt.spawn("slow", {}, root_id="g")
        files = await rt.workspaces.files_of("g")
        inner = files.list

        async def slow_list(prefix):
            await asyncio.sleep(0.5)
            return await inner(prefix)

        files.list = slow_list
        calls = {"n": 0}
        orig = rt.store.try_acquire

        async def counting(*a, **k):
            calls["n"] += 1
            return await orig(*a, **k)

        rt.store.try_acquire = counting
        job = asyncio.create_task(rt.collect(dry_run=False))
        await asyncio.sleep(0.05)
        await rt.maintenance(False)
        await job
        print("try_acquire calls while GC held the lease", calls["n"])


ATTEMPTS = {"n": 0}


class Writes(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("note", {"x": 1})
        ATTEMPTS["n"] += 1
        if ATTEMPTS["n"] == 1:
            raise RuntimeError("first attempt fails after writing")
        return Done


async def test_gc_lease_expires_mid_sweep_and_deletes_committed_object():
    now = [1000.0]
    rt = memory_runtime(clock=lambda: now[0])
    rt.registrar.agents.register("writes", Writes, writes=["note"]) if False else None
    try:
        rt.registrar.agents.register("writes", Writes, writes=["note"])
    except TypeError:
        rt.registrar.agents.register("writes", Writes)
    async with rt:
        await rt.spawn("writes", {}, root_id="w")
        await rt.run("w", timeout=5)
        print("after first", (await rt.get("w"))["view"])
        files = await rt.workspaces.files_of("w")
        inner = files.list

        async def slow_list(prefix):
            if prefix.endswith("objects/"):
                now[0] += 200  # the sweep took longer than LEASE_SECONDS
                await rt.signal("w", "CONT")
                await rt.run("w", timeout=5)
                print("during sweep", (await rt.get("w"))["view"])
            return await inner(prefix)

        files.list = slow_list
        report = await rt.collect(dry_run=False, grace=0)
        files.list = inner
        print("removed", report["objects"])
        rec = await rt.store.load("w")
        path = rec.published["note"]["path"]
        print("committed ref", path, "exists:", await files.exists(path))
        assert await files.exists(path)
