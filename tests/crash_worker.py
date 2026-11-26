"""A worker process for the crash tests: runs one executor on a SQLite store until the tree is
quiet, optionally dying (`os._exit`, which skips `finally` like SIGKILL) at a crash point.

    python crash_worker.py <db> <workspace> <crash point or "-"> [spawn]
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

from statecraft.agent import Agent, Done, Spawn, Wait, state
from statecraft.catalog import Catalog
from statecraft.engine import Engine
from statecraft.stores import SqliteStore
from statecraft.workspace import LocalFiles

ROOT = "crashroot"


class NoTools:
    async def call(self, server: str, tool: str, arguments: dict[str, Any]) -> Any:
        raise KeyError(tool)


class Leaf(Agent):
    start = "work"

    @state
    async def work(self, ctx, data):
        key = ctx.effect_key("log")
        await ctx.files.write(f"shared/log/{key.replace('/', '_')}-{os.getpid()}", key)
        marker = f"home/{ctx.id}/private/started"
        if ctx.input.get("slow") and not await ctx.files.exists(marker):
            await ctx.files.write(marker, "1")
            await asyncio.sleep(60)
        await ctx.write("leaf", {"n": ctx.input["n"]})
        return Done


class Chain(Agent):
    start = "spread"

    @state
    async def spread(self, ctx, data):
        return Spawn(
            {
                "a": ("agent.leaf", {"n": 1, "slow": ctx.input.get("slow", False)}),
                "b": ("agent.leaf", {"n": 2}),
            },
            then="gather",
        )

    @state
    async def gather(self, ctx, data):
        return Wait(join=["a", "b"], then="publish")

    @state
    async def publish(self, ctx, data):
        values = [(await ctx.read("leaf", by=n)).payload["n"] for n in ("a", "b")]
        await ctx.write("sum", {"value": sum(values)})
        return Done


CATALOG = {"t:chain": (Chain, None), "t:leaf": (Leaf, None)}


def make_engine(db: str, workspace: str) -> Engine:
    return Engine(
        store=SqliteStore(db),
        files=LocalFiles(workspace),
        tools=NoTools(),
        catalog=Catalog({}, extra=dict(CATALOG)),
    )


def arm(engine: Engine, point: str) -> None:
    """Make the store die at a crash point: `<method>:<before|after>:<nth call>`."""
    method, when, nth = point.split(":")
    store = engine.store
    original = getattr(store, method)
    count = {"n": 0}

    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        count["n"] += 1
        if count["n"] == int(nth) and when == "before":
            os._exit(9)
        result = await original(*args, **kwargs)
        if count["n"] == int(nth) and when == "after":
            os._exit(9)
        return result

    setattr(store, method, wrapped)


async def main(db: str, workspace: str, point: str, spawn: bool, slow: bool) -> None:
    engine = make_engine(db, workspace)
    if spawn:
        await engine.spawn("t:chain", {"slow": slow}, root_id=ROOT)
    if point != "-":
        arm(engine, point)
    await engine.run(ROOT, timeout=60, scan_every=0.02, lease_ttl=0.5, beat=0.1)


if __name__ == "__main__":
    args = sys.argv[1:]
    asyncio.run(main(args[0], args[1], args[2], "spawn" in args, "slow" in args))
