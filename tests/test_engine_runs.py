"""Agents on the engine: spawn, join, answers, faults handled by parents, signals, sleep, events.

What must hold: children run concurrently and their parent wakes when they end; artifacts are
published by the step's commit and read through references; a fault suspends the agent and its
parent decides (a CONT reruns the faulted state with its wake); STOP holds an agent between
states; TERM runs on_term and ends the agent and its children; a join is end synchronization; the
durable event log is gap-free and replayable from any sequence.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from statecraft.agent import Agent, ChildFailed, Done, Spawn, Wait, state
from statecraft.catalog import Catalog
from statecraft.engine import Engine
from statecraft.stores import MemoryStore, SqliteStore
from statecraft.workspace import LocalFiles

FLAKY: dict[str, int] = {}


class Tools:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def call(self, server: str, tool: str, arguments: dict[str, Any]) -> Any:
        self.calls.append((server, tool, arguments))
        if tool == "square":
            return {"value": arguments["n"] ** 2}
        raise KeyError(tool)


class Square(Agent):
    start = "compute"

    @state
    async def compute(self, ctx, data):
        n = ctx.input["n"]
        if ctx.input.get("flaky") and FLAKY.setdefault(ctx.id, 0) < 1:
            FLAKY[ctx.id] += 1
            raise RuntimeError(f"flaky {n}")
        result = await ctx.call("math.square", n=n)
        await ctx.write("square", {"n": n, "value": result["value"]}, summary={"n": n})
        return Done


class Sum(Agent):
    """Spawns squares in parallel, asks a person on a child's fault, sums the results."""

    start = "spread"

    @state
    async def spread(self, ctx, data):
        flaky = set(ctx.input.get("flaky", []))
        return Spawn(
            {
                f"sq{n}": ("agent.square", {"n": n, "flaky": n in flaky})
                for n in ctx.input["numbers"]
            },
            then="gather",
        )

    @state
    async def gather(self, ctx, data):
        names = [f"sq{n}" for n in ctx.input["numbers"]]
        return Wait(join=names, then="total", on_fault="child_failed")

    @state
    async def child_failed(self, ctx, data):
        data["failed"] = [f["child"] for f in ctx.woke["faulted"]]
        return Wait(
            answer={
                "name": "retry",
                "question": "a square failed; retry?",
                "schema": {"enum": ["retry", "end"]},
            },
            then="after_answer",
        )

    @state
    async def after_answer(self, ctx, data):
        for child in data["failed"]:
            ctx.signal(child, "CONT" if ctx.woke["answer"] == "retry" else "TERM")
        return "gather"

    @state
    async def total(self, ctx, data):
        if any(s != "succeeded" for s in ctx.woke["children"].values()):
            raise ChildFailed(str(ctx.woke["children"]))
        values = [
            (await ctx.read("square", by=f"sq{n}")).payload["value"] for n in ctx.input["numbers"]
        ]
        await ctx.write("total", {"value": sum(values)})
        return Done


class Sleeper(Agent):
    start = "nap"
    on_term = "cleanup"

    @state
    async def nap(self, ctx, data):
        data["naps"] = data.get("naps", 0) + 1
        return Wait(sleep=0.05, then="nap" if data["naps"] < 1000 else "end")

    @state
    async def end(self, ctx, data):
        return Done

    @state
    async def cleanup(self, ctx, data):
        await ctx.files.write(f"shared/cleaned-{ctx.effect_key('x').replace('/', '_')}", "yes")
        return Done


class Parent(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Spawn({"nap": ("agent.sleeper", {})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["nap"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


CATALOG = {
    "t:square": (Square, None),
    "t:sum": (Sum, None),
    "t:sleeper": (Sleeper, None),
    "t:parent": (Parent, None),
}


@pytest.fixture(params=["memory", "sqlite"])
def engine(request, tmp_path):
    FLAKY.clear()
    store = MemoryStore() if request.param == "memory" else SqliteStore(tmp_path / "s.db")
    return Engine(
        store=store,
        files=LocalFiles(tmp_path / "ws"),
        tools=Tools(),
        catalog=Catalog({}, extra=dict(CATALOG)),
    )


async def test_children_run_and_the_parent_reads_their_published_artifacts(engine) -> None:
    root = await engine.spawn("t:sum", {"numbers": [1, 2, 3]})
    table = await engine.run(root, timeout=10, scan_every=0.01)
    assert {r["id"]: r["view"] for r in table}[root] == "succeeded"
    assert (await engine.artifact(root, "total"))["payload"]["value"] == 14
    files = await engine.workspace(root).list("home")
    assert any("/objects/" in f for f in files)


async def test_a_child_fault_is_decided_by_the_parent_and_cont_reruns_the_state(engine) -> None:
    root = await engine.spawn("t:sum", {"numbers": [2, 3], "flaky": [3]})
    await engine.run(root, timeout=10, scan_every=0.01)
    [question] = await engine.questions(root)
    assert question["agent"] == root
    table = {r["id"]: r for r in await engine.table(root)}
    assert table[f"{root}/sq3"]["view"] == "suspended"
    assert "flaky 3" in table[f"{root}/sq3"]["error"]
    await engine.answer(question["question_id"], "retry")
    with pytest.raises(ValueError):
        await engine.answer(question["question_id"], "retry")
    table = await engine.run(root, timeout=10, scan_every=0.01)
    assert {r["view"] for r in table} == {"succeeded"}
    assert (await engine.artifact(root, "total"))["payload"]["value"] == 13


async def test_ending_a_failed_child_ends_the_join_but_not_as_success(engine) -> None:
    root = await engine.spawn("t:sum", {"numbers": [2], "flaky": [2]})
    await engine.run(root, timeout=10, scan_every=0.01)
    [question] = await engine.questions(root)
    await engine.answer(question["question_id"], "end")
    table = {r["id"]: r for r in await engine.run(root, timeout=10, scan_every=0.01)}
    assert table[f"{root}/sq2"]["view"] == "terminated"
    assert table[root]["view"] == "suspended" and "ChildFailed" in table[root]["error"]


async def test_stop_holds_an_agent_and_term_runs_on_term_and_ends_children(engine) -> None:
    root = await engine.spawn("t:parent")
    executor = engine.executor(scan_every=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(executor.run(stop))
    child = f"{root}/nap"
    for _ in range(200):
        await asyncio.sleep(0.01)
        record = await engine.store.find(child)
        if record is not None and record.step >= 3:
            break
    await engine.signal(child, "STOP")
    await asyncio.sleep(0.2)
    held = (await engine.store.load(child)).step
    await asyncio.sleep(0.2)
    assert (await engine.store.load(child)).step == held
    await engine.signal(child, "CONT")
    await asyncio.sleep(0.2)
    assert (await engine.store.load(child)).step > held
    await engine.signal(root, "TERM")
    for _ in range(200):
        await asyncio.sleep(0.01)
        if (await engine.store.load(child)).ended:
            break
    stop.set()
    await task
    assert (await engine.store.load(root)).status == "terminated"
    assert (await engine.store.load(child)).status == "terminated"
    cleaned = [f for f in await engine.workspace(root).list("shared") if "cleaned" in f]
    assert len(cleaned) == 1 and "_cleanup_" in cleaned[0]


async def test_the_event_log_is_gap_free_and_replayable(engine) -> None:
    root = await engine.spawn("t:sum", {"numbers": [1, 2]})
    seen: list[int] = []

    async def follow() -> None:
        async for event in engine.events(root, poll=0.02):
            seen.append(event["seq"])
            if event["agent"] == root and event["status"] == "succeeded":
                return

    follower = asyncio.create_task(follow())
    await engine.run(root, timeout=10, scan_every=0.01)
    await asyncio.wait_for(follower, 5)
    assert seen == list(range(1, len(seen) + 1))
    replay = [e["seq"] async for e in engine.events(root, since=3, follow=False)]
    assert replay[0] == 4 and replay[-1] >= seen[-1]
