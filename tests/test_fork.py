"""A fork: a run that takes over an earlier run's publications for the agents it does not run.

What must hold: the fork reads the kept agents' artifacts as its own (ctx.read, the cumulative
state, the views) without copying them; an agent it runs again overrides the inherited
one; agents not kept are not seen; the earlier run is left as it was; inheriting from a root that
does not exist is refused.
"""

from __future__ import annotations

import json

import pytest

from statecraft.runtime.views import StateView
from statecraft.sdk.agent import Agent, Done, Spawn, Wait, state
from statecraft.testing import permissive_runtime


class Step(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write(
            ctx.input["kind"],
            {"by": ctx.id, "value": ctx.input.get("value")},
            summary={"by": ctx.id},
        )
        return Done


class Chain(Agent):
    """Runs steps a, b, c from `from` on; c reads a and b."""

    start = "go"

    @state
    async def go(self, ctx, data):
        steps = ["a", "b", "c"]
        todo = steps[steps.index(ctx.input.get("from", "a")) :]
        return Spawn(
            {n: ("t:step", {"kind": f"out_{n}", "value": ctx.input.get("value")}) for n in todo},
            then="wait",
        )

    @state
    async def wait(self, ctx, data):
        steps = ["a", "b", "c"]
        return Wait(join=steps[steps.index(ctx.input.get("from", "a")) :], then="read")

    @state
    async def read(self, ctx, data):
        seen = {k: (await ctx.read(f"out_{k}")).payload["by"] for k in ("a", "b", "c")}
        await ctx.write("seen", seen)
        return Done


class Look(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("progress", await ctx.describe("run1"))
        return Done


@pytest.fixture
async def engine(tmp_path):
    e = permissive_runtime(
        {"t:chain": Chain, "t:step": Step, "t:look": Look}, files_root=tmp_path, views=[StateView()]
    )
    await e.start()
    yield e
    await e.close()


async def test_a_fork_reads_the_kept_agents_and_runs_the_rest(engine, tmp_path) -> None:
    await engine.spawn("t:chain", {"value": 1}, root_id="run1")
    await engine.run("run1", scan_every=0.01)
    before = (tmp_path / "run1" / "view" / "state.json").read_text()
    await engine.spawn(
        "t:chain",
        {"from": "c", "value": 2},
        root_id="fork1",
        inherit={"root": "run1", "agents": ["a", "b"]},
    )
    await engine.run("fork1", scan_every=0.01)
    assert {r["view"] for r in await engine.table("fork1")} == {"succeeded"}
    seen = (await engine.artifact("fork1", "seen"))["payload"]
    assert seen == {"a": "run1/a", "b": "run1/b", "c": "fork1/c"}
    view = json.loads((tmp_path / "fork1" / "view" / "state.json").read_text())
    assert set(view["agents"]) == {"chain", "step"}
    index = json.loads((tmp_path / "fork1" / "view" / "index.json").read_text())
    assert set(index) >= {"out_a", "out_b", "out_c", "seen"}
    assert index["out_a"].startswith("home/run1/")  # read where run1 published it
    assert (tmp_path / "run1" / "view" / "state.json").read_text() == before
    assert not list((tmp_path / "fork1").glob("home/run1/**/*.json"))  # nothing copied


async def test_agents_not_kept_are_not_seen(engine) -> None:
    await engine.spawn("t:chain", {}, root_id="run1")
    await engine.run("run1", scan_every=0.01)
    await engine.spawn(
        "t:chain", {"from": "c"}, root_id="fork1", inherit={"root": "run1", "agents": ["a"]}
    )
    table = await engine.run("fork1", scan_every=0.01)
    root = next(r for r in table if r["id"] == "fork1")
    assert root["view"] == "suspended" and "out_b" in root["error"]


async def test_inheriting_from_nothing_is_refused(engine) -> None:
    with pytest.raises(ValueError):
        await engine.spawn("t:chain", {}, inherit={"root": "nowhere"})


async def test_an_agent_can_read_another_runs_progress(engine) -> None:
    await engine.spawn("t:chain", {}, root_id="run1")
    await engine.run("run1", scan_every=0.01)

    await engine.spawn("t:look", {}, root_id="look1")
    await engine.run("look1", scan_every=0.01)
    progress = (await engine.artifact("look1", "progress"))["payload"]
    assert (progress["status"], sorted(progress["succeeded"])) == ("succeeded", ["a", "b", "c"])
