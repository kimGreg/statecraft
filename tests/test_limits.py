"""Limits on a tree of agents, enforced by the runtime (not left to the agents).

What must hold
  * `max_depth`: a spawn that would put a child below the limit faults the parent with the
    reason (a root is depth 0);
  * `max_total_agents`: the tree never has more agents than the limit, counting ended ones;
  * `max_live_agents`: no more agents of the tree than the limit are live at once (waiting ones
    count; execution slots are `runtime.concurrency`); once children end, new ones may start;
  * `ctx.spawn_problem` tells an agent in advance what the runtime would refuse;
  * the limits come from `Runtime(limits=...)` or the configuration's `runtime.limits`; an
    unknown limit is refused.
"""

from __future__ import annotations

import pytest

from statecraft import Agent, Done, Runtime, Spawn, Wait, state
from statecraft.testing import memory_runtime


class Fan(Agent):
    """Spawns `input.n` leaves at once (or in rounds of `input.round`), joins them."""

    start = "go"

    @state
    async def go(self, ctx, data):
        data["problem"] = await ctx.spawn_problem("leaf", ctx.input.get("n", 1))
        return "spawn"

    @state
    async def spawn(self, ctx, data):
        start = data.get("made", 0)
        count = ctx.input.get("round") or ctx.input.get("n", 1)
        names = [f"l{i}" for i in range(start, min(start + count, ctx.input.get("n", 1)))]
        data["made"] = start + len(names)
        return Spawn({n: ("leaf", {}) for n in names}, then="join")

    @state
    async def join(self, ctx, data):
        names = [f"l{i}" for i in range(data["made"])]
        return Wait(join=names, then="more")

    @state
    async def more(self, ctx, data):
        return "spawn" if data["made"] < ctx.input.get("n", 1) else Done


class Leaf(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        if ctx.input.get("deeper"):
            return Spawn({"x": ("leaf", {"deeper": ctx.input["deeper"] - 1})}, then="wait")
        return Done

    @state
    async def wait(self, ctx, data):
        return Wait(join=["x"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


def runtime_with(**limits: int) -> Runtime:
    runtime = memory_runtime(limits=limits)
    runtime.registrar.agents.register("fan", Fan, spawns=["leaf"])
    runtime.registrar.agents.register("leaf", Leaf, spawns=["leaf"])
    return runtime


async def test_max_depth_faults_the_parent_that_would_go_too_deep() -> None:
    async with runtime_with(max_depth=2) as runtime:
        root = await runtime.spawn("leaf", {"deeper": 5}, root_id="d")
        await runtime.run(root, timeout=10)
        tree = {r["id"]: r for r in await runtime.tree(root)}
    assert set(tree) == {"d", "d/x", "d/x/x"}
    assert tree["d/x/x"]["view"] == "suspended"
    assert "limit max_depth=2: a child would be at depth 3" in tree["d/x/x"]["error"]


async def test_max_live_lets_rounds_through_and_refuses_a_burst() -> None:
    async with runtime_with(max_live_agents=3) as runtime:
        rounds = await runtime.spawn("fan", {"n": 6, "round": 2}, root_id="rounds")
        rows = await runtime.run(rounds, timeout=10)
        assert {r["view"] for r in rows} == {"succeeded"} and len(rows) == 7
        burst = await runtime.spawn("fan", {"n": 3}, root_id="burst")
        await runtime.run(burst, timeout=10)
        record = await runtime.get(burst)
    assert record["view"] == "suspended" and "limit max_live_agents=3" in record["error"]
    assert record["data"]["problem"] == "limit max_live_agents=3: 4 agents would be live"


async def test_max_agents_counts_ended_agents_too() -> None:
    async with runtime_with(max_total_agents=4) as runtime:
        root = await runtime.spawn("fan", {"n": 4, "round": 1}, root_id="a")
        await runtime.run(root, timeout=10)
        rows = await runtime.tree(root)
    assert len(rows) == 4  # the root and three leaves; the fourth leaf was refused
    head = next(r for r in rows if r["id"] == "a")
    assert "limit max_total_agents=4: the tree would have 5" in head["error"]


def test_limits_come_from_the_configuration(tmp_path) -> None:
    runtime = Runtime.from_config(
        {
            "store": {"provider": "memory"},
            "workspace": {"provider": "memory"},
            "runtime": {"limits": {"max_depth": 3, "max_total_agents": 32, "max_live_agents": 8}},
        }
    )
    assert runtime.limits == {"max_depth": 3, "max_total_agents": 32, "max_live_agents": 8}
    with pytest.raises(ValueError, match="unknown limits"):
        memory_runtime(limits={"max_tokens": 10})


class Pair(Agent):
    """Spawns two fans, each of which spawns its own leaves."""

    start = "go"

    @state
    async def go(self, ctx, data):
        return Spawn({"a": ("fan", {"n": 2}), "b": ("fan", {"n": 2})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["a", "b"], then="end")

    @state
    async def end(self, ctx, data):
        return Done


async def test_concurrent_spawns_cannot_pass_a_limit_together() -> None:
    """Two agents of one tree commit spawns at once: each was checked against the tree as it
    was (the early check is skipped here, as in that race); the commit counts again in its own
    transaction, children not created yet included, and refuses the one that would pass it."""
    runtime = memory_runtime(limits={"max_total_agents": 5})
    runtime.registrar.agents.register("fan", Fan, spawns=["leaf"])
    runtime.registrar.agents.register("leaf", Leaf, spawns=["leaf"])
    runtime.registrar.agents.register("pair", Pair, spawns=["fan"])

    async def no_early_check(parent, count):
        return None

    runtime.limit_problem = no_early_check
    async with runtime:
        await runtime.spawn("pair", root_id="race")
        await runtime.run("race", timeout=10)
        tree = await runtime.store.tree("race")
        assert len(tree) <= 5
        refused = [r.id for r in tree if r.error and "limit max_total_agents=5" in r.error]
        assert len(refused) == 1 and refused[0] in ("race/a", "race/b")
