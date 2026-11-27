"""Intents are carried out once, even when the executor dies half way.

What must hold
  * a signal intent delivered to its target just before the executor died (the message is in
    the target's inbox, the intent not yet marked done) is delivered again with the same id when
    the intent is carried out again, so the target gets it once.
"""

from __future__ import annotations

from statecraft import Agent, Done, Spawn, Wait, state
from statecraft.testing import memory_runtime


class Listens(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        if not ctx.signals:
            return Wait(sleep=0.02, then="go")
        data["got"] = list(ctx.signals)
        return Wait(sleep=0.1, then="end")  # later copies would arrive meanwhile

    @state
    async def end(self, ctx, data):
        data["got"] += list(ctx.signals[len(data["got"]) :])
        return Done


class Tells(Agent):
    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        return Spawn({"c": ("listens", {})}, then="tell")

    @state
    async def tell(self, ctx, data):
        ctx.signal("c", ("USR", "once"))
        return Wait(join=["c"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


async def test_a_signal_intent_carried_out_twice_is_delivered_once() -> None:
    rt = memory_runtime(scan_every=0.01)
    rt.registrar.agents.register("tells", Tells, spawns=["listens"])
    rt.registrar.agents.register("listens", Listens)
    commit_control, crashed = rt.store.commit_control, []

    async def dies_once(agent_id, revision, change, now, **detail):
        intent = detail.get("intent", "")
        if (
            detail.get("kind") == "intent"
            and agent_id == "p"
            and intent not in ("p/c", "p/init")
            and not crashed
        ):
            crashed.append(intent)  # the message is in the inbox; the intent is not marked done
            raise RuntimeError("the executor died here")
        return await commit_control(agent_id, revision, change, now, **detail)

    rt.store.commit_control = dies_once
    async with rt:
        root = await rt.spawn("tells", root_id="p")
        await rt.run(root, timeout=10)
        child = await rt.store.load("p/c")
    assert crashed, "the signal intent was never carried out"
    assert (await rt.get(root))["view"] == "succeeded"
    assert child.data["got"] == ["once"]
