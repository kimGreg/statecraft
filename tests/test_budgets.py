"""Per-tree budgets for metered calls (roadmap E3).

What must hold
  * a metered call whose provider estimates it holds that estimate against its tree's budget in
    the store before it is sent, and is refused (not sent) when what was spent, what is held and
    the estimate would pass a limit; it settles with what the provider says it used;
  * holds are seen by every agent of the tree at once: two concurrent calls cannot both pass;
  * a call that provably was not sent releases its hold; one that may have been sent and said
    nothing spends its hold; the holds of an agent's dead attempts count as spent;
  * raising the limit and CONT lets a refused agent go on; a tree without a budget holds nothing;
  * the budget is read and set over HTTP (setting it needs admin, and is audited);
  * forgetting a run removes its budget.
"""

from __future__ import annotations

import asyncio

import httpx

from statecraft import Agent, Done, EffectDefinition, EffectError, EffectPolicy, state
from statecraft.server import server_app
from statecraft.testing import memory_runtime


class Meter:
    """A metered provider: each call is estimated at 4 units and uses 3."""

    def __init__(self, *, delay: float = 0.0, fail: str | None = None) -> None:
        self.sent = 0
        self.delay = delay
        self.fail = fail

    async def discover(self):
        return [
            EffectDefinition(
                name="meter.use", policy=EffectPolicy(kind="read_only", cost="metered")
            )
        ]

    def estimate_of(self, operation, arguments):
        return {"units": 4}

    def charge_of(self, operation, result):
        return {"units": 3}

    async def invoke(self, operation, arguments, context):
        if self.fail == "unsent":
            raise EffectError("unavailable", "down", sent=False)
        self.sent += 1
        await asyncio.sleep(self.delay)
        if self.fail == "unknown":
            raise EffectError("failed", "the answer was lost")
        return "ok"


class Uses(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        data["used"] = data.get("used", 0)
        while data["used"] < ctx.input.get("times", 1):
            await ctx.call("meter.use")
            data["used"] += 1
        return Done


def runtime(meter: Meter, **options):
    rt = memory_runtime(**options)
    rt.registrar.providers.register("meter", meter)
    rt.registrar.agents.register("uses", Uses, tools=["meter.*"])
    return rt


async def test_a_call_over_budget_is_refused_before_it_is_sent() -> None:
    meter = Meter()
    async with runtime(meter) as rt:
        await rt.spawn("uses", {"times": 4}, root_id="b", budget={"units": 10})
        await rt.run("b", timeout=5)
        record = await rt.get("b")
        assert record["view"] == "suspended" and "budget units=10" in record["error"]
        assert meter.sent == 3  # 0+4, 3+4, 6+4 fit; 9+4 does not
        found = await rt.budget("b")
        assert found["spent"] == {"units": 9} and found["held"] == {}
        await rt.set_budget("b", {"units": 30})
        await rt.signal("b", "CONT")
        await rt.run("b", timeout=5)
        # the faulted state runs again from its last commit: its calls are made (and paid) again
        assert (await rt.get("b"))["view"] == "succeeded" and meter.sent == 7
        assert (await rt.budget("b"))["spent"] == {"units": 21}


async def test_concurrent_calls_of_a_tree_cannot_both_pass() -> None:
    meter = Meter(delay=0.2)
    async with runtime(meter, executors=1) as rt:
        await rt.spawn("uses", root_id="one", budget={"units": 5})
        # another agent of the tree holds 4 while its call is out: this one sees that hold
        await rt.store.reserve("one", "elsewhere", {"units": 4}, agent="one/x", epoch=1)
        await rt.run("one", timeout=5)
        record = await rt.get("one")
        assert "budget units=5" in record["error"] and meter.sent == 0


async def test_unsent_calls_release_and_unknown_ones_spend() -> None:
    for fail, spent in (("unsent", {}), ("unknown", {"units": 4})):
        meter = Meter(fail=fail)
        async with runtime(meter) as rt:
            await rt.spawn("uses", root_id="f", budget={"units": 100})
            await rt.run("f", timeout=5)
            found = await rt.budget("f")
            assert found["held"] == {} and found["spent"] == spent, fail


async def test_holds_of_dead_attempts_count_as_spent() -> None:
    async with runtime(Meter()) as rt:
        await rt.spawn("uses", root_id="d", budget={"units": 10})
        store = rt.store
        await store.reserve("d", "old", {"units": 4}, agent="d", epoch=1)
        await store.reserve("d", "new", {"units": 1}, agent="d", epoch=2)
        found = await store.budget("d")
        assert found["spent"] == {"units": 4} and set(found["held"]) == {"new"}
        assert await store.reserve("d", "new", {"units": 1}, agent="d", epoch=2)  # once
        assert len((await store.budget("d"))["held"]) == 1


async def test_no_budget_holds_nothing_and_forget_removes_it() -> None:
    meter = Meter()
    async with runtime(meter) as rt:
        await rt.spawn("uses", {"times": 5}, root_id="free")
        await rt.run("free", timeout=5)
        assert await rt.budget("free") is None and meter.sent == 5
        await rt.spawn("uses", root_id="gone", budget={"units": 10})
        await rt.run("gone", timeout=5)
        await rt.forget("gone")
        assert await rt.budget("gone") is None


async def test_the_budget_over_http(tmp_path) -> None:
    from statecraft.server.tokens import TokenBook

    book = TokenBook(tmp_path / "tokens.json")
    ci, ops = book.add("ci", scopes=["control"]), book.add("ops", scopes=["admin"])
    async with runtime(Meter()) as rt:
        app = server_app(rt, tokens=book)

        def client(secret):
            return httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://b.example",
                headers={"authorization": f"Bearer {secret}"},
            )

        async with client(ci) as c:
            made = await c.post(
                "/agents", json={"agent": "uses", "root": "h", "budget": {"units": 3}}
            )
            assert made.status_code == 201
            assert (await c.get("/agents/h/budget")).json()["limit"] == {"units": 3}
            refused = await c.post("/agents/h/budget", json={"limit": {"units": 9}})
            assert refused.status_code == 403
        async with client(ops) as c:
            raised = await c.post("/agents/h/budget", json={"limit": {"units": 9}})
            assert raised.json()["limit"] == {"units": 9}
        audits = [e for e in await rt.store.read_events("h") if e["kind"] == "audit"]
        assert [(e["op"], e["by"]) for e in audits] == [("spawn", "ci"), ("budget", "ops")]
