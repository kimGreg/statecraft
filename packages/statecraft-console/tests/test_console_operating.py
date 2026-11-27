"""The console shows what an operator needs (roadmap D3): faults, unknown outcomes, undelivered
messages, maintenance and capacity on the overview; an agent's pending intents, undelivered
messages, joins and journaled calls; an unknown outcome is settled from it (admin only)."""

from __future__ import annotations

from starlette.testclient import TestClient
from statecraft_console import console_app

from statecraft import Agent, Wait, state
from statecraft.server.tokens import TokenBook
from statecraft.testing import memory_runtime


class Breaks(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        raise RuntimeError("broken on purpose")


class Holder(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "ok", "question": "?"}, then="go")


async def test_the_console_shows_and_settles(tmp_path) -> None:
    book = TokenBook(tmp_path / "tokens.json")
    ops, viewer = book.add("ops", scopes=["admin"]), book.add("viewer", scopes=["control"])
    runtime = memory_runtime()
    runtime.registrar.agents.register("breaks", Breaks)
    runtime.registrar.agents.register("holder", Holder)
    async with runtime:
        await runtime.spawn("breaks", root_id="b")
        await runtime.spawn("holder", root_id="h")
        await runtime.run(timeout=5)
        await runtime.store.append_inbox("h", "odd", {"kind": "not-a-message"})
        await runtime.run(timeout=5)
        key = "h/0/go/call/x"
        await runtime.store.begin_call("h", key, "e", "d", 1.0)
        await runtime.store.end_call(key, "indeterminate", clear_by="operator")
        app = console_app(runtime, tokens=book)
        with TestClient(app, base_url="http://console.example") as page:
            page.get(f"/?token={viewer}", follow_redirects=False)
            health = page.get("/api/health").json()
            assert [f["agent"] for f in health["faults"]] == ["b"]
            assert health["indeterminate_calls"] == 1 and health["undelivered_messages"] == 1
            assert health["maintenance"] is False
            agent = page.get("/api/agents/h").json()
            assert agent["undelivered"] and agent["calls"][0]["status"] == "indeterminate"
            assert agent["intents"] == [] and agent["joins"] == []
            refused = page.post("/api/calls/resolve", json={"key": key, "outcome": "failed"})
            assert refused.status_code == 403
        with TestClient(app, base_url="http://console.example") as page:
            page.get(f"/?token={ops}", follow_redirects=False)
            settled = page.post("/api/calls/resolve", json={"key": key, "outcome": "failed"})
            assert settled.status_code == 200, settled.text
        assert (await runtime.store.call(key))["status"] == "failed"
