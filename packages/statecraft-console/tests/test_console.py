"""The console over a runtime: what it reads, and its HTTP API over a run.

What must hold
  * the registry lists every agent with its contract and state machine (states, start, the
    transitions its code names);
  * the graph links spawns, providers and artifact flow; providers list their tools and who may
    call each;
  * runs list every root; a run shows its tree and open questions; an agent shows its walked path
    with its tool calls; the workspace is listed and read;
  * a person answers and signals over the API; events stream from the durable log, resumable;
  * the page and its scripts are served.
"""

from __future__ import annotations

import json

from starlette.testclient import TestClient
from statecraft_console import console_app, graph, machine, registry

from statecraft import Agent, Done, Spawn, Wait, state
from statecraft.runtime.views import StateView
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Add(Agent):
    start = "add"

    @state
    async def add(self, ctx, data):
        await ctx.write(
            "sum", {"value": await ctx.call("math.add", a=ctx.input["a"], b=ctx.input["b"])}
        )
        return Done


class Total(Agent):
    start = "split"

    @state
    async def split(self, ctx, data):
        return Spawn({"left": ("calc:add", {"a": 1, "b": 2})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["left"], then="ask", on_fault="ask")

    @state
    async def ask(self, ctx, data):
        return Wait(
            answer={"name": "scale", "question": "By?", "schema": {"type": "integer"}},
            then="finish",
        )

    @state
    async def finish(self, ctx, data):
        await ctx.files.write("shared/note.txt", "noted")
        await ctx.write("total", {"value": 3 * ctx.woke["answer"]})
        return Done


def make():
    runtime = memory_runtime(views=[StateView()])
    runtime.registrar.tools.register(add)
    runtime.registrar.agents.register("calc:add", Add, tools=["math.add"], writes=["sum"])
    runtime.registrar.agents.register(
        "calc:total", Total, spawns=["add"], reads=["sum"], writes=["total"], meta={"owner": "x"}
    )
    return runtime


def test_an_agent_kind_is_named_by_its_base_class() -> None:
    from statecraft_console.model import _kind

    class AgenticAgent(Agent):
        start = "a"

    class Mine(AgenticAgent):
        pass

    assert _kind(Mine, {}) == "agentic"  # only the suffix goes: not "ic"
    assert _kind(Total, {}) == "state machine"


def test_a_state_machine_is_read_from_its_class() -> None:
    m = machine(Total)
    assert m["start"] == "split" and m["states"] == ["split", "wait", "ask", "finish"]
    edges = {(e["from"], e["to"], e["kind"]) for e in m["transitions"]}
    assert ("split", "wait", "wake") in edges and ("wait", "ask", "fault") in edges
    assert ("finish", "(done)", "done") in edges


async def test_the_registry_and_the_graph() -> None:
    async with make() as runtime:
        [feature] = registry(runtime)["features"]
        found = {a["ref"]: a for a in feature["agents"]}
        assert feature["name"] == "calc" and found["calc:total"]["calls"] == ["add"]
        assert found["calc:total"]["meta"] == {"owner": "x"}
        assert found["calc:add"]["tools"] == ["math.add"]
        edges = {(e["from"], e["to"], e["kind"]) for e in graph(runtime)["edges"]}
        assert ("calc:total", "calc:add", "spawns") in edges
        assert ("calc:add", "server:python", "uses") in edges
        assert ("calc:add", "type:sum", "writes") in edges
        assert ("type:sum", "calc:total", "reads") in edges


async def test_a_run_over_the_api() -> None:
    runtime = make()
    async with runtime:
        root = await runtime.spawn("calc:total", root_id="job1")
        await runtime.run(root, timeout=10)
        with TestClient(console_app(runtime, title="demo")) as client:
            assert "statecraft console" in client.get("/").text
            assert client.get("/static/app.js").status_code == 200
            assert client.get("/api/overview").json()["runs"] == 1
            [server] = client.get("/api/servers").json()
            assert server["name"] == "python" and server["tools"][0]["used_by"] == ["calc:add"]
            [run] = client.get("/api/runs").json()
            assert run["id"] == "job1" and run["questions"] == 1
            detail = client.get("/api/runs/job1").json()
            assert [a["id"] for a in detail["agents"]] == ["job1", "job1/left"]
            [question] = detail["questions"]
            bad = client.post(
                "/api/answers", json={"question_id": question["question_id"], "answer": "x"}
            )
            assert bad.status_code == 409
            ok = client.post(
                "/api/answers", json={"question_id": question["question_id"], "answer": 2}
            )
            assert ok.status_code == 204
        await runtime.run(root, timeout=10)
        with TestClient(console_app(runtime)) as client:
            agent = client.get("/api/agents/job1").json()
            assert agent["record"]["status"] == "succeeded"
            assert agent["definition"]["machine"]["start"] == "split"
            child = client.get("/api/agents/job1/left").json()
            calls = [c for p in child["path"] for c in p["crossings"]]
            assert calls[0]["effect"] == "math.add" and calls[0]["ok"]
            paths = client.get("/api/runs/job1/files").json()
            assert "shared/note.txt" in paths and "view/state.json" in paths
            assert (
                client.get("/api/runs/job1/file", params={"path": "shared/note.txt"}).json()["text"]
                == "noted"
            )
            assert client.get("/api/runs/nope").status_code == 404
            assert (
                client.post("/api/signals", json={"agent": "nope", "signal": "STOP"}).status_code
                == 404
            )


async def test_the_events_stream_and_resume() -> None:
    runtime = make()
    async with runtime:
        root = await runtime.spawn("calc:total", root_id="job1")
        await runtime.run(root, timeout=10)
        log = await runtime.store.read_events("job1")
        client = TestClient(console_app(runtime))
        with client, client.stream("GET", "/api/runs/job1/events", params={"since": 2}) as response:
            seen = []
            for line in response.iter_lines():
                if line.startswith("data: "):
                    seen.append(json.loads(line[6:]))
                if len(seen) == len(log) - 2:
                    break
        assert [e["seq"] for e in seen] == [e["seq"] for e in log[2:]]
