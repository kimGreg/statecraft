"""The console: what it reads from an engine, and its HTTP API over a run.

What must hold
  * the registry lists every agent with its declarations, its kind and its state machine (the
    states, the start, the transitions its code names);
  * the graph links spawns, tool servers and artifact flow;
  * runs list every root; a run shows its tree and open questions; an agent shows its observed
    path of states; the workspace is listed and read;
  * a person answers and signals over the API; the events stream carries every kind, resumable;
  * the page and its scripts are served.
"""

from __future__ import annotations

import json

import pytest

from statecraft import Agent, Catalog, Done, Engine, Spawn, Wait, state
from statecraft.console import graph, machine, registry
from statecraft.console.app import console_app
from statecraft.spec import AgentSpec
from statecraft.stores import MemoryStore
from statecraft.testing import MemoryTools
from statecraft.views import StateView
from statecraft.workspace import LocalFiles

starlette = pytest.importorskip("starlette")
from starlette.testclient import TestClient  # noqa: E402


class Add(Agent):
    start = "add"

    @state
    async def add(self, ctx, data):
        result = await ctx.call("math.add", a=ctx.input["a"], b=ctx.input["b"])
        await ctx.write("sum", {"value": result["sum"]})
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
        question = {"name": "scale", "question": "By?", "schema": {"type": "integer"}}
        return Wait(answer=question, then="finish")

    @state
    async def finish(self, ctx, data):
        await ctx.files.write("shared/note.txt", "noted")
        await ctx.write("total", {"value": 3 * ctx.woke["answer"]})
        return Done


ADD = AgentSpec(id="add", entry="", reads=None, writes=("sum",), tools=("math.add",))
TOTAL = AgentSpec(
    id="total",
    entry="",
    reads=("sum",),
    writes=("total",),
    tools=("agent.add",),
    meta={"owner": "x"},
)


def make(tmp_path) -> Engine:
    return Engine(
        store=MemoryStore(),
        files=LocalFiles(tmp_path),
        tools=MemoryTools(handlers={"math.add": lambda a: {"sum": a["a"] + a["b"]}}),
        catalog=Catalog({}, extra={"calc:add": (Add, ADD), "calc:total": (Total, TOTAL)}),
        views=[StateView()],
    )


def test_a_state_machine_is_read_from_its_class() -> None:
    m = machine(Total)
    assert m["start"] == "split" and m["states"] == ["split", "wait", "ask", "finish"]
    edges = {(e["from"], e["to"], e["kind"]) for e in m["transitions"]}
    assert ("split", "wait", "wake") in edges and ("wait", "ask", "fault") in edges
    assert ("finish", "(done)", "done") in edges


def test_the_registry_and_the_graph(tmp_path) -> None:
    engine = make(tmp_path)
    found = {a["ref"]: a for a in registry(engine)["extra"]}
    total = found["calc:total"]
    assert total["kind"] == "state machine" and total["calls"] == ["add"]
    assert total["meta"] == {"owner": "x"} and found["calc:add"]["tools"] == ["math.add"]
    edges = {(e["from"], e["to"], e["kind"]) for e in graph(engine)["edges"]}
    assert ("calc:total", "calc:add", "spawns") in edges
    assert ("calc:add", "server:math", "uses") in edges
    assert ("calc:add", "type:sum", "writes") in edges and (
        "type:sum",
        "calc:total",
        "reads",
    ) in edges


async def test_a_run_over_the_api(tmp_path) -> None:
    engine = make(tmp_path)
    root = await engine.spawn("calc:total", root_id="job1")
    await engine.run(root, scan_every=0.01, timeout=10)
    with TestClient(console_app(engine, title="demo")) as client:
        assert "statecraft console" in client.get("/").text
        assert client.get("/static/app.js").status_code == 200
        assert client.get("/api/overview").json()["runs"] == 1
        assert [s["name"] for s in client.get("/api/servers").json()] == ["math"]
        [run] = client.get("/api/runs").json()
        assert run["id"] == "job1" and run["questions"] == 1
        detail = client.get("/api/runs/job1").json()
        assert [a["id"] for a in detail["agents"]] == ["job1", "job1/left"]
        [question] = detail["questions"]
        bad = client.post(
            "/api/answers", json={"question_id": question["question_id"], "answer": "x"}
        )
        assert bad.status_code == 409
        ok = client.post("/api/answers", json={"question_id": question["question_id"], "answer": 2})
        assert ok.status_code == 204
    await engine.run(root, scan_every=0.01, timeout=10)
    with TestClient(console_app(engine)) as client:
        agent = client.get("/api/agents/job1").json()
        assert agent["record"]["status"] == "succeeded"
        walked = [p["state"] for p in agent["path"] if p["kind"] == "step"]
        assert walked == ["wait", "ask", "finish", "finish"]  # each step's next state
        assert agent["definition"]["machine"]["start"] == "split"
        paths = client.get("/api/runs/job1/files").json()
        assert "shared/note.txt" in paths and "view/state.json" in paths
        assert (
            client.get("/api/runs/job1/file", params={"path": "shared/note.txt"}).json()["text"]
            == "noted"
        )
        assert client.get("/api/runs/nope").status_code == 404
        assert (
            client.post("/api/signals", json={"agent": "nope", "signal": "STOP"}).status_code == 404
        )


async def test_the_events_stream_every_kind_and_resume(tmp_path) -> None:
    engine = make(tmp_path)
    root = await engine.spawn("calc:total", root_id="job1")
    await engine.run(root, scan_every=0.01, timeout=10)
    log = await engine.store.read_events("job1")
    client = TestClient(console_app(engine))
    with client, client.stream("GET", "/api/runs/job1/events", params={"since": 2}) as response:
        seen = []
        for line in response.iter_lines():
            if line.startswith("data: "):
                seen.append(json.loads(line[6:]))
            if len(seen) == len(log) - 2:
                break
    assert [e["seq"] for e in seen] == [e["seq"] for e in log[2:]]
    assert {e["kind"] for e in seen} >= {"step"}
