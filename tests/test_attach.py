"""A root attached to another root's workspace: a conversation about a finished run.

What must hold: the attached root's tools and files work in the run's session; it reads the
run's views and shared files; it publishes under its own home; it never rewrites the run's views;
its input does not replace the run's; attaching to a root that does not exist is refused.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from statecraft.agent import Agent, Done, Spawn, Wait, state
from statecraft.catalog import Catalog
from statecraft.engine import Engine
from statecraft.stores import MemoryStore
from statecraft.views import StateView
from statecraft.workspace import LocalFiles


class Run(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.files.write("shared/notes.txt", "the run's note")
        await ctx.write("finding", {"value": 7}, summary={"value": 7})
        return Done


class Reader(Agent):
    start = "read"

    @state
    async def read(self, ctx, data):
        state_view = json.loads(await ctx.files.read_text("view/state.json"))
        note = await ctx.files.read_text("shared/notes.txt")
        await ctx.write("answer", {"agents": list(state_view["agents"]), "note": note})
        return Done


class Sessions:
    def __init__(self) -> None:
        self.bound: list[str] = []

    def __call__(self, session: str) -> Any:
        self.bound.append(session)
        return self

    async def call(self, server: str, tool: str, arguments: dict[str, Any]) -> Any:
        return {}


@pytest.fixture
def engine(tmp_path):
    tools = Sessions()
    e = Engine(
        store=MemoryStore(),
        files=LocalFiles(tmp_path),
        tools=tools,
        catalog=Catalog({}, extra={"t:run": (Run, None), "t:reader": (Reader, None)}),
        views=[StateView()],
    )
    return e, tools, tmp_path


async def test_an_attached_root_reads_the_run_and_leaves_its_views(engine) -> None:
    e, _tools, folder = engine
    run = await e.spawn("t:run", {"topic": {"name": "x"}}, root_id="run1")
    await e.run(run, scan_every=0.01)
    before = (folder / "run1" / "view" / "state.json").read_text()
    chat = await e.spawn("t:reader", {"message": "hi"}, root_id="chat1", workspace="run1")
    [row] = await e.run(chat, scan_every=0.01)
    assert row["view"] == "succeeded"
    answer = json.loads(
        (folder / "run1" / (await e.store.load(chat)).published["answer"]["path"]).read_text()
    )["payload"]
    assert answer == {"agents": ["run"], "note": "the run's note"}
    assert (folder / "run1" / "view" / "state.json").read_text() == before
    assert json.loads((folder / "run1" / "shared" / "input" / "input.json").read_text()) == {
        "topic": {"name": "x"}
    }
    assert (folder / "run1" / "shared" / "input" / "chat1.json").exists()
    assert not (folder / "chat1").exists()


async def test_attaching_to_nothing_is_refused(engine) -> None:
    e, _tools, _folder = engine
    with pytest.raises(ValueError):
        await e.spawn("t:reader", {}, workspace="nowhere")


class Asker(Agent):
    """Spawns a reader attached to `run`, then reads what it answered."""

    start = "ask"

    @state
    async def ask(self, ctx, data):
        return Spawn({"reader": ("t:reader", {})}, then="wait", attach={"reader": ctx.input["run"]})

    @state
    async def wait(self, ctx, data):
        return Wait(join=["reader"], then="done")

    @state
    async def done(self, ctx, data):
        await ctx.write("heard", (await ctx.read("answer", by="reader")).payload)
        return Done


async def test_a_child_can_be_attached_to_another_runs_workspace(engine) -> None:
    e, _tools, folder = engine
    e.services.catalog.extra["t:asker"] = (Asker, None)
    await e.spawn("t:run", {"topic": {"name": "x"}}, root_id="run1")
    await e.run("run1", scan_every=0.01)
    await e.spawn("t:asker", {"run": "run1"}, root_id="conv1")
    table = await e.run("conv1", scan_every=0.01)
    assert {r["view"] for r in table} == {"succeeded"}, table
    heard = (await e.artifact("conv1", "heard"))["payload"]
    assert heard == {"agents": ["run"], "note": "the run's note"}
    assert (folder / "run1" / "home" / "conv1" / "reader" / "objects").is_dir()
    assert not (folder / "conv1" / "home" / "conv1" / "reader").exists()
