"""A root attached to another root's workspace: a conversation about a finished run.

What must hold: the attached root's tools and files work in the run's session; it reads the
run's views and shared files; it publishes under its own home; it never rewrites the run's views;
its input does not replace the run's; attaching to a root that does not exist is refused.
"""

from __future__ import annotations

import json

import pytest

from statecraft.runtime.views import StateView
from statecraft.sdk.agent import Agent, Done, Spawn, Wait, state
from statecraft.testing import permissive_runtime


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


class Scribbler(Agent):
    """Tries to write into the workspace it works in."""

    start = "write"

    @state
    async def write(self, ctx, data):
        await ctx.files.write("shared/scribble.txt", "mine")
        return Done


@pytest.fixture
async def engine(tmp_path):
    e = permissive_runtime(
        {"t:run": Run, "t:reader": Reader, "t:asker": Asker, "t:scribbler": Scribbler},
        files_root=tmp_path,
        views=[StateView()],
    )
    await e.start()
    yield e, None, tmp_path
    await e.close()


async def test_an_attached_root_reads_the_run_and_leaves_its_views(engine) -> None:
    e, _tools, folder = engine
    run = await e.spawn("t:run", {"topic": {"name": "x"}}, root_id="run1")
    await e.run(run, scan_every=0.01)
    before = (folder / "run1" / "view" / "state.json").read_text()
    chat = await e.spawn("t:reader", {"message": "hi"}, root_id="chat1", attach="run1")
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
    assert (folder / "run1" / "home" / "chat1" / "input.json").exists()  # its own home
    assert not (folder / "chat1").exists()


async def test_attaching_to_nothing_is_refused(engine) -> None:
    e, _tools, _folder = engine
    with pytest.raises(ValueError):
        await e.spawn("t:reader", {}, attach="nowhere")


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
    await e.spawn("t:run", {"topic": {"name": "x"}}, root_id="run1")
    await e.run("run1", scan_every=0.01)
    await e.spawn("t:asker", {"run": "run1"}, root_id="conv1")
    table = await e.run("conv1", scan_every=0.01)
    assert {r["view"] for r in table} == {"succeeded"}, table
    heard = (await e.artifact("conv1", "heard"))["payload"]
    assert heard == {"agents": ["run"], "note": "the run's note"}
    assert (folder / "run1" / "home" / "conv1" / "reader" / "objects").is_dir()
    assert not (folder / "conv1" / "home" / "conv1" / "reader").exists()


async def test_an_attached_root_cannot_write_the_runs_shared_files(engine) -> None:
    e, _tools, folder = engine
    await e.spawn("t:run", {}, root_id="run1")
    await e.run("run1", scan_every=0.01)
    await e.spawn("t:scribbler", {}, root_id="pen1", attach="run1")
    [row] = await e.run("pen1", scan_every=0.01)
    assert row["view"] == "suspended" and "read-only" in row["error"]
    assert not (folder / "run1" / "shared" / "scribble.txt").exists()


async def test_writing_is_a_separately_granted_mode(engine) -> None:
    e, _tools, folder = engine
    await e.spawn("t:run", {}, root_id="run1")
    await e.run("run1", scan_every=0.01)
    await e.spawn("t:scribbler", {}, root_id="pen2", attach="run1", attach_mode="rw")
    [row] = await e.run("pen2", scan_every=0.01)
    assert row["view"] == "succeeded"
    assert (folder / "run1" / "shared" / "scribble.txt").read_text() == "mine"


class Attacher(Agent):
    """Spawns `writer` attached to `run` with `mode`; publishes how its child ended."""

    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        target = {"root": ctx.input["run"], "mode": ctx.input.get("mode", "ro")}
        return Spawn({"w": ("t:scribbler", {})}, then="wait", attach={"w": target})

    @state
    async def wait(self, ctx, data):
        return Wait(join=["w"], then="done", on_fault="done")

    @state
    async def done(self, ctx, data):
        ctx.signal("w", "TERM")
        await ctx.write("ended", {"child": ctx.woke.get("children", {}).get("w")})
        return Done


def attach_runtime(tmp_path, grant: str):
    rt = permissive_runtime({"t:run": Run, "t:scribbler": Scribbler}, files_root=tmp_path)
    rt.registrar.agents.register(
        "t:attacher", Attacher, spawns=["*"], writes=["ended"], attach=grant
    )
    return rt


async def test_an_attached_child_is_read_only_unless_granted_rw(tmp_path) -> None:
    async with attach_runtime(tmp_path, "rw") as rt:
        await rt.spawn("t:run", {}, root_id="run1")
        await rt.run("run1", scan_every=0.01)
        await rt.spawn("t:attacher", {"run": "run1"}, root_id="a1")  # ro by default
        await rt.run("a1", scan_every=0.01)
        assert (await rt.artifact("a1", "ended"))["payload"] == {"child": "active"}  # faulted
        assert not (tmp_path / "run1" / "shared" / "scribble.txt").exists()
        await rt.spawn("t:attacher", {"run": "run1", "mode": "rw"}, root_id="a2")
        await rt.run("a2", scan_every=0.01)
        assert (await rt.get("a2/w"))["view"] == "succeeded"
    assert (tmp_path / "run1" / "shared" / "scribble.txt").read_text() == "mine"


@pytest.mark.parametrize(
    ("grant", "mode", "run", "error"),
    [
        ("ro", "rw", "run1", "may not attach w rw"),
        ("none", "ro", "run1", "may not attach w ro"),
        ("rw", "ro", "future", "no root 'future'"),
    ],
)
async def test_attachments_need_a_grant_and_an_existing_root(tmp_path, grant, mode, run, error):
    async with attach_runtime(tmp_path, grant) as rt:
        await rt.spawn("t:run", {}, root_id="run1")
        await rt.run("run1", scan_every=0.01)
        await rt.spawn("t:attacher", {"run": run, "mode": mode}, root_id="a")
        await rt.run("a", scan_every=0.01)
        record = await rt.get("a")
        assert record["view"] == "suspended" and error in record["error"]
        assert record["children"] == {}  # nothing was spawned
    assert not (tmp_path / "future").exists()


async def test_attaching_to_an_attached_root_works_in_its_run(tmp_path) -> None:
    rt = permissive_runtime({"t:run": Run, "t:reader": Reader}, files_root=tmp_path)
    async with rt:
        await rt.spawn("t:run", {"topic": {"name": "x"}}, root_id="run1")
        await rt.run("run1", scan_every=0.01)
        await rt.spawn("t:reader", {}, root_id="chat1", attach="run1")
        await rt.spawn("t:reader", {}, root_id="chat2", attach="chat1")
        assert rt.workspaces.session_of("chat2") == ("run1", "ro")
