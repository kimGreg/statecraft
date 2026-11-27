"""Creating a root: refused before anything is written, recoverable after a crash.

What must hold
  * a root whose id exists is refused, and the live run keeps its input file and its session
    (nothing is written before the record is created);
  * a root id is one clean name and not a reserved folder name (`skills`, `private`, `home`,
    `shared`, ...): a reserved id would read as a folder in another agent's paths;
  * a root's workspace is prepared through its `init` intent: when the runtime dies after the
    record was created and before the input was written, the executor finishes it and the root
    runs as if nothing happened.
"""

from __future__ import annotations

import json

import pytest

from statecraft import Agent, Done, state
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.runtime.runtime import Runtime
from statecraft.testing import memory_runtime


class Echo(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("echo", {"input": ctx.input})
        return Done


def make(**options) -> Runtime:
    rt = memory_runtime(**options) if "store" not in options else Runtime(**options)
    rt.registrar.agents.register("echo", Echo)
    return rt


async def test_a_duplicate_root_leaves_the_live_run_untouched() -> None:
    async with make() as rt:
        await rt.spawn("echo", {"q": "original"}, root_id="r1")
        await rt.spawn("echo", {}, root_id="donor")
        with pytest.raises(ValueError, match="exists"):
            await rt.spawn("echo", {"q": "INTRUDER"}, root_id="r1", attach="donor")
        assert json.loads(await rt.read_file("r1", "shared/input/input.json")) == {"q": "original"}
        assert rt.workspaces.session_of("r1") == ("r1", "own")
        await rt.run("r1", timeout=10)
        assert (await rt.artifact("r1", "echo"))["payload"] == {"input": {"q": "original"}}


@pytest.mark.parametrize(
    "root_id",
    ["skills", "private", "objects", "staging", "home", "shared", ".hidden", "a b", "x/y"],
)
async def test_reserved_or_unclean_root_ids_are_refused(root_id) -> None:
    async with make() as rt:
        with pytest.raises(ValueError):
            await rt.spawn("echo", {}, root_id=root_id)


async def test_a_crash_between_the_record_and_the_workspace_is_finished_later(tmp_path) -> None:
    def disk() -> Runtime:
        return make(
            store=SqliteStore(tmp_path / "db"),
            workspace=LocalWorkspace(tmp_path / "ws"),
            executors=0,
            scan_every=0.01,
        )

    async with disk() as first:  # the record is created; the process dies before `initialize`
        definition = first.registry.agent("echo")
        record = first.new_record(definition, "r", data_input={"q": 1})
        await first.store.create(
            record.but(intents=[{"id": "r/init", "kind": "init", "done": False}])
        )
    assert not (tmp_path / "ws" / "r" / "shared" / "input" / "input.json").exists()
    async with disk() as second:
        assert (await second.get("r"))["view"] == "pending"  # not runnable before its workspace
        await second.run("r", timeout=10)
        assert (await second.get("r"))["view"] == "succeeded"
        assert json.loads(await second.read_file("r", "shared/input/input.json")) == {"q": 1}
