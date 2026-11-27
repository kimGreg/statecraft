"""Storage upkeep through the runtime (roadmap C2): collect, compact, forget.

What must hold
  * an object written by an attempt that never committed is collected once older than the grace
    period; an object a commit named stays, even after a later commit published another version
    (retained history names it), and so does one a fork still reads;
  * staging of a committed step goes; the current step's staging stays while the agent may still
    run it (an attempt of a step that has not committed), and an ended agent's staging goes;
  * an agent whose step is running is skipped, never swept; a dry run removes nothing;
  * forgetting an ended run with `files` removes its store rows and its workspace; a run still at
    work or still read by another is refused and keeps its files;
  * `statecraft store collect|compact|forget` are dry unless `--apply`.
"""

from __future__ import annotations

import pytest

from statecraft import Agent, Done, Wait, state
from statecraft.cli.main import main as cli
from statecraft.kernel.store import Busy
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.runtime.upkeep import OWNER
from statecraft.testing import memory_runtime


class Writer(Agent):
    """Publishes `note` twice (two versions), leaves staging behind, then waits for an answer."""

    start = "first"

    @state
    async def first(self, ctx, data):
        await ctx.files.write(f"{ctx.staging}/scratch.txt", "step 0")
        await ctx.write("note", {"v": 1})
        return "second"

    @state
    async def second(self, ctx, data):
        await ctx.files.write(f"{ctx.staging}/scratch.txt", "step 1")
        await ctx.write("note", {"v": 2})
        return Wait(answer={"name": "go", "question": "?"}, then="last")

    @state
    async def last(self, ctx, data):
        return Done


def runtime(**options):
    rt = memory_runtime(**options)
    rt.registrar.agents.register("writer", Writer, writes=["note"])
    return rt


async def files_of(rt, agent_id: str):
    return await rt.workspaces.files_of(agent_id)


async def test_collect_removes_only_what_no_commit_names() -> None:
    async with runtime() as rt:
        await rt.spawn("writer", root_id="w")
        await rt.run("w", timeout=5)
        files = await files_of(rt, "w")
        orphan = "home/w/objects/" + "0" * 64 + ".json"
        await files.create(orphan, b"{}")  # an attempt that wrote and never committed
        objects = await files.list("home/w/objects/")
        staging = await files.list("home/w/staging/")
        assert len(objects) == 3 and len(staging) == 2

        young = await rt.collect(dry_run=False, grace=3600)
        assert young["objects"] == []  # inside the grace period (staging has none)
        # both steps committed (it waits at step 2, whose attempt has not started)
        assert sorted(p.split("/")[3] for p in young["staging"]) == ["0", "1"]
        dry = await rt.collect(dry_run=True, grace=0)
        assert dry["objects"] == [orphan]
        assert await files.list("home/w/objects/") == objects  # dry: nothing removed

        done = await rt.collect(dry_run=False, grace=0)
        assert done["objects"] == [orphan]
        assert sorted(await files.list("home/w/objects/")) == sorted(set(objects) - {orphan})
        assert (await rt.artifact("w", "note"))["payload"] == {"v": 2}  # still readable

        [question] = await rt.questions("w")
        await rt.answer(question["question_id"], True)
        await rt.run("w", timeout=5)
        assert await files.list("home/w/staging/") == []


async def test_a_running_agent_is_skipped() -> None:
    async with runtime() as rt:
        await rt.spawn("writer", root_id="r")
        await rt.run("r", timeout=5)
        files = await files_of(rt, "r")
        orphan = "home/r/objects/" + "1" * 64 + ".json"
        await files.create(orphan, b"{}")
        lease = await rt.store.try_acquire("r", "someone", 60, rt.clock())
        assert lease is not None
        report = await rt.collect(dry_run=False, grace=0)
        assert {"agent": "r", "why": "a step is running"} in report["skipped"]
        assert await files.exists(orphan)
        await rt.store.release("r", lease)
        assert (await rt.collect(dry_run=False, grace=0))["objects"] == [orphan]
        assert await rt.store.lease("r") is None  # collection let go of its lease
        assert OWNER == "statecraft-upkeep"


async def test_forget_removes_an_ended_run_and_its_files(tmp_path) -> None:
    async with runtime(workspace=LocalWorkspace(tmp_path / "ws")) as rt:
        await rt.spawn("writer", root_id="done")
        await rt.run("done", timeout=5)
        await rt.spawn("writer", root_id="busy")
        await rt.run("busy", timeout=5)
        with pytest.raises(Busy):
            await rt.forget("busy", files=True)
        assert list((tmp_path / "ws" / "busy").rglob("*.json"))  # refused: files kept
        await rt.signal("done", "TERM")
        await rt.run("done", timeout=5)
        dry = await rt.forget("done", files=True, dry_run=True)
        assert dry["agents"] == 1 and dry["files"] > 0
        assert await rt.store.find("done") is not None
        await rt.forget("done", files=True)
        assert await rt.store.find("done") is None
        assert not [p for p in (tmp_path / "ws" / "done").rglob("*") if p.is_file()]
        assert await rt.store.find("busy") is not None


async def test_a_fork_keeps_what_it_reads(tmp_path) -> None:
    async with runtime() as rt:
        await rt.spawn("writer", root_id="base")
        await rt.run("base", timeout=5)
        await rt.signal("base", "TERM")
        await rt.run("base", timeout=5)
        await rt.spawn("writer", root_id="fork", inherit={"root": "base"})
        with pytest.raises(Busy, match="fork"):
            await rt.forget("base")
        await rt.collect(dry_run=False, grace=0)
        assert (await rt.store.load("base")).published["note"]["path"] in await (
            await files_of(rt, "base")
        ).list("home/base/objects/")


def test_the_cli_upkeep_is_dry_unless_applied(tmp_path, capsys) -> None:
    config = tmp_path / "statecraft.yaml"
    config.write_text(
        f"store: {{provider: sqlite, path: '{tmp_path}/state.db'}}\n"
        f"workspace: {{provider: local, root: '{tmp_path}/ws'}}\n"
    )
    assert cli(["store", "collect", "--config", str(config)]) == 0
    assert '"dry_run": true' in capsys.readouterr().out
    assert cli(["store", "collect", "--apply", "--grace", "0", "--config", str(config)]) == 0
    assert '"dry_run": false' in capsys.readouterr().out
    assert cli(["store", "compact", "nope", "--config", str(config)]) == 1
    with pytest.raises(SystemExit):
        cli(["store", "forget", "--config", str(config)])  # needs ROOT


async def test_the_current_steps_staging_stays_until_it_commits() -> None:
    async with runtime() as rt:
        await rt.spawn("writer", root_id="s")
        await rt.run("s", timeout=5)
        record = await rt.store.load("s")
        files = await files_of(rt, "s")
        current = f"{record.staging(9)}/left.txt"  # an attempt of the step it waits at
        await files.put(current, b"x")
        await rt.collect(dry_run=False, grace=0)
        assert await files.exists(current)
        await rt.signal("s", "TERM")
        await rt.run("s", timeout=5)
        await rt.collect(dry_run=False, grace=0)
        assert not await files.exists(current)  # it ended
