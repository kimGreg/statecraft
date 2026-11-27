"""A shell command as a durable effect (the local shell provider through the runtime).

What must hold
  * a command runs in the agent's workspace; a state that runs again (a crash before its
    commit) gets the command's recorded result and does not run it again;
  * a command that outlives its timeout is killed with its whole process group; its outcome is
    unknown (it may have done part of its work): the agent faults, CONT reconciles and does not
    run it again, and only an operator's replay runs it once more;
  * KILL ends the command's processes, children included, and the provider's capacity is
    taken until they are gone.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from typing import ClassVar

from statecraft import Agent, Done, EffectDefinition, EffectError, EffectPolicy, state
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.runtime.runtime import Runtime
from statecraft.testing.local_shell import LocalShell


class Runs(Agent):
    start = "go"
    crash_once: ClassVar[bool] = False

    @state
    async def go(self, ctx, data):
        result = await ctx.call("sh.exec", call_name="build", command=ctx.input["command"])
        if Runs.crash_once:  # the state fails after the command ran: it runs again
            Runs.crash_once = False
            raise RuntimeError("crashed after the command")
        data["result"] = result
        return Done


def make(tmp_path: Path, *, timeout: float = 30, concurrency: int | None = None) -> Runtime:
    rt = Runtime(
        store=SqliteStore(tmp_path / "db"),
        workspace=LocalWorkspace(tmp_path / "ws"),
        scan_every=0.01,
    )
    shell = LocalShell(
        receipts=tmp_path / "receipts", workspaces=tmp_path / "ws", concurrency=concurrency
    )
    rt.registrar.providers.register("sh", shell)
    rt.registrar.tools.register(
        EffectDefinition(
            name="sh.exec",
            provider="sh",
            operation="exec",
            policy=EffectPolicy(kind="deduplicated", timeout=timeout, max_attempts=1),
        )
    )
    rt.registrar.agents.register("runs", Runs, tools=["sh.*"])
    return rt


def counted(tmp_path: Path, root: str) -> int:
    log = tmp_path / "ws" / root / "shared" / "ran.txt"
    return len(log.read_text().split()) if log.exists() else 0


async def test_a_rerun_state_gets_the_recorded_result(tmp_path) -> None:
    Runs.crash_once = True
    async with make(tmp_path) as rt:
        root = await rt.spawn("runs", {"command": "echo x >> ran.txt; echo built"}, root_id="r")
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["view"] == "suspended"  # the state's own fault
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=10)
        record = await rt.store.load(root)
    assert record.status == "succeeded" and record.data["result"]["stdout"] == "built\n"
    assert counted(tmp_path, "r") == 1


async def test_a_timed_out_command_is_killed_and_never_replayed_by_cont(tmp_path) -> None:
    Runs.crash_once = False
    command = "echo x >> ran.txt; sleep 30 & echo $! > child.pid; wait"
    async with make(tmp_path, timeout=0.5) as rt:
        root = await rt.spawn("runs", {"command": command}, root_id="t")
        await rt.run(root, timeout=10)
        record = await rt.get(root)
        assert record["view"] == "suspended" and "indeterminate" in record["error"]
        child = int((tmp_path / "ws" / "t" / "shared" / "child.pid").read_text())
        await asyncio.sleep(0.2)
        assert not _alive(child)  # the command's children went with it
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["view"] == "suspended"
        assert counted(tmp_path, "t") == 1  # reconciled, not run again
        [entry] = await rt.calls(root)
        assert (entry["status"], entry["clear_by"]) == ("indeterminate", "reconcile")


class Killable(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        with contextlib.suppress(EffectError):
            await ctx.call("sh.exec", call_name="long", command=ctx.input["command"])
        return Done


async def test_kill_ends_the_process_tree_before_capacity_returns(tmp_path) -> None:
    rt = make(tmp_path, concurrency=1)
    rt.registrar.agents.register("killable", Killable, tools=["sh.*"])
    command = "sleep 30 & echo $! > child.pid; wait"
    async with rt:
        root = await rt.spawn("killable", {"command": command}, root_id="k")
        pid_file = tmp_path / "ws" / "k" / "shared" / "child.pid"
        for _ in range(300):
            if pid_file.exists() and pid_file.read_text().strip():
                break
            await asyncio.sleep(0.01)
        child = int(pid_file.read_text())
        await rt.signal(root, "KILL")
        slots = rt.effects._capacity["sh"]
        await asyncio.wait_for(slots.acquire(), 5)  # returned only once the group is gone
        assert not _alive(child)
        slots.release()
        assert (await rt.get(root))["view"] == "terminated"


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:  # a zombie is gone for our purposes
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split()[2] != "Z"
    except FileNotFoundError:
        return False
