"""The sandbox shell through the runtime, on the fake backend: the A2 failure points.

What must hold
  * one sandbox per agent, reused by its calls, created with the mount plan; an agent attached
    read-only gets `shared` mounted read-only;
  * a submission whose answer is lost is unknown: the agent faults, CONT does not submit again;
  * when the sandbox loses the execution's record (its command daemon restarted), the outcome is
    unknown, with that reason;
  * after the provider restarts (a new process, the same receipts), a rerun reattaches to the
    running command and gets its result, without submitting it again;
  * a sandbox whose create answer was lost is found again by its label, not created twice;
  * the sandbox of an agent that ended is destroyed;
  * the provider is configured through the plugin (`provider: opensandbox`, `backend: fake`).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import ClassVar

from statecraft_opensandbox import FakeBackend, OpenSandboxProvider

from statecraft import Agent, Done, EffectError, state
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.runtime.runtime import Runtime
from statecraft.testing import permissive_runtime


class Runs(Agent):
    start = "go"
    commands: ClassVar[list[str]] = ["echo one >> ran.txt", "echo two >> ran.txt"]

    @state
    async def go(self, ctx, data):
        data["out"] = []
        for n, command in enumerate(Runs.commands):
            result = await ctx.call("sh.exec", call_name=f"c{n}", command=command)
            data["out"].append(result)
        return Done


def make(tmp: Path, backend: FakeBackend, **options) -> Runtime:
    rt = Runtime(
        store=SqliteStore(tmp / "db"), workspace=LocalWorkspace(tmp / "ws"), scan_every=0.01
    )
    provider = OpenSandboxProvider(backend, receipts=tmp / "receipts.db", poll=0.02, **options)
    rt.registrar.providers.register("sh", provider)
    rt.registrar.agents.register("runs", Runs, tools=["sh.*"])
    return rt


def ran(tmp: Path, root: str) -> list[str]:
    log = tmp / "ws" / root / "shared" / "ran.txt"
    return log.read_text().split() if log.exists() else []


async def test_one_sandbox_per_agent_with_its_mounts(tmp_path: Path) -> None:
    Runs.commands = ["echo one >> ran.txt", "echo two >> ran.txt"]
    backend = FakeBackend()
    async with make(tmp_path, backend) as rt:
        root = await rt.spawn("runs", root_id="r")
        await rt.run(root, timeout=10)
        record = await rt.store.load(root)
    assert record.status == "succeeded" and [o["exit"] for o in record.data["out"]] == [0, 0]
    assert ran(tmp_path, "r") == ["one", "two"] and backend.creates == 1
    [box] = backend.sandboxes.values()
    assert [(m["target"], m["mode"]) for m in box["mounts"]] == [
        ("/workspace", "rw"),
        ("/home/agent", "rw"),
    ]


async def test_a_lost_submit_answer_is_never_submitted_again(tmp_path: Path) -> None:
    Runs.commands = ["echo one >> ran.txt"]
    backend = FakeBackend()
    backend.lose_submit_answer = True
    async with make(tmp_path, backend) as rt:
        root = await rt.spawn("runs", root_id="r")
        await rt.run(root, timeout=10)
        record = await rt.get(root)
        assert record["view"] == "suspended" and "submit answer was lost" in record["error"]
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["view"] == "suspended"
    await asyncio.sleep(0.1)
    assert backend.submits == 1 and ran(tmp_path, "r") == ["one"]


async def test_a_lost_record_is_unknown(tmp_path: Path) -> None:
    Runs.commands = ["sleep 0.5; echo one >> ran.txt"]
    backend = FakeBackend()
    async with make(tmp_path, backend) as rt:
        root = await rt.spawn("runs", root_id="r")
        for _ in range(200):
            if backend.submits:
                break
            await asyncio.sleep(0.01)
        [sandbox] = backend.sandboxes
        backend.lose_records(sandbox)
        await rt.run(root, timeout=10)
        record = await rt.get(root)
    assert record["view"] == "suspended" and "lost the execution's record" in record["error"]


async def test_a_restarted_provider_reattaches_to_the_running_command(tmp_path: Path) -> None:
    Runs.commands = ["sleep 1; echo one >> ran.txt; echo done"]
    backend = FakeBackend()
    first = make(tmp_path, backend)
    # the process dies: nobody interrupts the command or marks it cancelled
    provider = first.registry.providers["sh"]
    provider._cancel = _nothing
    await first.start()
    root = await first.spawn("runs", root_id="r")
    for _ in range(200):
        if backend.submits:
            break
        await asyncio.sleep(0.01)
    await first.stop(drain=0)
    await first.close()
    async with make(tmp_path, backend) as second:  # the same receipts, a new provider
        await second.signal(root, "CONT")
        await second.run(root, timeout=10)
        record = await second.store.load(root)
    assert record.status == "succeeded", record.error
    assert record.data["out"][0]["output"].strip() == "done"
    assert backend.submits == 1 and ran(tmp_path, "r") == ["one"]


async def _nothing(key: str) -> None:
    return None


async def test_a_lost_create_answer_is_found_by_its_label(tmp_path: Path) -> None:
    Runs.commands = ["echo one >> ran.txt"]
    backend = FakeBackend()
    backend.lose_create_answer = True
    async with make(tmp_path, backend) as rt:
        root = await rt.spawn("runs", root_id="r")
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["view"] == "succeeded"
    assert backend.creates == 1 and len(backend.sandboxes) == 1


async def test_the_sandbox_of_an_ended_agent_is_destroyed(tmp_path: Path) -> None:
    Runs.commands = ["echo one >> ran.txt"]
    backend = FakeBackend()
    async with make(tmp_path, backend) as rt:
        root = await rt.spawn("runs", root_id="r")
        await rt.run(root, timeout=10)
        assert len(backend.sandboxes) == 1
        await rt.registry.providers["sh"].cleanup()
    assert backend.sandboxes == {}


class Reader(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        try:
            data["out"] = await ctx.call("sh.exec", call_name="ls", command="ls")
        except EffectError as exc:
            data["error"] = exc.message
        return Done


async def test_an_attached_agent_gets_shared_read_only(tmp_path: Path) -> None:
    backend = FakeBackend()
    rt = permissive_runtime({"t:runs": Runs, "t:reader": Reader}, files_root=tmp_path / "ws")
    provider = OpenSandboxProvider(backend, receipts=tmp_path / "receipts.db", poll=0.02)
    rt.registrar.providers.register("sh", provider)
    Runs.commands = ["echo one >> ran.txt"]
    async with rt:
        await rt.spawn("t:runs", {}, root_id="run1")
        await rt.run("run1", timeout=10)
        await rt.spawn("t:reader", {}, root_id="c1", attach="run1")
        await rt.run("c1", timeout=10)
    modes = sorted(
        (m["target"], m["mode"])
        for box in backend.sandboxes.values()
        if box["labels"]["statecraft-agent"] == "c1"
        for m in box["mounts"]
    )
    assert modes == [("/home/agent", "rw"), ("/workspace", "ro")]


async def test_configured_through_the_plugin(tmp_path: Path) -> None:
    config = {
        "store": {"provider": "sqlite", "path": str(tmp_path / "db")},
        "workspace": {"provider": "local", "root": str(tmp_path / "ws")},
        "runtime": {"scan_every": 0.01},
        "plugins": ["opensandbox"],
        "effect_providers": {
            "sh": {
                "provider": "opensandbox",
                "backend": "fake",
                "receipts": str(tmp_path / "receipts.db"),
                "poll": 0.02,
            }
        },
    }
    Runs.commands = ["echo one >> ran.txt"]
    rt = Runtime.from_config(config)
    rt.registrar.agents.register("runs", Runs, tools=["sh.*"])
    async with rt:
        assert rt.registry.effects["sh.exec"].policy.kind == "deduplicated"
        root = await rt.spawn("runs", root_id="r")
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["view"] == "succeeded"
