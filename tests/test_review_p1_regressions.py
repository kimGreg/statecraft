"""Edges found by the independent review of B1 and A1.

What must hold
  * a keyed call the state itself cancels after it was sent (its own timeout) is unknown: the
    step is blocked and faults, and a new identity cannot run in its place;
  * KILL while a rerun waits for a reattached command ends the command, and the capacity it
    held returns only after it ended;
  * an operator's replay runs the command again (a new generation of the key at the provider);
  * a repin (another policy) keeps the call's identity: CONT settles it;
  * when journaling an unknown outcome fails, the step is still blocked;
  * `resolve_call` is refused while the agent runs a step, and never overwrites a settled call;
  * a receipt's process group from another boot is not taken for the command; a reconcile while
    a command is being started reports it running;
  * a tool argument named `call_name` reaches the tool (`ctx.invoke`).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from typing import Any, ClassVar

import pytest

from statecraft import Agent, Done, EffectDefinition, EffectError, EffectPolicy, state
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.runtime.runtime import Runtime
from statecraft.testing import memory_runtime
from statecraft.testing.local_shell import LocalShell


def shell_runtime(tmp: Path, shell: LocalShell, agent: type, *, timeout: float = 60) -> Runtime:
    rt = Runtime(
        store=SqliteStore(tmp / "db"), workspace=LocalWorkspace(tmp / "ws"), scan_every=0.01
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
    rt.registrar.agents.register("a", agent, tools=["sh.*"])
    return rt


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    with contextlib.suppress(FileNotFoundError):
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    return False


class Escapes(Agent):
    """Gives a long command 0.2 s, then tries the same effect under a new name."""

    start = "go"

    @state
    async def go(self, ctx, data):
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                ctx.call("sh.exec", call_name="first", command="echo first >> ran.txt; sleep 2"),
                0.2,
            )
        with contextlib.suppress(EffectError):
            await ctx.call("sh.exec", call_name="second", command="echo second >> ran.txt")
        return Done


async def test_a_call_the_state_cancels_after_sending_blocks_the_step(tmp_path: Path) -> None:
    shell = LocalShell(receipts=tmp_path / "r", workspaces=tmp_path / "ws")
    async with shell_runtime(tmp_path, shell, Escapes) as rt:
        root = await rt.spawn("a", root_id="r")
        await rt.run(root, timeout=10)
        record = await rt.get(root)
        calls = {c["key"].rsplit("/", 1)[1]: c["status"] for c in await rt.calls(root)}
    assert record["view"] == "suspended" and "unknown" in record["error"]
    assert calls == {"first": "indeterminate"}
    ran = (tmp_path / "ws" / "r" / "shared" / "ran.txt").read_text().split()
    assert ran == ["first"]


class Long(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        command = "echo $$ > pid.txt; sleep 30"
        data["r"] = await ctx.call("sh.exec", call_name="long", command=command)
        return Done


async def test_kill_while_reattached_ends_the_command_before_capacity_returns(
    tmp_path: Path,
) -> None:
    crashed = LocalShell(receipts=tmp_path / "r", workspaces=tmp_path / "ws", concurrency=1)
    crashed._cancel = lambda folder: None  # the process dies: nobody kills the command
    first = shell_runtime(tmp_path, crashed, Long)
    await first.start()
    root = await first.spawn("a", root_id="k")
    pid_file = tmp_path / "ws" / "k" / "shared" / "pid.txt"
    for _ in range(300):
        if pid_file.exists() and pid_file.read_text().strip():
            break
        await asyncio.sleep(0.01)
    pid = int(pid_file.read_text())
    await first.stop(drain=0)
    await first.close()
    assert alive(pid)
    shell = LocalShell(receipts=tmp_path / "r", workspaces=tmp_path / "ws", concurrency=1)
    second = shell_runtime(tmp_path, shell, Long)
    async with second:
        for _ in range(300):  # the rerun reattaches and holds the slot
            slots = second.effects._capacity.get("sh")
            if slots is not None and slots.locked():
                break
            await asyncio.sleep(0.01)
        await second.signal(root, "KILL")
        await asyncio.wait_for(second.effects._capacity["sh"].acquire(), 5)
        assert not alive(pid)


class Replays(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        data["r"] = await ctx.call(
            "sh.exec", call_name="once", command="echo run >> ran.txt; sleep $(cat delay)"
        )
        return Done


async def test_an_operator_replay_runs_the_command_again(tmp_path: Path) -> None:
    shared = tmp_path / "ws" / "p" / "shared"
    shared.mkdir(parents=True)
    (shared / "delay").write_text("5")
    shell = LocalShell(receipts=tmp_path / "r", workspaces=tmp_path / "ws")
    async with shell_runtime(tmp_path, shell, Replays, timeout=0.5) as rt:
        root = await rt.spawn("a", root_id="p")
        await rt.run(root, timeout=10)
        [entry] = await rt.calls(root)
        assert entry["status"] == "indeterminate"
        (shared / "delay").write_text("0")
        await rt.signal(root, "CONT")  # reconciled: cancelled midway, still unknown
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["view"] == "suspended"
        await rt.resolve_call(entry["key"], "replay")
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=10)
        record = await rt.store.load(root)
    assert record.status == "succeeded"
    assert (shared / "ran.txt").read_text().split() == ["run", "run"]


class Ledger:
    capabilities: ClassVar[frozenset[str]] = frozenset({"deduplicated"})

    def __init__(self) -> None:
        self.applied: dict[str, Any] = {}
        self.unknown = False
        self.lose = False  # apply, then lose the answer (a transport error after sending)

    async def invoke(self, operation, arguments, context):
        if self.unknown:
            raise EffectError("indeterminate", "lost track")
        self.applied.setdefault(context.key, arguments["amount"])
        if self.lose:
            raise EffectError("transient", "connection reset after the request was sent")
        return {"charged": self.applied[context.key]}


class Pays(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        with contextlib.suppress(EffectError):
            await ctx.call("pay.charge", call_name="charge", amount=10)
        with contextlib.suppress(EffectError):
            await ctx.call("pay.charge", call_name="again", amount=10)
        return Done


def ledger_runtime(ledger: Ledger, timeout: float = 5, **options) -> Runtime:
    rt = options.pop("rt", None) or memory_runtime(**options)
    rt.registrar.providers.register("pay", ledger)
    rt.registrar.tools.register(
        EffectDefinition(
            name="pay.charge",
            provider="pay",
            operation="charge",
            policy=EffectPolicy(kind="deduplicated", timeout=timeout),
        )
    )
    rt.registrar.agents.register("pays", Pays, tools=["pay.*"])
    return rt


async def test_a_failed_journal_write_still_blocks_the_step() -> None:
    ledger = Ledger()
    ledger.unknown = True
    rt = ledger_runtime(ledger)
    async with rt:
        original = rt.store.end_call

        async def failing(key, status, **detail):
            if status == "indeterminate":
                raise RuntimeError("the store is down")
            return await original(key, status, **detail)

        rt.store.end_call = failing
        root = await rt.spawn("pays", root_id="c")
        await rt.run(root, timeout=5)
        assert (await rt.get(root))["view"] == "suspended"
    assert ledger.applied == {}


async def test_a_repin_keeps_the_calls_identity(tmp_path: Path) -> None:
    def disk(timeout: float) -> Runtime:
        rt = Runtime(
            store=SqliteStore(tmp_path / "db"),
            workspace=LocalWorkspace(tmp_path / "ws"),
            executors=0,
            scan_every=0.01,
        )
        return ledger_runtime(ledger, timeout=timeout, rt=rt)

    ledger = Ledger()
    ledger.lose = True
    async with disk(5) as first:
        root = await first.spawn("pays", root_id="c")
        await first.run(root, timeout=5)
        assert (await first.calls(root))[0]["clear_by"] == "resend"
    ledger.lose = False
    async with disk(9) as second:  # restarted with another policy
        await second.repin(root)
        await second.signal(root, "CONT")
        await second.run(root, timeout=5)
        assert (await second.get(root))["view"] == "succeeded"
    assert len(ledger.applied) == 2  # the charge, settled under its key; then the second call


async def test_resolve_call_waits_for_the_step_and_never_overwrites() -> None:
    ledger = Ledger()
    ledger.unknown = True
    rt = ledger_runtime(ledger, executors=0)
    async with rt:
        root = await rt.spawn("pays", root_id="c")
        await rt.run(root, timeout=5)
        [entry] = await rt.calls(root)
        lease = await rt.store.try_acquire(root, "someone", 30, rt.clock())
        with pytest.raises(ValueError, match="running a step"):
            await rt.resolve_call(entry["key"], "failed")
        await rt.store.release(root, lease)
        await rt.resolve_call(entry["key"], "failed")
        with pytest.raises(ValueError):
            await rt.resolve_call(entry["key"], "replay")


async def test_a_process_group_of_another_boot_is_not_ours(tmp_path: Path) -> None:
    shell = LocalShell(receipts=tmp_path / "r", workspaces=tmp_path / "ws")
    folder = shell._folder("k")
    folder.mkdir()
    (folder / "started").write_text("")
    (folder / "pgid").write_text(f"{os.getpid()} 1 another-boot")  # this process's id
    assert shell._pgid(folder) is None and shell._state(folder) == "unknown"
    shell._kill(folder)  # kills nothing: we are still here
    starting = shell._folder("s")
    starting.mkdir()
    (starting / "started").write_text("")  # claimed, its process not recorded yet
    found = await shell.reconcile("exec", "s", None)
    assert found.status == "running"
    found.work.cancel()


class Named(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        data["r"] = await ctx.invoke("t.echo", {"call_name": "x", "idempotency_key": "y"})
        return Done


async def test_tool_arguments_may_be_named_like_identities() -> None:
    class Echo:
        async def invoke(self, operation, arguments, context):
            return arguments

    rt = memory_runtime()
    rt.registrar.providers.register("t", Echo())
    rt.registrar.tools.register(
        EffectDefinition(name="t.echo", provider="t", operation="echo", policy="read_only")
    )
    rt.registrar.agents.register("named", Named, tools=["t.*"])
    async with rt:
        root = await rt.spawn("named", root_id="n")
        await rt.run(root, timeout=5)
        assert (await rt.store.load(root)).data["r"] == {"call_name": "x", "idempotency_key": "y"}
