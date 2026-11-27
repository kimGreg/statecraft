"""Races between executors, provider instances and the sandbox (the review of A4).

What must hold
  * a failure before anything was submitted leaves the call free to be sent again (its receipt
    goes back to `reserved`), never `indeterminate`;
  * a slow start in one executor is not given up by another while the starter is alive (it beats);
    a starter that stopped beating is unknown, and a starter that finds its start given up
    interrupts what it submitted;
  * two provider instances on one receipts file make one sandbox per scope;
  * a sandbox that is gone while its command runs makes the outcome unknown at once;
  * a cancel never overwrites a call that settled;
  * `destroy_on_cancel` needs `scope: call`; a call-scope sandbox goes when its call settled;
  * a changed mount plan gets another sandbox.
"""

from __future__ import annotations

import asyncio
import types
from pathlib import Path

import pytest
from statecraft_opensandbox import FakeBackend, OpenSandboxProvider

from statecraft import EffectError


class View:
    def __init__(self, root: Path) -> None:
        self.root = root

    def host_path(self, workspace: str, relative: str) -> str:
        path = self.root / workspace / relative
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    async def ended(self, agent_ids):
        return set()


def ctx(key: str, mode: str = "rw") -> types.SimpleNamespace:
    return types.SimpleNamespace(
        key=key,
        agent="a",
        root="r",
        workspace="w",
        work=[],
        scope={"mounts": [{"source": "shared", "target": "/workspace", "mode": mode}]},
    )


def provider(tmp: Path, backend: FakeBackend, **options) -> OpenSandboxProvider:
    made = OpenSandboxProvider(backend, receipts=tmp / "receipts.db", poll=0.02, **options)
    made.bind(View(tmp / "ws"))
    return made


async def test_a_failure_before_submitting_leaves_the_call_free(tmp_path: Path) -> None:
    backend = FakeBackend()
    shell = provider(tmp_path, backend)
    create = backend.create

    async def broken(labels, mounts):
        raise RuntimeError("503 from the lifecycle server")

    backend.create = broken
    with pytest.raises(EffectError) as raised:
        await shell.invoke("exec", {"command": "echo hi"}, ctx("k"))
    assert raised.value.cls == "unavailable" and raised.value.sent is False
    assert (await shell.receipts.get("k"))["state"] == "reserved"
    backend.create = create
    result = await shell.invoke("exec", {"command": "echo hi"}, ctx("k"))
    assert result["output"] == "hi\n" and backend.submits == 1


async def test_a_slow_start_is_not_given_up_while_its_starter_beats(tmp_path: Path) -> None:
    backend = FakeBackend()
    first = provider(tmp_path, backend, start_grace=0.3)
    second = provider(tmp_path, backend, start_grace=0.3)
    create = backend.create

    async def slow(labels, mounts):
        await asyncio.sleep(1.0)
        return await create(labels, mounts)

    backend.create = slow
    starting = asyncio.ensure_future(
        first.invoke("exec", {"command": "sleep 0.2; echo ran"}, ctx("k"))
    )
    await asyncio.sleep(0.1)
    follower = ctx("k")
    again = await second.invoke("exec", {"command": "sleep 0.2; echo ran"}, follower)
    assert again["output"] == "ran\n"  # it waited for the start, then followed the command
    assert (await starting)["output"] == "ran\n" and backend.submits == 1


async def test_a_starter_that_was_given_up_interrupts_what_it_submitted(tmp_path: Path) -> None:
    backend = FakeBackend()
    shell = provider(tmp_path, backend)
    submit = backend.submit

    async def late(sandbox, command):  # the start is given up while the submit is out
        await shell.receipts.transition("k", ("starting",), state="cancelled", reason="given up")
        return await submit(sandbox, command)

    backend.submit = late
    with pytest.raises(EffectError) as raised:
        await shell.invoke("exec", {"command": "sleep 5; echo late >> ran.txt"}, ctx("k"))
    assert raised.value.cls == "indeterminate"
    await asyncio.sleep(0.2)
    [box] = backend.sandboxes.values()
    assert all(run["proc"].returncode is not None for run in box["runs"].values())


async def test_two_provider_instances_make_one_sandbox_per_scope(tmp_path: Path) -> None:
    backend = FakeBackend()
    first, second = provider(tmp_path, backend), provider(tmp_path, backend)
    create = backend.create

    async def slow(labels, mounts):
        await asyncio.sleep(0.2)
        return await create(labels, mounts)

    backend.create = slow
    results = await asyncio.gather(
        first.invoke("exec", {"command": "echo 1"}, ctx("k1")),
        second.invoke("exec", {"command": "echo 2"}, ctx("k2")),
    )
    assert [r["output"] for r in results] == ["1\n", "2\n"]
    assert backend.creates == 1 and len(backend.sandboxes) == 1


async def test_a_sandbox_gone_mid_command_is_unknown_at_once(tmp_path: Path) -> None:
    backend = FakeBackend()
    shell = provider(tmp_path, backend)
    call = asyncio.ensure_future(shell.invoke("exec", {"command": "sleep 30"}, ctx("k")))
    for _ in range(200):
        if backend.submits:
            break
        await asyncio.sleep(0.01)
    [sandbox] = backend.sandboxes
    await backend.delete(sandbox)
    with pytest.raises(EffectError) as raised:
        await asyncio.wait_for(call, 5)
    assert raised.value.cls == "indeterminate" and "gone" in raised.value.message


async def test_a_cancel_never_overwrites_a_settled_call(tmp_path: Path) -> None:
    backend = FakeBackend()
    shell = provider(tmp_path, backend)
    result = await shell.invoke("exec", {"command": "echo x"}, ctx("k"))
    await shell._cancel("k")
    found = await shell.reconcile("exec", "k", None)
    assert found.status == "completed" and found.result == result


async def test_destroy_on_cancel_needs_a_call_scope_and_call_sandboxes_go(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="scope: call"):
        provider(tmp_path, FakeBackend(), destroy_on_cancel=True)
    backend = FakeBackend()
    shell = provider(tmp_path, backend, scope="call", destroy_on_cancel=True)
    await shell.invoke("exec", {"command": "echo x"}, ctx("k"))
    assert backend.sandboxes == {} and await shell.receipts.sandboxes() == []


async def test_a_changed_mount_plan_gets_another_sandbox(tmp_path: Path) -> None:
    backend = FakeBackend()
    shell = provider(tmp_path, backend)
    await shell.invoke("exec", {"command": "echo 1"}, ctx("k1", mode="rw"))
    await shell.invoke("exec", {"command": "echo 2"}, ctx("k2", mode="ro"))
    modes = sorted(box["mounts"][0]["mode"] for box in backend.sandboxes.values())
    assert modes == ["ro", "rw"]
