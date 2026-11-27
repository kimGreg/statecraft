"""Edges found by the independent review of the P0.6 remediation.

What must hold
  * a spawn succeeds when an executor carries the root's `init` intent out first and starts its
    step while the caller is still preparing the workspace;
  * a workspace that cannot be prepared faults the root (with the reason) instead of being
    retried silently; after it is fixed, CONT finishes it;
  * a `CancelledError` raised by a state settles its calls like any fault: calls still running
    are cancelled and recorded with the fault;
  * an HTTP body with NaN or Infinity is 400 on every route;
  * an operator policy applies to registered tools as well as discovered ones, and a policy for
    a tool no provider offers refuses the start;
  * reading a FIFO from a local workspace returns at once (not a file), never blocks the loop;
  * a `repin` message without its fields is undelivered, and later messages are applied;
  * lease timing must be 0 < beat < lease_ttl.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import httpx
import pytest

from statecraft import Agent, Done, Wait, state, tool
from statecraft.kernel.executor import Executor
from statecraft.providers.workspaces import LocalFiles
from statecraft.runtime.config import configure
from statecraft.runtime.registry import RegistryError
from statecraft.server import server_app
from statecraft.testing import memory_runtime


class Slow(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await asyncio.sleep(0.2)
        return Done


async def test_a_spawn_succeeds_when_an_executor_activates_the_root_first() -> None:
    rt = memory_runtime(scan_every=0.01, executors=1)
    rt.registrar.agents.register("slow", Slow)
    original, calls = rt.initialize, []

    async def slow_initialize(record, intent):
        calls.append(1)
        await original(record, intent)
        if len(calls) == 1:  # the caller's preparation is slow; the executor's is not
            await asyncio.sleep(0.2)

    rt.initialize = slow_initialize
    async with rt:
        root = await rt.spawn("slow", root_id="r1")
        await rt.run(root, timeout=5)
        assert (await rt.get(root))["view"] == "succeeded"
        assert all(e.failures == [] for e in rt.executors)


async def test_a_workspace_that_cannot_be_prepared_faults_the_root() -> None:
    rt = memory_runtime(executors=0)
    rt.registrar.agents.register("slow", Slow)
    async with rt:
        definition = rt.registry.agent("slow")
        record = rt.new_record(definition, "r", data_input={})
        await rt.store.create(record.but(intents=[{"id": "r/init", "kind": "init", "done": False}]))
        original = rt.initialize

        async def broken(record, intent):
            raise PermissionError("read-only disk")

        rt.initialize = broken
        executor = Executor(rt)
        await executor.tick()
        view = await rt.get("r")
        assert view["view"] == "suspended" and "read-only disk" in view["error"]
        rt.initialize = original
        await rt.signal("r", "CONT")
        await rt.run("r", timeout=5)
        assert (await rt.get("r"))["view"] == "succeeded"


LOG: list[str] = []


@tool(name="work.slow", policy="idempotent", execution="async")
async def slow_call(seconds: float) -> bool:
    LOG.append("start")
    await asyncio.sleep(seconds)
    LOG.append("finished")
    return True


class CancelsWithACallRunning(Agent):
    start = "go"
    left: asyncio.Future | None = None

    @state
    async def go(self, ctx, data):
        data["call"] = None  # the call is left running on purpose
        CancelsWithACallRunning.left = asyncio.ensure_future(ctx.call("work.slow", seconds=0.3))
        await asyncio.sleep(0.05)
        raise asyncio.CancelledError()


async def test_a_state_that_cancels_itself_settles_its_calls() -> None:
    LOG.clear()
    rt = memory_runtime()
    rt.registrar.tools.register(slow_call)
    rt.registrar.agents.register("c", CancelsWithACallRunning, tools=["work.slow"])
    async with rt:
        root = await rt.spawn("c", root_id="c")
        await rt.run(root, timeout=5)
        record = await rt.get(root)
        fault = [h for h in await rt.history(root) if h["kind"] == "fault"][-1]
        await asyncio.sleep(0.5)
    assert "cancelled from inside" in record["error"]
    assert [c["effect"] for c in fault["crossings"]] == ["work.slow"]
    assert LOG == ["start"]  # cancelled when the fault was settled, never finished


class Asks(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "n", "question": "?"}, then="done")

    @state
    async def done(self, ctx, data):
        return Done


async def test_nan_bodies_are_client_errors() -> None:
    rt = memory_runtime()
    rt.registrar.agents.register("asks", Asks)
    async with rt:
        root = await rt.spawn("asks", root_id="a")
        await rt.run(root, timeout=5)
        question = (await rt.questions())[0]["question_id"]
        transport = httpx.ASGITransport(app=server_app(rt))
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as c:
            headers = {"content-type": "application/json"}
            for url, body in (
                (f"/questions/{question}/answer", '{"answer": NaN}'),
                ("/agents", '{"agent": "asks", "input": {"x": Infinity}}'),
                (f"/agents/{root}/signals", '{"signal": "USR", "message": NaN}'),
            ):
                response = await c.post(url, content=body, headers=headers)
                assert response.status_code == 400, (url, response.text)


@tool(name="m.square", policy="read_only")
def square(x: int) -> int:
    return x * x


async def test_operator_policies_apply_to_registered_tools_and_must_name_one() -> None:
    import sys
    import types

    module = types.ModuleType("review_square_tools")
    module.square = square
    sys.modules["review_square_tools"] = module
    rt = memory_runtime()
    configure(
        rt.registrar,
        {
            "effect_providers": {
                "m": {
                    "provider": "python",
                    "module": "review_square_tools",
                    "policies": {"square": {"timeout": "2s", "max_attempts": 5}},
                }
            }
        },
    )
    async with rt:
        policy = rt.registry.effects["m.square"].policy
    assert (policy.kind, policy.timeout, policy.max_attempts) == ("read_only", 2.0, 5)
    typo = memory_runtime()
    typo.registrar.tools.register(square)
    typo.registrar.tools.policy("m.sqaure", timeout=2.0)
    with pytest.raises(RegistryError, match=r"m\.sqaure"):
        await typo.start()
    await typo.close()


async def test_reading_a_fifo_never_blocks(tmp_path: Path) -> None:
    files = LocalFiles(tmp_path / "ws")
    await files.put("shared/x.txt", b"x")
    os.mkfifo(tmp_path / "ws" / "shared" / "out.json")
    started = time.monotonic()
    with pytest.raises(FileNotFoundError):
        await asyncio.wait_for(files.get("shared/out.json"), 2)
    assert time.monotonic() - started < 1


class Listens(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        if not ctx.signals:
            return Wait(sleep=0.02, then="go")
        data["got"] = list(ctx.signals)
        return Done


async def test_a_malformed_repin_is_undelivered_and_later_messages_arrive() -> None:
    rt = memory_runtime(executors=0)
    rt.registrar.agents.register("listens", Listens)
    async with rt:
        root = await rt.spawn("listens", root_id="l")
        await rt.store.append_inbox(root, "bad-repin", {"kind": "repin", "definition": {}})
        await rt.signal(root, ("USR", "hello"))
        await rt.run(root, timeout=5)
        record = await rt.store.load(root)
    assert record.status == "succeeded" and record.data["got"] == ["hello"]
    assert [u["reason"] for u in record.undelivered] == ["repin without a definition and bindings"]


@pytest.mark.parametrize(("lease_ttl", "beat"), [(0, 0.1), (1, 0), (1, 1), (1, 2)])
def test_lease_timing_must_leave_room_to_renew(lease_ttl, beat) -> None:
    with pytest.raises(ValueError, match="beat"):
        memory_runtime(lease_ttl=lease_ttl, beat=beat)
