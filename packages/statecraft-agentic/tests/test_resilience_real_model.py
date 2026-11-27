"""Failures on a real model: tools that fail or hang, a model that errors, stops, crashes,
children that fault or hang. The runtime recovers, or hands the failure to the model to work
around, and the answer is still right.

What must hold
  * transient tool errors are retried by the runtime (the model never sees them);
  * a tool that hangs once times out and is retried; one that always hangs or always fails comes
    back to the model as an error, and the model uses the backup tool;
  * a model call that fails transiently is retried; one that fails hard faults the agent; CONT
    resumes it in the same state, and no tool call that was committed runs again;
  * STOP in the middle of a run stops the agent at its next step boundary; CONT continues it;
  * a runtime stopped mid-run (its step cancelled) and a process killed inside a tool call both
    resume from the SQLite store in a new runtime / process; at most the interrupted call runs
    twice;
  * a child whose model fails faults; its lead's join wakes with the fault, the lead does that
    part itself and ends the child; a child that hangs is killed by an operator and its lead
    recovers the same way.

Runs only with `STATECRAFT_REAL_MODEL=1` and an OpenRouter key (`STATECRAFT_MODEL` picks the
model, default openai/gpt-oss-20b). Each scenario makes a few cheap model calls.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from statecraft_agentic import openrouter
from statecraft_agentic.models import OPENROUTER_KEY_ENVS

from statecraft import EffectError, Runtime
from statecraft.testing import memory_runtime

pytestmark = pytest.mark.skipif(
    os.environ.get("STATECRAFT_REAL_MODEL") != "1"
    or not any(os.environ.get(k) for k in OPENROUTER_KEY_ENVS),
    reason="a real-model run: set STATECRAFT_REAL_MODEL=1 and an OpenRouter key",
)

KIT_FILE = Path(__file__).with_name("resilience_kit.py")
_spec = importlib.util.spec_from_file_location("resilience_kit", KIT_FILE)
kit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(kit)


def calculator(model: Any = None, *, spawns: bool = False, **options: Any) -> Runtime:
    return kit.build(memory_runtime(**options), model, spawns=spawns)


async def crossings(runtime: Runtime, root: str) -> list[dict[str, Any]]:
    """Every committed call of the tree, in order."""
    found = []
    for row in await runtime.tree(root):
        for entry in await runtime.history(row["id"]):
            found += [{**c, "agent": row["id"]} for c in entry.get("crossings", [])]
    return found


def calc_calls(found: list[dict[str, Any]], ok: bool | None = None) -> list[dict[str, Any]]:
    return [c for c in found if c["effect"].startswith("calc.") and (ok is None or c["ok"] == ok)]


async def total_of(runtime: Runtime, root: str) -> Any:
    return (await runtime.artifact(root, "answer", by=root))["payload"]["total"]


async def children_by_task(runtime: Runtime, root: str) -> dict[str, list[str]]:
    """The views of the lead's children by the numbers of their task (the model names and words
    them as it likes)."""
    found: dict[str, list[str]] = {}
    for row in await runtime.tree(root):
        if row["parent"] == root:
            task = " and ".join(sorted(numbers_in((await runtime.get(row["id"]))["input"])))
            found.setdefault(task, []).append(row["view"])
    return found


async def until(check, timeout: float = 120, every: float = 0.2) -> None:
    deadline = time.monotonic() + timeout
    while not await check():
        if time.monotonic() > deadline:
            raise TimeoutError("the condition did not hold in time")
        await asyncio.sleep(every)


# ---- tools ------------------------------------------------------------------------------------


async def test_transient_tool_errors_are_retried_by_the_runtime() -> None:
    kit.reset(transient=2)
    async with calculator() as runtime:
        root = await runtime.spawn("calc", {"task": kit.TASK}, root_id="t")
        await runtime.run(root, timeout=300)
        assert await total_of(runtime, root) == kit.TOTAL
        first = calc_calls(await crossings(runtime, root))[0]
        messages = (await runtime.get(root))["data"]["messages"]
    assert first["ok"] and first["attempts"] == 3
    assert not any("unavailable" in str(m.get("content")) for m in messages)


async def test_a_tool_that_hangs_once_times_out_and_is_retried() -> None:
    kit.reset(hang=1)
    async with calculator() as runtime:
        root = await runtime.spawn("calc", {"task": kit.TASK}, root_id="h")
        started = time.monotonic()
        await runtime.run(root, timeout=300)
        assert await total_of(runtime, root) == kit.TOTAL
        first = calc_calls(await crossings(runtime, root))[0]
    assert first["ok"] and first["attempts"] == 2
    assert time.monotonic() - started < 120  # not the 30 s hang


@pytest.mark.parametrize("plan", [{"fail": True}, {"hang_always": True}], ids=["fails", "hangs"])
async def test_a_broken_tool_comes_back_to_the_model_which_works_around_it(plan) -> None:
    kit.reset(**plan)
    async with calculator() as runtime:
        root = await runtime.spawn("calc", {"task": kit.TASK}, root_id="b")
        await runtime.run(root, timeout=300)
        record = await runtime.get(root)
        assert record["view"] == "succeeded", record["error"]
        assert await total_of(runtime, root) == kit.TOTAL
        found = calc_calls(await crossings(runtime, root))
    broken = [c for c in found if c["effect"] == "calc.add"]
    assert broken and not any(c["ok"] for c in broken)
    expected = ("failed", 1) if plan.get("fail") else ("timeout", 3)
    assert {(c["failure"], c["attempts"]) for c in broken} == {expected}
    assert any(c["effect"] == "calc.add_backup" and c["ok"] for c in found)


# ---- the model ----------------------------------------------------------------------------------


async def test_model_errors_are_retried_or_fault_and_continue() -> None:
    def plan(n: int, messages: list[dict[str, Any]]) -> Any:
        if n in (1, 2):
            return EffectError("transient", "the model endpoint is overloaded")
        if n == 4:
            return EffectError("failed", "the model endpoint broke")
        return None

    kit.reset()
    model = kit.Flaky(openrouter(kit.MODEL), plan)
    async with calculator(model) as runtime:
        root = await runtime.spawn("calc", {"task": kit.TASK}, root_id="m")
        await runtime.run(root, timeout=300)
        record = await runtime.get(root)
        assert record["view"] == "suspended" and "the model endpoint broke" in record["error"]
        assert record["state"] == "think"
        await runtime.signal(root, "CONT")
        await runtime.run(root, timeout=300)
        assert (await runtime.get(root))["view"] == "succeeded"
        assert await total_of(runtime, root) == kit.TOTAL
        found = await crossings(runtime, root)
    first_model = next(c for c in found if c["effect"] == "model.default")
    assert first_model["ok"] and first_model["attempts"] == 3
    # the fault was in a model turn: every tool invocation is a committed one, none ran twice
    assert len(kit.CALLS) == len(calc_calls(found, ok=True))


# ---- stop, restart, crash ---------------------------------------------------------------------


async def test_stop_in_the_middle_then_continue() -> None:
    runtime = calculator()

    def stop() -> None:
        asyncio.get_running_loop().create_task(runtime.signal("s", "STOP"))

    kit.reset(on_call={2: stop})
    async with runtime:
        root = await runtime.spawn("calc", {"task": kit.TASK}, root_id="s")
        await runtime.run(root, timeout=300)
        record = await runtime.get(root)
        assert any(r["kind"] == "stopped" for r in record["reasons"]), record
        calls_when_stopped = len(kit.CALLS)
        assert calls_when_stopped == 2  # nothing ran after the stop
        await runtime.signal(root, "CONT")
        await runtime.run(root, timeout=300)
        assert await total_of(runtime, root) == kit.TOTAL


async def test_a_stopped_runtime_resumes_in_a_new_one(tmp_path) -> None:
    kit.reset(hang=0, on_call={3: lambda: kit.PLAN.update(hang=1, hang_seconds=1.5)})
    first = kit.on_disk(tmp_path, executors=1)
    async with first:
        await first.spawn("calc", {"task": kit.TASK}, root_id="job")

        async def busy() -> bool:
            return len(kit.CALLS) >= 3

        await until(busy)
        await first.stop(drain=0)  # the step in flight is cancelled, its lease released
    async with kit.on_disk(tmp_path) as second:
        await second.run("job", timeout=300)
        assert await total_of(second, "job") == kit.TOTAL
        committed = calc_calls(await crossings(second, "job"), ok=True)
    assert len(kit.CALLS) - len(committed) <= 1  # only the interrupted call ran again


def test_a_process_killed_inside_a_tool_call_resumes_in_a_new_process(tmp_path) -> None:
    log = tmp_path / "calls.jsonl"
    env = {**os.environ, "CALL_LOG": str(log)}

    def work(crash_at: int) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(KIT_FILE), str(tmp_path), str(crash_at)],
            capture_output=True,
            text=True,
            timeout=600,
            env=env,
        )

    died = work(3)
    assert died.returncode == 17, died.stderr[-2000:]
    resumed = work(0)
    assert resumed.returncode == 0 and resumed.stdout.strip() == "succeeded", resumed.stderr[-2000:]

    async def check() -> None:
        async with kit.on_disk(tmp_path) as runtime:
            assert await total_of(runtime, "job") == kit.TOTAL
            committed = calc_calls(await crossings(runtime, "job"), ok=True)
        lines = [json.loads(x) for x in log.read_text().splitlines()]
        assert len({x["pid"] for x in lines}) == 2
        assert len(lines) - len(committed) == 1  # the call the process died in ran again

    asyncio.run(check())


# ---- children -----------------------------------------------------------------------------------

LEAD = (
    'Spawn a calc child named `a` with input {"task": "add 3 and 4"} and one named `b` with '
    'input {"task": "add 5 and 6"}. Join both. For each child that succeeded, read its `answer` '
    "artifact (by=<name>). For a child that did not succeed, do its addition yourself with "
    "calc__add and end that child with signal_agent TERM. Then answer "
    '{"total": <the sum of both additions>}.'
)


def numbers_in(text: Any) -> set[str]:
    return set(re.findall(r"\d+", str(text)))


def child_b(messages: list[dict[str, Any]]) -> bool:
    """A model call of the child adding 5 and 6: its task message is its spawn input, a JSON
    object (the lead's task mentions it too, inside a longer text); the model may word it as it
    likes, so its numbers decide."""
    if len(messages) < 2:
        return False
    try:
        given = json.loads(str(messages[1].get("content")))
    except json.JSONDecodeError:
        return False
    return isinstance(given, dict) and numbers_in(json.dumps(given)) == {"5", "6"}


async def test_a_child_whose_model_fails_is_recovered_by_its_lead() -> None:
    def plan(n: int, messages: list[dict[str, Any]]) -> Any:
        return EffectError("failed", "b's model is down") if child_b(messages) else None

    kit.reset()
    model = kit.Flaky(openrouter(kit.MODEL), plan)
    async with calculator(model, spawns=True, limits={"max_depth": 1}) as runtime:
        root = await runtime.spawn("calc", {"task": LEAD}, root_id="lead")
        await runtime.run(root, timeout=300)
        lead = await runtime.get(root)
        assert lead["view"] == "succeeded", lead["error"]
        assert await total_of(runtime, root) == 18
        children = await children_by_task(runtime, root)
    calls = [
        (c["function"]["name"], c["function"]["arguments"])
        for m in lead["data"]["messages"]
        if m["role"] == "assistant"
        for c in m.get("tool_calls") or []
    ]
    assert "succeeded" in children.get("3 and 4", []), (children, calls)
    assert set(children.get("5 and 6", [])) == {"terminated"}, (children, calls)


async def test_a_hung_child_is_killed_and_its_lead_recovers() -> None:
    def plan(n: int, messages: list[dict[str, Any]]) -> Any:
        return "hang" if child_b(messages) else None

    class Hanging(kit.Flaky):
        async def complete(self, messages, tools):
            if self.plan(0, messages) == "hang":
                await asyncio.sleep(3600)  # stuck inside a model call
            return await self.inner.complete(messages, tools)

    kit.reset()
    runtime = calculator(Hanging(openrouter(kit.MODEL), plan), spawns=True, executors=1)
    async with runtime:
        root = await runtime.spawn("calc", {"task": LEAD}, root_id="lead")

        stuck: list[str] = []

        async def b_is_stuck() -> bool:
            lead = await runtime.get(root)
            if not any(r["kind"] == "join" for r in lead["reasons"]):
                return False
            for row in await runtime.tree(root):
                record = await runtime.get(row["id"])
                if row["parent"] == root and numbers_in(record["input"]) == {"5", "6"}:
                    stuck.append(row["id"])
            return bool(stuck)

        await until(b_is_stuck, timeout=180)
        await asyncio.sleep(2)
        for child in stuck:
            await runtime.signal(child, "KILL")  # the operator ends the hung child

        async def lead_ended() -> bool:
            return (await runtime.get(root))["status"] != "active"

        await until(lead_ended, timeout=300)
        lead = await runtime.get(root)
        assert lead["view"] == "succeeded", lead["error"]
        assert await total_of(runtime, root) == 18
        children = await children_by_task(runtime, root)
    assert set(children["5 and 6"]) == {"terminated"}
