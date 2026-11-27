"""Shared pieces of the real-model failure scenarios, and a worker process for the crash one.

    python resilience_kit.py <db> <workspace> <crash at the n-th tool call, or 0>

Tools (`calc.*`) misbehave as `PLAN` says: fail transiently, hang, or always fail; every
invocation is logged in `CALLS` (and in a file when `CALL_LOG` is set). `Flaky` wraps a model
client and raises the errors a plan gives for its n-th call.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from statecraft_agentic import AgenticAgent, add_model, openrouter
from statecraft_agentic import register as agentic

from statecraft import EffectError, Runtime, tool
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace

MODEL = os.environ.get("STATECRAFT_MODEL", "openai/gpt-oss-20b")
NUMBERS = (3, 5, 7, 11)
TOTAL = sum(NUMBERS)
TASK = (
    "Start from 0 and add these numbers one at a time, one tool call per turn: "
    + ", ".join(map(str, NUMBERS))
    + '. When all are added, answer {"total": <the total>}.'
)

#: what the tools do: {"transient": n, "hang": n, "fail": bool, "hang_always": bool,
#: "crash_at": n, "on_call": {n: callback}, "gate_at": n with "entered": asyncio.Event (the
#: n-th call sets it, then blocks until it is cancelled)}
PLAN: dict[str, Any] = {}
CALLS: list[dict[str, Any]] = []


def reset(**plan: Any) -> None:
    PLAN.clear()
    PLAN.update(plan)
    CALLS.clear()


def _log(name: str, a: float, b: float) -> int:
    CALLS.append({"tool": name, "a": a, "b": b})
    log = os.environ.get("CALL_LOG")
    if log:
        with open(log, "a", encoding="utf-8") as f:
            f.write(json.dumps({"tool": name, "a": a, "b": b, "pid": os.getpid()}) + "\n")
        with open(log, encoding="utf-8") as f:
            n = sum(1 for _ in f)
    else:
        n = len(CALLS)
    return n


@tool(name="calc.add", policy="read_only", timeout=2, max_attempts=3)
async def add(a: float, b: float) -> float:
    """Add two numbers."""
    n = _log("calc.add", a, b)
    if PLAN.get("crash_at") and n == PLAN["crash_at"]:
        os._exit(17)  # the process dies inside a tool call, as a SIGKILL would
    if PLAN.get("gate_at") == n:
        PLAN["entered"].set()
        await asyncio.Event().wait()  # held until the step is cancelled
    callback = (PLAN.get("on_call") or {}).get(n)
    if callback is not None:
        callback()
    if PLAN.get("transient", 0) > 0:
        PLAN["transient"] -= 1
        raise EffectError("transient", "the calculator is briefly unavailable")
    if PLAN.get("hang_always") or PLAN.get("hang", 0) > 0:
        PLAN["hang"] = PLAN.get("hang", 0) - 1
        await asyncio.sleep(PLAN.get("hang_seconds", 30))
    if PLAN.get("fail"):
        raise ValueError("the calculator is broken")
    return a + b


@tool(name="calc.add_backup", policy="read_only", timeout=10)
async def add_backup(a: float, b: float) -> float:
    """Add two numbers (the backup calculator: use it when calc__add fails)."""
    _log("calc.add_backup", a, b)
    return a + b


class Calculator(AgenticAgent):
    instruction = (
        "You add numbers with the calc__add tool. If a tool returns an error, use "
        "calc__add_backup for that addition instead. Delegate only when the task says so. "
        "When a child you joined did not succeed, do its part yourself."
    )
    max_model_turns = 40


class Flaky:
    """A model client that fails as `plan(n, messages)` says on its n-th call (from 1)."""

    def __init__(self, inner: Any, plan: Callable[[int, list[dict[str, Any]]], Any]) -> None:
        self.inner, self.plan, self.calls = inner, plan, 0

    async def complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> Any:
        self.calls += 1
        error = self.plan(self.calls, messages)
        if error is not None:
            raise error
        return await self.inner.complete(messages, tools)


def build(runtime: Runtime, model: Any = None, *, spawns: bool = False) -> Runtime:
    r = runtime.registrar
    agentic(r)
    add_model(r, "default", model or openrouter(MODEL))
    r.tools.register(add)
    r.tools.register(add_backup)
    r.agents.register(
        "calc",
        Calculator,
        tools=["model.default", "calc.add", "calc.add_backup"],
        spawns=["calc"] if spawns else [],
        writes=["answer"],
    )
    return runtime


def on_disk(folder: Path, **options: Any) -> Runtime:
    return build(
        Runtime(
            store=SqliteStore(folder / "state.db"),
            workspace=LocalWorkspace(folder / "ws"),
            **{"executors": 0, "scan_every": 0.05, **options},
        )
    )


async def _work(folder: Path, crash_at: int) -> str:
    PLAN.update(crash_at=crash_at)
    async with on_disk(folder) as runtime:
        if await runtime.store.find("job") is None:
            await runtime.spawn("calc", {"task": TASK}, root_id="job")
        await runtime.run("job", timeout=300)
        return (await runtime.get("job"))["view"]


if __name__ == "__main__":
    print(asyncio.run(_work(Path(sys.argv[1]), int(sys.argv[2]))))
