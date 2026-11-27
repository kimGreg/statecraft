"""Agents managing agents on a real model: delegate, work on, join, combine.

What must hold: a lead model spawns workers (the same agent type; the tree's depth limit keeps
them from spawning further), does its own addition, joins them, reads their answers and gives
the right total; every child ends (at least two succeed; extra ones may be ended with the lead),
none is left faulted or live. Whether the lead worked before joining is recorded in the
report (`-s`), not required: it is the model's choice the prompt asks for.

Runs only with `STATECRAFT_REAL_MODEL=1` and an OpenRouter key; `STATECRAFT_MODEL` picks the
model (default openai/gpt-oss-20b).
"""

from __future__ import annotations

import json
import os

import pytest
from statecraft_agentic import AgenticAgent, add_model, openrouter
from statecraft_agentic import register as agentic
from statecraft_agentic.models import OPENROUTER_KEY_ENVS

from statecraft.testing import memory_runtime
from statecraft.testing.tools import add

pytestmark = pytest.mark.skipif(
    os.environ.get("STATECRAFT_REAL_MODEL") != "1"
    or not any(os.environ.get(k) for k in OPENROUTER_KEY_ENVS),
    reason="a real-model run: set STATECRAFT_REAL_MODEL=1 and an OpenRouter key",
)

TASK = (
    "Delegate two additions to workers: spawn a worker named `a` with input "
    '{"task": "add 3 and 4"} and a worker named `b` with input {"task": "add 5 and 6"}. '
    "While they work, add 10 and 20 yourself with math__add. Then join them, read each one's "
    "`answer` artifact (read_artifact with by=<name>), and answer "
    '{"total": <the sum of all three results>}.'
)


class Worker(AgenticAgent):
    instruction = (
        "Do the task you are given with the math__add tool, then answer "
        '{"total": <the result>}. Delegate only if the task asks you to.'
    )
    max_model_turns = 30


async def test_a_lead_delegates_to_workers_and_combines() -> None:
    runtime = memory_runtime(limits={"max_depth": 1, "max_agents": 6})
    r = runtime.registrar
    agentic(r)
    add_model(r, "default", openrouter(os.environ.get("STATECRAFT_MODEL", "openai/gpt-oss-20b")))
    r.tools.register(add)
    r.agents.register(
        "worker", Worker, tools=["model.default", "math.add"], spawns=["worker"], writes=["answer"]
    )
    async with runtime:
        root = await runtime.spawn("worker", {"task": TASK}, root_id="lead")
        rows = await runtime.run(root, timeout=600)
        record = await runtime.get(root)
        assert record["view"] == "succeeded", (record["error"], rows)
        assert (await runtime.artifact(root, "answer", by=root))["payload"]["total"] == 48
        children = [r for r in rows if r["parent"] == root]
        # extra children the model started may have been ended when the lead finished
        views = [(r["name"], r["view"], r["error"]) for r in children]
        assert all(v in ("succeeded", "terminated") for _n, v, _e in views), views
        assert sum(v == "succeeded" for _n, v, _e in views) >= 2, views
    calls = [
        c["function"]["name"]
        for m in record["data"]["messages"]
        if m["role"] == "assistant"
        for c in m.get("tool_calls") or []
    ]
    first_join = next((i for i, n in enumerate(calls) if n in ("join_agents", "wait_for")), None)
    own_before = "math__add" in calls[: first_join if first_join is not None else len(calls)]
    print(json.dumps({"lead_calls": calls, "worked_before_joining": own_before}))
