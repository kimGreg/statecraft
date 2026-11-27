"""Model and tool errors in the agentic loop.

What must hold
  * the model client classifies endpoint answers: 5xx/429 transient (retried), 401/403 refused
    (code auth: the agent faults at once), a 400 about the context window `context_overflow`,
    a read timeout `timeout`;
  * a model call over the model's context window makes the agent compact and try again (even
    with no context budget configured); with nothing to compact it faults;
  * a tool that fails reaches the model as a structured error: class, code, message and
    whether it may work later.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from statecraft_agentic import AgenticAgent, ChatModel, ScriptedModel, add_model, call, say
from statecraft_agentic import models as models_module
from statecraft_agentic import register as agentic

from statecraft import EffectError, tool
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Worker(AgenticAgent):
    instruction = "Add with the tool."


@tool(name="calc.broken", policy="read_only")
async def broken(a: float) -> float:
    raise ValueError("the calculator is broken")


@tool(name="calc.down", policy="read_only", max_attempts=1)
async def down(a: float) -> float:
    raise EffectError("unavailable", "the calculator is down", code="down", sent=False)


def runtime_with(model: Any):
    runtime = memory_runtime()
    r = runtime.registrar
    agentic(r)
    add_model(r, "default", model)
    for t in (add, broken, down):
        r.tools.register(t)
    r.agents.register(
        "worker",
        Worker,
        tools=["model.default", "math.add", "calc.broken", "calc.down"],
        writes=["answer"],
    )
    return runtime


@pytest.mark.parametrize(
    ("status", "body", "cls", "code"),
    [
        (503, "busy", "transient", "503"),
        (401, "bad key", "refused", "auth"),
        (400, "maximum context length exceeded", "invalid", "context_overflow"),
    ],
)
async def test_the_model_client_classifies_endpoint_answers(
    monkeypatch, status, body, cls, code
) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(status, text=body))
    real = httpx.AsyncClient
    monkeypatch.setattr(
        models_module.httpx, "AsyncClient", lambda **kw: real(transport=transport, **kw)
    )
    model = ChatModel(name="m", base_url="http://model.test/v1", key_envs=())
    with pytest.raises(EffectError) as raised:
        await model.complete([{"role": "user", "content": "hi"}], [])
    assert (raised.value.cls, raised.value.code) == (cls, code)


async def test_a_model_timeout_is_a_timeout(monkeypatch) -> None:
    def hang(request):
        raise httpx.ReadTimeout("slow", request=request)

    real = httpx.AsyncClient
    transport = httpx.MockTransport(hang)
    monkeypatch.setattr(
        models_module.httpx, "AsyncClient", lambda **kw: real(transport=transport, **kw)
    )
    with pytest.raises(EffectError) as raised:
        await ChatModel(name="m", base_url="http://model.test/v1").complete([], [])
    assert raised.value.cls == "timeout"


async def test_an_auth_error_faults_the_agent_without_retries() -> None:
    def step(messages, tools):
        raise EffectError("refused", "answered 401", code="auth")

    async with runtime_with(ScriptedModel(step_fn=step)) as runtime:
        root = await runtime.spawn("worker", {})
        await runtime.run(root, timeout=10)
        record = await runtime.get(root)
        calls = [
            c
            for e in await runtime.history(root)
            for c in e.get("crossings", [])
            if c["effect"] == "model.default"
        ]
    assert record["view"] == "suspended" and "answered 401" in record["error"]
    assert [(c["attempts"], c["failure"]) for c in calls] == [(1, "refused")]  # never retried


def overflowing(window: int):
    """A model whose window holds `window` messages; it adds 1 six times, then answers."""

    def step(messages, tools):
        if messages[0]["content"].startswith("You compact"):
            return say("Summary: several additions were done; the last total is in the tools.")
        if len(messages) > window:
            raise EffectError("invalid", "maximum context length exceeded", code="context_overflow")
        done = sum(1 for m in messages if m["role"] == "tool")
        summary = any("[Summary" in str(m["content"]) for m in messages)
        if done + (4 if summary else 0) < 6:
            return call("math__add", a=done, b=1)
        return say(json.dumps({"done": True}))

    return ScriptedModel(step_fn=step)


async def test_a_context_overflow_compacts_and_tries_again() -> None:
    async with runtime_with(overflowing(window=9)) as runtime:
        root = await runtime.spawn("worker", {})
        await runtime.run(root, timeout=10)
        record = await runtime.get(root)
    assert record["view"] == "succeeded", record["error"]
    assert record["data"]["compaction"]["count"] >= 1


async def test_a_context_overflow_with_nothing_to_compact_faults() -> None:
    async with runtime_with(overflowing(window=1)) as runtime:
        root = await runtime.spawn("worker", {})
        await runtime.run(root, timeout=10)
        record = await runtime.get(root)
    assert record["view"] == "suspended"
    assert "nothing to compact" in record["error"]


async def test_tool_failures_reach_the_model_as_structured_errors() -> None:
    model = ScriptedModel([call("calc__broken", a=1), call("calc__down", a=1), say("{}")])
    async with runtime_with(model) as runtime:
        root = await runtime.spawn("worker", {})
        await runtime.run(root, timeout=10)
        messages = (await runtime.get(root))["data"]["messages"]
    errors = [json.loads(m["content"])["error"] for m in messages if m["role"] == "tool"]
    assert errors[0] == {
        "class": "failed",
        "code": None,
        "message": "ValueError: the calculator is broken",
        "may_work_later": False,
    }
    assert errors[1]["class"] == "unavailable" and errors[1]["code"] == "down"
    assert errors[1]["may_work_later"] is True
