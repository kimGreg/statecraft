"""Effects through the effect manager (architecture §7).

What must hold
  * an agent calls only the tools it was granted (its pinned bindings); others are refused;
  * arguments are checked against the input schema (`invalid`), results against the output
    schema (`failed`);
  * a transient failure or timeout is retried only when the policy makes a repeat safe; a
    `non_repeatable` tool is refused; a `deduplicated` tool needs an idempotency key;
  * `unavailable` (provably not sent) is retried; a dropped connection is not assumed unsent;
  * every call is recorded as a crossing in the step's history entry, failures included;
  * `@tool` takes its schemas from the signature and requires a policy.
"""

from __future__ import annotations

import pytest

from statecraft import Agent, Done, EffectError, state, tool
from statecraft.kernel.effects import EffectPolicy, may_retry
from statecraft.sdk.effect import EffectDefinition
from statecraft.testing import FunctionProvider, memory_runtime


class Caller(Agent):
    """Calls the tool named in its input; records the result or the failure class."""

    start = "call"

    @state
    async def call(self, ctx, data):
        try:
            result = await ctx.call(
                ctx.input["tool"],
                idempotency_key=ctx.effect_key("k") if ctx.input.get("keyed") else None,
                **ctx.input.get("args", {}),
            )
            await ctx.write("outcome", {"result": result})
        except EffectError as exc:
            await ctx.write("outcome", {"failure": exc.cls, "message": exc.message})
        return Done


def flaky(times: int, cls: str, sent: bool = True):
    count = {"n": 0}

    def fn(arguments):
        count["n"] += 1
        if count["n"] <= times:
            raise EffectError(cls, "boom", sent=sent)
        return {"ok": count["n"]}

    fn.count = count  # type: ignore[attr-defined]
    return fn


async def run_caller(tools: dict, call: dict, *, granted=("svc.*",)) -> tuple[dict, list]:
    runtime = memory_runtime()
    functions = {name: fn for name, (fn, _policy, _schema) in tools.items()}
    provider = FunctionProvider(functions)
    runtime.registrar.providers.register("svc", provider)
    for name, (_fn, policy, schema) in tools.items():
        runtime.registrar.tools.register(
            EffectDefinition(
                name=name,
                policy=EffectPolicy.of(policy),
                provider="svc",
                operation=name,
                input_schema=schema or {"type": "object"},
            )
        )
    runtime.registrar.agents.register("caller", Caller, tools=list(granted))
    runtime.effects.backoff = 0.001
    async with runtime:
        root = await runtime.spawn("caller", call)
        await runtime.run(root, timeout=10)
        outcome = (await runtime.artifact(root, "outcome"))["payload"]
        crossings = [c for e in await runtime.history(root) for c in e.get("crossings", [])]
        return outcome, crossings


async def test_a_read_only_tool_is_retried_after_a_transient_failure() -> None:
    fn = flaky(2, "transient")
    outcome, crossings = await run_caller(
        {"svc.get": (fn, {"kind": "read_only", "max_attempts": 3}, None)}, {"tool": "svc.get"}
    )
    assert outcome == {"result": {"ok": 3}}
    assert crossings[0]["attempts"] == 3 and crossings[0]["ok"]


async def test_a_retry_stops_at_max_attempts() -> None:
    fn = flaky(5, "timeout")
    outcome, crossings = await run_caller(
        {"svc.get": (fn, {"kind": "idempotent", "max_attempts": 2}, None)}, {"tool": "svc.get"}
    )
    assert outcome["failure"] == "timeout" and crossings[0]["attempts"] == 2


async def test_a_deduplicated_tool_needs_a_key_and_then_retries() -> None:
    fn = flaky(1, "transient")
    tools = {"svc.pay": (fn, "deduplicated", None)}
    outcome, _ = await run_caller(tools, {"tool": "svc.pay"})
    assert outcome["failure"] == "refused" and "idempotency_key" in outcome["message"]
    outcome, crossings = await run_caller(tools, {"tool": "svc.pay", "keyed": True})
    assert outcome == {"result": {"ok": 2}} and crossings[0]["attempts"] == 2


async def test_a_non_repeatable_tool_is_refused() -> None:
    outcome, _ = await run_caller(
        {"svc.send": (lambda a: {"sent": True}, "non_repeatable", None)}, {"tool": "svc.send"}
    )
    assert outcome["failure"] == "refused"


async def test_unavailable_is_retried_only_when_provably_not_sent() -> None:
    not_sent = flaky(1, "unavailable", sent=False)
    outcome, _ = await run_caller(
        {"svc.send": (not_sent, {"kind": "deduplicated", "max_attempts": 3}, None)},
        {"tool": "svc.send"},
    )
    assert outcome["failure"] == "refused"  # no key: refused before anything is sent
    policy = EffectPolicy(kind="non_repeatable")
    assert may_retry(policy, EffectError("unavailable", "x", sent=False), keyed=False, attempt=1)
    assert not may_retry(policy, EffectError("unavailable", "x"), keyed=False, attempt=1)
    assert not may_retry(policy, EffectError("transient", "x"), keyed=False, attempt=1)


async def test_tools_outside_the_grant_are_refused() -> None:
    outcome, crossings = await run_caller(
        {
            "svc.get": (lambda a: 1, "read_only", None),
            "svc.admin": (lambda a: 2, "read_only", None),
        },
        {"tool": "svc.admin"},
        granted=("svc.get",),
    )
    assert outcome["failure"] == "refused" and crossings[0]["failure"] == "refused"


async def test_arguments_are_checked_against_the_schema() -> None:
    schema = {"type": "object", "properties": {"q": {"type": "string"}}, "required": ["q"]}
    outcome, _ = await run_caller(
        {"svc.get": (lambda a: a["q"], "read_only", schema)}, {"tool": "svc.get", "args": {"q": 3}}
    )
    assert outcome["failure"] == "invalid"


@tool(name="calc.mul", policy="read_only")
async def mul(a: float, b: float = 2.0) -> float:
    """Multiply."""
    return a * b


def test_tool_takes_its_schemas_from_the_signature_and_requires_a_policy() -> None:
    definition = mul.__statecraft_effect__
    assert definition.input_schema["required"] == ["a"]
    assert definition.input_schema["properties"]["b"]["default"] == 2.0
    assert definition.output_schema == {"type": "number"}
    assert definition.description == "Multiply."
    with pytest.raises(TypeError):
        tool(name="x")
    assert definition.policy.repeatable(keyed=False)


async def test_a_python_tool_result_breaking_its_schema_fails() -> None:
    @tool(name="calc.bad", policy="read_only")
    async def bad() -> int:
        return "not an int"  # type: ignore[return-value]

    runtime = memory_runtime()
    runtime.registrar.tools.register(bad)
    runtime.registrar.agents.register("caller", Caller, tools=["calc.bad"])
    async with runtime:
        root = await runtime.spawn("caller", {"tool": "calc.bad"})
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "outcome"))["payload"]["failure"] == "failed"


def test_exceptions_and_http_answers_are_classified() -> None:
    from statecraft import classify
    from statecraft.kernel.effects import classify_status

    class ConnectError(Exception):  # named as HTTP libraries name theirs
        pass

    class ReadTimeout(Exception):
        pass

    class Response:
        def __init__(self, status: int, text: str = "") -> None:
            self.status_code, self.text = status, text

    class HTTPStatusError(Exception):
        def __init__(self, status: int, text: str = "") -> None:
            super().__init__(status)
            self.response = Response(status, text)

    found = {
        "refused": classify(ConnectionRefusedError()),
        "connect": classify(ConnectError("down")),
        "timeout": classify(ReadTimeout()),
        "reset": classify(ConnectionResetError()),
        "503": classify(HTTPStatusError(503)),
        "401": classify(HTTPStatusError(401, "bad key")),
        "bug": classify(ValueError("no")),
    }
    assert (found["refused"].cls, found["refused"].sent) == ("unavailable", False)
    assert (found["connect"].cls, found["connect"].sent) == ("unavailable", False)
    assert found["timeout"].cls == "timeout" and found["reset"].cls == "transient"
    assert found["503"].cls == "transient"
    assert (found["401"].cls, found["401"].code) == ("refused", "auth")
    assert found["bug"].cls == "failed"  # an ordinary exception is never retried
    overflow = classify_status(400, '{"error": "This model\'s maximum context length is 8192"}')
    assert (overflow.cls, overflow.code) == ("invalid", "context_overflow")
    assert classify_status(400, "bad field").code == "400"
    assert classify_status(408).cls == "timeout" and classify_status(429).cls == "transient"
