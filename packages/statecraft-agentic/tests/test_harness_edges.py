"""The agentic harness at its edges (roadmap B3).

What must hold
  * a model endpoint's error inside a 200 answer is classified by its code (429 and 5xx
    transient, 401/403 auth, overflow invalid), and so is an error on a choice; tool-call
    arguments sent as an object are taken as they are;
  * a steer that arrives as the agent finishes reaches the model before it ends;
  * the finish guard tells the model about every live child it was not told about yet;
  * with an instruction, every pinned skill is loadable;
  * a summary request too big for the model is made in chunks: the agent compacts and goes on;
  * a limit (turns, tool calls) that faulted the agent is raised by an `extend` message, and CONT
    goes on from where it stopped;
  * badly shaped arguments to a boundary tool come back to the model as an error, not a fault;
  * the registry refuses an unknown manifest key, and two providers offering one tool name; a
    call whose provider was unreachable at start says so.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from statecraft_agentic import AgenticAgent, ChatModel, ScriptedModel, add_model, call, say
from statecraft_agentic import models as models_module
from statecraft_agentic import register as agentic

from statecraft import Agent, Done, EffectDefinition, EffectError, Wait, state
from statecraft.runtime.registry import RegistryError
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


def answer_with(monkeypatch, body: dict[str, Any]) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=json.dumps(body)))
    real = httpx.AsyncClient
    monkeypatch.setattr(
        models_module.httpx, "AsyncClient", lambda **kw: real(transport=transport, **kw)
    )


def chat() -> ChatModel:
    return ChatModel(name="m", base_url="http://t/v1", key_envs=())


@pytest.mark.parametrize(
    ("body", "cls", "code"),
    [
        ({"error": {"code": 429, "message": "rate limit upstream"}}, "transient", "429"),
        ({"error": {"code": 401, "message": "bad key"}}, "refused", "auth"),
        (
            {"error": {"code": 400, "message": "maximum context length is 8192"}},
            "invalid",
            "context_overflow",
        ),
        (
            {
                "choices": [
                    {
                        "finish_reason": "error",
                        "message": {"role": "assistant", "content": ""},
                        "error": {"code": 502, "message": "upstream died"},
                    }
                ]
            },
            "transient",
            "502",
        ),
    ],
)
async def test_errors_in_an_answer_are_classified(monkeypatch, body, cls, code) -> None:
    answer_with(monkeypatch, body)
    with pytest.raises(EffectError) as raised:
        await chat().complete([], [])
    assert (raised.value.cls, raised.value.code) == (cls, code)


async def test_object_arguments_are_taken_as_they_are(monkeypatch) -> None:
    tool_call = {"id": "a", "function": {"name": "f", "arguments": {"x": 1}}}
    answer_with(monkeypatch, {"choices": [{"message": {"tool_calls": [tool_call]}}]})
    reply = await chat().complete([], [])
    assert reply["tool_calls"][0]["arguments"] == {"x": 1}


class Worker(AgenticAgent):
    instruction = "Add numbers with the tool."


def runtime_with(model: ScriptedModel, **extensions: Any):
    runtime = memory_runtime()
    r = runtime.registrar
    agentic(r)
    add_model(r, "default", model)
    r.tools.register(add)
    r.agents.register(
        "worker",
        Worker,
        tools=["model.default", "math.add"],
        writes=["answer"],
        extensions={"agentic": extensions},
    )
    return runtime


async def test_a_steer_at_the_finish_reaches_the_model() -> None:
    runtime_box: dict[str, Any] = {}

    def step(messages, tools):
        if sum(1 for m in messages if m["role"] == "assistant") == 0:
            return say('{"n": 1}')  # its final answer; a steer is sent meanwhile
        return say(json.dumps({"last": messages[-1]["content"]}))

    model = ScriptedModel(step_fn=step)
    original = model.complete

    async def complete(messages, tools):
        reply = await original(messages, tools)
        if len(model.seen) == 1:
            await runtime_box["rt"].signal("w", ("USR", {"type": "steer", "text": "use 7"}))
        return reply

    model.complete = complete
    async with runtime_with(model) as rt:
        runtime_box["rt"] = rt
        await rt.spawn("worker", root_id="w")
        await rt.run("w", timeout=10)
        assert (await rt.artifact("w", "answer"))["payload"] == {"last": "use 7"}


class Waiter(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "never", "question": "?"}, then="go")


class Lead(AgenticAgent):
    instruction = "Spawn children as told."


async def test_every_new_live_child_is_told_about() -> None:
    model = ScriptedModel(
        [
            call("spawn_agent", name="c1", agent="waiter", input={}),
            say('{"ok": 1}'),  # told: c1 is live
            call("signal_agent", name="c1", signal="TERM"),
            call("spawn_agent", name="c2", agent="waiter", input={}),
            say('{"ok": 2}'),  # told again: c2 is live
            call("signal_agent", name="c2", signal="TERM"),
            say('{"ok": 3}'),
        ]
    )
    runtime = memory_runtime()
    agentic(runtime.registrar)
    add_model(runtime.registrar, "default", model)
    runtime.registrar.agents.register("waiter", Waiter)
    runtime.registrar.agents.register(
        "lead", Lead, tools=["model.default"], spawns=["waiter"], writes=["answer"]
    )
    async with runtime:
        await runtime.spawn("lead", root_id="g")
        await runtime.run("g", timeout=10)
        assert (await runtime.artifact("g", "answer"))["payload"] == {"ok": 3}
    told = [m for m in model.seen[-1] if "children still live" in str(m.get("content"))]
    assert len(told) == 2


async def test_with_an_instruction_every_skill_is_loadable(tmp_path) -> None:
    from statecraft_agentic import add_skills

    skill = tmp_path / "skills" / "review"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("---\nname: review\ndescription: d\n---\nREVIEW BODY\n")
    model = ScriptedModel([call("load_skill", name="review"), say('{"ok": 1}')])
    runtime = runtime_with(model, skills=["review"], instruction="Do X.")
    add_skills(runtime.registrar, tmp_path / "skills")
    async with runtime:
        await runtime.spawn("worker", root_id="k")
        await runtime.run("k", timeout=10)
    assert "review" in model.seen[0][0]["content"] and "load_skill" in model.offered[0]
    assert any("REVIEW BODY" in str(m.get("content")) for m in model.seen[1])


async def test_a_summary_too_big_for_the_model_is_made_in_chunks() -> None:
    sizes: list[int] = []

    def step(messages, tools):
        text = json.dumps(messages)
        if messages[0]["content"].startswith("You compact"):
            if len(text) > 4_000:  # this model cannot take long summary requests
                sizes.append(-2)
                raise EffectError("invalid", "maximum context length", code="context_overflow")
            return say("summary")
        sizes.append(-1)  # a work turn (compaction shortens what the model sees)
        turns = sizes.count(-1)
        if turns < 12:
            return call("math.add", a=turns, b=1, note="x" * 600)
        return say('{"done": true}')

    model = ScriptedModel(step_fn=step)
    async with runtime_with(model, context={"max_tokens": 1500, "keep_tokens": 200}) as rt:
        await rt.spawn("worker", root_id="c")
        await rt.run("c", timeout=20)
        record = await rt.get("c")
    assert record["view"] == "succeeded", record["error"]
    assert sizes.count(-2) >= 1  # a summary request was too big ...
    assert record["data"]["compaction"]["count"] >= 2  # ... and was made in smaller chunks


async def test_a_limit_is_raised_by_extend_and_cont_goes_on() -> None:
    class Short(AgenticAgent):
        instruction = "Add."
        max_model_turns = 2

    model = ScriptedModel(
        [call("math.add", a=1, b=1), call("math.add", a=2, b=2), say('{"done": true}')]
    )
    runtime = memory_runtime()
    agentic(runtime.registrar)
    add_model(runtime.registrar, "default", model)
    runtime.registrar.tools.register(add)
    runtime.registrar.agents.register(
        "short", Short, tools=["model.default", "math.add"], writes=["answer"]
    )
    async with runtime:
        await runtime.spawn("short", root_id="s")
        await runtime.run("s", timeout=10)
        record = await runtime.get("s")
        assert record["view"] == "suspended" and "model turns" in record["error"]
        await runtime.signal("s", ("USR", {"type": "extend", "turns": 2}))
        await runtime.signal("s", "CONT")
        await runtime.run("s", timeout=10)
        assert (await runtime.artifact("s", "answer"))["payload"] == {"done": True}


async def test_bad_boundary_arguments_are_told_to_the_model() -> None:
    model = ScriptedModel([{"calls": [("join_agents", ["a"])]}, say('{"ok": 1}')])
    runtime = memory_runtime()
    agentic(runtime.registrar)
    add_model(runtime.registrar, "default", model)
    runtime.registrar.agents.register("waiter", Waiter)
    runtime.registrar.agents.register(
        "lead", Lead, tools=["model.default"], spawns=["waiter"], writes=["answer"]
    )
    async with runtime:
        await runtime.spawn("lead", root_id="b")
        await runtime.run("b", timeout=10)
        assert (await runtime.get("b"))["view"] == "succeeded"
    tool_results = [m["content"] for m in model.seen[-1] if m["role"] == "tool"]
    assert any("must be a JSON object" in str(c) for c in tool_results)


def test_the_registry_refuses_unknown_keys_and_name_collisions() -> None:
    import asyncio

    runtime = memory_runtime()
    runtime.registrar.agents.from_manifest(
        "t", {"entry": "statecraft.testing:memory_runtime", "tool": ["x"]}
    )
    with pytest.raises(RegistryError, match="unknown keys"):
        asyncio.run(runtime.start())

    class Offers:
        def __init__(self, name):
            self.name = name

        async def discover(self):
            return [EffectDefinition(name="t.same", policy="read_only")]

        async def invoke(self, operation, arguments, context):
            return self.name

    clash = memory_runtime()
    clash.registrar.providers.register("a", Offers("a"))
    clash.registrar.providers.register("b", Offers("b"))
    with pytest.raises(RegistryError, match="offered by a and by b"):
        asyncio.run(clash.start())


class Calls(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        try:
            await ctx.call("down.op")
        except EffectError as exc:
            data["error"] = (exc.cls, exc.message)
        return Done


async def test_a_call_to_a_provider_unreachable_at_start_says_so() -> None:
    class Down:
        async def start(self):
            raise ConnectionRefusedError("down")

        async def invoke(self, operation, arguments, context):
            return None

    runtime = memory_runtime()
    runtime.registrar.providers.register("down", Down())
    runtime.registrar.agents.register("calls", Calls, tools=["down.*"])
    async with runtime:
        await runtime.spawn("calls", root_id="c")
        await runtime.run("c", timeout=5)
        cls, message = (await runtime.store.load("c")).data["error"]
    assert cls == "unavailable" and "could not be reached" in message and "repin" in message
