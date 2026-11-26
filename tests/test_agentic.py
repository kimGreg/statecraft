"""AgenticAgent: the model loop as states, on the engine, with a scripted model.

What must hold: one tool call per transition; tool failures go back to the model; spawning,
waiting and asking a person are boundaries the engine checkpoints; the final answer is the
agent's output artifact and an unusable one is asked again (limited); limits are faults; the
decider's answer must match its schema.
"""

from __future__ import annotations

from typing import Any

from statecraft.agent import Agent, Done, Spawn, Wait, state
from statecraft.agentic import AgenticAgent, ScriptedModel, call, say
from statecraft.catalog import Catalog
from statecraft.engine import Engine
from statecraft.spec import AgentSpec
from statecraft.stores import MemoryStore
from statecraft.workspace import LocalFiles


class Tools:
    async def call(self, server: str, tool: str, arguments: dict[str, Any]) -> Any:
        if tool == "add":
            return {"sum": arguments["a"] + arguments["b"]}
        raise ValueError(f"no tool {tool}")

    def describe(self, server: str, tool: str) -> dict[str, Any]:
        return {
            "description": f"{tool} numbers",
            "input_schema": {"type": "object", "properties": {"a": {"type": "number"}}},
        }


class Adder(AgenticAgent):
    instruction = "Add numbers with the tool."


class Boss(AgenticAgent):
    instruction = "Delegate to the adder."


class Leaf(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("leaf_out", {"v": ctx.input.get("v", 0)})
        return Done


class AskDecider(Agent):
    start = "ask"

    @state
    async def ask(self, ctx, data):
        return Spawn(
            {
                "d": (
                    "core:decider",
                    {
                        "question": "go?",
                        "facts": {"risk": "low"},
                        "schema": {"enum": ["go", "hold"]},
                    },
                )
            },
            then="wait",
        )

    @state
    async def wait(self, ctx, data):
        return Wait(join=["d"], then="use", on_fault="use")

    @state
    async def use(self, ctx, data):
        ok = ctx.woke["children"].get("d") == "succeeded"
        answer = (await ctx.read("decision", by="d")).payload["answer"] if ok else "hold"
        await ctx.write("plan", {"answer": answer})
        return Done


def spec(agent_id: str, tools: tuple[str, ...], writes: tuple[str, ...]) -> AgentSpec:
    return AgentSpec(id=agent_id, entry="", tools=tools, writes=writes, reads=None)


def engine_with(tmp_path, model: ScriptedModel) -> Engine:
    extra = {
        "t:adder": (Adder, spec("adder", ("math.add", "math.where"), ("answer",))),
        "t:boss": (Boss, spec("boss", ("agent.leaf",), ("answer",))),
        "t:leaf": (Leaf, None),
        "t:ask": (AskDecider, None),
    }
    return Engine(
        store=MemoryStore(),
        files=LocalFiles(tmp_path),
        tools=Tools(),
        catalog=Catalog({}, extra=extra),
        model=model,
    )


async def test_tool_calls_are_steps_and_errors_go_back_to_the_model(tmp_path) -> None:
    model = ScriptedModel(
        [call("math__add", a=2, b=3), call("math__nope", x=1), say('{"total": 5}')]
    )
    engine = engine_with(tmp_path, model)
    root = await engine.spawn("t:adder", {"goal": "2+3"})
    await engine.run(root, timeout=10, scan_every=0.01)
    assert (await engine.artifact(root, "answer"))["payload"] == {"total": 5}
    record = await engine.store.load(root)
    tools = [m for m in record.data["messages"] if m["role"] == "tool"]
    assert '"sum": 5' in tools[0]["content"]
    assert "unknown tool" in tools[1]["content"]
    states = [h["record"]["state"] for h in await engine.store.history(root)]
    assert states.count("act_one") == 2 and states.count("think") == 3
    assert model.seen[0][-1]["role"] == "user"
    tool_list = model.seen[0]
    assert tool_list  # the model saw the conversation


async def test_spawn_and_wait_are_boundaries_then_the_artifact_is_read(tmp_path) -> None:
    model = ScriptedModel(
        [
            call("spawn_agent", name="l1", agent="leaf", input={"v": 7}),
            call("wait_for", names=["l1"]),
            call("read_artifact", type="leaf_out", by="l1"),
            say('{"v": 7}'),
        ]
    )
    engine = engine_with(tmp_path, model)
    root = await engine.spawn("t:boss", {"goal": "delegate"})
    table = {r["id"]: r["view"] for r in await engine.run(root, timeout=10, scan_every=0.01)}
    assert table == {root: "succeeded", f"{root}/l1": "succeeded"}
    record = await engine.store.load(root)
    contents = [m["content"] for m in record.data["messages"] if m["role"] == "tool"]
    assert '"l1": "succeeded"' in contents[1] and '"v": 7' in contents[2]


async def test_ask_person_waits_for_the_answer(tmp_path) -> None:
    model = ScriptedModel(
        [call("ask_person", question="which?", options=["a", "b"]), say('{"chosen": "b"}')]
    )
    engine = engine_with(tmp_path, model)
    root = await engine.spawn("t:adder", {"goal": "ask"})
    await engine.run(root, timeout=10, scan_every=0.01)
    [question] = await engine.questions(root)
    await engine.answer(question["question_id"], "b")
    await engine.run(root, timeout=10, scan_every=0.01)
    assert (await engine.artifact(root, "answer"))["payload"] == {"chosen": "b"}
    tool = [m for m in (await engine.store.load(root)).data["messages"] if m["role"] == "tool"]
    assert '"answer": "b"' in tool[0]["content"]


async def test_an_unusable_answer_is_asked_again_then_faults(tmp_path) -> None:
    engine = engine_with(tmp_path, ScriptedModel([say("not json"), say('{"ok": 1}')]))
    root = await engine.spawn("t:adder", {"goal": "x"})
    await engine.run(root, timeout=10, scan_every=0.01)
    assert (await engine.artifact(root, "answer"))["payload"] == {"ok": 1}
    engine = engine_with(tmp_path / "2", ScriptedModel(step_fn=lambda m, t: say("never json")))
    root = await engine.spawn("t:adder", {"goal": "x"})
    [row] = await engine.run(root, timeout=10, scan_every=0.01)
    assert row["view"] == "suspended" and "no usable final answer" in row["error"]


async def test_limits_are_faults(tmp_path) -> None:
    engine = engine_with(tmp_path, ScriptedModel(step_fn=lambda m, t: call("math__add", a=1, b=1)))
    root = await engine.spawn("t:adder", {"goal": "loop"})
    [row] = await engine.run(root, timeout=10, scan_every=0.01)
    assert row["view"] == "suspended" and "more than" in row["error"]


async def test_the_decider_answers_within_its_schema(tmp_path) -> None:
    engine = engine_with(
        tmp_path,
        ScriptedModel([say('{"answer": "maybe"}'), say('{"answer": "go", "reason": "low"}')]),
    )
    root = await engine.spawn("t:ask")
    await engine.run(root, timeout=10, scan_every=0.01)
    assert (await engine.artifact(root, "plan"))["payload"] == {"answer": "go"}


def test_format_tokens_a_model_leaks_into_a_tool_name_are_cut() -> None:
    from statecraft.agentic import _reply

    raw = {
        "choices": [
            {
                "message": {
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "a",
                            "function": {
                                "name": "read_file<|channel|>commentary",
                                "arguments": '{"path": "x"}',
                            },
                        }
                    ],
                }
            }
        ]
    }
    [found] = _reply(raw)["tool_calls"]
    assert (found["name"], found["arguments"]) == ("read_file", {"path": "x"})
