"""statecraft-agentic on the runtime, with scripted models.

What must hold
  * one tool call per transition; tool failures go back to the model; the final answer is the
    agent's output artifact;
  * a model call is an ordinary tool call (`model.<name>`): checked, recorded as a crossing with
    its usage;
  * spawning, waiting and asking a person are boundaries the runtime checkpoints;
  * the decider's answer must match its schema;
  * skills are pinned when the agent starts and reloaded identically after the skill changes;
    a skill cannot widen what the agent may call; the model is offered only granted tools;
  * the sequence and fan-out patterns run agents in order and in parallel.
"""

from __future__ import annotations

import textwrap

import pytest
from statecraft_agentic import AgenticAgent, ScriptedModel, add_model, add_skills, call, say
from statecraft_agentic import register as agentic

from statecraft import Agent, Done, state
from statecraft.runtime.registry import RegistryError
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add, square


class Helper(AgenticAgent):
    instruction = "Add numbers with the tool."


class Leaf(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("leaf_out", {"v": ctx.input.get("v", 0)})
        return Done


def runtime_with(
    model: ScriptedModel,
    *,
    tools=("model.default", "math.add"),
    offered=None,
    skills=None,
    skill_dir=None,
    spawns=(),
):
    runtime = memory_runtime()
    r = runtime.registrar
    agentic(r)
    add_model(r, "default", model)
    if skill_dir is not None:
        add_skills(r, skill_dir)
    r.tools.register(add)
    r.tools.register(square)
    settings = {}
    if offered is not None:
        settings["tools"] = offered
    if skills is not None:
        settings["skills"] = skills
    r.agents.register(
        "helper",
        Helper,
        tools=list(tools),
        spawns=list(spawns),
        writes=["answer"],
        extensions={"agentic": settings},
    )
    r.agents.register("leaf", Leaf, writes=["leaf_out"])
    return runtime


async def test_tool_calls_are_steps_and_errors_go_back_to_the_model() -> None:
    model = ScriptedModel(
        [call("math__add", a=2, b=3), call("math__nope", x=1), say('{"total": 5}')]
    )
    async with runtime_with(model) as runtime:
        root = await runtime.spawn("helper", {"goal": "2+3"})
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "answer"))["payload"] == {"total": 5}
        data = (await runtime.get(root))["data"]
        tools = [m["content"] for m in data["messages"] if m["role"] == "tool"]
        assert tools[0] == "5" and "unknown tool" in tools[1]
        states = [h["record"]["state"] for h in await runtime.history(root)]
        assert states.count("act_one") == 2 and states.count("think") >= 3


async def test_a_model_call_is_an_ordinary_recorded_tool_call() -> None:
    async with runtime_with(ScriptedModel([say('{"ok": true}')])) as runtime:
        root = await runtime.spawn("helper", {})
        await runtime.run(root, timeout=10)
        crossings = [c for e in await runtime.history(root) for c in e.get("crossings", [])]
        model_calls = [c for c in crossings if c["effect"] == "model.default"]
        assert model_calls and model_calls[0]["provider"] == "model"
        assert model_calls[0]["usage"] == {"prompt_tokens": 10, "completion_tokens": 5}
        policy = next(t for t in runtime.tools() if t["name"] == "model.default")["policy"]
        assert (policy["kind"], policy["deterministic"], policy["cost"]) == (
            "read_only",
            False,
            "metered",
        )


async def test_the_model_is_offered_only_granted_tools() -> None:
    model = ScriptedModel([say("{}")])
    async with runtime_with(model, offered=["math.add"]) as runtime:
        root = await runtime.spawn("helper", {})
        await runtime.run(root, timeout=10)
    offered = set(model.offered[0])
    assert "math__add" in offered and "math__square" not in offered
    assert not any(name.startswith("model__") for name in offered)
    runtime = runtime_with(ScriptedModel([say("{}")]), offered=["math.square"])
    with pytest.raises(RegistryError):  # offering a tool the agent may not call
        await runtime.start()


async def test_spawn_wait_and_ask_are_boundaries() -> None:
    model = ScriptedModel(
        [
            call("spawn_agent", name="l1", agent="leaf", input={"v": 7}),
            call("wait_for", names=["l1"]),
            call("read_artifact", type="leaf_out", by="l1"),
            call("ask_person", question="ok?", options=["yes", "no"]),
            say('{"v": 7}'),
        ]
    )
    async with runtime_with(model, spawns=["leaf"]) as runtime:
        root = await runtime.spawn("helper", {})
        await runtime.run(root, timeout=10)
        [question] = await runtime.questions(root)
        await runtime.answer(question["question_id"], "yes")
        rows = await runtime.run(root, timeout=10)
        assert {r["view"] for r in rows} == {"succeeded"}
        tools = [
            m["content"]
            for m in (await runtime.get(root))["data"]["messages"]
            if m["role"] == "tool"
        ]
        assert '"v": 7' in tools[2] and '"answer": "yes"' in tools[3]


class AskDecider(Agent):
    start = "ask"

    @state
    async def ask(self, ctx, data):
        from statecraft import Spawn

        return Spawn(
            {
                "d": (
                    "agentic:decider",
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
        from statecraft import Wait

        return Wait(join=["d"], then="use", on_fault="use")

    @state
    async def use(self, ctx, data):
        ok = ctx.woke["children"].get("d") == "succeeded"
        answer = (await ctx.read("decision", by="d")).payload["answer"] if ok else "hold"
        await ctx.write("plan", {"answer": answer})
        return Done


async def test_the_decider_answers_within_its_schema() -> None:
    model = ScriptedModel([say('{"answer": "maybe"}'), say('{"answer": "go", "reason": "low"}')])
    runtime = runtime_with(model)
    runtime.registrar.agents.register("asker", AskDecider, spawns=["agentic:decider"])
    async with runtime:
        root = await runtime.spawn("asker")
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "plan"))["payload"] == {"answer": "go"}


def write_skill(folder, name: str, body: str, tools: str = "") -> None:
    skill = folder / name
    skill.mkdir(parents=True, exist_ok=True)
    allowed = f"allowed-tools: {tools}\n" if tools else ""
    (skill / "SKILL.md").write_text(f"---\nname: {name}\ndescription: {name}\n{allowed}---\n{body}")


async def test_skills_are_pinned_at_start_and_reloaded_identically(tmp_path) -> None:
    write_skill(tmp_path, "review", "Version one: be thorough.", "math.add")
    write_skill(tmp_path, "notes", "Notes v1.")
    model = ScriptedModel(
        [
            call("ask_person", question="continue?"),
            call("load_skill", name="notes"),
            say('{"done": true}'),
        ]
    )
    runtime = runtime_with(model, skills=["review", "notes"], skill_dir=tmp_path)
    async with runtime:
        root = await runtime.spawn("helper", {})
        await runtime.run(root, timeout=10)
        pinned = (await runtime.get(root))["data"]["available_skills"]
        assert set(pinned) == {"review", "notes"}
        write_skill(tmp_path, "notes", "Notes v2 (changed after the agent started).")
        runtime.registry.services["agentic.skills"].add_folder(tmp_path)
        [question] = await runtime.questions(root)
        await runtime.answer(question["question_id"], "yes")
        await runtime.run(root, timeout=10)
        messages = (await runtime.get(root))["data"]["messages"]
        assert "Add numbers" in messages[0]["content"]  # the class instruction comes first
        loaded = [m["content"] for m in messages if m["role"] == "tool"][-1]
        assert "Notes v1." in loaded and "v2" not in loaded


async def test_a_skill_cannot_widen_permissions(tmp_path) -> None:
    write_skill(tmp_path, "greedy", "Use square.", "math.square")
    runtime = runtime_with(ScriptedModel([say("{}")]), skills=["greedy"], skill_dir=tmp_path)
    with pytest.raises(RegistryError) as raised:
        await runtime.start()
    assert "grants no permission" in str(raised.value)


async def test_sequence_and_fanout_patterns() -> None:
    runtime = memory_runtime()
    agentic(runtime.registrar)
    runtime.registrar.agents.register("leaf", Leaf, writes=["leaf_out"])
    async with runtime:
        seq = await runtime.spawn(
            "agentic:sequence", {"steps": [["leaf", {"v": 1}], ["leaf", {"v": 2}]]}
        )
        fan = await runtime.spawn(
            "agentic:fanout", {"children": {"a": ["leaf", {"v": 1}], "b": ["leaf", {"v": 2}]}}
        )
        for root in (seq, fan):
            rows = await runtime.run(root, timeout=10)
            assert {r["view"] for r in rows} == {"succeeded"}, rows
        assert (await runtime.artifact(seq, "sequence"))["payload"] == {"steps": 2}


def test_skill_frontmatter_is_checked(tmp_path) -> None:
    from statecraft_agentic.skills import load_skill

    write_skill(tmp_path, "Bad_Name", "x")
    with pytest.raises(ValueError):
        load_skill(tmp_path / "Bad_Name")
    assert textwrap.dedent("ok") == "ok"
