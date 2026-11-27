"""Agents managing agents: asynchronous spawn, optional join, children as processes.

What must hold (a lead delegates two tasks and works on while they run)
  * `spawn_agent` returns at once: the lead goes on with its own tool calls before it joins;
  * `join_agents` waits only for the children named; the lead reads a joined child's artifact
    while another child is still live;
  * `list_agents` / `inspect_agent` show each child's phase (running, waiting, ended), what it
    published and an open question;
  * `signal_agent` steers a child: the steer reaches the child's next model turn;
  * finishing with a live child is told to the model once; it then joins and finishes;
  * a spawn the runtime would refuse (over the tree's limits) is an error the model sees, not a
    fault, and the collaboration guidance is in the prompt only of agents that may spawn.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from statecraft_agentic import AgenticAgent, ScriptedModel, add_model, call, say
from statecraft_agentic import register as agentic

from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Lead(AgenticAgent):
    instruction = "LEAD: delegate the analysis and the review, combine them."


class Researcher(AgenticAgent):
    instruction = "RESEARCHER: analyse the architecture."


class Reviewer(AgenticAgent):
    instruction = "REVIEWER: review the tests."


def role(messages: list[dict[str, Any]]) -> str:
    return str(messages[0]["content"]).split(":", 1)[0]


def tool_results(messages: list[dict[str, Any]]) -> list[Any]:
    out = []
    for m in messages:
        if m["role"] == "tool":
            try:
                out.append(json.loads(m["content"]))
            except json.JSONDecodeError:
                out.append(m["content"])
    return out


class Scenario:
    """One scripted model for all three agents, by the role in their instruction."""

    def __init__(self) -> None:
        self.seen: dict[str, list[list[dict[str, Any]]]] = {}
        self.model = ScriptedModel(step_fn=self.step)

    def step(self, messages, tools):
        who = role(messages)
        self.seen.setdefault(who, []).append(messages)
        turn = sum(1 for m in messages if m["role"] == "assistant")
        if who == "RESEARCHER":
            return say('{"summary": "the kernel commits each step atomically"}')
        if who == "REVIEWER":
            if turn == 0:
                return call("ask_person", question="Which tests matter most?")
            asked = [m["content"] for m in messages if m["role"] == "user"][-1]
            answer = tool_results(messages)[-1]["answer"]
            return say(json.dumps({"focus": asked, "answer": answer}))
        script = [
            {
                "calls": [
                    (
                        "spawn_agent",
                        {
                            "name": "architecture",
                            "agent": "researcher",
                            "input": {"task": "analyse shared/src"},
                        },
                    ),
                    (
                        "spawn_agent",
                        {"name": "tests", "agent": "reviewer", "input": {"task": "review tests"}},
                    ),
                ]
            },
            call("math__add", a=40, b=2),  # its own work while the children run
            call("join_agents", names=["architecture"]),
            call("list_agents"),
            call("read_artifact", type="analysis", by="architecture"),
            call("signal_agent", name="tests", signal="steer", text="Focus on recovery tests."),
            say('{"report": "early"}'),  # tests is still live: the model is told
            call("inspect_agent", name="tests"),
            call("join_agents", names=["tests"]),
            call("read_artifact", type="review", by="tests"),
        ]
        if turn < len(script):
            return script[turn]
        results = tool_results(messages)
        return say(
            json.dumps(
                {"analysis": results[5]["payload"]["summary"], "review": results[-1]["payload"]}
            )
        )


def runtime_for(model: ScriptedModel, limits: dict[str, int] | None = None):
    runtime = memory_runtime(limits=limits)
    r = runtime.registrar
    agentic(r)
    add_model(r, "default", model)
    r.tools.register(add)
    r.agents.register(
        "lead",
        Lead,
        tools=["model.default", "math.add"],
        spawns=["researcher", "reviewer"],
        writes=["report"],
        reads=["analysis", "review"],
    )
    r.agents.register(
        "researcher",
        Researcher,
        tools=["model.default"],
        writes=["analysis"],
        spawns=["researcher"],
    )
    r.agents.register("reviewer", Reviewer, tools=["model.default"], writes=["review"])
    return runtime


async def test_a_lead_delegates_works_on_and_joins_what_it_needs() -> None:
    scenario = Scenario()
    async with runtime_for(scenario.model) as runtime:
        root = await runtime.spawn("lead", {"goal": "improve the project"}, root_id="lead")
        await runtime.run(root, timeout=20)
        [question] = await runtime.questions(root)  # tests waits for a person; lead for tests
        assert question["agent"] == "lead/tests"
        assert (await runtime.get(root))["reasons"][0]["kind"] == "join"
        await runtime.answer(question["question_id"], "the crash recovery tests")
        rows = await runtime.run(root, timeout=20)
        assert {r["view"] for r in rows} == {"succeeded"}, rows
        report = (await runtime.artifact(root, "report"))["payload"]
        lead = (await runtime.get(root))["data"]["messages"]

    assert report == {
        "analysis": "the kernel commits each step atomically",
        "review": {"focus": "Focus on recovery tests.", "answer": "the crash recovery tests"},
    }
    results = tool_results(lead)
    # spawning returned at once and the lead worked before it joined anything
    assert results[0] == {"spawned": "architecture"} and results[1] == {"spawned": "tests"}
    assert results[2] == 42
    assert results[3]["children"] == {"architecture": "succeeded"}
    listed = {c["name"]: c for c in results[4]}
    assert listed["architecture"]["phase"] == "succeeded"
    assert listed["architecture"]["published"] == ["analysis"]
    assert listed["tests"]["phase"] in ("running", "waiting")  # still live after the join
    assert results[6] == {"sent": "steer", "to": "tests", "after": "this step commits"}
    told = [m["content"] for m in lead if m["role"] == "user"]
    assert any("children still live: tests" in t for t in told)
    inspected = results[7]
    assert inspected["phase"] == "waiting" and inspected["question"] == "Which tests matter most?"
    system = scenario.seen["LEAD"][0][0]["content"]
    assert "Working with other agents" in system
    assert "Working with other agents" not in scenario.seen["REVIEWER"][0][0]["content"]


async def test_a_spawn_over_the_limits_is_told_to_the_model() -> None:
    def step(messages, tools):
        turn = sum(1 for m in messages if m["role"] == "assistant")
        depth_note = [m["content"] for m in messages if m["role"] == "tool"]
        if turn == 0:
            return call("spawn_agent", name="deeper", agent="researcher", input={"task": "go"})
        if turn == 1 and '"spawned"' in depth_note[0]:  # the root joins its child first
            return call("join_agents", names=["deeper"])
        return say(json.dumps({"summary": depth_note[0] if depth_note else ""}))

    model = ScriptedModel(step_fn=step)
    async with runtime_for(model, limits={"max_depth": 1}) as runtime:
        root = await runtime.spawn("researcher", {}, root_id="r")
        rows = await runtime.run(root, timeout=20)
        assert {r["view"] for r in rows} == {"succeeded"}
        child = (await runtime.artifact(root, "analysis", "r/deeper"))["payload"]
    assert "refused: limit max_depth=1" in child["summary"]  # its own spawn was refused


async def test_spawn_inputs_are_checked_against_the_childs_schema() -> None:
    offered: list[list[dict[str, Any]]] = []

    def step(messages, tools):
        offered.append(tools)
        turn = sum(1 for m in messages if m["role"] == "assistant")
        if role(messages) != "LEAD":
            return say('{"summary": "ok"}' if role(messages) == "RESEARCHER" else '{"r": 1}')
        if turn == 0:  # the reviewer's schema wants a task: refused, told to the model
            return call("spawn_agent", name="r1", agent="reviewer", input={})
        if turn == 1:  # the researcher has no schema: an empty input is accepted
            return call("spawn_agent", name="a1", agent="researcher", input={})
        if turn == 2:
            return call("join_agents", names=["a1"])
        return say("{}")

    model = ScriptedModel(step_fn=step)
    runtime = runtime_for(model)
    reviewer = runtime.registry.agents["reviewer"]
    runtime.registry.agents["reviewer"] = dataclasses.replace(
        reviewer,
        description="Reviews tests.",
        input_schema={"type": "object", "required": ["task"]},
    )
    async with runtime:
        root = await runtime.spawn("lead", {}, root_id="s")
        rows = await runtime.run(root, timeout=10)
        messages = (await runtime.get(root))["data"]["messages"]
    assert {r["view"] for r in rows} == {"succeeded"}
    results = tool_results(messages)
    assert "input for reviewer is invalid" in results[0]["error"]
    assert results[1] == {"spawned": "a1"}
    menu = next(
        t["function"]["description"] for t in offered[0] if t["function"]["name"] == "spawn_agent"
    )
    assert "- reviewer: Reviews tests. Input schema:" in menu and "- researcher:" in menu
