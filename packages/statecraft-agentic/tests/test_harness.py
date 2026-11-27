"""The agentic loop as a harness: context projection, compaction, steering, follow-ups.

What must hold
  * the model is sent a projection of the durable history: the system prompt and the task, a
    summary of compacted messages, then the rest; the history itself keeps every message;
  * a cut never separates a tool call from its result, so every request the model gets pairs
    each tool result with its call;
  * compaction is its own state, checkpointed like a turn: when the run faults after it and
    continues, the summary is not made again (one summary call per compaction);
  * a steer (USR) reaches the model on its next turn, after the tool calls of the current one;
  * a follow-up (USR) waits until the agent would finish, then the agent goes on with it and its
    last answer is the output;
  * project context files present in the workspace are part of the system prompt.
"""

from __future__ import annotations

import json
from typing import Any

from statecraft_agentic import AgenticAgent, ScriptedModel, add_model, call, say
from statecraft_agentic import register as agentic
from statecraft_agentic.context import (
    SUMMARIZE,
    boundaries,
    cut_for,
    estimate,
    project,
)

from statecraft import EffectError
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Worker(AgenticAgent):
    instruction = "Add numbers with the tool, one call at a time."


def runtime_with(model: ScriptedModel, context: dict[str, Any] | None = None):
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
        extensions={"agentic": {"context": context or {}}},
    )
    return runtime


def assistant(calls: list[str], text: str = "") -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": text or None,
        "tool_calls": [
            {"id": c, "type": "function", "function": {"name": "f", "arguments": "{}"}}
            for c in calls
        ],
    }


def result(call_id: str) -> dict[str, Any]:
    return {"role": "tool", "tool_call_id": call_id, "content": "ok"}


def paired(messages: list[dict[str, Any]]) -> bool:
    """Every tool result follows its call, and every call before the last message has a
    result."""
    called: set[str] = set()
    for m in messages:
        if m["role"] == "assistant":
            called |= {c["id"] for c in m.get("tool_calls") or []}
        if m["role"] == "tool" and m["tool_call_id"] not in called:
            return False
    answered = {m["tool_call_id"] for m in messages if m["role"] == "tool"}
    return called <= answered


HISTORY = [
    {"role": "system", "content": "sys"},
    {"role": "user", "content": "task"},
    assistant(["a"]),
    result("a"),
    assistant(["b", "c"]),
    result("b"),
    result("c"),
    {"role": "user", "content": "steer"},
    assistant(["d"]),
    result("d"),
]


def test_cuts_never_split_a_tool_group() -> None:
    assert boundaries(HISTORY) == [2, 4, 7, 8, 10]
    compaction = {"covered_until": 7, "summary": "did a, b, c", "count": 1}
    sent = project(HISTORY, compaction)
    assert [m["role"] for m in sent] == ["system", "user", "user", "user", "assistant", "tool"]
    assert "did a, b, c" in sent[2]["content"] and paired(sent)
    assert len(HISTORY) == 10  # the history is untouched
    for cut in boundaries(HISTORY):
        assert paired(project(HISTORY, {"covered_until": cut, "summary": "s", "count": 1}))
    assert cut_for(HISTORY, None, keep_tokens=1) == 8  # the latest boundary short of the end
    assert cut_for(HISTORY, None, keep_tokens=estimate(HISTORY[4:])) == 4
    assert cut_for(HISTORY, compaction, keep_tokens=10**6) is None


class Session:
    """A scripted model that adds 1 eight times (two calls in one turn once), then answers; it
    answers summary requests with a summary, and can fail once after the first compaction."""

    PAD = "thinking about the next number " * 12

    def __init__(self, fail_after_compaction: bool = False) -> None:
        self.summaries = 0
        self.requests: list[list[dict[str, Any]]] = []
        self.fail = fail_after_compaction
        self.turns = 0  # its own count: what it is sent is a projection
        self.model = ScriptedModel(step_fn=self.step)

    def step(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        if messages[0]["content"] == SUMMARIZE:
            self.summaries += 1
            return say(f"Summary {self.summaries}: the running total so far is kept in the tools.")
        if self.fail and self.summaries:
            self.fail = False
            raise EffectError("failed", "the model endpoint broke once")
        self.requests.append(messages)
        turns, self.turns = self.turns, self.turns + 1
        if turns == 2:
            return {
                "text": self.PAD,
                "calls": [("math__add", {"a": 1, "b": 1}), ("math__add", {"a": 2, "b": 2})],
            }
        if turns < 7:
            return {"text": self.PAD, "calls": [("math__add", {"a": turns, "b": 1})]}
        return say(json.dumps({"total": 8}))


async def test_compaction_projects_the_context_and_keeps_the_history() -> None:
    session = Session()
    async with runtime_with(session.model, {"max_tokens": 600, "keep_tokens": 150}) as runtime:
        root = await runtime.spawn("worker", {"goal": "add"})
        await runtime.run(root, timeout=20)
        assert (await runtime.artifact(root, "answer"))["payload"] == {"total": 8}
        data = (await runtime.get(root))["data"]
        states = [h["record"]["state"] for h in await runtime.history(root)]
    assert data["compaction"]["count"] >= 1 and states.count("compact") >= 1
    assert session.summaries == data["compaction"]["count"]
    later = [r for r in session.requests if any("[Summary" in str(m["content"]) for m in r)]
    assert later, "after a compaction the model is sent the summary"
    assert all(paired(r) for r in session.requests)
    assert all(
        r[0]["role"] == "system" and r[1]["content"] == data["messages"][1]["content"]
        for r in session.requests
    )
    assert max(len(r) for r in later) < len(data["messages"])
    tool_results = [m for m in data["messages"] if m["role"] == "tool"]
    assert len(tool_results) == 8  # seven turns of one call, one of two: all kept in the history


async def test_a_compaction_is_not_made_again_after_a_fault() -> None:
    session = Session(fail_after_compaction=True)
    async with runtime_with(session.model, {"max_tokens": 600, "keep_tokens": 150}) as runtime:
        root = await runtime.spawn("worker", {"goal": "add"})
        await runtime.run(root, timeout=20)
        record = await runtime.get(root)
        assert record["view"] == "suspended" and "broke once" in record["error"]
        assert session.summaries == record["data"]["compaction"]["count"] == 1
        await runtime.signal(root, "CONT")
        await runtime.run(root, timeout=20)
        data = (await runtime.get(root))["data"]
        assert (await runtime.get(root))["view"] == "succeeded"
    assert session.summaries == data["compaction"]["count"]  # no summary was made twice


async def test_a_steer_reaches_the_next_turn() -> None:
    seen: list[list[dict[str, Any]]] = []

    def step(messages, tools):
        seen.append(messages)
        turns = sum(1 for m in messages if m["role"] == "assistant")
        if turns == 0:
            return call("ask_person", question="Which number?")
        return say(json.dumps({"last": messages[-1]["content"]}))

    async with runtime_with(ScriptedModel(step_fn=step)) as runtime:
        root = await runtime.spawn("worker", {"goal": "add"})
        await runtime.run(root, timeout=10)
        [question] = await runtime.questions(root)
        await runtime.signal(root, ("USR", {"type": "steer", "text": "Use 7 instead."}))
        await runtime.answer(question["question_id"], "5")
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "answer"))["payload"] == {"last": "Use 7 instead."}
        data = (await runtime.get(root))["data"]
    roles = [m["role"] for m in data["messages"]]
    assert roles[-3:] == ["tool", "user", "assistant"]  # after the tool's result, before the turn
    assert data["steered"] == 1 and paired(seen[-1])


async def test_a_follow_up_runs_when_the_agent_would_finish() -> None:
    def step(messages, tools):
        asked = [m["content"] for m in messages if m["role"] == "user"]
        if asked[-1] == "Now double it.":
            return say('{"n": 2}')
        return say('{"n": 1}')

    model = ScriptedModel(step_fn=step)
    async with runtime_with(model) as runtime:
        root = await runtime.spawn("worker", {"goal": "one"}, root_id="f1")
        await runtime.signal(root, ("USR", {"type": "follow_up", "text": "Now double it."}))
        await runtime.signal(root, ("USR", {"type": "note", "text": "ignored"}))
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "answer"))["payload"] == {"n": 2}
        data = (await runtime.get(root))["data"]
        states = [h["record"]["state"] for h in await runtime.history(root)]
    answers = [m["content"] for m in data["messages"] if m["role"] == "assistant"]
    assert answers == ['{"n": 1}', '{"n": 2}'] and states.count("compact") == 0
    assert data["follow_ups"] == []


async def test_project_context_files_are_in_the_system_prompt() -> None:
    model = ScriptedModel([say("{}")])
    context = {"files": ["shared/AGENTS.md", "shared/missing.md"]}
    async with runtime_with(model, context) as runtime:
        files = await runtime.workspaces.open("p1")
        await files.put("shared/AGENTS.md", b"Always answer in JSON.\n")
        root = await runtime.spawn("worker", {}, root_id="p1")
        await runtime.run(root, timeout=10)
    system = model.seen[0][0]["content"]
    assert "Project context (shared/AGENTS.md):\nAlways answer in JSON." in system
    assert "missing.md" not in system
