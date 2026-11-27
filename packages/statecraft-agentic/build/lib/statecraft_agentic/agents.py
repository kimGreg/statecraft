"""Reusable agents of the extension: ordinary registered agents, no built-in names.

agentic:decider    answers one question within a schema   input {question, facts, schema}
agentic:skill      runs one skill as its own process      input {skill, task, output?}
agentic:sequence   runs agents one after another          input {steps: [[agent, input], ...]}
agentic:fanout     runs agents in parallel, then joins    input {children: {name: [agent, input]}}
"""

from __future__ import annotations

import json
from typing import Any

from statecraft import Agent, ChildFailed, Done, Spawn, Wait, state

from .loop import SKILLS_SERVICE, AgenticAgent


class Decider(AgenticAgent):
    """Answers one question: input {question, facts, schema, default}; output `decision`
    {answer, reason}. Its parent joins it and reads the decision, or uses its default."""

    output_type = "decision"

    def offered(self, ctx) -> list[str]:
        return []

    def spawnable(self, ctx) -> list[str]:
        return []

    def _tools(self, ctx, data) -> list[dict[str, Any]]:
        return []  # it answers; it does not wander off to ask a person or read files

    async def task_instruction(self, ctx, data) -> str:
        return str(ctx.input.get("instruction") or "Decide the question from the facts given.")

    async def task(self, ctx) -> str:
        spec = {k: ctx.input.get(k) for k in ("question", "facts", "schema")}
        return (
            json.dumps(spec, ensure_ascii=False, indent=1)
            + '\n\nAnswer as {"answer": <matching the schema>, "reason": "<one sentence>"}.'
        )

    def check_output(self, ctx, data: dict[str, Any], payload: Any) -> str | None:
        if "answer" not in payload:
            return "no answer field"
        schema = ctx.input.get("schema") or {}
        try:
            import jsonschema

            jsonschema.validate(payload["answer"], schema)
        except Exception as exc:
            return f"the answer does not match its schema: {str(exc)[:200]}"
        return None


class SkillAgent(AgenticAgent):
    """Runs one skill as its own process: the skill named in the input is its instruction. Its
    model is offered only the tools the skill allows that the agent is granted (a skill never
    widens a grant; `extensions.agentic.skill_agent.tools` is the grant)."""

    def skills_of(self, ctx) -> list[str]:
        return [str(ctx.input["skill"])]

    def offered(self, ctx) -> list[str]:
        catalog = ctx.service(SKILLS_SERVICE)
        skill = catalog.get(str(ctx.input["skill"])) if catalog is not None else None
        allowed = set(skill.allowed_tools) if skill is not None else set()
        return [t for t in super().offered(ctx) if t in allowed]

    def _output_type(self, ctx) -> str | None:
        return str(ctx.input.get("output") or "result")

    async def task(self, ctx) -> str:
        return str(ctx.input.get("task") or json.dumps(ctx.input, ensure_ascii=False))


class Sequence(Agent):
    """Runs `steps` one after another; a step that does not succeed stops the sequence."""

    start = "next"

    @state
    async def next(self, ctx, data):
        index = data.get("index", 0)
        if index and ctx.woke.get("children", {}).get(f"s{index - 1}") != "succeeded":
            raise ChildFailed(f"step {index - 1} did not succeed")
        steps = ctx.input["steps"]
        if index == len(steps):
            await ctx.write("sequence", {"steps": len(steps)})
            return Done
        agent, given = steps[index]
        data["index"] = index + 1
        return Spawn({f"s{index}": (agent, given)}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=[f"s{data['index'] - 1}"], then="next", on_fault="next")


class FanOut(Agent):
    """Runs `children` in parallel; succeeds when all of them did."""

    start = "spread"

    @state
    async def spread(self, ctx, data):
        return Spawn({n: (a, i) for n, (a, i) in ctx.input["children"].items()}, then="join")

    @state
    async def join(self, ctx, data):
        return Wait(join=list(ctx.input["children"]), then="done", on_fault="done")

    @state
    async def done(self, ctx, data):
        results = dict(ctx.woke.get("children", {}))
        if any(v != "succeeded" for v in results.values()):
            raise ChildFailed(str(results))
        await ctx.write("fanout", {"children": results})
        return Done


__all__ = ["Decider", "FanOut", "Sequence", "SkillAgent"]
