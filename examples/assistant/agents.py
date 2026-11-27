"""The assistant sample: an agentic agent using MCP tools and a skill, checked by a decider.

    quote       (native) spawn `assistant` to price the items ▸ spawn `agentic:decider` to approve
                ▸ write the quote
    assistant   (agentic) its instruction is the pinned `arithmetic-review` skill; it calls the
                MCP calculator through `calc.add` / `calc.multiply`

The model is scripted unless `STATECRAFT_REAL_MODEL` names an OpenRouter model (for example
`openai/gpt-oss-20b`, with OPENROUTER_API_KEY set): the same agents then run on a real model.
"""

from __future__ import annotations

import json
import os
from typing import Any

from statecraft_agentic import AgenticAgent, ScriptedModel, add_model, call, openrouter, say

from statecraft import Agent, Done, Spawn, Wait, state


class Assistant(AgenticAgent):
    """Prices a quote; its instruction is its first skill."""


class Quote(Agent):
    start = "price"

    @state
    async def price(self, ctx, data):
        return Spawn({"pricer": ("assistant", {"items": ctx.input["items"]})}, then="priced")

    @state
    async def priced(self, ctx, data):
        return Wait(join=["pricer"], then="approve")

    @state
    async def approve(self, ctx, data):
        data["price"] = (await ctx.read("priced", by="pricer")).payload
        return Spawn(
            {
                "judge": (
                    "agentic:decider",
                    {
                        "question": "Approve this quote?",
                        "facts": data["price"],
                        "schema": {"enum": ["approve", "review"]},
                    },
                )
            },
            then="approved",
        )

    @state
    async def approved(self, ctx, data):
        return Wait(join=["judge"], then="write", on_fault="write")

    @state
    async def write(self, ctx, data):
        ok = ctx.woke["children"].get("judge") == "succeeded"
        decision = (await ctx.read("decision", by="judge")).payload if ok else {"answer": "review"}
        await ctx.write("quote", {**data["price"], "decision": decision["answer"]})
        return Done


def scripted(messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
    """A stand-in model: prices the quote with the tools, then approves it."""
    system = messages[0]["content"]
    if "Decide the question" in system:
        return say('{"answer": "approve", "reason": "the total was computed with tools"}')
    items = json.loads(messages[1]["content"])["items"]
    done = [m for m in messages if m["role"] == "tool"]
    lines = [i["qty"] * i["price"] for i in items]
    if len(done) < len(items):
        item = items[len(done)]
        return call("calc__multiply", a=item["qty"], b=item["price"])
    if len(done) == len(items) and len(items) > 1:
        return call("calc__add", a=lines[0], b=sum(lines[1:]))
    total = float(done[-1]["content"])
    return say(json.dumps({"total": total, "lines": len(items)}))


def register(registrar: Any) -> None:
    """The sample's plugin: its model (scripted, or a real one) for the agentic extension."""
    real = os.environ.get("STATECRAFT_REAL_MODEL")
    add_model(registrar, "default", openrouter(real) if real else ScriptedModel(step_fn=scripted))
