"""The harness on a real model: a long tool session compacted, then a follow-up.

What must hold: with a small context budget the agent compacts at least once and still finishes
the additions with the right total (the summary carries what the model needs); a follow-up given
during the run is done after the first answer, and its answer is the output.

Runs only with `STATECRAFT_REAL_MODEL=1` and an OpenRouter key (`OPENROUTER_API_KEY`): it costs
money and needs the network. The model is `STATECRAFT_MODEL` (default openai/gpt-oss-20b).
"""

from __future__ import annotations

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
    "Start from 0 and add, in this order and one math__add call at a time (never two calls in "
    "one turn), each of these numbers to the running total: 3, 5, 7, 11, 13, 17, 19, 23. "
    'When all eight are added, answer with {"total": <the total>} and nothing else.'
)


class Adder(AgenticAgent):
    instruction = "You are a careful calculator. Use the math__add tool for every addition."
    max_model_turns = 40


async def test_a_long_session_is_compacted_and_followed_up() -> None:
    runtime = memory_runtime()
    r = runtime.registrar
    agentic(r)
    add_model(r, "default", openrouter(os.environ.get("STATECRAFT_MODEL", "openai/gpt-oss-20b")))
    r.tools.register(add)
    r.agents.register(
        "adder",
        Adder,
        tools=["model.default", "math.add"],
        writes=["answer"],
        extensions={"agentic": {"context": {"max_tokens": 600, "keep_tokens": 150}}},
    )
    async with runtime:
        root = await runtime.spawn("adder", {"task": TASK}, root_id="real")
        follow = 'Now add 100 to that total with one math__add call and answer {"total": <it>}.'
        await runtime.signal(root, ("USR", {"type": "follow_up", "text": follow}))
        await runtime.run(root, timeout=600)
        record = await runtime.get(root)
        assert record["view"] == "succeeded", record["error"]
        data = record["data"]
        assert (data.get("compaction") or {}).get("count", 0) >= 1, "no compaction happened"
        assert (await runtime.artifact(root, "answer"))["payload"]["total"] == 198
        added = [m for m in data["messages"] if m["role"] == "tool"]
        assert len(added) >= 9  # every addition is in the history, compacted or not
