"""An agentic run under a tree budget (roadmap E3): each model request holds its estimate in
`tokens` (and `usd` when the client has prices) and settles with the reply's usage; a request
that would pass the budget is refused before it is sent and the agent stops on it."""

from __future__ import annotations

from statecraft_agentic import AgenticAgent, ModelProvider, ScriptedModel, add_model, call, say
from statecraft_agentic import register as agentic

from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Worker(AgenticAgent):
    instruction = "Add numbers with the tool."


def runtime_with(model: ScriptedModel):
    runtime = memory_runtime()
    agentic(runtime.registrar)
    add_model(runtime.registrar, "default", model)
    runtime.registrar.tools.register(add)
    runtime.registrar.agents.register(
        "worker", Worker, tools=["model.default", "math.add"], writes=["answer"]
    )
    return runtime


def script() -> ScriptedModel:
    return ScriptedModel([call("math.add", a=1, b=2), say('{"sum": 3}')])


async def test_tokens_are_held_and_settled_from_usage() -> None:
    model = script()
    async with runtime_with(model) as rt:
        await rt.spawn("worker", root_id="w", budget={"tokens": 1_000_000})
        await rt.run("w", timeout=10)
        assert (await rt.get("w"))["view"] == "succeeded"
        found = await rt.budget("w")
    assert found["held"] == {}
    assert 0 < found["spent"]["tokens"] < 2 * 4096  # usage, not the reply allowance


async def test_a_request_over_budget_is_not_sent() -> None:
    model = script()
    async with runtime_with(model) as rt:
        await rt.spawn("worker", root_id="s", budget={"tokens": 100})
        await rt.run("s", timeout=10)
        record = await rt.get("s")
    assert record["view"] == "suspended" and "budget tokens=100" in record["error"]
    assert model.seen == []  # refused before it was sent


def test_prices_give_usd() -> None:
    from statecraft_agentic import ChatModel

    provider = ModelProvider(reply_tokens=10)
    provider.add(
        "m", ChatModel(name="m", base_url="http://x", prices={"prompt": 1e-6, "completion": 2e-6})
    )
    held = provider.estimate_of("m", {"messages": [{"role": "user", "content": "hi"}]})
    assert held["tokens"] > 10 and held["usd"] > 0
    used = provider.charge_of("m", {"usage": {"prompt_tokens": 100, "completion_tokens": 50}})
    assert used == {"tokens": 150, "usd": 100 * 1e-6 + 50 * 2e-6}
    assert provider.charge_of("m", {"usage": {"total_tokens": 7, "cost": 0.5}}) == {
        "tokens": 7,
        "usd": 0.5,
    }
