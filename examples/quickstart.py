"""Two children add in parallel; the parent waits for both, asks a person, then scales the sum.

    python examples/quickstart.py

With `Runtime.local(folder)` the state lives in SQLite: kill the process at any point, run it
again, and it continues from the last committed step.
"""

from __future__ import annotations

import asyncio
import tempfile

from statecraft import Agent, Done, Runtime, Spawn, Wait, state, tool


@tool(name="math.add", policy="read_only")
async def add(a: float, b: float) -> float:
    """Add two numbers."""
    return a + b


class Add(Agent):
    start = "add"

    @state
    async def add(self, ctx, data):
        await ctx.write(
            "sum", {"value": await ctx.call("math.add", a=ctx.input["a"], b=ctx.input["b"])}
        )
        return Done


class Total(Agent):
    start = "split"

    @state
    async def split(self, ctx, data):
        return Spawn(
            {"left": ("add", {"a": 1, "b": 2}), "right": ("add", {"a": 3, "b": 4})}, then="wait"
        )

    @state
    async def wait(self, ctx, data):
        return Wait(join=["left", "right"], then="ask")

    @state
    async def ask(self, ctx, data):
        for name in ("left", "right"):
            data[name] = (await ctx.read("sum", by=name)).payload["value"]
        return Wait(
            answer={
                "name": "scale",
                "question": "Multiply the total by?",
                "schema": {"type": "integer"},
                "default": 1,
            },
            then="finish",
        )

    @state
    async def finish(self, ctx, data):
        await ctx.write("total", {"value": (data["left"] + data["right"]) * ctx.woke["answer"]})
        return Done


async def main(folder: str) -> None:
    runtime = Runtime.local(folder, executors=0)
    runtime.registrar.tools.register(add)
    runtime.registrar.agents.register("add", Add, tools=["math.add"])
    runtime.registrar.agents.register("total", Total, spawns=["add"])
    async with runtime:
        root = await runtime.spawn("total", root_id="job1")
        await runtime.run(root)  # until a person must answer
        [question] = await runtime.questions(root)
        print("asked:", question["question"])
        await runtime.answer(question["question_id"], 10)
        await runtime.run(root)
        print("total:", (await runtime.artifact(root, "total"))["payload"]["value"])
        for row in await runtime.tree(root):
            print(row["id"], row["view"])


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as folder:
        asyncio.run(main(folder))
