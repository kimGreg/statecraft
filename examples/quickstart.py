"""Two children add in parallel; the parent waits for both, asks a person, then sums.

    python examples/quickstart.py

Run it with a SQLite store and kill it at any point: run it again and it continues from the
last committed step.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from statecraft import Agent, Catalog, Done, Engine, Spawn, Wait, state
from statecraft.stores import SqliteStore
from statecraft.testing import MemoryTools
from statecraft.workspace import LocalFiles


class Add(Agent):
    start = "add"

    @state
    async def add(self, ctx, data):
        result = await ctx.call("math.add", a=ctx.input["a"], b=ctx.input["b"])
        await ctx.write("sum", {"value": result["sum"]})
        return Done


class Total(Agent):
    start = "split"

    @state
    async def split(self, ctx, data):
        children = {"left": ("calc:add", {"a": 1, "b": 2}), "right": ("calc:add", {"a": 3, "b": 4})}
        return Spawn(children, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["left", "right"], then="ask")

    @state
    async def ask(self, ctx, data):
        for name in ("left", "right"):
            data[name] = (await ctx.read("sum", by=name)).payload["value"]
        question = {
            "name": "scale",
            "question": "Multiply the total by?",
            "schema": {"type": "integer"},
            "default": 1,
        }
        return Wait(answer=question, then="finish")

    @state
    async def finish(self, ctx, data):
        await ctx.write("total", {"value": (data["left"] + data["right"]) * ctx.woke["answer"]})
        return Done


async def main(folder: Path) -> None:
    engine = Engine(
        store=SqliteStore(folder / "state.db"),
        files=LocalFiles(folder / "workspaces"),
        tools=MemoryTools(handlers={"math.add": lambda a: {"sum": a["a"] + a["b"]}}),
        catalog=Catalog({}, extra={"calc:add": (Add, None), "calc:total": (Total, None)}),
    )
    root = await engine.spawn("calc:total", root_id="job1")
    await engine.run(root)  # runs until a person must answer
    [question] = await engine.questions(root)
    print("asked:", question["question"])
    await engine.answer(question["question_id"], 10)
    await engine.run(root)
    print("total:", (await engine.artifact(root, "total"))["payload"]["value"])
    for row in await engine.table(root):
        print(row["agent"], row["view"])


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as folder:
        asyncio.run(main(Path(folder)))
