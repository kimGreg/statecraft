"""The research sample: parallel work at three levels.

research   spawns one `searcher` per topic (children in parallel), joins, writes the report
searcher   two searches at once inside one state (`asyncio.gather` over `ctx.call`), then a
           CPU-heavy ranking that runs in a process, off the event loop
"""

from __future__ import annotations

import asyncio
import time

from statecraft import Agent, ChildFailed, Done, Spawn, Wait, state


class Research(Agent):
    start = "spread"

    @state
    async def spread(self, ctx, data):
        data["started"] = time.time()
        return Spawn(
            {
                f"t{n}": ("searcher", {"topic": topic})
                for n, topic in enumerate(ctx.input["topics"])
            },
            then="join",
        )

    @state
    async def join(self, ctx, data):
        return Wait(join=[f"t{n}" for n in range(len(ctx.input["topics"]))], then="report")

    @state
    async def report(self, ctx, data):
        failed = [n for n, s in ctx.woke["children"].items() if s != "succeeded"]
        if failed:
            raise ChildFailed(f"searchers failed: {failed}")
        sections = {}
        for n, topic in enumerate(ctx.input["topics"]):
            sections[topic] = (await ctx.read("findings", by=f"t{n}")).payload["top"]
        await ctx.write("report", {"sections": sections})
        return Done


class Searcher(Agent):
    start = "search"

    @state
    async def search(self, ctx, data):
        started = time.monotonic()
        web, papers = await asyncio.gather(
            ctx.call("search.web", query=ctx.input["topic"]),
            ctx.call("search.papers", query=ctx.input["topic"]),
        )
        data["documents"] = [*web, *papers]
        data["search_seconds"] = time.monotonic() - started
        return "rank"

    @state
    async def rank(self, ctx, data):
        ranked = await ctx.call("text.rank", documents=data["documents"])
        await ctx.write("findings", {"top": ranked[:3], "search_seconds": data["search_seconds"]})
        return Done
