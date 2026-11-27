"""The plugin's agent.

notes:digest   normalize (Python tool) ▸ count words (MCP tool) ▸ [slow on its first attempt,
               so a test can kill the process] ▸ spawn `agentic:skill` with the plugin's
               skill ▸ write the digest
"""

from __future__ import annotations

import asyncio

from statecraft import Agent, ChildFailed, Done, Spawn, Wait, state


class Digest(Agent):
    start = "normalize"

    @state
    async def normalize(self, ctx, data):
        data["notes"] = await ctx.call("notes.normalize", notes=ctx.input["notes"])
        return "count"

    @state
    async def count(self, ctx, data):
        data["words"] = await ctx.call("words.count", text=" ".join(data["notes"]))
        marker = f"home/{ctx.id}/private/counted"
        if ctx.input.get("slow") and not await ctx.files.exists(marker):
            await ctx.files.write(marker, "1")
            await asyncio.sleep(60)  # kill the process here; the next one reruns `count`
        return Spawn(
            {
                "summary": (
                    "agentic:skill",
                    {
                        "skill": "summarize-notes",
                        "task": "\n".join(data["notes"]),
                        "output": "summary",
                    },
                )
            },
            then="wait",
        )

    @state
    async def wait(self, ctx, data):
        return Wait(join=["summary"], then="write")

    @state
    async def write(self, ctx, data):
        if ctx.woke["children"].get("summary") != "succeeded":
            raise ChildFailed("the summary did not succeed")
        summary = (await ctx.read("summary", by="summary")).payload
        await ctx.write(
            "digest",
            {"notes": len(data["notes"]), "words": data["words"], "summary": summary["summary"]},
        )
        return Done
