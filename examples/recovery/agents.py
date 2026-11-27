"""The recovery sample: a three-stage pipeline that survives a killed process.

    pipeline   fetch ▸ transform ▸ publish; each stage records its run in `shared/log/`
               (keyed by `ctx.effect_key`, so a rerun of the same state has the same key)

`transform` is slow on its first attempt, so a test (or you) can kill the process while it runs.
The next process resumes from the store: `fetch`, committed, is not run again; `transform` runs
again with the same key; `publish` runs once.
"""

from __future__ import annotations

import asyncio
import os

from statecraft import Agent, Done, state


class Pipeline(Agent):
    start = "fetch"

    async def _log(self, ctx, stage: str) -> None:
        key = ctx.effect_key(stage).replace("/", "_")
        await ctx.files.write(f"shared/log/{key}-{os.getpid()}", stage)

    @state
    async def fetch(self, ctx, data):
        await self._log(ctx, "fetch")
        data["rows"] = list(range(ctx.input.get("rows", 5)))
        return "transform"

    @state
    async def transform(self, ctx, data):
        await self._log(ctx, "transform")
        marker = f"home/{ctx.id}/private/started"
        if ctx.input.get("slow") and not await ctx.files.exists(marker):
            await ctx.files.write(marker, "1")
            await asyncio.sleep(60)  # a long first attempt: kill the process now
        data["rows"] = [r * r for r in data["rows"]]
        return "publish"

    @state
    async def publish(self, ctx, data):
        await self._log(ctx, "publish")
        await ctx.write("result", {"rows": data["rows"]})
        return Done
