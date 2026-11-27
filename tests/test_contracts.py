"""Output contracts: what an agent publishes meets its type's contract, or is not published.

What must hold: with contracts, a payload that breaks its type's contract is refused before it is
staged, the state faults saying why, and nothing is published; a payload that meets it is
published; without contracts nothing is checked.
"""

from __future__ import annotations

from typing import Any

from statecraft.runtime.runtime import Runtime
from statecraft.sdk.agent import Agent, Done, state
from statecraft.testing import permissive_runtime


class Contracts:
    async def check(self, artifact_type: str, payload: dict[str, Any]) -> list[str]:
        return [] if "count" in payload else [f"{artifact_type}: count is required"]


class Writer(Agent):
    start = "write"

    @state
    async def write(self, ctx, data):
        await ctx.write("tally", dict(ctx.input))
        return Done


async def engine(tmp_path, contracts: Any) -> Runtime:
    e = permissive_runtime({"t:writer": Writer}, files_root=tmp_path, contracts=contracts)
    return await e.start()


async def test_a_broken_payload_is_refused_and_not_published(tmp_path) -> None:
    e = await engine(tmp_path, Contracts())
    root = await e.spawn("t:writer", {"other": 1})
    [row] = await e.run(root, scan_every=0.01)
    assert row["view"] == "suspended" and "count is required" in row["error"]
    assert (await e.store.load(root)).published == {}


async def test_a_payload_that_meets_it_is_published(tmp_path) -> None:
    e = await engine(tmp_path, Contracts())
    root = await e.spawn("t:writer", {"count": 2})
    [row] = await e.run(root, scan_every=0.01)
    assert row["view"] == "succeeded"
    assert (await e.artifact(root, "tally"))["payload"] == {"count": 2}


async def test_without_contracts_nothing_is_checked(tmp_path) -> None:
    e = await engine(tmp_path, None)
    root = await e.spawn("t:writer", {"other": 1})
    [row] = await e.run(root, scan_every=0.01)
    assert row["view"] == "succeeded"
