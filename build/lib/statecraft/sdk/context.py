"""`ctx`: what a running state may do, and nothing else (architecture §6.2). The runtime builds
one for each step (`statecraft.runtime.context.StepContext`); this is its contract.

    await ctx.call("search.query", query="...")          # a permitted tool, through the runtime
    await ctx.write("report", payload, summary=...)       # staged; published by the step's commit
    await ctx.read("sources", by="collector")             # a committed artifact of the tree
    await ctx.files.read_text("shared/input/input.json")  # workspace files, with its rules
    ctx.effect_key("send")                                # stable across reruns of this state
    ctx.input, ctx.woke, ctx.signals, ctx.children        # durable inputs of this step
    ctx.signal("child", "STOP")                           # sent after the commit
    ctx.note(count=3)                                     # small facts kept on the record
    await ctx.describe("run-1")                           # another run's progress, read only

Spawning, joining, asking a person and sleeping are what a state returns, not ctx calls. There is
no model on ctx: a model call is a tool an extension registers.
"""

from __future__ import annotations

from typing import Any, Protocol


class Files(Protocol):
    async def read(self, path: str) -> bytes: ...
    async def read_text(self, path: str) -> str: ...
    async def exists(self, path: str) -> bool: ...
    async def write(self, path: str, data: bytes | str) -> None: ...
    async def list(self, prefix: str) -> list[str]: ...


class Context(Protocol):
    id: str
    name: str
    input: dict[str, Any]
    woke: dict[str, Any]
    children: dict[str, str]
    step: int
    state_name: str
    files: Files

    @property
    def signals(self) -> list[Any]: ...

    async def call(self, effect: str, /, **arguments: Any) -> Any: ...
    async def write(self, artifact: Any, payload: Any = None, **envelope: Any) -> str: ...
    async def read(self, kind: str, by: str | None = None) -> Any: ...
    async def has(self, kind: str, by: str | None = None) -> bool: ...
    def effect_key(self, name: str) -> str: ...
    def signal(self, target: str, signal: Any) -> None: ...
    def note(self, **entry: Any) -> None: ...
    async def describe(self, root: str) -> dict[str, Any]: ...
    async def children_view(self) -> list[dict[str, Any]]: ...
    def spawn_target(self, target: str) -> dict[str, Any] | None: ...
    async def spawn_problem(
        self, target: str, count: int = 1, input: dict[str, Any] | None = None
    ) -> str | None: ...


__all__ = ["Context", "Files"]
