"""Testing helpers: a runtime in memory, run until quiet.

runtime = memory_runtime()
runtime.registrar.agents.register("add", Add, tools=["math.add"])
runtime.registrar.tools.register(add)
await runtime.start()
root = await runtime.spawn("add", {"a": 1, "b": 2})
await runtime.run(root)
"""

from __future__ import annotations

from typing import Any

from ..kernel.effects import EffectError
from ..providers.stores import MemoryStore
from ..providers.workspaces import MemoryWorkspace
from ..runtime.runtime import Runtime


def memory_runtime(**options: Any) -> Runtime:
    """Memory store and workspace, no background executors (use `runtime.run(root)`)."""
    return Runtime(
        store=MemoryStore(),
        workspace=MemoryWorkspace(),
        **{"executors": 0, "scan_every": 0.01, **options},
    )


class FunctionProvider:
    """A provider whose operations are plain functions `fn(arguments) -> result` (tests); raise
    `EffectError` to simulate a classified failure."""

    def __init__(self, functions: dict[str, Any], *, concurrency: int | None = None) -> None:
        self.functions = dict(functions)
        self.concurrency = concurrency
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any:
        self.calls.append((operation, arguments))
        if operation not in self.functions:
            raise EffectError("invalid", f"no operation {operation}", sent=False)
        result = self.functions[operation](arguments)
        if hasattr(result, "__await__"):
            result = await result
        return result


def permissive_runtime(
    agents: dict,
    *,
    tools: dict | None = None,
    store=None,
    files_root=None,
    views=None,
    **options,
):
    """A runtime with test agents registered permissively (any tool, any spawn, read-only
    attachments) and test tools
    as plain functions: `tools={"math.square": (fn, "read_only")}`. Not started."""
    from ..kernel.effects import EffectPolicy
    from ..providers.workspaces import LocalWorkspace
    from ..sdk.effect import EffectDefinition

    runtime = Runtime(
        store=store or MemoryStore(),
        workspace=LocalWorkspace(files_root) if files_root else MemoryWorkspace(),
        views=views,
        **{"executors": 0, "scan_every": 0.01, **options},
    )
    r = runtime.registrar
    for name, cls in agents.items():
        r.agents.register(name, cls, tools=["*"], spawns=["*"], attach="ro")
    if tools:
        functions = {}
        for full, (fn, policy) in tools.items():
            functions[full] = fn
            r.tools.register(
                EffectDefinition(
                    name=full, policy=EffectPolicy.of(policy), provider="test", operation=full
                )
            )
        r.providers.register("test", FunctionProvider(functions))
    return runtime


__all__ = ["FunctionProvider", "memory_runtime", "permissive_runtime"]
