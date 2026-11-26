"""Testing agents without servers: tools answered by plain functions.

    from statecraft.testing import MemoryTools

    tools = MemoryTools(handlers={"math.add": lambda a: {"sum": a["a"] + a["b"]}})
    engine = Engine(..., tools=tools, ...)
    tools.calls                                  # every call, in order

A tool with no handler fails with `ToolFailed` (code `not_found`), as a transport raises for a
tool's error result.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

Handler = Callable[[dict[str, Any]], Any]


class ToolFailed(RuntimeError):
    """A tool answered with an error (what a transport raises for an error result)."""

    def __init__(self, server: str, tool: str, code: str, message: str) -> None:
        super().__init__(f"{server}.{tool}: {code}: {message}")
        self.server, self.tool, self.code, self.message = server, tool, code, message


@dataclass
class MemoryTools:
    handlers: dict[str, Handler] = field(default_factory=dict)
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def call(self, server: str, tool: str, arguments: dict[str, Any] | None = None) -> Any:
        arguments = dict(arguments or {})
        self.calls.append({"server": server, "tool": tool, "arguments": arguments})
        handler = self.handlers.get(f"{server}.{tool}")
        if handler is not None:
            return handler(arguments)
        return self.missing(server, tool, arguments)

    def missing(self, server: str, tool: str, arguments: dict[str, Any]) -> Any:
        """A call no handler answers (a subclass may answer whole servers here)."""
        raise ToolFailed(server, tool, "not_found", "no such tool in MemoryTools")


__all__ = ["Handler", "MemoryTools", "ToolFailed"]
