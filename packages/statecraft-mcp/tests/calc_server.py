"""A small MCP server for the tests (in memory, stdio, or HTTP)."""

from __future__ import annotations

import sys

from mcp import types
from mcp.server import MCPServer

server = MCPServer("calc")


@server.tool(annotations=types.ToolAnnotations(readOnlyHint=True))
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


@server.tool(annotations=types.ToolAnnotations(idempotentHint=True))
def store(key: str, value: str) -> str:
    """Store a value under a key (idempotent)."""
    return f"{key}={value}"


@server.tool()
def send(message: str) -> str:
    """Send a message (no annotation: not safe to repeat)."""
    return "sent"


@server.tool(annotations=types.ToolAnnotations(readOnlyHint=True))
def boom() -> str:
    """Always fails."""
    raise ValueError("boom")


@server.tool(annotations=types.ToolAnnotations(readOnlyHint=True))
async def slow(seconds: float) -> str:
    """Answers after a while."""
    import asyncio

    await asyncio.sleep(seconds)
    return "done"


@server.tool()
def die() -> str:
    """Ends the server's process (stdio) in the middle of a call."""
    import os

    os._exit(1)


if __name__ == "__main__":
    if sys.argv[1:] == ["stdio"]:
        server.run("stdio")
