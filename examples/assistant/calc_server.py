"""An MCP server for the assistant sample (run over stdio: `python calc_server.py`)."""

from __future__ import annotations

from mcp import types
from mcp.server import MCPServer

server = MCPServer("calc")


@server.tool(annotations=types.ToolAnnotations(readOnlyHint=True))
def add(a: float, b: float) -> float:
    """Add two numbers."""
    return a + b


@server.tool(annotations=types.ToolAnnotations(readOnlyHint=True))
def multiply(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b


if __name__ == "__main__":
    server.run("stdio")
