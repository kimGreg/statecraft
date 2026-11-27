"""The plugin's MCP server (stdio: `python -m notes_plugin.words_server`)."""

from __future__ import annotations

from mcp import types
from mcp.server import MCPServer

server = MCPServer("words")


@server.tool(annotations=types.ToolAnnotations(readOnlyHint=True))
def count(text: str) -> int:
    """Count the words of a text."""
    return len(text.split())


if __name__ == "__main__":
    server.run("stdio")
