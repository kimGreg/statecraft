"""statecraft-mcp: MCP servers as statecraft tool providers (streamable HTTP, stdio, in memory).

plugins: [mcp]
effect_providers:
  search: {provider: mcp, url: http://localhost:8001/mcp, session_header: X-Session-Id,
           policies: {query: read_only}}
"""

from .provider import MCPProvider, register

__all__ = ["MCPProvider", "register"]
