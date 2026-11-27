# statecraft-mcp

MCP servers as statecraft tool providers: `<provider>.<tool>`, with schemas from the server.
Enable with `plugins: [mcp]`.

```yaml
effect_providers:
  search: {provider: mcp, url: http://localhost:8001/mcp, session_header: X-Session-Id,
           policies: {query: read_only}}
  files:  {provider: mcp, command: python, args: [-m, my_server], trusted: true}
```

Policies come from the operator (`policies`); a `trusted` server's annotations are proposals
(read-only hint → `read_only`, idempotent hint → `idempotent`); anything else is
`non_repeatable` and refused until a policy is set. With `session_header`, each root gets its own
session. A server that cannot be reached is a startup warning, and its calls are `unavailable`.
