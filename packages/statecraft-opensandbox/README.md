# statecraft-opensandbox

Shell commands in [OpenSandbox](https://github.com/opensandbox-group/OpenSandbox) sandboxes as a
durable statecraft effect: `<provider>.exec` with policy `deduplicated`. Enable with
`plugins: [opensandbox]`.

```yaml
effect_providers:
  sh: {provider: opensandbox, endpoint: http://127.0.0.1:8080, api_key: ${OSB_KEY},
       image: python:3.12-slim, scope: agent, receipts: ./state/sandbox-receipts.db}
```

- One sandbox per agent by default (`scope: run` or `call`), created with the runtime's mount
  plan: the workspace's `shared` folder at `/workspace` (read-only for an agent attached without
  a write grant) and the agent's private folder at `/home/agent`. The workspace must be
  mountable (`workspace: {provider: local}`); OpenSandbox's `allowed_host_paths` must cover it.
- Receipts (`receipts`) are a SQLite file the provider owns, outside every workspace and
  sandbox: one call identity runs one command, even across retries, CONT and several executors.
- Recovery: a command whose submit answer was lost, whose record the sandbox lost, or that was
  cancelled midway is `indeterminate` (the agent faults; an operator settles it with
  `resolve_call`); a running command is reattached after a restart.
- Cancelling interrupts the command and holds capacity until the sandbox says it stopped.
  `destroy_on_cancel: true` destroys the sandbox instead, which also ends processes that left
  the command's process group (the provider then declares `tree_termination`).
- `backend: fake` runs commands on this machine without isolation (tests).

Before relying on it as a security boundary (A2 findings): give each sandbox its own network or
an `execd` credential, run commands as a non-root user (or in a user namespace) with `nosuid`
mounts, and close egress by default.
