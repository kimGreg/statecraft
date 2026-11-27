# Writing a provider

A provider is any object the runtime calls through a port; it never imports the runtime. Three
kinds, each with a public conformance kit that runs the same tests the built-in ones pass.

## Effect providers (tools)

```python
class MyProvider:
    capabilities = frozenset({"deduplicated", "reconcile"})   # optional contracts
    concurrency = 4                                           # optional: calls at once
    async def discover(self): return [EffectDefinition(name="my.op", policy=...)]
    async def invoke(self, operation, arguments, context): ...
    async def reconcile(self, operation, key, context): ...   # with "reconcile"
```

- `context` (`statecraft.ports.effects.EffectContext`): root, agent, step, state, workspace,
  `key` (the call's identity for keyed policies), deadline, attempt, `scope` (what the agent may
  expose: workspace, shared ro/rw, private, mounts), `work` (register work a cancel cannot stop).
- Raise `EffectError(cls, message, sent=...)` with a class from `classify`/`classify_status`;
  `sent=False` only when the request provably was not sent.
- Optional: `bind(view)` (a read-only runtime view: `ended`, `host_path`), `cancel`,
  `estimate_of(operation, arguments)` and `charge_of(operation, result)` (budgets, for metered
  effects), `usage_of(result)` (crossings).
- Capabilities: `deduplicated`, `reconcile`, `remote_work`, `tree_termination`,
  `shared_capacity`; `capacity_scope` says where `concurrency` holds; `workspace_requires`
  (e.g. `mountable`) is checked at start.

Conformance: `statecraft.testing.effect_contract` (an `effect_harness` fixture; the baseline runs
for every provider, each capability's tests when declared).

## State stores

A store subclasses `statecraft.kernel.store.StateStore` and gives `transaction(write)` yielding
the adapter tables (`Tables`): records, leases, inbox, events, history, calls, settings, and the
upkeep operations. The kernel does fencing, CAS and atomicity on top; the adapter makes each
transaction atomic and isolated. Offer it to configuration with an entry point:

```toml
[project.entry-points."statecraft.stores"]
mystore = "my_package:from_config"      # from_config(settings, base_folder) -> StateStore
```

Conformance: `statecraft.testing.store_contract` (a `store` fixture). `statecraft-postgres` runs
it unchanged.

## Workspaces

A workspace provider opens `Files` (`get`, `put`, `create` (create-only), `exists`, `list`) per
workspace and may declare `mountable` (`host_path`), `internal_links` and `collectable`
(`remove`, `modified`, `prune`: what garbage collection needs).

Conformance: `statecraft.testing.workspace_contract` (a `workspace` or `files` fixture; each
capability's tests when declared).
