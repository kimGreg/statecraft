# statecraft

A durable execution runtime for composable agents: checkpointed state machines whose steps commit
atomically to a state store, with tools behind a contract, people in the loop, and recovery from
any crash. Start with the [README](https://github.com/kimGreg/statecraft#readme) (quick start,
configuration, packages), then:

- [Guarantees](guarantees.md): exactly-once logical commits, and what is not exactly once.
- [Writing a provider](providers.md): effect providers, stores and workspaces, with the
  conformance kits.
- [Stability and versions](stability.md): the stable surface, deprecations, store formats and the
  compatibility matrix.
- [Design](design.md) (how a step runs) and [Architecture](architecture.md) (what surrounds it).
