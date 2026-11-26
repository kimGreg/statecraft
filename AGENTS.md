# Rules for this repository

statecraft is a state machine executor for agents, nothing more: light and stable. Its design is
docs/design.md.

- Owns the engine: records and their pure lifecycle rules, agents as state machines, the state
  store contract and its adapters, the executor, `ctx`, the agentic loop, views, the artifact
  envelope.
- Knows no application: an application supplies its tools, files, model, envelope, state and
  views through the ports.
- Imports only the standard library and its declared dependencies (`tests/test_standalone.py`).
- No UI, no deployment files.
- Tests use fakes; they never need a server or a model key.
- A change to the execution rules changes docs/design.md in the same commit.
