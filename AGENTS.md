# Rules for this repository

statecraft is a durable execution runtime for composable agents. Its execution design is
docs/design.md; what surrounds the kernel is docs/architecture.md.

- Layers point one way: kernel ← sdk ← providers ← runtime ← server, client, cli, testing
  (`tests/test_layers.py`). Extensions (`packages/`) use the public API only; statecraft never
  imports them.
- The kernel knows no model, prompt, skill, tool protocol or storage technology. Model-driven
  behaviour, MCP and the console are extensions.
- Every external call is a tool with a declared policy; never add a path that bypasses the
  effect manager.
- No application or domain names anywhere; samples use neutral examples.
- Tests use fakes and need no server or model key (the real-model sample test is opt-in).
- A change to the execution rules changes docs/design.md; a change to the parts around it
  changes docs/architecture.md, in the same commit.
