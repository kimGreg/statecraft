# Samples

Each sample is a folder with its agents, its tools, a `statecraft.yaml`, and a test in
`tests/test_samples.py`. Run them from the repository root.

| Sample | Shows |
|---|---|
| [orders](orders) | native state machines; Python tools with policies; a person approves a backorder; a payment whose response is lost is retried with its idempotency key and charged once; a child's fault is decided by a person |
| [research](research) | children in parallel; two tool calls at once inside a state; a CPU-heavy ranking in a process; slots |
| [assistant](assistant) | an agentic agent pricing a quote with MCP tools (stdio) under a pinned skill, approved by a decider; a scripted model, or a real one with `STATECRAFT_REAL_MODEL=openai/gpt-oss-20b` and an OpenRouter key |
| [recovery](recovery) | a process killed in the middle of a state resumes from the store: the committed state is not run again, the unfinished one reruns with the same key |
| [shared_coding](shared_coding) | a lead and two workers in one shared workspace, each in its own sandbox (`statecraft-opensandbox`, fake backend by default): asynchronous spawn, selective join, folder ownership, repeat-safe edits, shell commands as durable effects; a lost submit answer leaves a worker for an operator, and the lead waits for it |
| [notes_plugin](notes_plugin) | a package unrelated to statecraft, installed and enabled by name (`statecraft.plugins` entry point): an agent, a Python tool, an MCP server, a skill |

```bash
statecraft run order --config examples/orders/statecraft.yaml \
  --input '{"order_id": "o1", "item": "lamp", "qty": 2, "amount": 30, "address": "1 Main St"}'
statecraft run research --config examples/research/statecraft.yaml \
  --input '{"topics": ["robots", "batteries"]}'
statecraft run quote --config examples/assistant/statecraft.yaml \
  --input '{"items": [{"qty": 2, "price": 3.5}, {"qty": 1, "price": 10}]}'
pip install -e examples/notes_plugin
statecraft run notes:digest --config examples/notes_plugin/statecraft.yaml \
  --input '{"notes": ["buy milk", "call Ann"]}'
python -m pytest -q examples
```
