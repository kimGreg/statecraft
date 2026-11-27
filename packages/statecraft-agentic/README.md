# statecraft-agentic

Model-driven agents as ordinary statecraft state machines. Enable with `plugins: [agentic]`.

- `AgenticAgent`: `prepare ▸ think ▸ act_one ▸ wait_result ▸ finish`; one tool call per
  transition, so a crash reruns only the unfinished model turn or call.
- Models are tools (`model.<name>`, policy `read_only`, not deterministic, metered) carried out by
  `ModelProvider` with `ChatModel` (OpenAI-compatible; `openrouter(...)` with zero retention) or
  `ScriptedModel` (tests).
- Skills (Agent Skills format) are pinned when an agent starts, with an immutable copy in the
  workspace; a skill never widens what an agent may call.
- Reusable agents: `agentic:decider`, `agentic:skill`, `agentic:sequence`, `agentic:fanout`.

The loop is a harness over the runtime's durable execution:

- **Context projection.** The conversation in `data["messages"]` is the durable history and only
  grows; each model turn is sent a projection of it: the system prompt and the task, a summary
  of compacted messages, then the rest (`context.project`).
- **Compaction.** When the projection is over the budget (`context.max_tokens`; measured from the
  prompt tokens the model reported for its last turn, plus an estimate of what was added),
  `compact` summarizes the oldest messages with the model. It is its own state: once committed, a
  crash or fault never makes the summary again. A cut never separates a tool call from its
  result, and `context.keep_tokens` of recent messages stay as they are.
- **Steering and follow-ups.** `("USR", {"type": "steer", "text": ...})` reaches the model on its
  next turn, after the current turn's tool calls; `("USR", {"type": "follow_up", "text": ...})`
  waits until the agent would finish, then the agent goes on with it (its last answer is the
  output). Signals are durable, so a steer sent before a crash is not lost. Other USR messages go
  to `on_signal`.
- **Project context.** `context.files` (workspace paths such as `shared/AGENTS.md`) are added to
  the system prompt when present.

```yaml
plugins: [agentic]
extensions:
  agentic:
    models: {default: {provider: openrouter, model: openai/gpt-oss-20b}}
    skill_paths: [./skills]
agents:
  researcher:
    entry: my_agents:Researcher            # a subclass of AgenticAgent
    tools: [model.default, search.query]
    extensions:
      agentic:
        model: default
        tools: [search.query]
        skills: [review]
        context: {max_tokens: 60000, keep_tokens: 15000, files: [shared/AGENTS.md]}
```

```python
await runtime.signal(root, ("USR", {"type": "steer", "text": "Write the tests first."}))
await runtime.signal(root, ("USR", {"type": "follow_up", "text": "Then update the README."}))
```

Tests: `tests/test_harness.py` (scripted); `tests/test_harness_real_model.py` runs a long session
that must compact, then a follow-up, on a real model (`STATECRAFT_REAL_MODEL=1` and an OpenRouter
key; `STATECRAFT_MODEL` picks the model, default `openai/gpt-oss-20b`).
