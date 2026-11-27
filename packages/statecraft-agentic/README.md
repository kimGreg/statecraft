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
    extensions: {agentic: {model: default, tools: [search.query], skills: [review]}}
```
