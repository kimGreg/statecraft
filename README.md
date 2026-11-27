# statecraft

A durable execution runtime for composable agents. Bring your own agents, tools, models and
storage; statecraft manages execution, concurrency, persistence, coordination and recovery.

Agents are checkpointed state machines. Every step commits atomically to a state store, so a run
survives crashes, restarts and deploys: committed steps are never lost, and an unfinished state
simply runs again. Agents depend on each other only by spawning and joining; people answer
questions; signals stop, continue and end agents; a parent decides what a child's fault means.

- **Small kernel.** Pure lifecycle rules, fenced commits (a stale or dead executor can never
  commit), durable intents carried out exactly once. No model, prompt or skill inside.
- **Tools with a contract.** Every external call goes through one effect manager: permission,
  schemas, deadlines, a declared retry policy (`read_only`, `idempotent`, `deduplicated`,
  `non_repeatable`), classified failures, and a record of the call in the step's history.
- **Concurrency.** Agents run in parallel within slots; a waiting agent holds none; tool calls run
  in parallel inside a state; blocking and CPU-heavy tools run in threads or processes; KILL and
  a lost lease cancel what is in flight.
- **Registries.** Agents and tools register by Python API, configuration or installed plugins;
  each instance pins the code and tool contracts it started with.
- **Embedded or served.** One `Runtime` API in your process, or `statecraft serve` over HTTP with
  a client and a CLI; same semantics.
- **Extensions.** `statecraft-agentic` (model-driven agents as state machines, models as tools,
  skills), `statecraft-mcp` (MCP servers as tool providers), `statecraft-console` (the
  visualizer).

Design: [docs/design.md](docs/design.md) (how a step runs) and
[docs/architecture.md](docs/architecture.md) (what surrounds it).

## Quick start

```python
from statecraft import Agent, Done, Runtime, Spawn, Wait, state, tool


@tool(name="math.add", policy="read_only")
async def add(a: float, b: float) -> float:
    return a + b


class Add(Agent):
    start = "add"

    @state
    async def add(self, ctx, data):
        await ctx.write(
            "sum", {"value": await ctx.call("math.add", a=ctx.input["a"], b=ctx.input["b"])}
        )
        return Done


class Total(Agent):
    start = "split"

    @state
    async def split(self, ctx, data):
        return Spawn(
            {"left": ("add", {"a": 1, "b": 2}), "right": ("add", {"a": 3, "b": 4})}, then="wait"
        )

    @state
    async def wait(self, ctx, data):
        return Wait(join=["left", "right"], then="ask")

    @state
    async def ask(self, ctx, data):
        for name in ("left", "right"):
            data[name] = (await ctx.read("sum", by=name)).payload["value"]
        return Wait(
            answer={
                "name": "scale",
                "question": "Multiply the total by?",
                "schema": {"type": "integer"},
            },
            then="finish",
        )

    @state
    async def finish(self, ctx, data):
        await ctx.write("total", {"value": (data["left"] + data["right"]) * ctx.woke["answer"]})
        return Done


runtime = Runtime.local("./statecraft-data", executors=0)
runtime.registrar.tools.register(add)
runtime.registrar.agents.register("add", Add, tools=["math.add"])
runtime.registrar.agents.register("total", Total, spawns=["add"])
async with runtime:
    root = await runtime.spawn("total")
    await runtime.run(root)  # until a person must answer
    [question] = await runtime.questions(root)
    await runtime.answer(question["question_id"], 10)
    await runtime.run(root)
    (await runtime.artifact(root, "total"))["payload"]  # {"value": 100}
```

The full script is [examples/quickstart.py](examples/quickstart.py). Kill it at any point and run
it again on the same folder: it continues from the last committed step.

## Configuration, server, CLI

```yaml
# statecraft.yaml
store: {provider: sqlite, path: ./statecraft.db}
workspace: {provider: local, root: ./workspaces}
runtime: {concurrency: 16, executors: 1}
plugins: [agentic, mcp, my-package]          # installed plugins are enabled by name
effect_providers:
  python: {provider: python, module: my_package.tools}
  search: {provider: mcp, url: http://localhost:8001/mcp, policies: {query: read_only}}
agents:
  report: {entry: my_package.agents:Report, tools: [search.query], writes: [report]}
```

```bash
statecraft serve --config statecraft.yaml            # HTTP API with server-sent events
statecraft run report --config statecraft.yaml --input '{"topic": "robots"}'
statecraft resume ROOT --config statecraft.yaml      # continue a root after a crash
statecraft ps | inspect ID | signal ID STOP | answer QUESTION VALUE (--config F | --url U)
python -m statecraft_console --config statecraft.yaml  # the visualizer
```

Configuration is strict: an unknown key or provider is an error, durations take a unit
(`30s`, `5m`), and an unset `${NAME}` is an error (`${NAME:-default}` for an optional one).

The server and the console answer loopback only, refuse cross-site browser requests and accept
changes only as JSON. To serve beyond the machine, give a token (`--token` or
`STATECRAFT_TOKEN`); clients send it as `Authorization: Bearer <token>` (the CLI and
`StatecraftClient` read `STATECRAFT_TOKEN`). It is one shared operator secret, not user
authentication.

A plugin is a package that declares `register(registrar)` under the `statecraft.plugins` entry
point; see [examples/notes_plugin](examples/notes_plugin).

## Samples

[examples/](examples/README.md): orders (tool policies, people, faults), research (parallel
work, CPU in processes), assistant (an agentic agent on MCP tools with a skill and a decider;
scripted or a real model), recovery (a killed process resumes), and a plugin package.

## Packages

| Package | What |
|---|---|
| `statecraft` | kernel, sdk, runtime, providers (memory, SQLite, local workspace, Python tools), server, client, CLI, testing |
| `statecraft-agentic` (`packages/`) | `AgenticAgent`, model tools, skills, decider, skill agent, sequence and fan-out |
| `statecraft-mcp` (`packages/`) | MCP servers (HTTP, stdio, in memory) as tool providers |
| `statecraft-console` (`packages/`) | the web console over the runtime API |

## Develop

```bash
python -m pip install -e ".[dev]" -e packages/statecraft-agentic -e packages/statecraft-mcp \
  -e packages/statecraft-console -e examples/notes_plugin
python -m pytest -q
```

The tests need no server and no model key: crash recovery is tested by killing processes with
`SIGKILL` and recovering from the SQLite file alone. Rules for working here: [AGENTS.md](AGENTS.md).
