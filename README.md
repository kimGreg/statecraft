# statecraft

A durable state machine executor for agents.

Agents are checkpointed state machines. Every state change commits atomically to a state store,
so a run survives crashes, restarts and deploys: committed steps are never lost, and an
unfinished state simply runs again. Agents depend on each other only by spawning and joining.
People answer questions, signals stop and resume agents, and a parent decides what a child's
fault means. A model-driven agent is a state machine too, one tool call per step.

- **Small kernel.** The lifecycle rules are pure functions (`transitions.py`); the executor and
  the store do the rest. No graph DSL, no replay, no hidden call stacks.
- **Fenced commits.** Leases with epochs: a stale or dead executor can never commit or publish.
- **Durable control.** Spawns and signals are intents with deterministic ids, carried out exactly
  once even if the parent dies; questions and answers are records.
- **Content-addressed artifacts**, published by the step that commits their reference.
- **Forks and attachments.** A run can reuse an earlier run's results, or work in its workspace.
- **Ports, not frameworks.** State store (memory, SQLite), files, tools, model: an application
  plugs its own.

Design: [docs/design.md](docs/design.md).

## Quick start

```python
from statecraft import Agent, Catalog, Done, Engine, Spawn, Wait, state
from statecraft.stores import SqliteStore
from statecraft.testing import MemoryTools
from statecraft.workspace import LocalFiles


class Add(Agent):
    start = "add"

    @state
    async def add(self, ctx, data):
        result = await ctx.call("math.add", a=ctx.input["a"], b=ctx.input["b"])
        await ctx.write("sum", {"value": result["sum"]})
        return Done


class Total(Agent):
    start = "split"

    @state
    async def split(self, ctx, data):
        children = {"left": ("calc:add", {"a": 1, "b": 2}), "right": ("calc:add", {"a": 3, "b": 4})}
        return Spawn(children, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["left", "right"], then="ask")

    @state
    async def ask(self, ctx, data):
        for name in ("left", "right"):
            data[name] = (await ctx.read("sum", by=name)).payload["value"]
        question = {
            "name": "scale",
            "question": "Multiply the total by?",
            "schema": {"type": "integer"},
            "default": 1,
        }
        return Wait(answer=question, then="finish")

    @state
    async def finish(self, ctx, data):
        await ctx.write("total", {"value": (data["left"] + data["right"]) * ctx.woke["answer"]})
        return Done


engine = Engine(
    store=SqliteStore("state.db"),
    files=LocalFiles("workspaces"),
    tools=MemoryTools(handlers={"math.add": lambda a: {"sum": a["a"] + a["b"]}}),
    catalog=Catalog({}, extra={"calc:add": (Add, None), "calc:total": (Total, None)}),
)
root = await engine.spawn("calc:total")
await engine.run(root)  # until a person must answer
[question] = await engine.questions(root)
await engine.answer(question["question_id"], 10)
await engine.run(root)
(await engine.artifact(root, "total"))["payload"]  # {"value": 100}
```

The full script is [examples/quickstart.py](examples/quickstart.py). Kill it at any point and run
it again on the same `state.db`: it continues from the last committed step.

## Pieces

| Piece | Where |
|---|---|
| The record, the lease, a step's outcome (data only) | `process.py` |
| The pure lifecycle rules: transitions, faults, signals, answers, waits, intents | `transitions.py` |
| `Agent`, `@state`, `Spawn`, `Wait`, `Done` | `agent.py` |
| `AgenticAgent` (a model loop as states), the `Model` port, the decider | `agentic.py` |
| The `StateStore` contract (fencing, atomic commits, history, events); memory and SQLite | `store.py`, `stores/` |
| The files port (`LocalFiles`, per-root scoping) | `workspace.py` |
| Agents and skills as data (`agent.yaml`, `SKILL.md`), the catalog | `spec.py`, `catalog.py` |
| `ctx`, the executor loop, the engine (spawn, signal, answer, events, run, fork, attach) | `context.py`, `executor.py`, `engine.py` |
| The artifact envelope; views and the cumulative state | `artifacts.py`, `views.py` |
| Tools answered by plain functions, for tests | `testing.py` |

## Develop

```bash
python -m pip install -e ".[dev]"
python -m pytest -q tests
```

The tests need no server and no model key: crash recovery is tested by killing a worker process
with `SIGKILL` at each boundary and recovering from the SQLite file alone.

Rules for working here: [AGENTS.md](AGENTS.md).
