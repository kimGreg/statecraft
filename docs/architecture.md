# Architecture

> Statecraft is a durable execution runtime for composable agents. Bring your own agents,
> tools, models and storage; statecraft manages execution, concurrency, persistence,
> coordination and recovery.

Status: **implemented** (statecraft 0.2), except section 18.3's step 8 (remote workers, a
distributed store, worker isolation, hot reload). [design.md](design.md) is the kernel's
execution design (records, steps, fencing, intents, recovery) and stays authoritative for how a
step runs; this document says what surrounds the kernel to make it a product, and fixes the
contracts between the parts: the SDK an agent is written with, the concurrency rules agents and
providers must follow, the registries that resolve agents and effects, the providers that
connect them, the runtime that runs them locally or as a server, and the extensions built on top.
Section 17 is the product's acceptance checklist; section 18 the migration from the earlier code;
section 20 notes where the implementation settles details this design left open.

## 0. Identity

Statecraft is closer to an operating system's process runtime than to a resource platform. An
operating system runs any program and drives any device without understanding what either does;
statecraft runs any state machine and calls any effect without understanding what either means.

| Operating system | Statecraft |
|---|---|
| program | AgentDefinition |
| executable package | AgentPackage (the installed code) |
| process | AgentInstance (the record) |
| process context | state, data, wake input |
| scheduler | executor |
| system call | context primitives (`ctx.call`, `ctx.read`, `ctx.write`, `ctx.files`) |
| device driver | provider |
| file system | workspace |
| signal | signal (STOP, CONT, TERM, KILL, USR) |
| IPC | spawn, join, messages |
| checkpoint | the committed transition |
| process table | the store's records, read through the runtime API |
| shell | CLI |
| system libraries | extensions: the agentic loop, the decider, flow patterns |

**The kernel has no notion of a model, a prompt, a conversation, a skill or reasoning.** It runs
state machines, manages the effects they perform, and persists their state and results.
Model-driven behaviour is one pattern built on top, like any other.

Three layers, and only three:

| Layer | Guarantees | Contains |
|---|---|---|
| **Kernel** | execution | state machines, checkpoints, concurrency, recovery, process lifecycle |
| **Runtime** | connection | SDK, registries, providers, configuration, API, server, CLI |
| **Extensions** | patterns | the agentic loop and models, skills, MCP, reusable agents, the console |

The product test of this layering: **with no model or extension installed, statecraft is a
complete asynchronous, parallel state-machine runtime; installing the agentic extension turns
the same runtime into an AI agent runtime.**

## 1. What changed from the previous drafts, and why

| Earlier | Now |
|---|---|
| everything a Kubernetes-style resource with one CRUD API | each concern has the interface its lifecycle needs; the console may *display* things uniformly |
| declarative lifecycle (`spec.lifecycle`) | imperative signals: "continue after a fault" is a command, not a desired state |
| MCP as the only tool model, inside the core | **effects** are the kernel's concept; MCP is one provider, in its own package |
| retry a non-idempotent tool when given a key | a key only helps if the provider deduplicates; four policies (section 7.2) |
| `unavailable` always retried | retried only when the request provably was not sent, or the policy allows a repeat |
| model calls simply `read_only` | retry-safe and deterministic are separate guarantees (`deterministic`, `cost`) |
| definitions stored in the state store | a registry built at start, immutable while running; instances pin their bindings |
| resolve effects by name | resolve through the instance's pinned binding (name, provider, version, schema and policy digests) |
| admission forbids spawn cycles and unwritten reads | recursion is legitimate; those become lint; dynamic transitions are checked when they happen |
| six controllers | the executor loop, plus plain functions and periodic maintenance |
| ModelProvider as a core resource | a model call is an effect registered by the agentic extension |
| skills pinned when first loaded | the skills an agent may use are pinned when it starts |
| attach writable | attach is read-only by default |
| no concurrency contract | section 5: invariants, slots, parallelism, cancellation, blocking work |
| no runtime lifecycle | section 11: start, stop, close; stopping the runtime does not end agents |

## 2. Concepts

| Concept | Question it answers | Owned by |
|---|---|---|
| **Agent** | who executes: a process with state | kernel (execution), runtime (registry) |
| **Effect** | what it invokes: an external call a state makes through the runtime | kernel (policy, recording), runtime (registry), providers (implementation) |
| **Provider** | how an effect is carried out | providers |
| **Skill** | what it knows: instructions and resources for a task | the agentic extension |
| **Runtime** | how it executes: running, persisting, coordinating, recovering | statecraft |

Words, kept consistent:

| Term | Meaning |
|---|---|
| effect | the execution model's word: an external call made through the runtime |
| tool | the SDK's word for a callable capability an agent may use (`@tool` is an alias of `@effect`) |
| model tool | a tool offered to a model, chosen by the agentic extension |
| provider | the implementation that carries an effect out |
| skill | knowledge and procedure an agent consults |

There is one registry for effects and tools; "tool" is a name, not a second mechanism.

Spawn, join, wait, signal and answer are not effects: they are the kernel's durable control
operations (design §6, §8).

## 3. Layers and packages

```
 applications and third-party packages: agents, tools, skills
───────────────────────────────────────────────────────────────────────────────
 statecraft-agentic     statecraft-mcp         statecraft-console
 (agentic loop, model   (MCP effect provider,  (observes and controls through
  effects, decider,      MCP-backed workspace)  the runtime API)
  skill catalog,
  patterns)
───────────────────────────────────────────────────────────────────────────────
 statecraft
   runtime      Runtime (embedded) and server; registries; plugins; configuration;
                effect manager; workspace service
   server/client/cli   the same API over HTTP, a client, a shell
   providers    stores (memory, sqlite), workspaces (local), python effects
   sdk          Agent, @state, Spawn/Wait/Done, Context, @effect/@tool, contracts
   kernel       process, transitions, executor, store contract, signals, effect policy
   testing      in-memory providers and helpers
```

One repository, several distributions: `statecraft`, `statecraft-mcp`, `statecraft-agentic`,
`statecraft-console` (later others, e.g. object-store workspaces, a postgres store).

Dependencies point one way, each rule enforced by a test:

- `kernel` imports only the standard library and its own modules (plus `pydantic` for contracts);
- `sdk` imports `kernel`; `runtime` imports `sdk` and `kernel`; `providers` implement interfaces
  from `kernel` and `runtime`;
- extensions import only the public API (`sdk`, `runtime`); the console only the runtime API or
  the HTTP client;
- nothing in `statecraft` names an extension, a built-in agent or an application.

## 4. Kernel

The kernel is today's durable core, unchanged in its rules (design §2 to §10): process records,
the pure transition rules, leases with epochs, fenced step commits and CAS control commits,
intents carried out exactly once, at-least-once state execution, faults decided by parents,
history and the durable event log in the same transaction as the change.

What the kernel knows: the handler to run, its input, what it is permitted to do, the result of
each step, and how to store all of that safely. What it does not know: whether an effect is MCP
or a Python function, which model is behind a call, whether files are local or in object storage,
what an agent's work means, what a skill is, how the console draws anything.

The kernel depends on two small interfaces the runtime implements, both resolving **pinned
references**, never bare names:

```
AgentResolver.resolve(definition_ref)  -> ResolvedAgent   # name, version, code digest
EffectResolver.resolve(binding)        -> ResolvedEffect  # the instance's pinned binding
```

Control operations stay imperative and keep their semantics:

```
signal(agent, STOP | CONT | TERM | KILL | (USR, message))
answer(question_id, value)
spawn(definition, input, ...)
```

Status views (for display) may be offered; they are not the kernel's model.

## 5. Concurrency and parallel execution

### 5.1 Invariants

These extend design §2 and bind agents, providers and the runtime:

1. **Each agent executes at most one valid step at a time.** A worker whose lease expired may
   still be running; it can never commit, and it stops calling effects once it knows (5.5).
2. **Different agents execute concurrently**, up to the runtime's slots.
3. **State handlers and effect invocations are asynchronous** (`async def`).
4. **A suspended agent occupies no slot**: waiting for children, a person, a time or a stop costs
   nothing until it wakes.
5. **Work started inside a state ends before the state commits.** Effect calls made through
   `ctx.call` are tracked per step: at commit they must have finished; on a fault or lease loss
   the runtime cancels the ones still running. Tasks an agent creates itself with `asyncio`
   outside `ctx` are not allowed (checked by lint, not enforceable at run time).
6. **Durable parallel work is child agents.** Parallel effects inside one state are cheap but
   recover as a unit: a rerun of the state repeats all of them (subject to their policies).
7. **Blocking and CPU-heavy work never runs on the executor's event loop.** It runs in a thread
   or a process (5.4), so heartbeats and other agents keep moving.

### 5.2 Four kinds of parallelism

| Kind | How | Unit of recovery |
|---|---|---|
| agents in parallel | executor slots | each agent's state |
| children in parallel | `Spawn` several, `Wait(join=[...])` | each child agent |
| effects in parallel inside a state | `asyncio.gather` over `ctx.call` (structured: all end before commit) | the whole state |
| CPU-parallel work | a provider with `execution: process` or a remote provider | the effect, or the state |

### 5.3 Slots and limits

- `runtime.concurrency`: the number of steps an executor runs at once; a runnable agent waits for
  a slot (today every runnable agent starts a task at once: a migration item).
- Per provider `concurrency`: calls beyond it wait asynchronously for capacity, inside the step's
  deadline.
- Per agent nothing extra: invariant 1 already serializes an agent's steps.

### 5.4 Blocking and CPU-heavy work

| Work | Runs |
|---|---|
| async I/O | on the event loop |
| blocking I/O, short CPU work | in a thread (`execution: thread`) |
| CPU-heavy work | in a process pool (`execution: process`) or a remote provider |

A Python effect declares its execution mode; the effect manager dispatches it. A state handler
that must do blocking work does it through such an effect, not inline.

### 5.5 Cancellation and lease loss

| Situation | What happens |
|---|---|
| a provider's capacity is full | the call waits asynchronously for capacity |
| an effect's deadline passes | `timeout`: retried or faulted by policy |
| `KILL` | the store bumps the lease epoch (no commit possible), and the executor holding the step cancels its task and its in-flight calls |
| the lease is lost (renewal fails) | the step stops making new calls, in-flight calls are cancelled, the result is not committed; another executor runs the state again |
| a provider connection drops | only the calls on that connection fail; the provider reopens on demand |
| the runtime stops | section 11.2 |

## 6. SDK: the execution contract

### 6.1 Agents

The kernel executes anything that implements one asynchronous method:

```
class ExecutableAgent(Protocol):
    async def step(self, state: str, ctx: Context, data: dict) -> Transition: ...
```

`Agent` with `@state` methods is the SDK's convenient way to implement it (as today), together
with `Spawn`, `Wait`, `Done` and raising for a fault. Another authoring style can come later
without touching the kernel.

### 6.2 Context

What a state may do, and nothing else:

| Primitive | Meaning |
|---|---|
| `await ctx.call(effect, **arguments)` | invoke a permitted effect through the effect manager (section 7) |
| `await ctx.read(type, by=)`, `await ctx.write(type, payload)` | read a committed artifact; stage an artifact published by this step's commit |
| `ctx.files` | the agent's workspace files, with the workspace rules (section 8) |
| `ctx.effect_key(name)` | a key stable across reruns of this state, for deduplicated effects |
| `ctx.input`, `ctx.woke`, `ctx.signals`, `ctx.children` | durable inputs of this step |
| `ctx.signal(agent, signal)` | an intent, sent after the commit |
| `ctx.note(...)` | small facts kept on the record |
| `await ctx.describe(root)` | another run's progress, read only |

There is no `ctx.model`.

### 6.3 Tools

```
@tool(name="math.add", policy="read_only")            # @tool is @effect
async def add(a: float, b: float) -> float: ...

@tool(name="report.render", policy="idempotent", execution="process")
def render(document: dict) -> bytes: ...              # blocking: dispatched to a process
```

The input schema comes from the signature and annotations; the output schema from the return
annotation when present.

## 7. Effects

### 7.1 EffectDefinition

```yaml
name: search.query
description: Search documents
input_schema: {type: object, properties: {query: {type: string}}, required: [query]}
output_schema: {...}                 # optional
policy:
  kind: read_only                    # read_only | idempotent | deduplicated | non_repeatable
  deterministic: true                # the same request gives the same result
  cost: none                         # none | metered (a repeat costs again)
  timeout: 30s
  max_attempts: 3
binding: {provider: search, operation: query, version: "1.0"}
```

### 7.2 Policies

A state can run again, so an effect called from it can be sent again. The policy says whether
that is safe; `deterministic` and `cost` say what a repeat gives and costs. Being safe to repeat
and returning the same result are different guarantees.

| Kind | Means | Repeated automatically (transient failure, timeout, rerun of the state) |
|---|---|---|
| `read_only` | no side effects on the outside world | yes |
| `idempotent` | the same request repeated has the same effect | yes |
| `deduplicated` | the provider guarantees an idempotency key is applied once | yes, with `ctx.effect_key(...)`; refused without a key |
| `non_repeatable` | none of the above | no; refused from a state unless the definition marks a reconciliation path; initially refused outright |

MCP annotations (`readOnlyHint`, `idempotentHint`, ...) are hints. A provider may *propose* a
policy from them; the policy that counts is the one set in the definition or the provider's
configuration by whoever answers for that service.

Committed results are never re-requested: recovery reuses what a committed step recorded and
repeats only what an uncommitted state did (design §7).

### 7.3 The effect manager

Common to every effect, in this order:

```
permission (the agent's pinned bindings) ▸ input validation ▸ provider capacity
▸ invoke with deadline and context ▸ classify a failure ▸ retry by policy
▸ output validation ▸ record a crossing (section 14)
```

Failures have one type with a class:

| Class | Example | Handling |
|---|---|---|
| `invalid` | bad arguments, schema violation | returned to the caller |
| `refused` | not permitted, policy forbids | fault |
| `transient` | 5xx, 429, a reset after the request was accepted for processing | repeated only if the policy allows a repeat, then fault |
| `timeout` | deadline passed | as `transient` |
| `unavailable` | the request **provably was not sent** (connection refused, no session) | retried with backoff within the deadline; otherwise as `transient` |
| `failed` | the effect reported an error result | returned to the caller |

A dropped connection does not prove a request was not executed: unless the provider can show it
was never sent, it is `transient`, and only repeated when the policy allows it.

### 7.4 Providers

A provider carries effects out and owns its sessions, connections and authentication:

```
class EffectProvider(Protocol):
    async def start(self) -> None: ...                           # connect, warm up
    async def discover(self) -> list[EffectDefinition]: ...      # optional; static definitions also work
    async def invoke(self, operation: str, arguments: dict,
                     context: EffectContext) -> EffectResult: ...
    async def close(self) -> None: ...                           # release connections
```

`EffectContext` carries the root (the session), the agent, the step, the effect key, the deadline
and a cancellation signal. Providers must be safe for concurrent `invoke` calls up to their
declared `concurrency`, and must honour cancellation.

| Provider | Package | Discovery |
|---|---|---|
| Python functions (`@tool`) | `statecraft` | from the functions |
| MCP (HTTP, stdio, in-memory) | `statecraft-mcp` | `tools/list`, refreshed on list-changed (applied at the next start, section 10) |
| HTTP / custom | applications | static or custom |
| model calls | `statecraft-agentic` | from its configuration (section 12.2) |

## 8. Workspace

The workspace is a runtime service because the kernel's guarantees depend on it: immutable
references in checkpoints, atomic publication of artifacts, sharing results between agents,
per-attempt staging, preserving earlier results across recovery. Its **rules** hold on any
provider (design §7, §11): `shared/` for the tree; `home/<agent>/` with `private/`,
`staging/<step>/<epoch>/`, content-addressed create-only `objects/`, installed bundles; another
agent's private and staging are unreadable; artifacts are read through committed references.

A **workspace provider** stores bytes:

```
class WorkspaceProvider(Protocol):
    async def open(self, workspace_id: str) -> Files: ...    # get, put, create-only, exists, list
    async def close(self) -> None: ...
```

`local` ships with statecraft; object storage and an MCP-backed workspace ship as separate
packages. Agents see the same `ctx.files`, `ctx.read`, `ctx.write` on all of them.

Three ways a root gets a workspace:

| Way | Means | Writes |
|---|---|---|
| **create** (default) | a new, independent workspace for the root's tree | the tree writes `shared/` and its homes |
| **attach** | the root works against another root's workspace | **read-only by default**: it reads the other run's files and outputs; its own outputs are published under its own home (its own publication); it never writes the other run's `shared/` |
| **fork** | a new root inherits an earlier run's committed outputs for the agents it keeps | its own workspace; inherited outputs are read by reference, nothing copied |

Attach with write access is an explicit, separately granted mode, not the default, because
several roots attached to one workspace break the assumption that one tree owns `shared/`.
Snapshots, retention and workspace garbage collection need reference tracking and policies of
their own, and come later.

## 9. Agents

### 9.1 Three things, kept apart

| Concept | What it is |
|---|---|
| **AgentDefinition** | the contract: name, version, entry, input schema, tools, spawns, artifacts, extension settings |
| **AgentPackage** | the executable code and its dependencies, installed where the agent runs |
| **AgentInstance** | a running process: the record in the store |

Storing an entry point is not deploying code. A definition is runnable only where its package is
installed; until remote workers exist, that is every executor sharing the configuration.

### 9.2 AgentDefinition

```yaml
name: research
version: 1.0.0
entry: my_agents.research:ResearchAgent
input: {type: object, properties: {query: {type: string}}, required: [query]}
tools: [search.query, analysis.summarize]         # effects it may call
spawns: [researcher, reviewer]                    # agents it may start
artifacts:
  reads: [sources]
  writes: [report]
extensions:                                       # opaque to the kernel; each extension validates its own
  agentic: {...}
```

Today's `agent.yaml` keeps its shape: `agent.<id>` entries move from `tools` to `spawns`; unknown
keys move under `extensions`.

### 9.3 Registration

All three ways produce the same AgentDefinition, through a **registrar**: an object that can
only register (agents, tools, providers, extension settings), never start runs or touch the store.

| Way | Suits |
|---|---|
| **Python API**: `registrar.agents.register("research", ResearchAgent, version=...)` | tests, embedded use |
| **Manifest**: `agents:` in the configuration, with an entry and an optional contract file | a standalone server |
| **Package discovery**: the entry-point group `statecraft.plugins`, each exposing `register(registrar)` | distributing agents, tools and skills as a package |

```
def register(registrar):
    registrar.agents.register("research", Research)
    registrar.agents.register("search_agent", SearchAgent)
    registrar.tools.register(add)
    registrar.providers.register("custom", CustomProvider)
```

Discovery is not activation: an installed plugin is registered only when the configuration
enables it. Restricting a plugin to a registrar separates responsibilities; it is not a security
boundary for Python code (section 15).

### 9.4 Validation

| Check | When |
|---|---|
| the entry imports and implements the contract; `start` and the declared states exist | validation phase (10.1) |
| each `tools` name resolves to an effect; each `spawns` name resolves to a definition | validation phase, after every plugin and provider registered |
| input matches the input schema | at spawn |
| a returned transition names an existing state | when it happens (a fault) |
| an effect or spawn is permitted | when it happens (a refusal) |
| spawn cycles (recursion is legitimate), reads without a writer (artifacts may come from outside), unreachable states | optional lint |
| providers reachable | health checks |

No graph DSL: what can only be known at run time is checked at run time.

### 9.5 Pinning

When an instance is created, its record pins what was resolved:

```yaml
definition: {name: research, version: 1.0.0, code: sha256:...}
bindings:
  search.query:
    provider: search
    operation: query
    version: "1.0"
    schema_digest: sha256:abc...
    policy_digest: sha256:def...      # the policy too: same schema, different safety is a change
```

Every `ctx.call` resolves through the instance's binding, so an instance keeps the effects it
started with; new instances get the current ones. If, after a restart, the runtime cannot provide
an instance's pinned definition or bindings, the instance is **suspended with that reason**, not
run; the operator (or the definition's migration rule, design §5) decides. An unchanged schema
digest does not prove a remote service behaves the same; it is the best the runtime can check.

### 9.6 Kinds of agents: native first, adapter friendly

| Kind | How it runs | Recovery |
|---|---|---|
| native state machine | its states, step by step | per committed state |
| agentic agent (extension) | a native state machine | per model turn and per effect call |
| an agent of another framework | wrapped by an adapter, usually as one state | what the adapter provides; at worst the whole call reruns |
| a remote agent | through a remote protocol | the remote implementation's |
| a plain program | wrapped as one state (as a process effect when blocking) | the whole program reruns |

## 10. Registries

Registries resolve names; the store keeps executions.

| | Registry | State store |
|---|---|---|
| holds | agent definitions, effect definitions, providers, extension settings | records, leases, inboxes, history, events |
| changes | at start only (immutable while running) | continuously while running |
| role in recovery | provides the same executable environment | restores how far execution got |
| source | configuration, enabled plugins, the Python API | SQLite (memory for tests; others later) |

### 10.1 Startup

Registration and validation are separate phases, so plugin order never changes the result:

| Phase | Does |
|---|---|
| 1. load configuration | read the file, resolve environment references, list enabled plugins |
| 2. load plugins | call each enabled plugin's `register(registrar)`: agents, tools, provider factories, extension settings |
| 3. start providers | construct and `start()` each provider; collect discovered effect definitions |
| 4. build registries | all agent and effect definitions together; then freeze them |
| 5. validate | references, permissions, schemas, contracts (9.4); refuse to start on errors, report warnings |
| 6. start executors | resume existing instances (re-resolving their pins, 9.5) and accept new spawns |

There is no hot reload at first: a changed configuration or plugin takes effect at the next
start, and running instances keep their pins.

## 11. Runtime

### 11.1 One API, embedded or served

```
async with Runtime.from_config("statecraft.yaml") as runtime:     # or Runtime.local("./statecraft")
    root = await runtime.spawn("research", input={...})
    await runtime.signal(root, "STOP"); await runtime.signal(root, "CONT")
    await runtime.answer(question_id, value)
    async for event in runtime.events(root): ...
```

```
statecraft serve --config statecraft.yaml          # the runtime as a server (HTTP + events)
StatecraftClient("http://...").spawn(...)          # the same API and semantics, remotely

POST /agents                      spawn
GET  /agents/{id}                 the record (tree with ?tree=1)
GET  /agents/{id}/history         its committed steps
GET  /agents/{id}/events          server-sent events from the durable log, resumable
POST /agents/{id}/signals         STOP | CONT | TERM | KILL | USR
POST /questions/{id}/answer       answer a waiting question
GET  /registry/agents | /registry/tools | /registry/providers
```

The CLI uses the same API: `statecraft agents list`, `tools list`, `run`, `ps`, `inspect`,
`signal`, `answer`, `events`.

### 11.2 Lifecycle

| Operation | Means |
|---|---|
| `start()` | phases 1 to 6 of section 10.1 |
| `stop(drain=...)` | no new steps; running steps finish within the drain time, or are cancelled and their leases released, so a restart need not wait for leases to expire |
| `close()` | close providers, workspace providers, the store and server connections |

`async with` is `start` on entry and `stop` then `close` on exit.

**Stopping the runtime is not ending agents.** No agent receives TERM because the runtime stops;
unfinished agents stay in the store and resume at the next start, by this runtime or another one
sharing the store.

## 12. Extensions

### 12.1 What an extension may do

Register agents, tools and providers through the registrar; read its own configuration section
(`extensions.<name>` in the configuration and in definitions) and validate it; add observation
hooks. It never patches the kernel.

### 12.2 statecraft-agentic

- **AgenticAgent**: the model loop as a native state machine (`prepare ▸ think ▸ act ▸
  wait_result ▸ finish`, design §9). `think` calls a model effect; `act` calls the chosen tool.
  Committed model replies are reused; an unfinished `think` calls the model again.
- **Model effects**: the extension reads its configured models and registers one effect per
  model, `model.<name>` (e.g. `model.default`), with policy `read_only`, `deterministic: false`,
  `cost: metered`: safe to repeat, but a repeat may differ in content and costs again. Timeouts,
  retries and recording are the effect manager's. No `Engine(model=...)`, no `ctx.model`.
- **Tools versus model tools**: `tools` is what the agent may call; `extensions.agentic.tools` is
  the subset offered to the model. `model.default` can be callable by the agent without being
  offered to the model, so a model cannot reach internal or administrative effects.
- **Skills**: a `SkillCatalog` (Agent Skills format: `SKILL.md` and resources) discovers skills
  from configured folders and plugins.
  - When an agentic agent **starts**, the skills its definition allows are pinned in its `data`
    (`available_skills: {name: digest}`); loading one later reads that pinned version, so a crash
    and a changed skill between start and first use change nothing.
  - A digest alone cannot restore content: the catalog keeps an immutable copy of each pinned
    bundle (in the workspace's objects) or resolves it from a version-pinned package.
  - Content is read on demand (progressive disclosure), not put into the model's context upfront.
  - A skill grants no permission: tools it mentions run only if the definition permits them;
    scripts in a skill run only as registered tools or through an isolated provider.
- **Decider** and **SkillAgent**: ordinary registered agents (`agentic:decider`,
  `agentic:skill`). A SkillAgent runs one skill as its own process, with its own checkpoints,
  faults and outputs.
- **Patterns**: reusable agents for sequence, fan-out and ask-a-person-on-fault.

### 12.3 statecraft-mcp

The MCP effect provider (HTTP, stdio, in-memory transports; discovery from `tools/list`; the
session named by the root; annotations proposed as policies) and an MCP-backed workspace provider.

### 12.4 statecraft-console

The visualizer, over the runtime API only: registry (agents with their states, tools with schemas
and policies, providers with health), runs (the live tree, each agent's walked path, data,
outputs, crossings, waits, pinned bindings), questions answered in place, signals, workspace
files, events. It shows uniform lists for display; it does not impose a resource model.

## 13. Configuration

```yaml
runtime: {concurrency: 16, drain: 30s}
store: {provider: sqlite, path: ./statecraft.db}
workspace: {provider: local, root: ./workspaces}

plugins: [statecraft-agentic, research-agents]       # enabled; installed ones not listed stay off

effect_providers:
  search: {provider: mcp, url: http://localhost:8001/mcp, concurrency: 8,
           policies: {query: read_only}}
  analysis: {provider: mcp, url: http://localhost:8002/mcp}
  reports: {provider: python, module: research_agents.reports, concurrency: 2, execution: process}

extensions:
  agentic:
    models:
      default: {provider: openai-compatible, endpoint: ${MODEL_ENDPOINT},
                api_key: ${MODEL_API_KEY}}           # resolved at start; never stored or logged
    skill_paths: [./skills]

agents:
  researcher:
    entry: statecraft_agentic:AgenticAgent
    tools: [model.default, search.query, analysis.summarize]
    extensions:
      agentic: {model: default, skills: [literature-review],
                tools: [search.query, analysis.summarize]}
  report:
    entry: research_agents.agents:ReportAgent
```

`extensions.agentic.models.default` is read by the agentic extension, which registers
`model.default`; the kernel never reads model settings.

## 14. Observability

Every effect call made in a step produces a **crossing**: effect, provider, binding, sizes,
duration, wait for capacity, attempts, failure class, effect key, and usage when the provider
reports it. Crossings are committed with the step in its history entry; a step that does not
commit leaves its crossings in a per-attempt diagnostic area for recovery tooling. Payloads are
referenced, not copied. Extensions add observation hooks without the kernel understanding them.
An OpenTelemetry exporter is optional.

## 15. Security and trust

`ctx` enforces an agent's contract; it is not a sandbox. Code imported into an executor can do
anything its process can.

| Mode | Trust |
|---|---|
| embedded runtime | the developer's own code |
| standalone runtime | packages installed and enabled by the operator |
| isolated workers | agents run in a separate process or container |
| multi-tenant | strong isolation and access control required |

The first product runs trusted, operator-installed packages only. Accepting third parties' agent
code makes isolated workers a requirement, not an option; this boundary comes before namespaces
or tenants. Credentials are referenced in configuration and resolved at start; they never enter
records, artifacts, events or logs.

## 16. Package layout

```
statecraft/
  kernel/      process, transitions, executor, store contract, signals, effect policy
  sdk/         agent, context, effect and tool, contracts
  runtime/     runtime and lifecycle, registries and registrar, plugins, configuration,
               effect manager, workspace service
  providers/   memory and sqlite stores, local workspace, python effects
  server/  client/  cli/  testing/
statecraft-mcp/        provider, discovery, transports, MCP-backed workspace
statecraft-agentic/    agentic loop, model effects, decider, skill agent, skill catalog, patterns
statecraft-console/    the visualizer
```

The directories need not all exist from day one; the responsibilities and the dependency
direction must.

## 17. Product acceptance checklist

Beyond the kernel's recovery checklist (design §13), the product is accepted when each holds:

**Plugins and registries**
1. An external Python package registers an agent.
2. An external Python package registers a tool.
3. An MCP server's tools are discovered and registered.
4. References resolve regardless of plugin order.
5. After a registry change and a restart, running instances keep their pinned bindings, or are
   suspended with a reason when a binding cannot be provided.

**Asynchronous and parallel execution**
6. Several agents run concurrently, within `runtime.concurrency`.
7. A parent waiting on a join holds no slot.
8. Effects run in parallel inside a state, and all end before the commit.
9. One agent's fault does not stop another.
10. Per-provider concurrency limits hold.
11. A CPU-heavy tool runs off the event loop; heartbeats and other agents keep moving.
12. KILL and a lost lease cancel the step's in-flight calls; nothing is committed.

**Extension independence**
13. The kernel and runtime run with no extension installed.
14. Python tools run without the MCP extension.
15. A model call runs as an ordinary effect.
16. Skills are pinned at agent start and reloaded identically after a crash.
17. A skill does not extend an agent's permissions.

**Workspace**
18. An attached root cannot write the attached run's files.

**Standalone runtime**
19. An HTTP client spawns, observes and signals an agent.
20. After the runtime stops, unfinished agents resume at the next start, without waiting for
    leases to expire.
21. A provider failure is handled by class and policy.
22. The same API behaves the same embedded and served.
23. **A new package, unrelated to the first application, installs statecraft, registers its own
    agent, a Python tool, an MCP tool and a skill, and runs a job that recovers from a killed
    process.**

## 18. Migration

### 18.1 Where today's code goes

| Today | Moves to |
|---|---|
| `process.py`, `transitions.py`, `store.py`, `stores/`, `executor.py` | `kernel` (stores to `providers`) |
| `agent.py`, `context.py` | `sdk` (without `ctx.model`) |
| `engine.py` | `runtime` (`Runtime`, with the lifecycle) |
| `spec.py`, `catalog.py` | `runtime` registries and registrar; `agent.<id>` moves to `spawns`; no `core:decider` or skill-agent special cases; cycle and unwritten-read checks become lint |
| `workspace.py` | rules in `runtime`; `LocalFiles` in `providers` |
| `artifacts.py` | `sdk` contracts (applications may extend the envelope) |
| `views.py` | an extension (views are derived files) |
| `agentic.py` | `statecraft-agentic`; model clients become model effects |
| `testing.py` | `testing` |
| `console/` | `statecraft-console`, over the runtime API |
| the first application's MCP transport and file adapters | `statecraft-mcp` |
| the first application's envelope, session view, cumulative state and model filter | stay in that application |

### 18.2 Gaps in today's code the contracts close

| Gap | Contract |
|---|---|
| every runnable agent starts a task at once; no slot limit | 5.3 slots |
| nothing runs off the event loop; a CPU-bound state stalls heartbeats | 5.4 thread and process execution |
| on a lost lease the step keeps calling effects; KILL does not cancel the running task | 5.5 cancellation |
| the code hash is recorded but never compared on resume; no bindings or policies pinned | 9.5 pinning |
| an attached root can write the attached run's `shared/` | 8 read-only attach |
| tool sessions are closed only by tests; no runtime lifecycle | 7.4 provider `close`, 11.2 lifecycle |
| the catalog loads lazily; registration and validation are implicit | 10.1 startup phases |

### 18.3 Order

Each step keeps the kernel's recovery tests green:

| Step | Scope | Done when |
|---|---|---|
| 1. Freeze and separate the kernel | `kernel` and `sdk` carved out; no model, skill or application in them; import tests | the recovery suite passes on `kernel` + `sdk` alone |
| 2. Concurrency contract | slots, thread and process execution, cancellation on KILL and lease loss, structured in-state effects | checklist 6 to 12 |
| 3. Public SDK | `ExecutableAgent`, `Context`, `@tool`, `EffectDefinition`, policies with `deterministic` and `cost` | an agent and a Python tool are written against the SDK only |
| 4. Registries and runtime lifecycle | registrar, plugins, startup phases, pinning of definitions and bindings, `start`/`stop`/`close` | checklist 1, 2, 4, 5, 20 |
| 5. Providers and workspace | sqlite and memory stores, local workspace, Python tools; `statecraft-mcp`; read-only attach | checklist 3, 14, 18, 21 |
| 6. Agentic extension | `statecraft-agentic`: loop, model effects, decider, skill catalog with pinning, skill agent | checklist 13, 15, 16, 17; the first application's agentic agents behave as before |
| 7. Standalone product | configuration, CLI, HTTP server, client, console | checklist 19, 22, 23 |
| 8. Production | remote workers, a distributed store, worker isolation, retention, hot reload | several workers recover each other's runs |

## 19. Decisions

| Question | Decision |
|---|---|
| models, prompts, conversations, skills in the kernel | no |
| how agents are known | AgentDefinition resolved by the runtime; the kernel resolves pinned references |
| how external capabilities are known | EffectDefinition resolved through the instance's pinned binding |
| effect, tool, model tool | effect: the execution model's word; tool: the SDK's word for the same thing; model tool: a tool offered to a model; one registry |
| a model call | an effect registered by the agentic extension (`read_only`, not deterministic, metered) |
| MCP | an official effect provider, in its own package |
| registration | through a registrar: Python API, manifest, entry points; discovery is not activation |
| startup | load configuration, load plugins, start providers, build and freeze registries, validate, start executors |
| registry while running | immutable; changes apply at the next start |
| pinning | definition code digest and each binding's provider, version, schema and policy digests |
| skills | the agentic extension's catalog; pinned at agent start with an immutable copy |
| concurrency | at most one valid step per agent; slots; suspended agents hold none; in-state work ends before commit; blocking work off the loop |
| runtime lifecycle | `start`, `stop`, `close`; stopping the runtime never ends agents |
| attach | read-only by default; writing is a separately granted mode |
| lifecycle API | imperative signals and answers |
| source of truth for execution | the state store |
| source of truth for business data | the workspace |
| unit of recovery | the committed state transition |

Still open: the distributed store (design §14); worker isolation and its protocol; hot reload;
namespaces and tenants, after the trust boundary.

## 20. Implementation notes

Where the implementation settles details this design left open:

| Topic | As implemented |
|---|---|
| layer order | kernel ← sdk ← providers ← runtime ← server, client, cli, testing (`tests/test_layers.py`); the file port (`Files`, `WorkspaceProvider`) lives in `kernel/files.py` so providers need nothing above the sdk; the runtime may construct default providers |
| derived views | `StateView` stays in `runtime/views.py` (opt-in `Runtime(views=[...])`) |
| extension services | `registrar.provide(name, obj)` and `ctx.service(name)`: how an extension's agents reach the extension's own objects (the skill catalog); the runtime never reads them |
| keyed Python tools | a `deduplicated` Python tool that declares an `idempotency_key` parameter receives the call's key; the parameter is not part of its input schema |
| tool policies | `@tool` requires a policy (no default); MCP tools of an untrusted server, and unannotated ones, are `non_repeatable` until the operator sets a policy |
| repin | `runtime.repin(agent)` records the current definition and bindings for an instance suspended by a pin change; then CONT |
| `resume` | `statecraft resume ROOT --config` runs an existing root until quiet (after a crash) |
| samples | `examples/` (orders, research, assistant, recovery, a plugin package) cover the acceptance checklist; the assistant also runs on a real model when one is configured |
| tree limits | `Runtime(limits={max_depth, max_agents, max_live})` (configuration `runtime.limits`) is checked on every spawn; a violation faults the parent with the reason; `ctx.spawn_problem(target)` tells an agent in advance. Spawns committed at the same moment by two agents of one tree are each checked against the tree as it was, so `max_agents`/`max_live` can be exceeded by those. A token budget per tree is not implemented |
| children as processes | `ctx.children_view()` gives each child's phase (running, waiting, faulted, stopped, ended), state, step, publications, error and open question; the agentic loop offers it to models as `list_agents`/`inspect_agent`, with `join_agents` and `signal_agent` |
