# Architecture

> Statecraft is a durable execution runtime for composable agents. Bring your own agents,
> effects, models and storage; statecraft manages execution, persistence, coordination and
> recovery.

Status: **design.** [design.md](design.md) is the kernel's execution design (records, steps,
fencing, intents, recovery) and stays authoritative for how a step runs; this document does not
change it. This document says what surrounds the kernel to make it a product: the SDK an agent is
written with, the registries that resolve agents and effects, the providers that connect them,
the runtime that runs them locally or as a server, and the extensions built on top. Section 15 is
the migration from today's code.

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

What statecraft must do well: **run, connect, observe, recover.**

## 1. What changed from the previous draft, and why

The previous draft modelled everything as a Kubernetes-style resource (spec and status, a
generic CRUD API, admission webhooks, a controller per concern). That blurred the runtime's
identity: agents, definitions, workspaces, servers and questions have different lifecycles, and
one management model for all of them made the execution runtime a resource platform.

| Previous draft | Now |
|---|---|
| everything a resource with one CRUD API | each concern has the interface its lifecycle needs; the console may still *display* things uniformly |
| declarative lifecycle (`spec.lifecycle`) | imperative signals stay: "continue after a fault" is a command, not a desired state |
| MCP as the only tool model, inside the core | **effects** are the kernel's concept; MCP is one provider, in its own package |
| retry a non-idempotent tool when given a key | a key only helps if the provider guarantees deduplication; four effect policies (section 6.2) |
| definitions stored in the state store | a registry built from configuration and packages; each instance pins what it resolved |
| admission forbids spawn cycles and unwritten reads | recursion is legitimate; those become lint, dynamic transitions are checked when they happen |
| six controllers | the executor loop, plus plain functions and periodic maintenance |
| ModelProvider as a core resource | a model call is an effect registered by the agentic extension |
| skills projected by the core | skills belong to the agentic extension's catalog |

What carries over: the kernel unchanged, the agentic loop out of the kernel, a registry with
versions, workspace providers, crossings, one configuration, the console, import-direction tests.

## 2. Concepts

| Concept | Question it answers | Owned by |
|---|---|---|
| **Agent** | who executes: a process with state | kernel (execution), runtime (registry) |
| **Effect** | what it invokes: an external capability called from a state | kernel (policy, recording), runtime (registry), providers (implementation) |
| **Skill** | what it knows: instructions and resources for a task | the agentic extension |
| **Runtime** | how it executes: running, persisting, coordinating, recovering | statecraft |

A **tool** is an effect that is exposed to a model; that exposure is the agentic extension's
decision (section 10.2). Spawn, join, wait, signal and answer are not effects: they are the
kernel's durable control operations (design §6, §8).

## 3. Layers

```
 applications and third-party packages: agents, effects, skills
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
   sdk          Agent, @state, Spawn/Wait/Done, Context, @effect, contracts
   kernel       process, transitions, executor, store contract, signals, effects policy
   testing      in-memory providers and helpers
```

One repository, several distributions: `statecraft`, `statecraft-mcp`, `statecraft-agentic`,
`statecraft-console` (later others, e.g. object-store workspaces, postgres store).

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

The kernel depends on two small interfaces the runtime implements:

```
AgentResolver.resolve(definition_ref)  -> ResolvedAgent   (executable, contract, digest)
EffectResolver.resolve(effect_name)    -> ResolvedEffect  (policy, schemas, binding, digest)
```

Control operations stay imperative and keep their semantics:

```
signal(agent, STOP | CONT | TERM | KILL | (USR, message))
answer(question_id, value)
spawn(definition, input, ...)
```

`spec`/`status`-style views may be offered for display; they are not the kernel's model.

## 5. SDK: the execution contract

### 5.1 Agents

The kernel executes anything that implements one method:

```
ExecutableAgent.step(state: str, ctx: Context, data: dict) -> Transition
```

`Agent` with `@state` methods is the SDK's convenient way to implement it (as today), together
with `Spawn`, `Wait`, `Done` and raising for a fault. Another authoring style can come later
without touching the kernel.

### 5.2 Context

What a state may do, and nothing else:

| Primitive | Meaning |
|---|---|
| `ctx.call(effect, **arguments)` | invoke a permitted effect through the effect manager (section 6) |
| `ctx.read(type, by=)`, `ctx.write(type, payload)` | read a committed artifact; stage an artifact published by this step's commit |
| `ctx.files` | the agent's workspace files, with the workspace rules (section 7) |
| `ctx.effect_key(name)` | a key stable across reruns of this state, for deduplicated effects |
| `ctx.input`, `ctx.woke`, `ctx.signals`, `ctx.children` | durable inputs of this step |
| `ctx.signal(agent, signal)` | an intent, sent after the commit |
| `ctx.note(...)` | small facts kept on the record |
| `ctx.describe(root)` | another run's progress, read only |

There is no `ctx.model`.

### 5.3 Effects

```
@effect(name="math.add", policy="read_only")
async def add(a: float, b: float) -> float: ...
```

The input schema comes from the signature and annotations; the output schema from the return
annotation when present.

## 6. Effects

### 6.1 EffectDefinition

```yaml
name: search.query
description: Search documents
input_schema: {type: object, properties: {query: {type: string}}, required: [query]}
output_schema: {...}                 # optional
policy:
  kind: read_only                    # read_only | idempotent | deduplicated | non_repeatable
  timeout: 30s
  max_attempts: 3
binding: {provider: search, operation: query}
```

### 6.2 Policies

The kernel's recovery guarantees (design §7) must survive every provider. A state can run again,
so an effect called from it can be sent again; the policy says whether that is safe:

| Policy | Means | Retried automatically (transient failure, timeout, or a rerun of the state) |
|---|---|---|
| `read_only` | no side effects | yes |
| `idempotent` | the same request repeated has the same effect | yes |
| `deduplicated` | the provider guarantees that an idempotency key is applied once | yes, with `ctx.effect_key(...)`; refused without a key |
| `non_repeatable` | none of the above | no; refused from a state unless the definition marks a reconciliation path; initially refused outright |

MCP annotations (`readOnlyHint`, `idempotentHint`, ...) are hints. A provider may use them to
*propose* a policy; the policy that counts is the one in the definition or the provider's
configuration, set by whoever answers for that service. A key passed to a provider that does not
deduplicate is not a guarantee.

### 6.3 The effect manager and providers

The **effect manager** (runtime) is common to every effect: permission (the agent's `effects`),
input and output validation, deadline, retry by policy, the idempotency key, failure
classification, and recording the call as a crossing of the step (section 12).

A **provider** is how an effect is reached; it owns sessions, connections and authentication:

```
EffectProvider.discover() -> list[EffectDefinition]      # optional; static definitions also work
EffectProvider.invoke(operation, arguments, context) -> EffectResult
```

`context` carries the root (session), the agent, the step, the effect key and the deadline.

| Provider | Package | Discovery |
|---|---|---|
| Python functions (`@effect`) | `statecraft` | from the functions |
| MCP (HTTP, stdio, in-memory) | `statecraft-mcp` | `tools/list`, refreshed on list-changed |
| HTTP / custom | applications | static or custom |
| model calls | `statecraft-agentic` | from its configuration (section 10.2) |

Failures have one type with a class:

| Class | Example | Handling |
|---|---|---|
| `invalid` | bad arguments, schema violation | returned to the caller |
| `refused` | not permitted, policy forbids | fault |
| `transient` | connection reset, 5xx, 429 | retried by policy, then fault |
| `timeout` | deadline passed | as `transient` when the policy allows a retry, else fault |
| `unavailable` | provider down | retried with backoff within the lease, then fault |
| `failed` | the effect reported an error result | returned to the caller |

## 7. Workspace

The workspace is a runtime service, not a provider detail, because the kernel's guarantees depend
on it: immutable references in checkpoints, atomic publication of artifacts, sharing results
between agents, per-attempt staging, preserving earlier results across recovery. Its **rules**
stay the runtime's on any provider (design §7, §11): `shared/` for the tree, `home/<agent>/` with
`private/`, `staging/<step>/<epoch>/`, content-addressed create-only `objects/`, installed bundles;
another agent's private and staging are unreadable; artifacts are read through committed
references.

A **workspace provider** stores bytes:

```
WorkspaceProvider.open(workspace_id) -> Files      # get, put, create-only, exists, list
```

`local` ships with statecraft; object storage and an MCP-backed workspace ship as separate
packages. Agents see the same `ctx.files`, `ctx.read`, `ctx.write` on all of them.

**Attach** (a root works in another root's workspace) and **fork** (a root inherits an earlier
run's publications without copying) already exist in the kernel and are used: a long-lived
conversation attaches to the current run, and a rerun from the middle is a fork. They stay
runtime features. Snapshots, retention and workspace garbage collection need reference tracking
and policies of their own, and come later.

## 8. Agents

### 8.1 Three things, kept apart

| Concept | What it is |
|---|---|
| **AgentDefinition** | the contract: name, version, entry, input schema, effects, spawns, artifacts, extension settings |
| **AgentPackage** | the executable code and its dependencies, installed where the agent runs |
| **AgentInstance** | a running process: the record in the store |

Storing an entry point is not deploying code. A definition is runnable only where its package is
installed; until remote workers exist, that is every executor sharing the configuration, and the
runtime checks at start that each enabled definition's entry imports.

### 8.2 AgentDefinition

```yaml
name: research
version: 1.0.0
entry: my_agents.research:ResearchAgent
input: {type: object, properties: {query: {type: string}}, required: [query]}
effects: [search.query, analysis.summarize]       # what it may call
spawns: [researcher, reviewer]                    # what it may start
artifacts:
  reads: [sources]
  writes: [report]
extensions:                                       # opaque to the kernel; each extension validates its own
  agentic: {...}
```

Today's `agent.yaml` keeps its shape: `tools` becomes `effects`, `agent.<id>` entries move to
`spawns`, unknown keys move under `extensions`.

### 8.3 Registration

All three ways produce the same AgentDefinition in the registry:

| Way | Suits |
|---|---|
| **Python API**: `runtime.agents.register("research", ResearchAgent, version=...)` | tests, embedded use |
| **Manifest**: `agents:` in the configuration, with an entry and an optional contract file | a standalone server |
| **Package discovery**: the standard entry-point group `statecraft.plugins`, each exposing `register(runtime)` | distributing agents, effects and skills as a package |

Discovery is not activation: an installed plugin is only registered when the configuration
enables it.

### 8.4 Validation

| Check | When |
|---|---|
| the entry imports and implements the contract; `start` and the declared states exist | at registration |
| input matches the input schema | at spawn |
| each `effects` name resolves; each `spawns` name resolves | at registration (warning when a provider is unreachable) |
| a returned transition names an existing state | when it happens (a fault) |
| an effect or spawn is permitted | when it happens (a refusal) |
| spawn cycles (recursion is legitimate), reads without a writer (artifacts may come from outside), unreachable states | optional lint |
| providers reachable | health checks |

No graph DSL: what can only be known at run time is checked at run time.

### 8.5 Pinning

When an instance is created, its record pins what was resolved: the definition's name, version
and code digest (the record's `code` today), and for each effect it may call, the provider and the
schema digest (and the provider's version when it has one). A resume under a different digest is
reported; whether it proceeds is the definition's migration rule (design §5) or the operator's
choice. An unchanged schema digest does not prove a remote service behaves the same; it is the
best the runtime can check.

### 8.6 Kinds of agents: native first, adapter friendly

| Kind | How it runs | Recovery |
|---|---|---|
| native state machine | its states, step by step | per committed state |
| agentic agent (extension) | a native state machine | per model turn and per effect call |
| an agent of another framework | wrapped by an adapter, usually as one state | what the adapter provides; at worst the whole call reruns |
| a remote agent | through a remote protocol | the remote implementation's |
| a plain program | wrapped as one state | the whole program reruns |

Native agents get full state-level recovery; adapters are honest about what they cannot recover.

## 9. Registries and the runtime

Registries resolve names; the store keeps executions.

| | Registry | State store |
|---|---|---|
| holds | agent definitions, effect definitions, provider bindings | records, leases, inboxes, history, events |
| changes | at install, deploy, configuration change | continuously while running |
| role in recovery | provides the same executable environment | restores how far execution got |
| source | configuration, enabled plugins, the Python API | SQLite (memory for tests; others later) |

The registry is built when the runtime starts (configuration, enabled plugins, explicit
registrations) and handed to the kernel as its resolvers. It does not need to be a database.

The **Runtime** is the product's one API, embedded or served, with the same semantics:

```
runtime = Runtime.from_config("statecraft.yaml")   # or Runtime.local("./statecraft")
runtime.agents / runtime.effects                    # registries
root = await runtime.spawn("research", input={...})
await runtime.get(root); await runtime.signal(root, "STOP"); await runtime.answer(qid, value)
async for event in runtime.events(root): ...
```

```
statecraft serve --config statecraft.yaml          # the runtime as a server (HTTP + events)
StatecraftClient("http://...").spawn(...)          # the same API, remotely

POST /agents                      spawn
GET  /agents/{id}                 the record (tree with ?tree=1)
GET  /agents/{id}/history         its committed steps
GET  /agents/{id}/events          server-sent events from the durable log, resumable
POST /agents/{id}/signals         STOP | CONT | TERM | KILL | USR
POST /questions/{id}/answer       answer a waiting question
GET  /registry/agents | /registry/effects | /registry/providers
```

The CLI uses the same API: `statecraft agents list`, `effects list`, `run`, `ps`, `inspect`,
`signal`, `answer`, `events`.

## 10. Extensions

### 10.1 What an extension may do

Register agents, effects and providers through the public API; read its own configuration
section (`extensions.<name>` in the configuration and in definitions); add observation hooks.
It never patches the kernel.

### 10.2 statecraft-agentic

- **AgenticAgent**: the model loop as a native state machine (`prepare ▸ think ▸ act ▸
  wait_result ▸ finish`, design §9). `think` calls a model effect; `act` calls the chosen effect.
  Recovery stays as designed: committed model replies are reused; an unfinished `think` calls the
  model again.
- **Model effects**: the extension reads its configured models and registers one effect per
  model, `model.<name>` (e.g. `model.default`), with policy `read_only` (no side effects; a repeat
  may differ in content and cost). Timeouts, retries and recording are the effect manager's, so
  model calls are observed like everything else. No `Engine(model=...)`, no `ctx.model`.
- **Effects versus tools**: `effects` is what the agent may call; `extensions.agentic.tools` is
  the subset offered to the model. `model.default` can be callable by the agent without being
  offered to the model, so a model cannot reach internal or administrative effects.
- **Skills**: a `SkillCatalog` (Agent Skills format: `SKILL.md` and resources) discovers skills
  from configured folders and plugins. A definition lists the skills its loop may use; the loop
  offers their names, loads one on demand, and reads its resources. The loaded skill's digest is
  pinned in the agent's `data`, so recovery reloads the same version. A skill grants no
  permission: tools it mentions run only if the definition permits them; scripts in a skill run
  only as registered effects or through an isolated provider.
- **Decider** and **SkillAgent**: ordinary registered agents (`agentic:decider`,
  `agentic:skill`). A SkillAgent runs one skill as its own process, with its own checkpoints,
  faults and outputs.
- **Patterns**: reusable agents for sequence, fan-out and ask-a-person-on-fault.

### 10.3 statecraft-mcp

The MCP effect provider (HTTP, stdio, in-memory transports; discovery from `tools/list`; the
session named by the root; annotations proposed as policies) and an MCP-backed workspace provider.

### 10.4 statecraft-console

The visualizer, over the runtime API only: registry (agents with their states, effects with
schemas and policies, providers with health), runs (the live tree, each agent's walked path,
data, outputs, crossings, waits), questions answered in place, signals, workspace files, events.
It shows uniform lists for display; it does not impose a resource model.

## 11. Configuration

```yaml
runtime: {concurrency: 16}
store: {provider: sqlite, path: ./statecraft.db}
workspace: {provider: local, root: ./workspaces}

plugins: [statecraft-agentic, research-agents]       # enabled; installed ones not listed stay off

effect_providers:
  search: {provider: mcp, url: http://localhost:8001/mcp, policies: {query: read_only}}
  analysis: {provider: mcp, url: http://localhost:8002/mcp}

extensions:
  agentic:
    models:
      default: {provider: openai-compatible, endpoint: ${MODEL_ENDPOINT},
                api_key: ${MODEL_API_KEY}}           # resolved at start; never stored or logged
    skill_paths: [./skills]

agents:
  researcher:
    entry: statecraft_agentic:AgenticAgent
    effects: [model.default, search.query, analysis.summarize]
    extensions:
      agentic: {model: default, skills: [literature-review],
                tools: [search.query, analysis.summarize]}
  report:
    entry: research_agents.agents:ReportAgent
```

`extensions.agentic.models.default` is read by the agentic extension, which registers
`model.default`; the kernel never reads model settings.

## 12. Observability

Every effect call made in a step produces a **crossing**: effect, provider, sizes, duration,
attempts, failure class, effect key, and usage when the provider reports it. Crossings are
committed with the step in its history entry; a step that does not commit leaves its crossings in
a per-attempt diagnostic area for recovery tooling. Payloads are referenced, not copied.
Extensions add observation hooks (timings, diagnostics) without the kernel understanding them.
An OpenTelemetry exporter is optional.

## 13. Security and trust

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

## 14. Package layout

```
statecraft/
  kernel/      process, transitions, executor, store contract, signals, effect policy
  sdk/         agent, context, effect, contracts
  runtime/     runtime, registries, plugins, configuration, effect manager, workspace service
  providers/   memory and sqlite stores, local workspace, python effects
  server/  client/  cli/  testing/
statecraft-mcp/        provider, discovery, transports, MCP-backed workspace
statecraft-agentic/    agentic loop, model effects, decider, skill agent, skill catalog, patterns
statecraft-console/    the visualizer
```

The directories need not all exist from day one; the responsibilities and the dependency
direction must.

## 15. Migration

| Today | Moves to |
|---|---|
| `process.py`, `transitions.py`, `store.py`, `stores/`, `executor.py` | `kernel` (stores to `providers`) |
| `agent.py`, `context.py` | `sdk` (without `ctx.model`) |
| `engine.py` | `runtime` (`Runtime`) |
| `spec.py`, `catalog.py` | `runtime` registries; `tools` → `effects`, `spawns` split; no `core:decider` or skill-agent special cases; cycle and unwritten-read checks become lint |
| `workspace.py` | rules in `runtime`; `LocalFiles` in `providers` |
| `artifacts.py` | `sdk` contracts (applications may extend the envelope) |
| `views.py` | an extension (views are derived files) |
| `agentic.py` | `statecraft-agentic`; model clients become model effects |
| `testing.py` | `testing` |
| `console/` | `statecraft-console`, over the runtime API |
| the first application's MCP transport and file adapters | `statecraft-mcp` |
| the first application's envelope, session view, cumulative state and model filter | stay in that application |

Order, each step keeping the kernel's recovery tests green:

| Step | Scope | Done when |
|---|---|---|
| 1. Freeze and separate the kernel | `kernel` and `sdk` carved out; no model, skill or application in them; import tests | the recovery suite passes on `kernel` + `sdk` alone |
| 2. Public SDK | `ExecutableAgent`, `Context`, `@effect`, `EffectDefinition`, policies, transitions | an agent and a Python effect are written against the SDK only |
| 3. Registries | agent and effect registries; Python API, manifest, entry points; validation; pinning | agents and effects of an external package resolve and are pinned on their records |
| 4. Providers | sqlite and memory stores, local workspace, Python effects; `statecraft-mcp` | an MCP server's tools are effects with policies |
| 5. Agentic extension | `statecraft-agentic`: loop, model effects, decider, skill catalog, skill agent | the first application's agentic agents run on the extension unchanged in behaviour |
| 6. Standalone product | configuration, CLI, HTTP server, client, console | `statecraft serve --config` runs an application without its own server code |
| 7. Production | remote workers, a distributed store, worker isolation, retention | several workers recover each other's runs |

The first product test is not internal completeness: **a new package, unrelated to the first
application, installs statecraft, registers its own agent, a Python effect, an MCP effect and a
skill, and runs a job that recovers from a killed process.**

## 16. Decisions

| Question | Decision |
|---|---|
| models, prompts, conversations, skills in the kernel | no |
| how agents are known | AgentDefinition resolved by the runtime; the kernel sees a resolver |
| how external capabilities are known | EffectDefinition resolved by the runtime; the kernel sees a resolver |
| effect versus tool | a tool is an effect offered to a model |
| a model call | an effect registered by the agentic extension |
| MCP | an official effect provider, in its own package |
| agent registration | Python API, manifest, entry points; discovery is not activation |
| effect registration | Python API, provider discovery |
| skills | the agentic extension's catalog; used as instructions or run by a SkillAgent |
| registry storage | built from configuration and plugins; instances pin what they resolved |
| extension settings | opaque to the kernel, validated by each extension |
| lifecycle API | imperative signals and answers |
| source of truth for execution | the state store |
| source of truth for business data | the workspace |
| unit of recovery | the committed state transition |

Still open: the distributed store (design §14); worker isolation and its protocol; namespaces
and tenants, after the trust boundary.
