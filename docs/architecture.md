# Architecture

> The core is one thing: durable execution of state machines. Everything else is a resource in
> its store, a provider behind one of its interfaces, or an add-on built on its public API.

Status: **design, not implemented.** [design.md](design.md) is the core's execution design
(records, steps, fencing, intents, recovery) and stays authoritative for *how a step runs*. This
document says *what the system is made of*: the resource model, the layers, the providers and
the add-ons, and how today's code moves into them (section 11).

## 0. Why

statecraft was built core-first, and the core is right: pure transition rules, fenced commits,
intents carried out exactly once, recovery tested by killing processes. The layers around it were
drawn wrong:

- **Inside the engine but not the engine's**: the model loop (`AgenticAgent`), model clients
  (`ChatModel`, `openrouter`), the decider and the skill agent. They are agents built on the
  engine, yet the catalog special-cases them (`core:decider`, the skill-agent entry), the context
  exposes `ctx.model`, and `Engine(model=...)` takes one.
- **Outside the engine but the engine's**: tools and workspaces. An agent's tool calls and its
  files are what a step does; yet tool transport (MCP), tool discovery, sessions and remote
  workspaces were left to each application, behind an opaque `call(server, tool, args)` port.

Kubernetes solved the same shape of problem: one hard core (a consistent store and an API of
resources with spec and status, owners, versions and watch), controllers that reconcile, and
everything else (pods, volumes, services, deployments) as resources, drivers and add-ons. This
document applies that design.

## 1. Principles

1. **One core problem.** The core executes state machines durably and stores resources. Nothing
   else is in the core.
2. **Everything is a resource.** Agents, agent definitions, workspaces, tool servers and
   questions are resources in the store, with the same shape and the same API.
3. **Every execution is stored.** An agent is a resource; each step's commit is a new version of
   it, with its history entry and its event in the same transaction (design §2).
4. **Desired and observed are separate.** People, parents and applications write `spec`; the
   core writes `status`.
5. **Drivers behind interfaces.** Where bytes are kept (workspaces), how tools are reached, where
   the store lives and how changes are pushed are providers, chosen by configuration.
6. **Add-ons have no back door.** Base agents (the model loop, the decider) use the public API
   like any application. The core has no built-in agent names.
7. **Check before run.** Admission validates a resource when it is created: an agent's tools
   exist, its spawns resolve, its state graph is consistent.
8. **The log is the truth.** Watch is the durable event log; channels only make readers faster.

## 2. The analogy

| Kubernetes | statecraft |
|---|---|
| etcd + API server: create, get, list, watch, patch; `resourceVersion` | the **store** and the **API**; a resource's `revision` is its version; the event log is the watch stream |
| Pod | **Agent**: one running instance of a state machine |
| container image, CustomResourceDefinition | **AgentDefinition**: a state machine class and its declaration, versioned by code hash |
| kubelet and the container runtime | the **executor**: leases an agent, runs one step, commits it fenced |
| ownerReferences, cascading deletion | `owner` (the parent): an agent's subtree ends with it |
| finalizers | `on_term`: the state that runs before an agent ends |
| PersistentVolumeClaim, StorageClass, CSI driver | **Workspace** (a claim), **WorkspaceClass**, **workspace provider** |
| a volume mounted read-only; VolumeSnapshot and clone | **attach** (read another run's workspace); **fork** (inherit an earlier run's outputs) |
| ConfigMap mounted into a pod | **skills**: file bundles a definition ships, projected into the agent's workspace |
| Service and endpoints | **ToolServer** (MCP): spec says how to reach it, status lists its tools |
| Secret | credentials, referenced by name, never stored in a resource |
| admission webhooks | **admission**: validation when a resource is created |
| Events | **crossings**: what a step called, committed with the step |
| Deployment, Job, CronJob controllers | **base agents** (add-on): model loop, decider, skill agent, flow patterns |
| kubectl, Dashboard | the **CLI** and the **console**, generic over every kind |
| Namespace | reserved (`meta.namespace`), not implemented yet |

## 3. Resources

Every resource has one shape:

```yaml
kind: Agent                     # Agent, AgentDefinition, Workspace, WorkspaceClass, ToolServer, Question
meta:
  name: report/claim_check      # unique per kind (agents: the path in the tree)
  namespace: default            # reserved; one namespace until namespaces are designed
  owner: report                 # the resource this one belongs to (cascades)
  labels: {feature: research}
  revision: 42                  # bumped by every change; watch resumes from it
  created: 2026-11-27T10:00:00Z # in meta only, never inside outputs (outputs stay reproducible)
spec: {...}                     # desired: written by people, parents, the application
status: {...}                   # observed: written by the core and its controllers
```

API, identical for every kind: `create`, `get`, `list` (by kind, owner, labels), `watch` (from a
revision), `patch spec`, `delete`. Status is written only by the core. The store's existing
contract (fenced step commits, CAS control commits, history and events in the same transaction)
is how the API is implemented for Agents; other kinds use plain CAS on their revision.

### 3.1 Agent

The instance: today's record, presented as spec and status.

```yaml
kind: Agent
meta: {name: report/claim_check, owner: report}
spec:
  definition: research:claim_check@<code hash>   # pinned at creation
  input: {...}
  workspace: report                              # the claim it works in (3.3)
  lifecycle: run                                 # run | stop | end
  inherits: {from: run1, agents: [outline]}      # a fork (optional)
status:
  phase: active                                  # active | succeeded | terminated
  view: suspended                                # running | runnable | suspended | pending | ended
  state: write
  step: 7
  waits: [{kind: join, children: [verdict]}]
  outputs: {claim_report: {object: home/.../ab12.json, sha256: ab12...}}
  conditions: [{type: DefinitionChanged, status: false}]
  error: null
  lease: {owner: executor-2, epoch: 3, expires: ...}
```

**Lifecycle is declarative.** STOP and CONT set `spec.lifecycle` to `stop` and `run`; `end` (or
deletion) terminates, with `on_term` as the finalizer; a forced deletion is KILL; USR messages are
a mailbox subresource. Internally nothing changes: a spec change is applied as today's inbox
entry by a control commit (design §8), so the fencing and exactly-once rules stay as they are.
Spawning a child is creating an Agent with `owner` set, done by the parent's committed intent
(design §6).

`data` (the state machine's working data) stays in the record and is readable through the API,
but it is the agent's, not part of the spec/status contract.

### 3.2 AgentDefinition

```yaml
kind: AgentDefinition
meta: {name: research:claim_check}
spec:
  entry: my_research.agents.claim_check.agent:ClaimCheck
  description: ...
  tools: [search.facts]             # server.tool: its allow-list
  spawns: [outline]                 # agents it may create (moved out of tools)
  reads: [sources]
  writes: [claim_report]
  skills: [claim-style]             # file bundles projected into its workspace
  workspace: {access: tree}         # tree | home-only | own
  meta: {...}                       # keys the core does not read, for the application
status:
  code: <hash of the folder>
  graph: {start: gather, states: [...], transitions: [...]}   # read from the class
  valid: true
  problems: []
```

Feature packages are scanned as today (`agents/<...>/agent.yaml + agent.py`,
`skills/<...>/SKILL.md`) and **applied** to the store as definitions, like applying CRDs. Every
executor reads definitions from the store, so all agree; an Agent pins the code hash it started
with, and a resume under another hash sets the `DefinitionChanged` condition (design §5's
migration rule decides what may proceed).

A skill is a definition's resource bundle, not an agent: the core projects it into the agent's
home when it starts. What a skill *means* (instructions for a model) is the agentic add-on's
business; a skill that declares outputs becomes an agent through that add-on (section 7).

### 3.3 Workspace and WorkspaceClass

```yaml
kind: Workspace                     # a claim
meta: {name: report, owner: report}
spec:
  class: local                      # which provider
  access: tree                      # tree (read-write for the tree) | ro (attach)
  source: {clone-of: run1}          # fork: start from another workspace's committed objects
status:
  phase: bound
  provider: local
  location: /var/statecraft/workspaces/report
```

```yaml
kind: WorkspaceClass
meta: {name: local}
spec: {provider: local, parameters: {root: /var/statecraft/workspaces}}
```

The **rules** inside a workspace stay the core's, enforced on any provider: `shared/` for the
tree; `home/<agent>/` with `private/`, `staging/<step>/<epoch>/` (per attempt), `objects/`
(content-addressed, create-only), `skills/`; another agent's private and staging are unreadable;
outputs are read only through committed references (design §7, §11).

A **workspace provider** implements: provision, bind, get, put, create-only, exists, list,
snapshot. Providers: `local` (a folder), `object-store` (S3-compatible, a prefix per claim),
`mcp-files` (files kept by an MCP server, reached through it). Attach is a second claim with
`access: ro` on an existing workspace; fork is a claim with `clone-of`, served by snapshot or by
reference (today's inheritance: nothing copied).

### 3.4 ToolServer

```yaml
kind: ToolServer
meta: {name: search}
spec:
  transport: mcp-http               # mcp-http | mcp-stdio | in-memory
  address: ${SEARCH_URL}
  session: {header: X-Session-Id}   # the agent's workspace claim names the session
  trusted: true                     # annotations decide retries
  manifest: manifests/search.json   # pinned listing: admission works without the server
  limits: {concurrency: 8, deadline: 120s}
  credentials: secret:search-token  # by reference
status:
  health: up
  fingerprint: 3f2a...
  tools:
    - {name: facts, input_schema: {...}, output_schema: {...},
       annotations: {readOnlyHint: true, idempotentHint: true}}
```

MCP is the tool model: in-process tools are served over the in-memory transport, so every tool is
listed, described and called the same way. A **discovery controller** fills `status` from
`tools/list` (and refreshes on list-changed); a listing that differs from `manifest` is drift.

**A tool call is core**: `ctx.call("search.facts", ...)` resolves the server, checks the
definition's allow-list, validates arguments against the input schema, sends with the agent's
workspace claim as the session, classifies a failure, retries by effect class, and records a
crossing in the step (section 6). Sessions are scoped to a workspace claim, opened on first use,
closed when the claim's tree ends or after an idle time.

### 3.5 Question

A person's decision a run waits on: `spec` is the question (text, schema, default, deadline),
`status` the answer and who gave it. Today's answer waits become Questions owned by the waiting
agent, so pending decisions are listed and watched like everything else.

## 4. Layers and packages

```
applications           agents, tool servers and conventions of one system
───────────────────────────────────────────────────────────────────────────────
statecraft.agents      add-on: model loop, ModelProvider kind, decider, skill agent, flow
                       patterns, views
statecraft.console     add-on: the visualizer, over the API only
───────────────────────────────────────────────────────────────────────────────
statecraft.providers   workspace: local, object-store, mcp-files
                       tools: in-memory, mcp-http, mcp-stdio
                       store: memory, sqlite (postgres later)
                       channel: in-process (postgres, redis later)
───────────────────────────────────────────────────────────────────────────────
statecraft (core)      resources and API, store contract, admission, executor, the five core
                       kinds, the provider interfaces, the test kit
```

One repository, three distributions: `statecraft` (core and the basic providers),
`statecraft-agents`, `statecraft-console`. Providers with heavy dependencies are extras
(`statecraft[mcp]`, `statecraft[s3]`, later `statecraft[postgres]`).

Import rules, each enforced by a test:

- the core imports only the standard library, `pydantic`, `pyyaml` and its own modules;
- a provider imports the core's interfaces and its own dependencies;
- `statecraft.agents` and `statecraft.console` import only the core's public API (and providers'
  public classes when configured); the core never imports them;
- no core module names an add-on, a built-in agent, or an application.

## 5. Controllers

Each is a loop over resources, like the executor already is:

| Controller | Watches | Does |
|---|---|---|
| executor | Agents | leases a runnable agent, runs one step, commits it fenced; carries out intents (design §6, §10) |
| lifecycle | Agent spec changes | turns `lifecycle` and deletion into control commits (stop, continue, terminate, kill) |
| workspace binder | Workspaces | binds a claim to its class's provider; provisions, snapshots, releases |
| tool discovery | ToolServers | lists tools, checks health, records the fingerprint and drift |
| admission | creation of any resource | validates (section 9) |
| garbage collector | owners | ends what an ended owner leaves; releases idle tool sessions and workspaces by policy |

Controllers are idempotent and may run several times (design §2): each acts on a resource's
revision with CAS, like the executor's control commits.

## 6. Crossings

Every tool call, workspace operation outside the agent's own home, and (in the agents add-on)
model call made inside a step produces a **crossing**: what was called, sizes, duration, failure
class, retries, effect key, and usage when there is one. Crossings are collected during the step
and committed with it, in its history entry; a step that does not commit leaves them in a
per-attempt diagnostic area (like staging) for recovery tooling. Payloads are referenced, not
copied. An OpenTelemetry exporter is optional.

Tool failures have one type with a class:

| Class | Example | Retried by the core | Then |
|---|---|---|---|
| `invalid` | bad arguments, unknown tool, schema violation | no | returned to the caller |
| `refused` | not in the allow-list | no | fault |
| `transient` | connection reset, 5xx, 429 | by effect class | fault when exhausted |
| `timeout` | deadline passed | as `transient` if idempotent | fault |
| `unavailable` | server down | with backoff, within the lease | fault |
| `failed` | the tool reported an error result | no | returned to the caller |

Effect class comes from the tool's annotations when its server is trusted, else the conservative
MCP defaults (destructive, not idempotent): read-only or idempotent tools are retried;
non-idempotent tools only when called with `idempotency_key=ctx.effect_key(...)`; otherwise the
fault goes to the parent (design §7).

## 7. The agents add-on

`statecraft.agents` is a feature package like any application's, plus one resource kind:

- **AgenticAgent**: the model loop as states (`prepare ▸ think ▸ act_one ▸ wait_result ▸
  finish`, design §9). It calls tools through `ctx.call` like any agent, so tool checks,
  sessions, retries and crossings come from the core.
- **ModelProvider** (a resource kind the add-on defines): transport (`openai-compatible`,
  `scripted`), address, credentials by reference, request body (e.g. zero retention), layers
  (deadline, retry, filters such as masking or redaction). An agentic definition names its
  model provider; the core never sees a model.
- **Decider** and **SkillAgent**: ordinary definitions in this package (`agents:decider`,
  `agents:skill`), no longer built-in names. The add-on reads a definition's skills (projected by
  the core) as its instructions.
- **Flow patterns**: reusable definitions for sequence, fan-out and retry-with-a-person, so
  applications stop rewriting orchestrators.
- **Views**: `StateView` and other derived files are controllers this add-on offers.

Why models are not tools: a model call carries a conversation, tool definitions and streaming,
and its meaning (a turn of a loop) belongs to the loop, not to the engine; MCP sampling exists but
would push the loop's semantics into every server.

## 8. Configuration

One file (or the same data as a Python object) declares what an engine runs with:

```yaml
store: sqlite:/var/statecraft/state.db
channel: in-process
features: {research: my_research.agents}           # applied as AgentDefinitions
workspaceClasses:
  local: {provider: local, parameters: {root: /var/statecraft/workspaces}}
defaultWorkspaceClass: local
toolServers:
  search: {transport: mcp-http, address: ${SEARCH_URL}, session: {header: X-Session-Id},
           trusted: true, manifest: manifests/search.json}
addons:
  statecraft.agents:
    modelProviders:
      default: {transport: openai-compatible, address: https://openrouter.ai/api/v1,
                credentials: env:OPENROUTER_API_KEY,
                body: {provider: {zdr: true, data_collection: deny}}}
hooks:                                              # application extensions by module:function
  envelope: my_app.envelope:Artifact
```

```
Engine.from_config("statecraft.yaml")
python -m statecraft serve   --config statecraft.yaml     # controllers and executors
python -m statecraft apply   --config statecraft.yaml     # apply definitions and servers
python -m statecraft.console --config statecraft.yaml     # the visualizer
```

## 9. Admission

Checked when a resource is created or applied; a failure is a refusal with reasons, not a crash
later:

- **AgentDefinition**: the class loads; `start` and every named state exist; transitions the code
  names point at states (dynamic ones are marked); every `server.tool` exists on its ToolServer
  (live or pinned); skills exist and their `allowed-tools` are within `tools`; spawns resolve;
  no spawn cycle; every read has a writer somewhere.
- **Agent**: its definition exists and is valid; its input meets the definition's input schema
  when one is declared; its workspace claim exists or is created; an owner exists.
- **ToolServer**: name unique; transport known; manifest readable.
- **Workspace**: class exists; a `clone-of` source exists and is ended or read-only.

## 10. The console

The console reads the API only: every kind is listed and shown generically (meta, spec, status,
revision history, owner tree), with dedicated views for the kinds that need them:

- **AgentDefinitions**: the state graph, tools, spawns, skills; the graph of spawns, tool use and
  data flow across definitions;
- **Agents**: the live tree, each agent's walked path on its definition's graph, data, outputs,
  crossings, waits; lifecycle controls (they patch `spec.lifecycle`);
- **ToolServers**: health, tools with schemas and annotations, drift, which definitions use them;
- **Workspaces**: claims, bindings, files, attach and fork relations;
- **Questions**: open questions, answered in place.

Because add-ons define their own kinds (`ModelProvider`), the generic view shows them without the
console knowing them.

## 11. Migration from today's code

| Today | Moves to |
|---|---|
| `process.py`, `transitions.py`, `agent.py`, `store.py`, `stores/`, `executor.py`, `engine.py`, `context.py` | core; records gain the resource shape (meta, spec, status) as a view over today's fields first, then as the stored shape |
| `spec.py`, `catalog.py` | core (definitions, applied to the store), without the `core:decider` and skill-agent special cases |
| `workspace.py` | rules stay in the core; `LocalFiles` becomes `providers.workspace.local` |
| `artifacts.py` | core (an Agent's outputs); applications extend it through a hook |
| `views.py` | `statecraft.agents` (views are add-on controllers) |
| `agentic.py` | `statecraft.agents`; `ctx.model` and `Engine(model=...)` leave the core |
| `testing.py` | core test kit, as an in-memory tool provider |
| `console/` | `statecraft.console`, reading resources only |
| the first application's MCP transport, files and contracts adapters | `statecraft.providers` (`mcp-http`, `mcp-files`) |
| the first application's envelope, session view, cumulative state, masking | stay in the application (a hook, a view, a model filter) |

Order, each step keeping the core's recovery tests green:

| Step | Scope | Done when |
|---|---|---|
| 1. Split the layers | move `agentic.py`, the decider, the skill agent and views into `statecraft.agents`; remove `ctx.model`, `Engine(model=)`, `core:` names; import rules as tests | the core imports no add-on; the first application runs unchanged through the add-on |
| 2. Resource shape and API | meta, spec, status over today's records; `create/get/list/watch/patch/delete`; declarative lifecycle over the inbox | the console and CLI use only the API; signals are spec patches |
| 3. Definitions in the store | apply feature packages as AgentDefinitions; pin code hashes; state graph in status; admission | a bad transition or missing spawn is refused at apply |
| 4. Tool servers | ToolServer kind, MCP transports in providers, discovery controller, allow-list and schema checks, failure classes, crossings | the first application's transport and adapters are deleted; a killed server mid-run is retried or faulted by effect class |
| 5. Workspaces | Workspace and WorkspaceClass kinds, provider interface, `local`, `object-store`, `mcp-files`; attach and fork as claims | a run moves between providers by configuration only |
| 6. Configuration and channels | `Engine.from_config`, `statecraft serve/apply`, the console from the same file; push channels | one file builds every entry point; events reach another process without polling |
| 7. Questions and the rest | Question kind; garbage collection; drift and definition-change conditions on resume | pending decisions are listed and watched; resumes report what changed |

## 12. Decisions

| Question | Decision |
|---|---|
| lifecycle | declarative (`spec.lifecycle`, deletion with `on_term` as finalizer); the inbox stays the internal mechanism |
| agent definitions | stored resources, applied from feature packages, pinned by code hash |
| packaging | one repository, three distributions (`statecraft`, `statecraft-agents`, `statecraft-console`); import direction tested |
| namespaces | later; `meta.namespace` reserved |
| models | a `ModelProvider` kind owned by the agents add-on; not a core concept and not an MCP tool |
| tools | MCP is the tool model; in-process tools use the in-memory transport |
| skills | file bundles of a definition, projected by the core; interpreted by the agents add-on |

Still open: the distributed store adapter (design §14); multi-tenancy once namespaces are
designed.
