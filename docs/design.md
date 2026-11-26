# statecraft design: a durable state machine executor for agents

> Agents are checkpointed state machines; models are optional reasoning components.

Status: sections 1 to 11 are implemented; section 12 is planned. Items marked *(planned)* are
specified but not built yet. The recovery properties the tests check are in section 13.

## 0. Goals

statecraft runs **semi-agentic** systems: fixed procedures (code) and model-driven agents
working together, for hours or days, with people answering questions on the way. So:

1. **Recovery from anywhere.** A worker, a machine or a deploy may die at any time; an agent may
   fail at any state. **Committed transitions are never lost. Unfinished states may be executed
   again. External effects must be idempotent, deduplicated, or explicitly reconciled** (section 7).
2. **One execution model** for scripted and agentic agents.
3. **A standalone engine.** No web framework, broker or server inside; deployments connect to it
   through small ports whose semantics the engine defines (section 10).
4. **Light.** A small kernel: the process transition rules as pure functions, plus an executor
   and a store. No graph DSL, replay engine, generic effect journal, session manager or separate
   orchestrator.

## 1. Model

- The engine runs a **pool of agent processes**. Each has an id, a parent, the input it was
  spawned with, its contract (`agent.yaml`), its **state machine** and its record.
- The **root agent** is the job (like init). A "run" is a root agent's tree. An application keeps
  only the root's id. *Root finished* means the root is terminal; *run finished* means the whole
  tree is.
- Agents depend on each other only through **spawn** and **join**. Everything else they share goes
  through the **workspace**.
- **Every agent is a state machine.** Scripted agents subclass `Agent`; model-driven agents
  subclass `AgenticAgent` (section 9). A parent does not know or care how a child works.
- **No agent framework inside.** A model is a port: a chat client with tool calling.
- The **execution context is managed, not a call stack**: an agent's context is its record in the
  state store (state, `data`, waits, wake input, children, intents). A state handler's stack exists
  only while that one step runs.

## 2. Execution invariants

1. **One valid execution owner per agent.** At most one executor holds a valid lease; only its
   step commits are accepted (lease owner, epoch and expiry are checked at the commit).
2. **Atomic transitions.** The next state, `data`, wait reasons, intents, published references and
   the step's durable events and history entry are committed together, or not at all.
3. **At-least-once state execution.** A state that did not commit may run again, with the data and
   wake input it started with.
4. **No hidden execution context.** Nothing durable lives on a Python stack or on `self`.
5. **Durable control operations.** Spawns, signals and answers are never lost and never applied
   twice: each has a deterministic id and is recorded before it takes effect.
6. **Deterministic wake conditions.** Whether a wait has cleared is decided from committed records
   and the clock against committed deadlines only. Notifications only make it faster.
7. **Explicit failure policy.** An agent-level fault is decided by its parent (or the engine user,
   for the root); the engine never retries an agent's state on its own.
8. **Derived views are rebuildable.** The process table, open questions, artifact index and
   summaries are projections of the records.
9. **Terminal states are final.** A succeeded or terminated agent never runs again without an
   explicit restore (section 13).
10. **External effects have explicit recovery semantics** (section 7).

## 3. The record: three counters and the lifecycle

| Field | Meaning | Changes when |
|---|---|---|
| `revision` | the store's CAS version of the record | any change to the record |
| `step` | the agent's logical progress: how many states it has committed | a state commits (and only then) |
| `lease.epoch` | the generation of execution ownership | a lease is acquired, or ownership is revoked (`KILL`) |

**Effect keys, question ids and staging paths use `step` and the state's name**, never
`revision`: `<agent>/<step>/<state>/...`. They stay the same however many times the same logical
state is retried (after a crash, a fault, a `CONT`, a lease change), and differ when a control
operation moves the agent to another state at the same step (`TERM` moves it to `on_term`).
Later, `set_state` adds a lineage: `<agent>/<lineage>/<step>/<state>/...`.

Lifecycle:

```
status  = "active" | "succeeded" | "terminated"
reasons = [ join{children} | answer{question_id} | sleep{until} | stopped | fault{error, state} ]
lease   = None | {owner, epoch, expires}          (held by the store apart from the record's step CAS)
wake    = None | {kind, ...}                       (the input of the state that woke; section 8)
```

| Derived view | Condition |
|---|---|
| running | `active`, a lease that has not expired |
| suspended | `active`, at least one reason |
| runnable | `active`, no reason, no unexpired lease, no required intent pending |
| succeeded / terminated | the status |

| Reason | Set by | Cleared when |
|---|---|---|
| `join {children}` | the agent's `Wait(join=...)` | every listed child is succeeded or terminated, or one of them faults (with `on_fault`) |
| `answer {question_id}` | the agent's `Wait(answer=...)` | a valid answer to that id is committed |
| `sleep {until}` | the agent's `Wait(sleep=...)` | the committed deadline has passed (checked by the clock) |
| `stopped` | `STOP` | `CONT` |
| `fault {error, state}` | its current state raised | `CONT` (the faulted state runs again) |

## 4. Every agent is a state machine

An agent is a set of named **states**: async methods registered with `@state` (other methods are
helpers). A state does work and returns what happens next:

```python
return "next_state"
return Spawn({"area": ("agent.operational_area_identification", {...})}, then="wait_area")
return Wait(join=["area"], then="features", on_fault="child_failed")
return Wait(
    answer={"name": "child_fault", "question": "...", "schema": {...}, "default": "end"},
    then="after_fault",
)
return Wait(sleep=30, then="poll")
return Done
# raising an exception: a fault at this state
```

### One step

```
executor holds a lease (owner, epoch e) on the agent at step s
→ run the current state's handler with (ctx, a copy of data); ctx.woke = the record's wake
→ it returns a transition (or raises)
→ commit_step(agent, lease, expected_step=s, outcome)         ← the step's only durable write
     outcome = Transition{state or reasons, data, intents, published refs, durable events,
                          history entry}: wake consumed (set to None), step becomes s + 1
             | Fault{error}: reason fault; step, state, data and wake unchanged
→ after the commit: carry out the intents (section 6)
```

| What happened | Committed afterwards | Continuing does |
|---|---|---|
| A finished, returned B | `state = B` | runs B |
| A finished, then `STOP` arrived | `state = B`, reason `stopped` | **resume**: runs B (= Next(A)) |
| A returned `Wait(..., then=B)` | `state = B`, the wait's reason | **resume**, once it clears: runs B (wake set) |
| A raised (fault) | `state = A`, data and wake from before A, reason `fault` | **recovery** (after `CONT`): runs A again |
| the executor died while A ran | nothing: `state = A`, data and wake from before A | **recovery** (lease expired): runs A again |
| it died after the commit, before carrying out intents | `state = B`, intents pending | intents completed first, then B |

### Rules for writing states

- **Inside a state**: tool calls, workspace reads, writes to the agent's private and staging space,
  artifact staging. These may run again (invariant 3) and follow the effect rules (section 7).
- **Only in what a state returns**: spawn, join, asking a person, sleep, signals, finishing.
  (`ctx.signal` only records an intent; it is sent after the commit.)
- **Nothing on `self`.** Everything later states need goes into `data`.
- **`ctx.woke` is durable input to the woken state**: it is stored in the record and stays the same
  however many times that state is retried, until the state commits. Copy what later states need
  into `data`.
- **`data` is JSON and small.** Large content goes to files; `data` holds immutable references.

### Examples

A leaf agent that measures with a tool, asks a decision agent, and checks its answer:

```python
from statecraft import Agent, Spawn, Wait, Done, state


class ClaimCheck(Agent):
    start = "gather"

    @state
    async def gather(self, ctx, data):
        data["facts"] = await ctx.call("search.facts", claim=ctx.input["claim"])
        return Spawn(
            {
                "verdict": (
                    "core:decider",
                    {
                        "question": question_of(data["facts"]),
                        "schema": {"enum": ["true", "false"]},
                        "default": "false",
                    },
                )
            },
            then="wait_verdict",
        )

    @state
    async def wait_verdict(self, ctx, data):
        return Wait(join=["verdict"], then="write", on_fault="write")

    @state
    async def write(self, ctx, data):
        answer = None
        if ctx.woke["children"].get("verdict") == "succeeded":
            answer = (await ctx.read("decision", by="verdict")).payload["answer"]
        await ctx.write("claim_report", {"facts": data["facts"], "verdict": answer or "unknown"})
        return Done
```

A procedure: one step, then several in parallel, then a sequence; a person decides about a
child's fault:

```python
class ResearchReport(Agent):
    start = "outline"

    @state
    async def outline(self, ctx, data):
        return Spawn({"outline": ("agent.outline", {})}, then="wait_outline")

    @state
    async def wait_outline(self, ctx, data):
        data["waiting_in"] = "wait_outline"
        return Wait(join=["outline"], then="sources", on_fault="child_failed")

    @state
    async def sources(self, ctx, data):
        if ctx.woke["children"]["outline"] != "succeeded":  # a join ends waiting, it does not
            raise ChildFailed("outline")  # promise success: the agent decides
        return Spawn({name: (f"agent.{name}", {}) for name in SOURCES}, then="wait_sources")

    @state
    async def wait_sources(self, ctx, data):
        data["waiting_in"] = "wait_sources"
        return Wait(join=SOURCES, then="section", on_fault="child_failed")

    @state
    async def section(self, ctx, data):
        index = data.get("index", 0)
        if index == len(SECTIONS):
            return Spawn({"report": ("agent.report", {})}, then="wait_report")
        data["index"] = index + 1
        return Spawn({SECTIONS[index]: (f"agent.{SECTIONS[index]}", {})}, then="wait_section")

    @state
    async def wait_section(self, ctx, data):
        data["waiting_in"] = "wait_section"
        return Wait(join=[SECTIONS[data["index"] - 1]], then="section", on_fault="child_failed")

    @state
    async def wait_report(self, ctx, data):
        data["waiting_in"] = "wait_report"
        return Wait(join=["report"], then="done", on_fault="child_failed")

    @state
    async def done(self, ctx, data):
        return Done

    @state
    async def child_failed(self, ctx, data):
        fault = ctx.woke["faulted"][0]  # one fault at a time (section 8)
        data["failed"] = fault["child"]
        return Wait(
            answer={
                "name": "child_fault",
                "question": fault["error"],
                "schema": {"enum": ["retry", "end"]},
                "default": "end",
            },
            then="after_fault",
        )

    @state
    async def after_fault(self, ctx, data):
        ctx.signal(data["failed"], "CONT" if ctx.woke["answer"] == "retry" else "TERM")
        return data["waiting_in"]  # back to waiting
```

**A join is end synchronization, not a success guarantee**: a child ended by `TERM` (for
instance after a person answered `end`) clears the join as `terminated`, and each parent checks
the results it needs. The engine adds no such policy. `SOURCES` and `SECTIONS` are plain lists in
the agent's own module. No sequence, parallel or graph
helpers. A child that faults again later produces a new question (its id carries the parent's step,
section 8), and is found again when the parent waits again.

## 5. The record and its history

The authoritative record of an agent lives in the **state store** (section 10):

```json
{ "id": "report/claim_check", "agent": "research:claim_check",
  "parent": "report", "name": "claim_check", "input": {...},
  "code": "<hash of the agent's folder>", "schema_version": 1, "mode": "python",
  "revision": 42, "step": 7,
  "status": "active", "state": "write", "data": {...},
  "reasons": [], "wake": {"kind": "join", "children": {"verdict": "succeeded"}},
  "intents": [{"id": ".../verdict", "kind": "spawn", "done": true}],
  "children": {"verdict": "report/claim_check/verdict"},
  "published": {"claim_report": {"path": "home/<id>/objects/ab12….json",
                                      "sha256": "ab12…", "step": 6}},
  "inbox_applied": 3, "error": null }
```

**History**: the step commit includes a history entry (a snapshot of the record after the step);
so do faults, wait resolutions and applied lifecycle signals. Lease changes, intent completion
marks and projection updates are not history. Large content is referenced immutably, so snapshots
stay small and keep their meaning; a retention policy bounds them.

**Code changes**: `code` and `schema_version` are recorded. A new version of an agent is applied
when the agent is **suspended** (stopped or faulted), not in the middle of running. When its
`schema_version` is newer than the record's, the engine calls the agent's
`migrate_data(old_version, data)` first; without a compatible migration it refuses with a clear
error. A stored state that no longer exists is refused the same way.

## 6. Intents (the engine's outbox)

A state's control actions are **committed as intents** with deterministic ids, then carried out:

| Intent | Id | Carried out as |
|---|---|---|
| spawn a child | `<parent id>/<name>` | create the child's record if absent (create-only) |
| signal an agent | `<agent id>/<step>/signal/<n>` | append to the target's inbox if that id is not there |

- **Idempotent**: carrying out an intent twice changes nothing (create-only, append-if-absent).
- **Marked done by CAS** (`commit_control`), never by overwriting the record with an older copy.
- **Required before the next state**: an agent with a pending spawn or signal intent is not
  runnable until they are done.
- **Committed means it happened**: a committed spawn is always carried out, even if the parent is
  killed right after. **Intents outlive the agent**: the executor works on every record with work
  left (`active`, or terminal with intents pending), not only active ones.
- **An agent's end records `TERM` intents** for its live children, in the same commit (or kill)
  that ends it; a child created later from a pending spawn of an ended parent gets its `TERM`
  intent when the spawn is carried out. So no orphan is left.

Artifact publication is **not** an intent: the commit that contains the published reference is the
publication (section 7). Durable events and history entries are written by the commit itself.

## 7. Effects and artifacts

State execution is at-least-once, so every effect has a class, which a tool should declare
(MCP tools, for instance, in their annotations).

| Effect | Rule |
|---|---|
| reads, pure computation (most tools) | repeat freely |
| idempotent writes (overwrite by key, create-if-absent) | repeat with the same key |
| model calls | may repeat (a recovered `think` asks again from the same history) |
| the agent's staging space | per attempt: `staging/<step>/<epoch>/`, so a stale worker never touches the current attempt's files |
| the agent's private space | scratch that may be lost or rewritten by a stale worker; nothing durable relies on it |
| artifacts | stored immutably, published by the commit's reference (below) |
| spawn, signal, answer | engine intents and records with deterministic ids |
| non-repeatable external effects | `ctx.call(..., idempotency_key=ctx.effect_key("name"))`, key = `<agent id>/<step>/<name>`; a tool given the same key with a different payload must refuse; a tool that cannot honour keys must not be called from a state that can rerun, or the agent writes a reconciliation step |

### Artifacts

1. `ctx.write(type, payload)` stores the payload as a **content-addressed, create-only object**:
   `home/<agent-id>/objects/<sha256>.json`. Writing the same content again is a no-op; nothing ever
   rewrites an object.
2. The step's commit records the reference (`published[type] = {path, sha256, step}`). **That
   commit is the publication.**
3. The artifact index (`run/views/artifacts.json`) is a projection of the records' `published`.

A worker that lost its lease can still write objects, but it can never publish one (its commit is
refused), and it can never overwrite one that was published. **Other agents read an object only
through a committed reference**: `ctx.read` follows `published`, and `ctx.files` refuses another
agent's `objects/` path that no committed reference names.

## 8. Signals, waits, wakes and answers

### Signals

Signals are intents (section 6) ending in the target's inbox. The engine applies them with
`commit_control`: at once if the target is suspended or runnable, after its running step otherwise.

| Signal | Meaning | Handled by the agent? |
|---|---|---|
| `STOP` | no new step starts until `CONT` | no |
| `CONT` | clears `stopped` and `fault` (the faulted state runs again) | no |
| `TERM` | cooperative end: waits cleared, it moves to `on_term`; **when `on_term` commits, the agent is `terminated`** (an agent without `on_term` is terminated at once) | yes, `on_term` (cleanup only) |
| `KILL` | logically ends it now: `terminated`, lease epoch bumped (a running step's commit is refused); cancelling the local task is best effort, and a tool call already sent to a server is not undone | no |
| `USR {message}` | a message for the agent's own protocol; in `ctx.signals` until a step commits past it | yes |
| `CHLD` | a notification to a parent that a child faulted or ended; the parent's wait is decided from the child's record, not from the notification | n/a |

| Situation | Behaviour |
|---|---|
| `fault` and `stopped` both hold | one `CONT` clears both |
| `TERM` while `stopped` | waits cleared, moves to `on_term` (or ends) |
| a step commit after `KILL` | refused (epoch) |
| a parent ends (succeeded or terminated) | its live children get `TERM` |
| a joined child is terminated | the join clears with `"terminated"` for that child (not success) |
| the child ended before the `Wait` was committed | the wait is evaluated at commit time and clears at once |
| an answer while `stopped` | stored and the answer wait cleared; `stopped` remains |
| several children fault | the join clears with `wake.faulted = [{child, error}, ...]`; the parent may handle them one at a time, and any it leaves are found again when it waits again |

### Wakes

When a wait clears, the engine commits (`commit_control`) the resolution **and the wake input** into
the record: `wake = {kind: "join", children: {...}, faulted: [...]}`, `{kind: "answer",
question_id, answer}`, `{kind: "sleep"}`. The woken state sees it as `ctx.woke`; it stays in the
record through faults, crashes and retries of that state, and is consumed when the state commits.
`ctx.signals` (USR) follow the same rule (`inbox_applied` advances only at a commit).

### Questions and answers

- A state asks with a business **name**; the engine makes the **id**:
  `<agent id>/<step>/question/<name>`. A retry of the same state gets the same id; a later question
  (a later step) gets a new one, so an old answer is never reused for a new question.
- `engine.answer(question_id, answer)` is checked atomically: the question is open on that agent,
  not answered yet, and the answer matches its schema. A second answer is refused.
- Timeouts: an answer wait may carry a deadline, then it clears with the question's default and
  `by: "timeout"`.

## 9. `Agent` and `AgenticAgent`

### `Agent`

`@state` methods, a `start` state, optional `on_term`, `schema_version` and `migrate_data`.

### `AgenticAgent`: a model loop as states

| Feature | How |
|---|---|
| instruction | `instruction`, or the agent's first skill (`SKILL.md`); `task_instruction(ctx)` and `task(ctx)` are overridable |
| tools | its declared tools (`model_tools(ctx)` may narrow them), plus the agent primitives as tools: `read_artifact`, `read_file`, `list_files`, `load_skill`, `spawn_agent`, `wait_for`, `ask_person` |
| skills | installed in its home when it starts; listed in the instruction, loaded on demand (`load_skill`) |
| structured output | JSON by default (`json_output`); `check_output` refuses an answer, which is asked again (`max_output_retries`); `shape_output` normalizes it |
| result | **the final answer is written as its output artifact** (`output_type`, or its one declared type); its parent joins, then reads it |
| model | the `Model` port; `model_override` names a model per agent |
| history | the conversation, in `data`; the model's private reasoning is never kept |
| limits | `max_model_turns`, `max_tool_calls`, `max_children` |
| compaction, callbacks, a token budget, a depth limit | *(planned)* |

States, **one tool call per transition**:

```
prepare ─▶ think ─▶ act_one ─▶ act_one ─▶ ... ─▶ think ─▶ ... ─▶ finish ─▶ Done
                       │
                       └─ a boundary tool (spawn / join / ask_person / sleep) returns Spawn/Wait;
                          wait_result appends its result as the tool response, then act_one
                          (calls remain) or think
```

```python
data = {
    "history": [...],
    "pending_calls": [...],
    "tool_cursor": 2,
    "pending_boundary": None,
    "model_turns": 7,
    "tool_calls": 31,
}
```

Errors inside the loop:

| Error | Handling |
|---|---|
| invalid tool arguments, unknown tool | returned to the model as the tool's error response |
| a business failure reported by a tool | returned as the tool result |
| a transient connection error | limited infrastructure retry, then a fault |
| a permission violation (undeclared tool, write outside its space) | refused; returned as an error response, or a fault by policy |
| a limit exceeded (turns, calls, children, depth, budget) | fault (the parent decides) |
| an engine or agent code error | fault |

A **decision agent** (`decider`) is an `AgenticAgent` whose input is a question
(`{question, facts, schema, default}`) and whose output artifact is one `decision`.

## 10. The engine

### Interface for applications (non-blocking)

```python
root = await engine.spawn("research:report", input={...})  # a new root and workspace
await engine.signal(agent_id, "STOP" | "CONT" | "TERM" | "KILL" | ("USR", message))
await engine.answer(question_id, answer)
async for event in engine.events(root, since=seq):
    ...
```

A root may be **attached** to another root's workspace (`engine.spawn(ref, input,
workspace=<root>)`): its tools and files work in that session, so a conversation about a finished
run reads the run's files and views; its own objects stay under its home, its input is written
beside the run's, and it never rewrites the run's views.

A root may be a **fork** (`engine.spawn(ref, input, inherit={"root": <run>, "agents": [...]})`):
it reads the earlier run's publications of the kept agents as its own (`ctx.read`, the cumulative
state, the views), unless it runs an agent of that name itself; nothing is copied and the earlier
run is left as it was. `ctx.describe(root)` reads another run's progress (status, the agents that
succeeded or faulted, what each published), read only.

A spawned child may be attached too (`Spawn(children, then=..., attach={name: <root>})`): its
subtree works in that root's workspace while its parent stays in its own, so a long-lived
conversation can ask about whichever run is current.

### Two stores with different guarantees

| | `StateStore` | `Workspace` |
|---|---|---|
| holds | agent records, leases, inboxes, history, the event log | business data: inputs, artifacts (objects), files, skills |
| operations | CAS, leases, atomic updates, append-if-absent | read, write, create-only write, list |
| data | small, structured | large files included |
| concurrency | strict | immutable objects; private spaces |
| local adapter | **SQLite** (Python standard library) | local folder |
| distributed adapter | a transactional store *(planned)* | any service with `get`, `put`, `create` (the `Files` port) |

The workspace layout below is logical. With SQLite, the authoritative records are in SQLite and
the JSON files in `run/` are projections for people and tools. (Atomic file renames alone do not
give a safe read-modify-write CAS, so files are not used as the local state store.)

### `StateStore` contract

```python
class StateStore(Protocol):
    async def load(self, agent_id) -> Record
    async def scan_work(self) -> AsyncIterator[Record]                 # active, or intents pending
    async def create(self, record) -> bool                             # create-only (spawn)
    async def try_acquire(self, agent_id, owner, ttl) -> Lease | None  # new epoch
    async def renew(self, lease) -> Lease | None                       # owner, epoch, not expired
    async def release(self, lease) -> None                             # owner, epoch
    async def commit_step(self, agent_id, lease, expected_step, outcome) -> Record
        # outcome: Transition or Fault. Applies only if: the lease's owner and epoch are current
        # AND it has not expired, and the record's step == expected_step. Lease renewals never
        # make it fail. A stale worker's fault is refused like its transition.
    async def commit_control(self, agent_id, expected_revision, change) -> Record
        # engine changes that are not a state's result: apply a signal, resolve a wait (with its
        # wake), mark an intent done; CAS on revision, and only lifecycle-valid
        # changes (pure functions decide them, below)
    async def kill(self, agent_id) -> Record                           # terminated + epoch bump, atomically
    async def append_inbox(self, agent_id, message_id, message) -> bool   # if absent
    async def read_events(self, root_id, after_seq) -> list[Event]
    async def changes(self) -> AsyncIterator[str]                      # an optimization only
```

| Operation | Checked |
|---|---|
| `commit_step` (transition or fault) | a current, unexpired lease (owner, epoch); `expected_step` |
| `commit_control` | `expected_revision`; no other executor holds a valid lease (a control change waits for the running step); the change is valid for the record's lifecycle |
| `renew`, `release` | owner, epoch; `renew` also not expired |
| `kill` | none: it always wins, atomically |

Leases are kept apart from the record's step CAS, so a long state whose executor keeps renewing its
lease still commits normally.

### The kernel: pure transition rules

The rules are pure functions without I/O; the store adapters and the executor only persist and
serialize what they decide:

```python
apply_transition(record, transition) -> record
apply_signal(record, signal) -> record
resolve_wait(record, children, inbox, now) -> record      # the wait cleared? with its wake
is_runnable(record, lease, now) -> bool
```

### Executor

```
on start, and then periodically:   scan_work(): every record with work left  (correctness)
on changes() notifications:         the changed agents              (speed)
for each agent found:
    1. apply its inbox (signals, answers) with commit_control
    2. resolve its waits: children's records, answers, now vs sleep deadlines
    3. complete its pending intents, marking them done with commit_control
    4. if runnable: try_acquire, run one step, commit_step, release
renew the leases of running steps
```

- **Correctness never depends on notifications**: the periodic scan finds every active agent,
  passed deadlines and intents left by a dead executor. `changes()` and SSE only make it faster.
- One executor runs many agents concurrently on one asyncio loop; several executors may share a
  store (invariant 1).

### Events

- **Durable events** (lifecycle, state transitions, questions, publications, signals) are written
  by the store **in the same transaction** as the record and history change that caused them, with
  a per-root sequence (appending is internal to the adapter; an existing event id keeps its
  sequence). Notifications go out only after the transaction. The sequence is log order, not
  causal order; a UI keeps the newest record per agent by `revision`.
- **Diagnostic events** (`tool.called`, `tool.returned`, `model.turn`) are emitted while a step runs
  and may be lost if the step does not commit; a committed step's turns and tool calls are also in
  its durable history (for `AgenticAgent`, in its history `data`).
- `engine.events(root, since=seq)` subscribes to the live channel first, then reads the log after
  `seq`, and on every notification reads the log again after the last sequence it delivered; the
  notification's payload is never trusted. Nothing falls between the read and the subscription.
  SSE is a notification path, not the source of truth. Delivery is at-least-once; consumers
  deduplicate by event id.

### Ports

| Port | Ships with the engine | An application may supply |
|---|---|---|
| `StateStore` | SQLite; in-memory for tests | a transactional store *(planned)* |
| `Files` (workspace) | a local folder, scoped per root | any store with `get`, `put`, `create` (a remote file service, object storage) |
| `Tools` | `testing.MemoryTools` (plain functions) | anything with `async call(server, tool, arguments)`: in-process servers, MCP over HTTP |
| `Model` | an OpenAI-compatible chat client (OpenRouter, vLLM); a scripted model for tests | any object with `complete(messages, tools)`, a wrapper that filters what is sent |
| events | `engine.events(root, since)` from the durable log | a push channel (SSE) over it |
| envelope, state, views | `Artifact`, `agents_state`, `StateView` | its own envelope class, cumulative state and views |

The engine imports no web framework, broker or model SDK beyond the chat client; a test enforces
it. A web deployment is a small HTTP gateway (control calls, events out) and executor processes.

### Module layout

```
statecraft/
  process.py      the record, lease, transition and fault (data only)
  transitions.py  the pure lifecycle rules
  agent.py        Agent, @state, Spawn, Wait, Done, running one state
  store.py        the StateStore contract: fencing and atomicity over an adapter's tables
  stores/         memory.py, sqlite.py
  workspace.py    the Files port, LocalFiles, per-root scoping
  catalog.py      feature packages -> agent specs and classes
  spec.py         agent.yaml and SKILL.md as data
  artifacts.py    the artifact envelope
  views.py        views and the default cumulative state
  testing.py      tools answered by plain functions
  context.py      ctx for one step
  executor.py     the loop
  engine.py       spawn, signal, answer, events, run
  agentic.py      AgenticAgent, the Model port, the decider
```

`transitions.py` and `process.py` hold the pure rules; `executor.py` runs steps; `store.py` defines
the contract.

## 11. Workspace layout (logical)

```
<workspace>/
  shared/                       read-write for every agent of the tree
    input/                      the root's input, if an application writes it there
    ...                         shared files, written as new immutable files
  home/<agent-id>/
    private/                    the agent's own scratch space: only it reads and writes
    staging/<step>/<epoch>/     work in progress, per attempt: only it reads and writes
    objects/                    content-addressed, create-only; others read them only through
                                committed references
    skills/<name>/              its installed skills
  run/                          reserved for the engine; readable by every agent
  <view paths>                  what the views write (`StateView`: `view/`); readable by every agent
```

- An agent may work in a sub-space of its own (`workspace: own`).
- `ctx` enforces the permissions: another agent's `private/` and `staging/` cannot be read; no
  agent writes another's home or `run/`; typed artifacts are read through published references.

## 12. Advanced recovery (planned)

**Setting an agent back** (`engine.set_state(agent, history_entry)`) restores an earlier execution
context, not the world: it starts a new **lineage** (part of every key and id from then on);
children spawned after that point are terminated, and their names may be spawned again in the new
lineage; artifacts published after it are marked stale in the index.

Recovery tooling for a faulted root: CLI first (show the fault, `CONT` after a fix, `TERM`), a UI
later.

## 13. Recovery properties

The tests check each of these.

Crash points:
1. The executor dies just before a state runs.
2. It dies right after a tool call returns.
3. It dies just before the commit.
4. It dies right after the commit.
5. A spawn intent is committed, the child not yet created.
6. It dies right after the child is created.
7. A child finishes before its parent's wait is committed.
8. It dies right after a person's answer is stored.
9. The whole engine stops and restarts from the stores.

Concurrency and leases:
10. Two executors race for the same lease.
11. An executor with an expired lease (not yet re-acquired) tries to commit: refused.
12. A stale executor's `renew` and `release` are refused.
13. A long state keeps renewing its lease and commits normally.
14. A control commit races a step commit.
15. A running step responds after `KILL`: refused.

Effects and artifacts:
16. The effect key stays the same after a lease change.
17. The effect key stays the same after a fault and `CONT`.
18. A stale executor cannot overwrite or publish an artifact.
19. Staged (unpublished) work is invisible to other agents.

Waits, signals, answers:
20. `STOP` arrives while a state is finishing.
21. The same child faults twice: a new question each time.
22. The state handling an answer faults: its wake is kept.
23. A crash while handling `USR` messages: they are delivered again.
24. A parent is killed with a spawn pending: no orphan.
25. A sleep deadline passes with no store notification.
26. Several children fault at once.
27. A fault, a code fix (with a data migration), then `CONT`.

History, events, views:
28. A step commit and its history entry are never separated.
29. Several executors append events: sequences stay consistent.
30. Notifications are lost: active agents are still found by the scan.
31. All projections deleted: rebuilt from the records.
32. The event channel fails, and a client reconnects with `since`.

Further:
33. A terminated agent's pending spawn intents are carried out after a restart (and the child gets
    its `TERM`).
34. A fault reported by a worker whose lease expired is refused.
35. An effect key in `on_term` differs from the interrupted state's key at the same step.
36. No event is lost between reading the log and following the live channel.
37. A stale worker cannot modify the current attempt's staging files.

Three levels of tests: **pure kernel** (record in, record out), **store contract** (the same
suite for the memory and SQLite stores: atomicity, leases, fencing), **crash integration** (a
subprocess killed with `SIGKILL`, then recovery from the SQLite file and the workspace alone;
never a raised exception, which runs `finally` blocks).

Items that need several executors on a distributed store are checked again when that adapter
exists.

## 14. Decisions

| Question | Decision |
|---|---|
| handler style | class methods registered with `@state`; nothing durable on `self` |
| history | snapshots of state-level changes, committed with them; immutable references; retention policy |
| `join any` | not planned |
| non-repeatable tools | effect classes in the tool contract; idempotency keys by step; same key with a different payload refused |
| faulted-root tooling | CLI first |
| state store | SQLite locally; separate from the workspace |
| code updates | applied while suspended; `schema_version` and explicit `migrate_data` |
| `AgenticAgent` result | its output artifact, read by the parent after `join` |

Still open: the distributed state store.

## 15. Research: how other frameworks recover

| Framework | Saves | After a crash | Human input | Note |
|---|---|---|---|---|
| Temporal | every step's result (event history) | re-runs the code; recorded results replay | signals | code failures are retried task failures; fix, then reset or patch |
| DBOS | a row per step and the inputs (Postgres) | restarts the function; finished steps are skipped | n/a | reordering steps breaks recovery |
| Restate | a journal of side effects | replays the journal | suspension until a promise completes | journals grow |
| LangGraph | the graph state at each step (checkpointer) | continues from the last step | `interrupt()` / `Command(resume=...)` | a resumed node starts from its beginning |
| MS Agent Framework | everything at each superstep | resume from any checkpoint | pending requests are checkpointed | executors save their own state |
| OpenAI Agents SDK | `RunState` (serializable) | pass the state back to `run` | tool approvals as interruptions | |
| Google ADK | session events (`ResumabilityConfig`, experimental) | resume the invocation from its last event | long-running tools pause | custom agents need explicit state classes |

What this design takes: LangGraph's checkpoint at every step and "a resumed step starts from its
beginning"; Restate's suspension that frees the worker; Temporal's "a code failure is not the end:
fix and continue"; leases with fencing tokens from distributed systems; process-style composition
(spawn, join, signals) between agents.

Sources: [ADK resume](https://adk.dev/runtime/resume/),
[ADK long-running agents](https://developers.googleblog.com/en/build-long-running-ai-agents-that-pause-resume-and-never-lose-context-with-adk/),
[LangGraph durable execution](https://docs.langchain.com/oss/python/langgraph/durable-execution),
[LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts),
[Temporal event history](https://docs.temporal.io/encyclopedia/event-history/event-history-python),
[Temporal: fixing long-running workflows](https://community.temporal.io/t/what-is-the-most-effective-way-to-fix-a-bug-in-long-running-workflows/10172),
[DBOS durability](https://www.mintlify.com/dbos-inc/dbos-transact-py/concepts/durability),
[Restate durable execution](https://docs.restate.dev/concepts/durable_execution),
[Microsoft Agent Framework checkpoints](https://learn.microsoft.com/en-us/agent-framework/workflows/checkpoints),
[OpenAI Agents SDK RunState](https://openai.github.io/openai-agents-python/ref/run_state/),
[Pydantic AI durable execution](https://pydantic.dev/docs/ai/durable-execution/overview/),
[Durable execution for agents (Martin Fowler)](https://martinfowler.com/articles/durable-execution-agents.html).
