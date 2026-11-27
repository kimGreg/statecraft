# Statecraft roadmap: from P0.6 to 1.0

Status: approved with amendments (applied below). In progress on branch `p1`: B1 done;
A1 done (identity, journal, indeterminate, scope, capabilities, provider view, local shell,
contract kits, native edits); A2 done; A3 done on `p1-a3` (internal links beneath their area,
mount plan, mountable and workspace_requires, host-path rule, folder fsync, W9, W10); A4 done
(`packages/statecraft-opensandbox`: the effect contract kit passes on the fake backend and on a
real OpenSandbox 1.2.1 server, in both cancel modes; the A4 hardening rows are configuration of
the OpenSandbox deployment, documented in the package README, not yet automated). Baseline `main` at `84830e5` (P0.6 remediation merged, P0.7 gate passed).
This plan follows the revised architecture: the kernel stays minimal, sandboxes and volumes are
provider concerns, and every new guarantee is a generic effect or workspace contract with a test.

## 0. What "complete" means

Statecraft 1.0 is a durable execution runtime that a team can deploy for its own agents:

1. **Correct.** The kernel's invariants hold under chaos on every supported store, including
   several hosts on one database. Every audit finding is fixed or explicitly out of scope.
2. **Safe to act.** Agents can run shells and other side-effecting tools through supported
   providers that prevent unsafe replay, or report `indeterminate` when the outcome cannot be
   established; an indeterminate outcome stops the agent, survives restarts, and is never
   replayed without an operator's decision.
3. **Shared work.** Several agents work in one workspace from isolated sandboxes; native file
   access and the shell see the same files; permissions hold in both.
4. **Operable.** Authenticated API, versioned store format with migrations, observability,
   recovery tooling, documented upgrade path.
5. **Extensible.** Providers and extensions plug in through public contracts; the core installs
   and runs alone; contract test kits let third parties verify their providers.
6. **Released.** Frozen public API with semantic versions, packages published, documentation,
   and the acceptance checklist (architecture §17) passing, including item 23 (an unrelated
   package builds and recovers a job).

Non-goals for 1.0: our own container or sandbox runtime; a transactional file system;
exactly-once external effects (only logical commits are exactly once); multi-tenant hosting;
`join any`.

## 1. Where we are

Done: kernel, runtime, SQLite and memory stores, local and memory workspaces, Python and MCP
effects, agentic extension, console, HTTP server, CLI; P0 (finish rule, message accounting, spawn
schemas, failure classes); P0.5 audit; P0.6 remediation of AUD-01 to AUD-12 with the P0.7 gate.

Known open items, carried into this plan:

| Source | Open |
|---|---|
| Audit AUD-13 | K4 (STOP/TERM/answer applied one step late), K6 (re-wait woken by the same fault), K9 (TERM while stopped), K10 (signal to a missing target, answer atomicity, stranded on_term child), R9 (stop then start) |
| Audit AUD-14 | M2 to M8 (dead stdio server, eviction with calls in flight, connect lock, connect-cancel leak, JSON-RPC errors as transient, refused on held session, list-changed) |
| Audit AUD-15 | A4 (bad arguments to offered boundary tools), A6, A7, A8, A11, A12, A13; V8 (unknown manifest keys) |
| Audit AUD-16 | R7 (name collisions), R8 (unreachable provider pins nothing), R11 (crossings of uncommitted steps) |
| Audit AUD-18/19/20 | A10, S6 (storage growth), W12 (`create` without folder fsync), W13 (POSIX only), CHLD and per-attempt diagnostics documented but missing |
| Audit tests | T8, T9, T10, T13, T14 |
| Workspace | W9 (attached root reading the run's publications), W10 (`exists` vs `read` across workspaces) |
| Release gates | HTTP authentication; store format version and migrations |
| Code review of the P1 design | agentic loop never passes an idempotency key; `non_repeatable` always refused; no unknown-outcome class; no reconciliation source for providers; all symlinks refused |
| Integration | command-agentic pinned at `2e56619` |

## 2. Principles (from the revised architecture)

- **Kernel executes, runtime orchestrates, providers implement, extensions specialize,
  applications decide.**
- The kernel knows nothing of models, MCP, shells, sandboxes, volumes or mounts.
- Three infrastructure contracts only: `StateStore` (kernel), workspace `Files` and
  `WorkspaceProvider`, `EffectProvider`. No `SandboxProvider` or `VolumeProvider` in core.
- A sandbox is a resource an effect provider owns. Volumes and mounts are provider details.
- Add a contract only when a real backend shows the existing ones cannot express a guarantee,
  and make it generic (payments and remote jobs need it as much as shells).
- Every rule ships with the test that enforces it; every phase ends with a gate.
- Provider conformance suites are public testing API, split into a baseline every provider
  passes and optional capabilities a provider declares.

## 3. Tracks and order

```
Track A (execution contract)   A1 ═ A2 (together) ─► A3 ─► A4 ─► A5        A6 (optional, needs B2)
Track B (robustness backlog)   B1 (now, with A1/A2) ─► B2 ─► B3
Track C (storage)              C1 ─► C2 ─► C3 (after A4, before 1.0) ─► C4 (optional)
Track D (security, operations) D1 ─► D3 ─► D4 ─► D2 (required only for untrusted agent code)
Track E (recovery, features)   E1 ─► E3          E2 (post-1.0)
Track F (applications)         F1 (after A5)     F2 command-agentic: independent, any time
Release                        R1 (API freeze) ─► R2 (1.0)
```

- A1 and A2 run together: A1's interfaces are drafts until A2's backend behaviour confirms them.
- A3 follows the validated A1/A2 contracts; then A4, then A5.
- A5 uses a minimal deterministic coding example; it does not depend on F1.
- The native OpenSandbox adapter (A4) does not depend on MCP robustness; only A6 depends on B2.
- F2 (command-agentic) is an independent integration track; nothing in Track A waits for it.

Sizes are relative: S (a session), M (a few sessions), L (a week or more of sessions).

---

## Track A: durable effects, workspaces for shells, sandbox adapter (revised P1)

The kernel stays as it is: no `SandboxProvider`, no `VolumeProvider`, no sandbox-specific kernel
resource. Sandboxes, mounts, volumes, execution receipts and backend resource mappings are
provider implementation concerns. Everything below that is not provider code is a generic effect
contract (payments, remote jobs and database changes need it as much as shells).

### A1. Effect execution contract — M (with A2)

**A1.a Logical call identity (before any automatic key).**

| # | Work |
|---|---|
| A1.1 | Define a **logical call identity**: what makes two calls "the same call" across a rerun of the state. It is the agent, the step, the state (already in `record.key`) plus a call name the code gives (`ctx.call(effect, ..., call="charge-order")`, or `ctx.effect_key(name)` today) |
| A1.2 | Automatic identity only from committed data: the agentic loop names each tool call by the `tool_call_id` of the committed model reply. The runtime never derives an identity from the order of calls, branches or parallelism (whether a state makes one call or several cannot be known when the first call is made); a `deduplicated` call without an identity is refused with the reason |
| A1.3 | **Request identity record**, persisted and checked across restarts: identity → digest of (effect binding: provider, version, schema and policy digests; operation; arguments; workspace identity; authorized scope from A1.11). Same identity with a different digest is refused before anything is sent. The record lives in the state store (generic, provider-neutral) and is written before the first send |

**A1.b Indeterminate outcomes.**

| # | Work |
|---|---|
| A1.4 | Failure class `indeterminate`: the request may have been applied and the outcome cannot be established |
| A1.5 | **Blocking, not advisory.** The moment a call ends `indeterminate`, the step's context refuses every further managed effect call, and the step cannot commit normally: `settle()` turns it into a fault even if the agent's code caught the exception |
| A1.6 | **Durable.** The unresolved identity is recorded in the store with the fault, so it survives runtime restarts and executor changes |
| A1.7 | **CONT reconciles, never replays.** On CONT the runtime asks the provider to reconcile the recorded identity (status of the existing execution). Completed: its result is used and the state reruns with it. Still running: reattach. Still unknown: the agent stays faulted. Replay needs an explicit operator decision (D4), never CONT alone |
| A1.8 | **No escape hatch by new key.** While an identity is unresolved for an agent, calls to that effect binding (with any identity) are refused for that agent until it is resolved; the guard applies to the agent and is recorded in the store |

**A1.c Remote execution and capacity.**

| # | Work |
|---|---|
| A1.9 | Distinguish a **local future** (the awaiting coroutine, a thread, a local process) from **remote execution**. Cancelling or timing out the coroutine never releases capacity by itself; capacity is released when the provider reports the work ended (local work finished, or remote cancel or completion confirmed) |
| A1.10 | Concurrency guarantee scope, stated in the docs: `concurrency` on a provider is per runtime process. Limits across processes or hosts are enforced by a shared provider mechanism (for example a slot table in the provider's own store) or by the backend; the provider declares which (`capacity_scope: process | shared | backend`) |
| A1.11 | Authorized scope in `EffectContext`: the workspace identity and the paths and modes this agent may expose (`shared` ro/rw by grant and attachment, own `private` rw), computed by the runtime; part of the request digest (A1.3) |
| A1.12 | Reconciliation source for providers: a read-only public runtime API over the durable event log (agent ended, step committed, fault recorded), so a provider can clean up and reconcile after a restart without a new notification mechanism |

**A1.d Native file mutations.**

| # | Work |
|---|---|
| A1.13 | Native file tools get their own repeat-safety contract: `write` of a whole file with a content digest is idempotent; `create` is create-only; `edit` carries the expected digest of the file before the change (applied only if it still matches, otherwise reported); `append` is not idempotent and needs a logical identity with a recorded outcome. Arbitrary writes, edits and appends are never classified idempotent by default |

**A1.e Contract test kits (public testing API).**

| # | Work |
|---|---|
| A1.14 | `statecraft.testing.contracts`: public, versioned conformance suites. **Baseline** (every effect provider): classification, `sent` honesty, timeouts, cancellation of the awaiting call, schema. **Optional capabilities**, each declared by the provider and tested only when declared: `deduplicated` (identity, digest conflict, reattach), `reconcile` (A1.7), `remote_work` (A1.9), `shared_capacity` (A1.10), `mounts` (A3). The same layout for workspace providers (baseline files; optional `mountable`, `internal_links`) and stores |
| A1.15 | A fake shell provider with durable receipts (file-backed, outside the workspace) that declares every optional capability and passes the kit; used by all later tests |
| A1.16 | Boundary moves: `kernel/files.py` to the runtime's workspace port; `classify_status` and the context-overflow phrases to the agentic package; layer test: nothing in `runtime/` except `config.py` and `Runtime.local` constructs a concrete provider; the kernel test keeps it free of models, prompts, tool semantics, MCP, sandboxes, volumes, mounts and infrastructure technologies |

Gate: the baseline and optional kits green on the fake shell; mutants caught for each guarantee
(settle-fault on indeterminate, the post-indeterminate call block, the new-key escape, digest
conflict across a restart, capacity released on coroutine cancel); full suite; chaos seeds.

### A2. Backend feasibility: OpenSandbox — S/M (with A1, outside the repo)

Run OpenSandbox locally; scripts and results kept with the audit material. Validate A1's draft
contracts against real behaviour before they are final.

| Question | Needed for |
|---|---|
| Two sandboxes on one host directory or volume, one ro and one rw | shared workspace, ro attach |
| An execution id that survives a client restart; inspecting it later | reconcile, reattach |
| Cancel kills the whole process tree; completion observable after cancel | capacity (A1.9) |
| Running commands when the sandbox service restarts | indeterminate vs known |
| Lifecycle: create, find by label, reuse, destroy, quotas | per-agent scope |
| Isolation: host file system, network, other sandboxes | security |
| Server-side dedup by request id, or none | receipt design |

**Lost-response tests** (drop the response, kill the client, or restart the service at each
point): sandbox creation, command submission, completion, cancellation. For each, record what
the backend lets us establish afterwards. Wherever the outcome cannot be determined, the adapter
reports `indeterminate`; it never replays.

Outcome: a decision record naming the reference backend (OpenSandbox, or another backend such as
E2B or a Kubernetes agent sandbox for the same contract), and the A1 interface changes the
evidence requires.

**Result (done).** Decision record and evidence: `statecraft-spikes/a2-opensandbox/REPORT.md`
(outside the repo; OpenSandbox server 1.2.1, `execd` 1.1.0). OpenSandbox is the reference
backend for A4, with provider-side receipts; not a security boundary until the A4 hardening
below is in place. Findings that shaped A1: no server-side dedup (receipts are the provider's);
execution ids are chosen by the server and arrive in the first stream event (a submission whose
answer is lost before that is `indeterminate`); command records live in memory in `execd` and are
lost when it restarts (`unknown`, with a reason); cancellation ends the process group but not a
process that left it (capability `tree_termination` added; only destroying the sandbox ends
everything); mounts do what A3 needs (shared ro/rw, symlinks resolved inside the container,
host paths checked).

### A3. Workspaces for shells — M (after A1/A2)

| # | Work |
|---|---|
| A3.1 | Mount plan from A1.11: `shared/` with the granted mode, the agent's `private/` rw; never `home/*/staging`, `objects`, `skills`, `run/`, or any receipt storage; another root's workspace only through an attach grant, ro unless granted rw |
| A3.2 | Ordinary shared file-system semantics stay (no transactions). Internal symlinks are allowed only when the target stays within the agent's authorized scope and resolution is race-safe (`openat2` with `RESOLVE_BENEATH` on Linux, resolved against the scope root); links on platforms where that cannot be guaranteed are refused, as today |
| A3.3 | Host-path rule: no host-side code touches a workspace folder by path (only through `LocalFiles` or inside a sandbox); enforced by a test, plus a test with a link planted from a sandbox |
| A3.4 | Capability check at start: a shell provider with a workspace that is not `mountable` (memory, object store) is refused with the reason |
| A3.5 | `create` fsyncs its folder (W12); W9 and W10 fixed |

Gate: native read and shell `cat` agree; ro attachment refuses shell writes; an escaping link
planted by the shell is refused natively; an internal link within scope works on Linux and is
refused where `RESOLVE_BENEATH` is unavailable; containment tests stay green.

### A4. `statecraft-opensandbox` package — L (after A3; independent of B2)

Plugin and effect provider for `sh.exec` (later `sh.spawn`/`sh.poll` for long jobs), declaring
the A1 optional capabilities the backend supports (A2 evidence).

| Part | Contents |
|---|---|
| Sandbox lifecycle | per-agent scope by default (`run` and `call` configurable); created on first call; labelled with root and agent; destroyed when the event log says the agent ended; reused after restart |
| Receipts | durable and provider-owned; stored outside every agent-writable and shell-mounted directory; survive runtime and sandbox re-creation; atomic reservation so concurrent recovery by several executors never starts one identity twice; states not started, running, completed, indeterminate; digest check |
| Execution | output with size limits; exit code and output as the result; timeout requests cancel and waits for confirmation |
| Recovery | on start, reconcile every receipt: completed keeps its result, running reattaches, unknown becomes `indeterminate` |
| Capacity | declared scope (A1.10); backend or provider-store slots across processes |
| Configuration | `effect_providers.sh: {provider: opensandbox, endpoint, image, scope, limits}`; strict keys |
| Hardening (from A2) | a network per sandbox or an `execd` credential (any sandbox can reach another's `execd` otherwise); commands as a non-root user or in a user namespace, mounts `nosuid` (root and setuid files land in the workspace otherwise); egress closed by default; unique creation labels and cleanup of sandboxes stuck in `Pending`; the execution id stored in the receipt the moment it arrives; `tree_termination` declared only when cancellation destroys the sandbox |

Gate: the contract kits against a real backend; kill the runtime, the provider and the sandbox
service during a command at each lost-response point; the stored result, a reattach, or
`indeterminate`, never a second run.

### A5. Deterministic multi-agent coding example — M (after A4; not F1)

| # | Work |
|---|---|
| A5.1 | Minimal tools `sh`, `read`, `write`, `edit`, `ls` for the example only (file tools on the native workspace with A1.13 contracts, `sh` on the shell provider) |
| A5.2 | `examples/shared-coding`: a scripted (no model) lead and two workers in one workspace, each in its own sandbox; asynchronous spawn, selective join, directory ownership per worker, git for change tracking |
| A5.3 | Deterministic failure tests with the fake shell and with the real backend: KILL during a command (process tree ends; capacity returned only after), runtime crash mid-command, provider restart, unknown outcome faults and blocks, CONT reconciles without replay |
| A5.4 | Chaos harness extended with a shell effect and a receipt invariant (every identity executed at most once, or reported indeterminate) |
| A5.5 | A real-model run (cheap model) only after A5.3 and A5.4 pass |

### A6. MCP exposure (optional) — S/M (needs B2)

Expose the same shell service through MCP when needed; the MCP path passes the same kits
(identity in `_meta`, declared dedup, reconcile, cancellation).

**Track A gate (P1 acceptance):** the revised architecture's acceptance list, including: no
sandbox or volume concept in kernel or runtime; backend swap without agent code changes; same
files natively and in the shell; ro attachment enforced in the shell; workspace survives sandbox
re-creation; capacity returned only after real termination; no duplicate execution per identity;
an indeterminate outcome faults, blocks further effects and survives restarts, and CONT never
replays; shell works without MCP and without a model; P0 invariants and chaos seeds still green.

---

## Track B: robustness backlog (deferred audit items)

### B1. Executor ordering and lifecycle (AUD-13) — M (now, with A1/A2)

K4 apply STOP/TERM/answers when a queued agent gets its slot, before its step; K6 a join
resolution looks at the child's unapplied inbox, so a CONT in flight is not read as a new
fault; K9 decide and document TERM while stopped; K10 signal intents to missing targets are
recorded, `runtime.answer` is atomic with its append, a child terminated in its parent's final
step cannot be stranded; R9 `stop()` then `start()` starts executors again.

**Selective interruptible joins:** a join on some children that also wakes on a signal (USR
message, a steer) or a deadline, returning which condition woke it and which children are still
live, without cancelling them; the parent can rejoin the rest later.

Each item with a forced-interleaving test (the audit repro scripts become tests).

### B2. MCP provider robustness (AUD-14) — M

M2 detect a dead stdio server and reconnect; M3 reference-counted sessions (no eviction with a
call in flight); M4 per-key connect futures instead of one provider lock; M5 cancel during
connect cleans up its task; M6 JSON-RPC protocol errors are `invalid`, not `transient`; M7
refused or closed on a held session is `unavailable` with `sent=False` when provable; M8
list-changed applied at the next start. Run the provider contract kits (A1.14) against MCP.

### B3. Agentic harness and registry (AUD-15, AUD-16) — M

A4 badly typed arguments to offered boundary tools become a tool error the model sees, and CONT
recovers a pending call; A6 a steer arriving during finish is applied, not dropped; A7 limit
faults (turns, tool calls, guard) recoverable by CONT with a raised limit; A8 the finish guard
re-arms per child set; A11 a single skill with an instruction stays reachable; A12 error bodies
with 429, choice-level errors and dict arguments classified properly; A13 compaction chunks when
the summary request itself would overflow; V8 unknown manifest keys refused unless namespaced
(`x-...` or an extension name); R7 policy name collisions refused; R8 an unreachable provider
at start suspends instances that need it with a clear reason, and recovers after repin; R11
crossings of steps that did not commit are kept per attempt (with E1).

**Compaction quality validation:** a deterministic suite of long conversations with planted
facts (decisions, file names, open tasks, tool results the agent must reuse); after compaction
the agent must still answer them, measured with a scripted model and, separately, a real cheap
model; thresholds recorded and checked on every compaction change.

Tests: T8 (detect a rerun of a CPU tool), T9 (manager-level `unavailable` retry), T10 (crash tests
compare execution counts, not sets), T13 (remove wall-clock and global-state flakiness), T14
(strong assertions in server and skills tests).

---

## Track C: storage

### C1. Store format version and migrations — M (release gate)

A `schema_version` in every store; `statecraft store migrate` and `store check`; refusing to open
a newer format; migrations tested from every released format with stored fixtures; record and
history format documented as a contract.

### C2. Storage growth and garbage collection (AUD-18) — M

History stores deltas or per-revision changes instead of full snapshots (or snapshots every N
revisions); retention policy for history and events of ended roots; inbox compaction after
apply; agentic conversation moved out of `data` into workspace objects with references (A10);
`scan_work` visits only agents with work (index). Measure with the 84-record / 17 MB case.

**Artifact and staging garbage collection:** staging folders of attempts that did not commit
are removed once their step committed or the agent ended; objects no committed reference names
(including references in retained history and forks that inherit them) are collected after a
grace period; collection never races a commit (mark from committed references, sweep with the
same create-only rules); a dry-run report first.

### C3. PostgreSQL store — L

`statecraft-postgres` package: the same transaction semantics (`SELECT ... FOR UPDATE` or
serializable), leases and fencing in the commit transaction, `LISTEN/NOTIFY` for `changes()`
as an optimization only. Must pass the store contract suite unchanged, the multi-process tests,
and the chaos harness with executors on several hosts against one database (design §13 items
that need a distributed store).

### C4. Object-store workspace — M (optional for 1.0)

`Files` over S3-compatible storage (create-only via conditional put); not mountable, so refused
with a shell provider (A3.4); useful for artifacts and attachments.

---

## Track D: security and operations

### D1. Authentication and authorization — M (release gate)

Replace the single shared token with named API tokens (hashed at rest), scopes (read, control,
admin) and per-root ownership; the console signs in with the same tokens; audit log of control
operations (spawn, signal, answer, repin) in the durable event log. Keep the existing guard
(origin, JSON, host) in front.

### D2. Worker isolation (trust boundary) — L

Agent code in a separate worker process that talks to the runtime through a narrow protocol
(step in, transition out, effects through the runtime). Required before running third-party
agent code; the architecture lists it as the boundary before namespaces or tenants. Sandboxed
workers may reuse the A4 backend.

### D3. Observability and console — M

OpenTelemetry exporter (spans per step and effect call, from crossings); metrics (runnable,
running, waiting, faults, capacity waits, retries, indeterminate count); health and readiness
endpoints; structured logs with agent and root ids; never credentials.

Console support for the new lifecycle and diagnostic fields: undelivered messages with
reasons, `init` intents, indeterminate effects and their reconciliation state, capacity held by
lingering work, per-attempt diagnostics (E1), budgets (E3), interruptible joins (B1).

### D4. Recovery tooling — S/M

CLI and console: list faulted roots with errors, show the failing step and its crossings, CONT,
TERM, `repin` (missing in the CLI today), drain and maintenance mode. Resolving an
`indeterminate` effect by hand: record the outcome the operator established (with a result), or
authorize exactly one replay of that identity; both recorded in the audit log (D1) and the
history. This is the only path to replay (A1.7).

---

## Track E: advanced recovery and features

### E1. Per-attempt diagnostics and CHLD — S/M

Implement per-attempt diagnostic crossings (documented, missing; feeds R11) or remove them from
the documents; implement `CHLD` as a notification or remove it from the documents. No
documented feature stays unimplemented (AUD-20).

### E2. Set an agent back (design §12) — L — post-1.0

`runtime.set_state(agent, history_entry)` with lineages: keys and ids carry the lineage, later
children are terminated, later artifacts are marked stale. Needs C1 (format) and E1.

### E3. Tree limits and budgets — M (in 1.0)

**Atomic tree-wide spawn limits:** `max_total_agents` and `max_live_agents` are reserved in the
same store transaction as the spawn (today two agents committing at the same moment can each be
checked against the tree as it was and together exceed the limit).

**Generic cost reservations:** a per-tree reservation and settlement in the store, reserved
before a metered effect is sent and settled with its reported usage, refused when the budget
would be exceeded; the agentic extension maps tokens and model cost onto it. No tokens in the
kernel.

Post-1.0: E2 (setting an agent back), hot reload of definitions, namespaces and multi-tenancy.

---

## Track F: applications and integration

### F1. `statecraft-coding` — M

A coding agent on the agentic harness: prompts, project discovery, `AGENTS.md` loading, tool
descriptions and output shaping for `sh`/`read`/`write`/`edit`/`ls`, a CLI session. No shell
process management of its own (that is A4).

### F2. command-agentic compatibility — M (separate track)

Run command-agentic's suite against current statecraft `main`; adapt to P0.6 changes (strict
configuration, attach grants, agentic grants for skill agent and patterns, deduplicated tools,
token for non-loopback serving); unpin only when green. Not a prerequisite for Track A.

---

## Release

### R1. API freeze and packaging — M

Public API list (`statecraft.__all__`, SDK, provider protocols, configuration schema, HTTP API
with an OpenAPI document) marked stable; deprecation policy; versions for core and each
package with a compatibility matrix; wheels for every package; installation test of each
extension alone; documentation site from `docs/` with a getting-started, the guarantees table
(what is and is not exactly once), the provider author guide with the contract kits.

### R2. 1.0 gate

- Every track's gate green; the audit and P1 finding maps updated to fixed or out of scope.
- Architecture §17 acceptance list including item 23, run from a clean environment.
- Chaos: the 11 original seeds, the shell seeds (A5.4), and multi-host seeds on PostgreSQL.
- Mutants: the 40 audit mutants plus new ones for every guarantee added in this plan
  (indeterminate settle, key digest, receipt reservation, mount plan, migrations, auth scopes).
- A fresh independent review of the whole diff since 0.2.
- Release gates: D1 authentication, C1 migrations.
- Guarantee wording in the release notes and docs: exactly-once **logical commits** (steps,
  intents, messages, publications); not exactly-once arbitrary external effects. Supported
  side-effecting providers prevent unsafe replay or report `indeterminate`.
- The kernel test still shows the kernel knows nothing about models, prompts, tools' business
  semantics, MCP, sandboxes, volumes, mounts or external infrastructure technologies.

## 4. Verification used in every phase

| Check | When |
|---|---|
| Targeted tests for each change, written to fail first | every change |
| Full suite and lint | every commit |
| Store contract suite on every store | any store or kernel change |
| Provider contract kits (A1.14): baseline plus declared capabilities | any provider change |
| Chaos seeds | end of each phase touching kernel, store, effects or workspaces |
| Mutation runs | end of each track |
| Independent read-only review | end of each track |
| design.md and architecture.md updated | with the change, not after |

## 5. Sequence

1. A1 and A2 together, with B1 alongside.
2. A3 on the validated contracts; C1 and D1 started early because they gate any release.
3. A4 (independent of B2); B2 alongside.
4. A5 (minimal deterministic example); B3; F1 on top of A5 afterwards; F2 whenever convenient.
5. C2, D3, D4, E1, E3.
6. C3 (PostgreSQL) and multi-host chaos; D2 if untrusted agent code is planned.
7. R1 and R2.

## 6. Decisions (approved)

| Decision | 1.0 |
|---|---|
| Sandbox scope | per agent by default (`run`, `call` configurable) |
| Reference sandbox backend | selected by A2 |
| Receipts | durable, provider-owned, outside agent-writable and shell-mounted directories |
| Symlinks | internal links only within the authorized scope and race-safe; refused where that cannot be guaranteed |
| Automatic effect keys | only from stable logical identities; otherwise an explicit identity is required |
| E3 budgets and atomic tree limits | in scope |
| E2 historical rollback | post-1.0 |
| Hot reload, multi-tenancy | post-1.0 |
| PostgreSQL | after the initial shell adapter (A4), before 1.0 |
| Worker isolation (D2) | required for untrusted agent code; optional for a trusted-code 1.0 deployment |
| Object-store workspace (C4), MCP shell exposure (A6) | optional |

## 7. Risks

| Risk | Mitigation |
|---|---|
| The sandbox backend cannot reattach or confirm cancellation | A2 with A1; `indeterminate` makes the gap safe (fault, block, no replay) |
| An automatic key silently names two different calls | identities only from stable sources (A1.2); digest record across restarts (A1.3) |
| Shells bypass workspace rules through links or mounts | mount plan from grants only; host-path rule; containment tests |
| Storage growth makes long agentic runs slow before C2 | prioritize C2 if real runs exceed limits |
| Scope grows into a platform | non-goals above; every new contract needs a backend that requires it |
| command-agentic drifts further | F2 kept running against `main` once started |
