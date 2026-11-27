"""The Runtime: the product's one API, embedded or served (architecture §10, §11).

    async with Runtime.local("./statecraft") as runtime:       # or Runtime.from_config(...)
        root = await runtime.spawn("research", input={...})
        await runtime.signal(root, "STOP"); await runtime.signal(root, "CONT")
        await runtime.answer(question_id, value)
        async for event in runtime.events(root): ...

Register before `start()` (`runtime.registrar`, plugins, configuration). `start()` runs the
startup phases: start providers and collect their tools, freeze the registry, validate (refuse to
start on errors), start the executors. `stop()` starts nothing new and lets running steps finish
for `drain` seconds, then cancels them and releases their leases; it never ends agents: unfinished
agents stay in the store and resume at the next start. `close()` closes providers, workspaces
and the store.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import time
import uuid
import weakref
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import jsonschema

from ..kernel.executor import Executor, signal_name
from ..kernel.process import Fault, Lease, Record, Transition
from ..kernel.store import Busy, Conflict, Ended, StateStore
from ..kernel.transitions import (
    InvalidAnswer,
    apply_answer,
    mark_intent_done,
    signal_problem,
    view,
)
from ..sdk.agent import run_state
from ..sdk.contracts import Artifact
from .context import StepContext, dumps
from .effects import EffectManager
from .registry import AgentDefinition, Registrar, Registry, RegistryError
from .views import agents_state
from .workspace import HOME_FOLDERS, WorkspaceProvider, WorkspaceService, input_path

#: Limits a runtime can put on every tree (`Runtime(limits=...)`, configuration `runtime.limits`).
LIMITS = ("max_depth", "max_total_agents", "max_live_agents")


ROOT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
#: Names a root may not have: its id is a folder of the workspace provider and a segment of
#: `home/<root>/...` paths, so it may not read as a home folder or the top of a workspace.
RESERVED_ROOT_IDS = frozenset({*HOME_FOLDERS, "home", "shared", "run"})


def root_id_problem(root_id: str) -> str | None:
    if not ROOT_ID.fullmatch(root_id):
        return (
            f"a root id is a letter or digit, then letters, digits, '.', '_' or '-' "
            f"(at most 128), not {root_id!r}"
        )
    if root_id in RESERVED_ROOT_IDS:
        return f"{root_id!r} is reserved; choose another root id"
    return None


class NotStarted(RuntimeError):
    """The runtime must be started (`await runtime.start()`) first."""


def agent_of_question(question_id: str) -> str:
    """`<agent>/<step>/<state>/question/<name>` -> `<agent>`."""
    head, sep, _name = question_id.rpartition("/question/")
    if not sep:
        raise InvalidAnswer(f"{question_id} is not a question id")
    return head.rsplit("/", 2)[0]


class Runtime:
    def __init__(
        self,
        *,
        store: StateStore,
        workspace: WorkspaceProvider,
        registry: Registry | None = None,
        concurrency: int = 16,
        executors: int = 1,
        lease_ttl: float = 120,
        beat: float = 30,
        scan_every: float = 1.0,
        drain: float = 30,
        views: list[Any] | None = None,
        envelope: type[Artifact] = Artifact,
        state: Callable[..., Any] | None = None,
        contracts: Any = None,
        limits: dict[str, int] | None = None,
        clock: Callable[[], float] = time.time,
        owner: str | None = None,
    ) -> None:
        self.store = store
        self.registry = registry or Registry()
        self.registrar = Registrar(self.registry)
        self.effects = EffectManager(self.registry)
        self.workspaces = WorkspaceService(workspace, store)
        if lease_ttl <= 0 or beat <= 0 or beat >= lease_ttl:
            raise ValueError(
                f"leases need 0 < beat < lease_ttl (beat {beat}, lease_ttl {lease_ttl}): a lease "
                "that expires as it is taken, or is renewed too late, lets nothing commit"
            )
        self.clock = clock
        self.concurrency = concurrency
        self.executor_count = executors
        self.executor_options = {
            "lease_ttl": lease_ttl,
            "beat": beat,
            "scan_every": scan_every,
            "concurrency": concurrency,
        }
        self.drain = drain
        self.views = list(views or [])
        self.envelope = envelope
        self.state_fn = state
        self.contracts = contracts
        unknown = set(limits or {}) - set(LIMITS)
        if unknown:
            raise ValueError(f"unknown limits {sorted(unknown)}; known: {list(LIMITS)}")
        #: limits on every tree (a root and its descendants), checked on each spawn
        self.limits = {k: int(v) for k, v in (limits or {}).items() if v is not None}
        self.owner = owner or f"runtime-{uuid.uuid4().hex[:8]}"
        self.warnings: list[str] = []
        self.started = False
        self.executors: list[Executor] = []
        self.listeners: list[Callable[[dict[str, Any]], None]] = []
        self.wakeup = asyncio.Event()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._seen_views: dict[str, Any] = {}
        self._temporary: list[Executor] = []  # executors `run()` makes while it runs
        #: every executor made over this runtime, however it was made (KILL reaches them all)
        self._every_executor: weakref.WeakSet[Executor] = weakref.WeakSet()

    # ---- construction -------------------------------------------------------------------------
    @classmethod
    def local(cls, folder: str | Path, **options: Any) -> Runtime:
        """SQLite in `<folder>/state.db`, workspaces in `<folder>/workspaces/`."""
        from ..providers.stores import SqliteStore
        from ..providers.workspaces import LocalWorkspace

        folder = Path(folder)
        return cls(
            store=SqliteStore(folder / "state.db"),
            workspace=LocalWorkspace(folder / "workspaces"),
            **options,
        )

    @classmethod
    def from_config(cls, config: str | Path | dict[str, Any], **overrides: Any) -> Runtime:
        from .config import runtime_from_config

        return runtime_from_config(config, **overrides)

    # ---- lifecycle ----------------------------------------------------------------------------
    async def start(self) -> Runtime:
        if self.started:
            return self
        self.warnings += await self.registry.start_providers()
        self.registry.freeze()
        errors, warnings = self.registry.validate()
        self.warnings += warnings
        if errors:
            raise RegistryError(errors)
        self._stop.clear()
        for n in range(self.executor_count):
            executor = Executor(self, owner=f"{self.owner}-{n}", **self.executor_options)
            self.executors.append(executor)
            self._tasks.append(
                asyncio.create_task(executor.run(self._stop, self.wakeup, self.drain))
            )
        self.started = True
        self.notify()
        return self

    async def stop(self, drain: float | None = None) -> None:
        """No new steps; running ones finish within `drain` seconds or are cancelled and their
        leases released. Agents are not ended."""
        if drain is not None:
            self.drain = drain
        await asyncio.gather(*(e.stop(self.drain) for e in self.executors))
        self._stop.set()
        self.wakeup.set()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        self.executors.clear()

    async def close(self) -> None:
        await self.stop()
        for provider in list(self.registry.providers.values()):
            close = getattr(provider, "close", None)
            if close is not None:
                with contextlib.suppress(Exception):
                    await close()
        await self.workspaces.close()
        closer = getattr(self.store, "close", None)
        if closer is not None:
            with contextlib.suppress(Exception):
                result = closer()
                if asyncio.iscoroutine(result):
                    await result
        self.started = False

    async def __aenter__(self) -> Runtime:
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def _require_started(self) -> None:
        if not self.started:
            raise NotStarted("start the runtime first: `await runtime.start()` or `async with`")

    # ---- the executor's environment -----------------------------------------------------------
    def adopt(self, executor: Executor) -> None:
        """An executor was made over this runtime (called by the executor itself)."""
        self._every_executor.add(executor)

    def killed(self, agent_id: str) -> None:
        """An agent was killed: every executor of this runtime cancels its running step."""
        for executor in list(self._every_executor):
            executor.cancel(agent_id, "killed")

    def notify(self) -> None:
        self.wakeup.set()
        for listener in list(self.listeners):
            listener({"kind": "changed"})

    async def prepare(self, agent_id: str) -> None:
        await self.workspaces.prepare(agent_id)

    def definition_of(self, record: Record) -> AgentDefinition:
        return self.registry.agent(record.definition.get("name") or record.agent)

    async def run_step(
        self, record: Record, lease: Lease, root_input: dict[str, Any]
    ) -> Transition | Fault:
        try:
            definition = self.definition_of(record)
        except KeyError as exc:
            return Fault(f"cannot run {record.agent}: {exc}")
        changed = self.registry.pin_problems(record.definition, record.bindings)
        if changed:
            return Fault(
                "pinned definition or tools changed: "
                + "; ".join(changed)
                + " (repin the agent to accept the current ones, then CONT)"
            )
        cls = definition.cls
        if cls is None:
            return Fault(f"agent {definition.name} is not loaded")
        schema_version = None
        current_version = getattr(cls, "schema_version", 1)
        if record.schema_version != current_version:
            try:
                data = cls().migrate_data(record.schema_version, dict(record.data))
            except Exception as exc:
                return Fault(f"data migration failed: {exc}")
            record = record.but(data=data)
            schema_version = current_version
        await self.refresh_views(record.id)
        files = await self.workspaces.files_of(record.id)
        ctx = StepContext(
            self,
            record,
            lease,
            definition,
            root_input,
            files,
            self.workspaces.session_of(record.id),
        )
        ctx.effects.schema_version = schema_version
        outcome = await run_state(cls, record, ctx)
        problem = await self.effects.settle(ctx)
        if problem is not None:
            if isinstance(outcome, Fault):  # the state's own error comes first
                return Fault(f"{outcome.error} ({problem})", crossings=list(ctx.effects.crossings))
            return Fault(problem, crossings=list(ctx.effects.crossings))
        if isinstance(outcome, Transition):
            problem = self._check_spawns(definition, outcome)
            if problem is None and outcome.attach:
                problem = await self.attach_problem(definition, outcome)
            if problem is None and outcome.done:
                problem = await self.live_children_problem(record, outcome)
            if problem is None and outcome.spawns:
                problem = await self.limit_problem(record, len(outcome.spawns))
            if problem:
                return Fault(problem, crossings=list(outcome.crossings))
        return outcome

    async def attach_problem(self, parent: AgentDefinition, t: Transition) -> str | None:
        """Children attached to another run: the parent's definition grants the mode (`attach:
        ro` or `rw`), and the target is an existing root."""
        for name, target in t.attach.items():
            root = target["root"] if isinstance(target, dict) else target
            mode = target.get("mode", "ro") if isinstance(target, dict) else "ro"
            if mode not in ("ro", "rw"):
                return f"{name}: attach mode is ro or rw, not {mode!r}"
            granted = {"none": (), "ro": ("ro",), "rw": ("ro", "rw")}[parent.attach]
            if mode not in granted:
                return (
                    f"agent {parent.name} may not attach {name} {mode} to {root!r} "
                    f"(its definition grants attach: {parent.attach})"
                )
            if not isinstance(root, str) or "/" in root or await self.store.find(root) is None:
                return f"{name}: no root {root!r} to attach to"
        return None

    async def attach_target(self, target: str) -> str:
        """The workspace an attachment to `target` works in: that root's own, or the run it is
        attached to itself."""
        await self.workspaces.prepare(target)
        return self.workspaces.session_of(target)[0]

    async def live_children_problem(self, record: Record, t: Transition) -> str | None:
        """An agent may not finish while a child is live (running, waiting, faulted or
        stopped), unless it ends that child (TERM or KILL) in this step or ended it before:
        children are joined or ended deliberately, never cut off by a parent finishing."""
        ending = {
            target for target, signal in t.signals if signal_name(signal) in ("TERM", "KILL")
        } | {
            i["target"]
            for i in record.intents
            if i["kind"] == "signal" and signal_name(i["signal"]) in ("TERM", "KILL")
        }
        live = []
        for name, child_id in record.children.items():
            if child_id in ending:
                continue
            child = await self.store.find(child_id)
            if child is not None and not child.ended:
                live.append(name)
        if not live:
            return None
        return (
            f"{record.agent} finished while children are live: {', '.join(sorted(live))} "
            "(join them, or end them with TERM or KILL, before finishing)"
        )

    async def limit_problem(self, parent: Record, count: int) -> str | None:
        """Why `count` new children of `parent` would break the tree's limits, or None.
        `max_depth`: a root is depth 0; `max_total_agents`: every agent the tree ever had;
        `max_live_agents`: agents of the tree not ended (running, waiting or suspended)."""
        if not self.limits or count <= 0:
            return None
        depth = parent.id.count("/") + 1
        if "max_depth" in self.limits and depth > self.limits["max_depth"]:
            return f"limit max_depth={self.limits['max_depth']}: a child would be at depth {depth}"
        if "max_total_agents" in self.limits or "max_live_agents" in self.limits:
            tree = await self.store.tree(parent.id.split("/")[0])
            total = len(tree) + count
            most = self.limits.get("max_total_agents")
            if most is not None and total > most:
                return f"limit max_total_agents={most}: the tree would have {total}"
            live = sum(1 for r in tree if not r.ended) + count
            most = self.limits.get("max_live_agents")
            if most is not None and live > most:
                return f"limit max_live_agents={most}: {live} agents would be live"
        return None

    def _check_spawns(self, parent: AgentDefinition, t: Transition) -> str | None:
        for name, (target, child_input) in t.spawns.items():
            if name in HOME_FOLDERS or "/" in name or not name:
                return f"a child may not be named {name!r}"
            if not self.registry.may_spawn(parent, target):
                return f"agent {parent.name} may not spawn {target}"
            child = self.registry.resolve_spawn(parent, target)
            if child is None:
                return f"cannot spawn {target}: not registered"
            if child.input_schema is not None:
                try:
                    jsonschema.validate(child_input, child.input_schema)
                except jsonschema.ValidationError as exc:
                    return f"input for {target} ({name}) is invalid: {exc.message[:200]}"
        return None

    def new_record(
        self,
        definition: AgentDefinition,
        record_id: str,
        *,
        parent: str | None = None,
        name: str | None = None,
        data_input: dict[str, Any] | None = None,
        workspace: str | None = None,
        workspace_mode: str = "ro",
    ) -> Record:
        cls = definition.cls
        return Record(
            id=record_id,
            agent=definition.name,
            state=cls.start,
            parent=parent,
            name=name,
            input=dict(data_input or {}),
            workspace=workspace,
            workspace_mode=workspace_mode,
            code=definition.code,
            schema_version=getattr(cls, "schema_version", 1),
            term_state=getattr(cls, "on_term", None),
            definition=definition.pin(),
            bindings=self.registry.bindings_for(definition),
        )

    async def create_child(self, parent: Record, intent: dict[str, Any]) -> Record:
        parent_definition = self.definition_of(parent)
        definition = self.registry.resolve_spawn(parent_definition, intent["agent"])
        if definition is None:
            raise KeyError(f"cannot spawn {intent['agent']}: not registered")
        workspace = intent.get("workspace")
        if workspace:
            workspace = await self.attach_target(workspace)
        return self.new_record(
            definition,
            intent["id"],
            parent=parent.id,
            name=intent["name"],
            data_input=intent["input"],
            workspace=workspace,
            workspace_mode=intent.get("workspace_mode") or "ro",
        )

    async def child_created(self, child: Record) -> None:
        self.workspaces.remember(child)

    # ---- control ------------------------------------------------------------------------------
    async def spawn(
        self,
        name: str,
        input: dict[str, Any] | None = None,
        *,
        root_id: str | None = None,
        attach: str | None = None,
        attach_mode: str = "ro",
        inherit: dict[str, Any] | None = None,
    ) -> str:
        """A new root. `attach`: work against that root's workspace (read-only unless
        `attach_mode="rw"`); `inherit` ({"root", "agents"}): a fork reading that run's outputs
        for the agents it keeps."""
        self._require_started()
        definition = self.registry.agent(name)
        given = dict(input or {})
        if definition.input_schema is not None:
            try:
                jsonschema.validate(given, definition.input_schema)
            except jsonschema.ValidationError as exc:
                raise ValueError(f"input for {name} is invalid: {exc.message}") from exc
        root_id = root_id or f"{name.rpartition(':')[2]}-{uuid.uuid4().hex[:8]}"
        problem = root_id_problem(root_id)
        if problem:
            raise ValueError(problem)
        if attach is not None:
            if "/" in attach or await self.store.find(attach) is None:
                raise ValueError(f"no root {attach} to attach to")
            attach = await self.attach_target(attach)
        if attach_mode not in ("ro", "rw"):
            raise ValueError("attach_mode is 'ro' or 'rw'")
        if inherit is not None and await self.store.find(str(inherit.get("root"))) is None:
            raise ValueError(f"no root {inherit.get('root')} to inherit from")
        record = self.new_record(
            definition, root_id, data_input=given, workspace=attach, workspace_mode=attach_mode
        ).but(inherits=dict(inherit) if inherit else None)
        init = {"id": f"{root_id}/init", "kind": "init", "done": False}
        record = record.but(intents=[init])
        # the record first: an id that exists is refused before anything is written, so a
        # duplicate never touches the live run; then the workspace, through the `init` intent,
        # so a crash in between is finished by the executor like any other intent
        if not await self.store.create(record):
            raise ValueError(f"{root_id} exists")
        await self.initialize(record, init)
        await self._activate(root_id, init["id"])
        await self.refresh_views(root_id)
        self.notify()
        return root_id

    async def _activate(self, root_id: str, intent_id: str) -> None:
        """Mark a new root's `init` intent done, unless an executor already did (it may have
        prepared the workspace meanwhile and be running the first step): either way the root is
        created and active, so the spawn succeeds."""
        for _ in range(5):
            created = await self.store.load(root_id)
            if all(i["done"] for i in created.intents if i["id"] == intent_id):
                return
            try:
                await self.store.commit_control(
                    root_id,
                    created.revision,
                    lambda r: mark_intent_done(r, intent_id),
                    self.clock(),
                    kind="intent",
                    intent=intent_id,
                )
                return
            except Conflict:
                continue  # changed meanwhile: look again
            except Busy:
                return  # an executor holds it, so it carried the intent out
        # still changing: the executor that keeps changing it carries the intent out

    async def initialize(self, record: Record, intent: dict[str, Any]) -> None:
        """A new root's workspace: its session known, its input written. Safe to repeat."""
        self.workspaces.remember(record)
        files = await self.workspaces.files_of(record.id)
        await files.put(input_path(record), dumps(record.input))

    async def signal(self, agent_id: str, signal: Any, *, message_id: str | None = None) -> None:
        """Raises `ValueError` for a value that is not a signal, and `Ended` for an agent that
        has ended (the message would never be read)."""
        problem = signal_problem(signal)
        if problem is not None:
            raise ValueError(problem)
        record = await self.store.load(agent_id)
        if record.ended:
            raise Ended(f"{agent_id} has ended ({record.status}); it takes no more signals")
        if signal_name(signal) == "KILL":
            await self.store.kill(agent_id)
            self.killed(agent_id)
        else:
            await self.store.append_inbox(
                agent_id,
                message_id or f"user/{uuid.uuid4().hex}",
                {"kind": "signal", "signal": list(signal) if isinstance(signal, tuple) else signal},
            )
        self.notify()

    async def answer(self, question_id: str, answer: Any, *, by: str = "person") -> None:
        """Checked now (open, valid) and stored once; a second answer is refused."""
        agent_id = agent_of_question(question_id)
        apply_answer(await self.store.load(agent_id), question_id, answer, by=by)
        stored = await self.store.append_inbox(
            agent_id,
            question_id,
            {"kind": "answer", "question_id": question_id, "answer": answer, "by": by},
        )
        if not stored:
            raise InvalidAnswer(f"{question_id} is already answered")
        self.notify()

    async def repin(self, agent_id: str) -> dict[str, Any]:
        """Accept the current definition and tools for an instance whose pins changed (then CONT
        it). Recorded like a signal."""
        self._require_started()
        record = await self.store.load(agent_id)
        definition = self.registry.agent(record.definition.get("name") or record.agent)
        pins = {"definition": definition.pin(), "bindings": self.registry.bindings_for(definition)}
        await self.store.append_inbox(
            agent_id, f"repin/{uuid.uuid4().hex}", {"kind": "repin", **pins}
        )
        self.notify()
        return pins

    # ---- reading back -------------------------------------------------------------------------
    def row(self, record: Record, lease: Lease | None) -> dict[str, Any]:
        return {
            "id": record.id,
            "agent": record.agent,
            "parent": record.parent,
            "name": record.name,
            "state": record.state,
            "step": record.step,
            "revision": record.revision,
            "status": record.status,
            "view": view(record, lease, self.clock()),
            "reasons": record.reasons,
            "error": record.error,
            "published": sorted(record.published),
            "children": dict(record.children),
            "workspace": record.workspace,
            "workspace_mode": record.workspace_mode if record.workspace else None,
            "inherits": record.inherits,
            "definition": record.definition,
        }

    async def get(self, agent_id: str) -> dict[str, Any]:
        record = await self.store.load(agent_id)
        lease = await self.store.lease(agent_id)
        return {
            **self.row(record, lease),
            "input": record.input,
            "data": record.data,
            "notes": record.notes,
            "bindings": record.bindings,
            "intents": record.intents,
            "lease": None
            if lease is None
            else {"owner": lease.owner, "epoch": lease.epoch, "expires": lease.expires},
        }

    async def tree(self, root: str) -> list[dict[str, Any]]:
        return [self.row(r, await self.store.lease(r.id)) for r in await self.store.tree(root)]

    table = tree

    async def runs(self) -> list[dict[str, Any]]:
        found = []
        for root in await self.store.roots():
            tree = await self.store.tree(root.id)
            views: dict[str, int] = {}
            for record in tree:
                v = view(record, await self.store.lease(record.id), self.clock())
                views[v] = views.get(v, 0) + 1
            found.append(
                {
                    **self.row(root, await self.store.lease(root.id)),
                    "agents": len(tree),
                    "views": views,
                    "questions": sum(
                        1 for r in tree for x in r.reasons if x.get("kind") == "answer"
                    ),
                }
            )
        return found

    async def questions(self, root: str | None = None) -> list[dict[str, Any]]:
        roots = [root] if root else [r.id for r in await self.store.roots()]
        found = []
        for r in roots:
            for record in await self.store.tree(r):
                found += [
                    {"agent": record.id, **x} for x in record.reasons if x["kind"] == "answer"
                ]
        return found

    async def history(self, agent_id: str) -> list[dict[str, Any]]:
        return await self.store.history(agent_id)

    async def artifact(self, root: str, kind: str, by: str | None = None) -> dict[str, Any]:
        """A published artifact of the tree; `by` names its publisher (agent id, name or kind)."""
        published = [r for r in await self.workspaces.visible_tree(root) if kind in r.published]
        found = published
        if by is not None:
            found = [r for r in published if by in (r.id, r.name)] or [
                r for r in published if r.agent == by or r.agent.endswith(":" + by)
            ]
        if len(found) != 1:
            raise LookupError(f"{kind}: published by {[r.id for r in found]}")
        return json.loads(await self.workspaces.read_object(found[0].published[kind]["path"]))

    async def files(self, root: str, prefix: str = "") -> list[str]:
        return sorted(await (await self.workspaces.files_of(root)).list(prefix))

    async def read_file(self, root: str, path: str) -> bytes:
        return await (await self.workspaces.files_of(root)).get(path)

    async def events(
        self, root: str, since: int = 0, *, follow: bool = True, poll: float = 0.5
    ) -> AsyncIterator[dict[str, Any]]:
        """Durable events after `since`: subscribe first, then read the log, and read it again
        after the last sequence on every notification or poll, so nothing falls in between."""
        changed = asyncio.Event()

        def listener(_event: dict[str, Any]) -> None:
            changed.set()

        self.listeners.append(listener)
        try:
            last = since
            while True:
                changed.clear()
                for event in await self.store.read_events(root, last):
                    last = event["seq"]
                    yield event
                if not follow:
                    return
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(changed.wait(), poll)
        finally:
            self.listeners.remove(listener)

    # ---- registry listings --------------------------------------------------------------------
    def agents(self) -> list[dict[str, Any]]:
        return [self.registry.describe_agent(n) for n in sorted(self.registry.agents)]

    def tools(self) -> list[dict[str, Any]]:
        return [self.registry.describe_effect(n) for n in sorted(self.registry.effects)]

    def providers(self) -> list[dict[str, Any]]:
        found = []
        for name, provider in sorted(self.registry.providers.items()):
            found.append(
                {
                    "name": name,
                    "kind": type(provider).__name__,
                    "status": "unreachable" if name in self.registry.unreachable else "up",
                    "concurrency": getattr(provider, "concurrency", None),
                    "tools": sorted(
                        e for e, d in self.registry.effects.items() if d.provider == name
                    ),
                }
            )
        return found

    # ---- views and state ----------------------------------------------------------------------
    @property
    def view_paths(self) -> tuple[str, ...]:
        return tuple(p for v in self.views for p in getattr(v, "paths", ()))

    async def state_of(self, tree: list[Record]) -> dict[str, Any]:
        return await (self.state_fn or agents_state)(
            tree, self.workspaces.read_object, self.registry.agents.get
        )

    async def refresh_views(self, agent_id: str) -> None:
        if not self.views:
            return
        root = agent_id.split("/")[0]
        await self.workspaces.prepare(root)
        if self.workspaces.session_of(root)[0] != root:
            return  # attached: the views are the other run's
        tree = await self.workspaces.visible_tree(root)
        mark = tuple((r.id, r.revision) for r in tree if r.published or r.notes or not r.parent)
        if self._seen_views.get(root) == mark:
            return
        files = await self.workspaces.files_of(root)
        for v in self.views:
            await v.refresh(
                files, tree, read=self.workspaces.read_object, spec_of=self.registry.agents.get
            )
        self._seen_views[root] = mark

    # ---- running in-process (tests, the CLI's `run`) ------------------------------------------
    async def run(
        self, root: str | None = None, *, timeout: float | None = None, **options: Any
    ) -> list[dict[str, Any]]:
        """Until nothing can move without someone outside: every agent of the tree ended or
        suspended on an answer, a stop or a fault (sleeps are waited out). Uses a temporary
        executor when the runtime has none of its own."""
        self._require_started()
        own = None
        temporary = self._temporary
        if not self.executors:
            own = Executor(self, owner=f"{self.owner}-run", **{**self.executor_options, **options})
            temporary.append(own)
            if root:
                own.roots = {root}

        async def until_quiet() -> None:
            while True:
                self.wakeup.clear()
                moved = await own.tick() if own is not None else False
                busy = bool(own.running) if own is not None else await self._busy(root)
                # quiet only if nothing happened since this pass began (a step that ended
                # during the pass notifies; its agent may have work the pass did not see)
                if (
                    not moved
                    and not busy
                    and not self.wakeup.is_set()
                    and not await self._sleeping(root)
                ):
                    return
                if not moved:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            self.wakeup.wait(),
                            (own.scan_every if own else self.executor_options["scan_every"]),
                        )

        try:
            await asyncio.wait_for(until_quiet(), timeout)
        finally:
            if own is not None:
                await own.drain()
                temporary.remove(own)
            if root:
                await self.refresh_views(root)
        return await self.tree(root) if root else []

    async def _busy(self, root: str | None) -> bool:
        records = await self.store.tree(root) if root else await self.store.scan_work()
        return any((not r.ended and not r.reasons) or r.pending_intents for r in records) or any(
            e.running for e in self.executors
        )

    async def _sleeping(self, root: str | None) -> bool:
        records = await self.store.tree(root) if root else await self.store.scan_work()
        now = self.clock()
        for rec in records:
            if rec.ended and not rec.pending_intents:
                continue
            lease = await self.store.lease(rec.id)
            if lease is not None and lease.valid(now):
                return True
            if any(x["kind"] in ("stopped", "fault") for x in rec.reasons):
                continue
            if any(
                r["kind"] == "sleep" or (r["kind"] == "answer" and r.get("deadline"))
                for r in rec.reasons
            ):
                return True
        return False


__all__ = ["LIMITS", "NotStarted", "Runtime", "agent_of_question"]
