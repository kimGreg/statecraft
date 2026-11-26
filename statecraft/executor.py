"""The executor: a simple loop over the store. It holds no state that matters after a crash.

For each record with work left (found by `scan_work`, periodically; notifications only make it
sooner), in order:
    1. apply its inbox (signals, answers) with `commit_control`
    2. resolve its waits (children's records, deadlines)
    3. carry out its pending intents (create children, send signals) and mark them done
    4. if runnable: take a lease, run one state, `commit_step` (which releases the lease)
Steps run as tasks on one asyncio loop; a heartbeat renews each running step's lease. Several
executors (in one process or many) may share a store: leases and CAS keep them apart.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .agent import run_state
from .artifacts import Artifact
from .catalog import Catalog
from .context import Context
from .process import ChildView, Fault, Lease, Record, Transition
from .spec import AGENT_PREFIX, AgentSpec
from .store import Busy, Conflict, Fenced, StateStore
from .transitions import (
    InvalidAnswer,
    InvalidTransition,
    apply_answer,
    apply_signal,
    is_runnable,
    mark_intent_done,
    resolve_wait,
)
from .views import agents_state
from .workspace import HOME_FOLDERS, Files, ScopedFiles, home

log = logging.getLogger("statecraft.executor")
#: Paths every agent may read besides the views': shared files and the engine's files.
READABLE = ("shared/", "run/")


@dataclass
class Services:
    """What steps need from the deployment: the ports."""

    store: StateStore
    files: Files
    tools: Any
    catalog: Catalog
    model: Any = None
    contracts: Any = None
    clock: Callable[[], float] = time.time
    #: The artifact envelope (`artifacts.Artifact` or an application's subclass).
    envelope: type[Artifact] = Artifact
    #: The tree's cumulative state, `state(tree, read, spec_of)` (default `views.agents_state`).
    state: Callable[..., Any] | None = None
    listeners: list[Callable[[dict[str, Any]], None]] = field(default_factory=list)
    wakeup: asyncio.Event = field(default_factory=asyncio.Event)

    _tools: dict[str, Any] = field(default_factory=dict)
    _files: dict[str, Any] = field(default_factory=dict)
    _sessions: dict[str, str] = field(default_factory=dict)

    def tools_of(self, agent_id: str) -> Any:
        """The tools bound to the agent's root (its session): `tools` itself, or what the
        factory `tools(root_id)` gives, once per root."""
        if hasattr(self.tools, "call"):
            return self.tools
        session = self.session_of(agent_id)
        if session not in self._tools:
            self._tools[session] = self.tools(session)
        return self._tools[session]

    def files_of(self, agent_id: str) -> Any:
        """The agent's root workspace: a folder of `files` scoped to the root, or what the
        factory `files(root_id)` gives (a session the tools also see), once per root."""
        session = self.session_of(agent_id)
        if hasattr(self.files, "get"):
            return ScopedFiles(self.files, session)
        if session not in self._files:
            self._files[session] = self.files(session)
        return self._files[session]

    def session_of(self, agent_id: str) -> str:
        """The workspace (session) an agent works in: its root's, or the one its nearest attached
        ancestor (itself included) was attached to (`prepare` learns them from the records)."""
        parts = agent_id.split("/")
        for depth in range(len(parts), 0, -1):
            found = self._sessions.get("/".join(parts[:depth]))
            if found:
                return found
        return parts[0]

    async def prepare(self, agent_id: str) -> None:
        parts = agent_id.split("/")
        for depth in range(1, len(parts) + 1):
            key = "/".join(parts[:depth])
            if key not in self._sessions:
                record = await self.store.find(key)
                if record is None:
                    continue
                self._sessions[key] = record.workspace or (parts[0] if depth == 1 else "")

    def catalog_spec(self, ref: str) -> AgentSpec | None:
        try:
            return self.catalog.spec(ref)
        except KeyError:
            return None

    views: list[Any] = field(default_factory=list)
    _seen: dict[str, Any] = field(default_factory=dict)

    async def refresh_views(self, agent_id: str) -> None:
        """Rebuild the root's views when its tree changed since the last refresh."""
        if not self.views:
            return
        root = agent_id.split("/")[0]
        if self.session_of(root) != root:
            return  # an attached root reads another run's workspace; its views are that run's
        tree = await self.visible_tree(root)
        mark = tuple((r.id, r.revision) for r in tree if r.published or r.notes or not r.parent)
        if self._seen.get(root) == mark:
            return
        files = self.files_of(root)
        for view in self.views:
            await view.refresh(files, tree, read=self.read_object, spec_of=self.spec_of)
        self._seen[root] = mark

    async def visible_tree(self, root: str) -> list[Record]:
        """The root's tree, and what it inherits from earlier runs: their records for the agents
        it keeps (by name or agent id), unless its own tree has an agent of that name."""
        own = await self.store.tree(root)
        record = next((r for r in own if r.id == root), None)
        inherits = record.inherits if record is not None else None
        if not inherits:
            return own
        mine = {r.name for r in own if r.parent}
        kept = set(inherits.get("agents") or [])
        found = []
        for other in await self.visible_tree(str(inherits["root"])):
            if other.parent is None or other.name in mine:
                continue
            short = other.agent.partition(":")[2]
            if not kept or other.name in kept or short in kept:
                found.append(other)
        return [*own, *found]

    async def read_object(self, path: str) -> bytes:
        """An object or file of any agent, read in the workspace of the agent whose home the path
        names (`home/<agent id>/<objects|private|staging|skills>/...`)."""
        parts = path.split("/")
        owner = ""
        if parts[0] == "home":
            folder = next((i for i, p in enumerate(parts) if i > 1 and p in HOME_FOLDERS), None)
            owner = "/".join(parts[1:folder]) if folder else parts[1]
        if owner:
            await self.prepare(owner)
        return await self.files_of(owner).get(path)

    def spec_of(self, ref: str) -> AgentSpec | None:
        return self.catalog_spec(ref)

    @property
    def readable(self) -> tuple[str, ...]:
        """Paths every agent may read: shared files, the engine's files and the views' paths."""
        return (*READABLE, *(p for view in self.views for p in getattr(view, "paths", ())))

    async def state_of(self, tree: list[Record]) -> dict[str, Any]:
        return await (self.state or agents_state)(tree, self.read_object, self.spec_of)

    def notify(self) -> None:
        self.wakeup.set()
        for listener in list(self.listeners):
            listener({"kind": "changed"})

    def diagnostic(self, record: Record, kind: str, **detail: Any) -> None:
        """Events emitted while a step runs; lost if the step does not commit."""
        event = {
            "kind": kind,
            "agent": record.id,
            "step": record.step,
            "diagnostic": True,
            **detail,
        }
        for listener in list(self.listeners):
            listener(event)


async def install_skills(services: Services, record: Record) -> None:
    """Copy the agent's declared skills into its home (idempotent overwrite)."""
    spec = services.catalog_spec(record.agent)
    if spec is None or not spec.skills:
        return
    files = services.files_of(record.id)
    for skill_id in spec.skills:
        folder = services.catalog.skill_folder(record.agent, skill_id)
        for path in sorted(folder.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                target = f"{home(record.id)}/skills/{skill_id}/{path.relative_to(folder)}"
                await files.put(target, path.read_bytes())


def new_record(
    catalog: Catalog,
    ref: str,
    record_id: str,
    *,
    parent: str | None = None,
    name: str | None = None,
    data_input: dict[str, Any] | None = None,
    workspace: str | None = None,
) -> Record:
    cls = catalog.agent_class(ref)
    return Record(
        id=record_id,
        agent=ref,
        state=cls.start,
        parent=parent,
        name=name,
        input=dict(data_input or {}),
        workspace=workspace,
        code=catalog.code(ref),
        schema_version=cls.schema_version,
        term_state=cls.on_term,
    )


class Executor:
    def __init__(
        self,
        services: Services,
        *,
        owner: str | None = None,
        lease_ttl: float = 120,
        beat: float = 30,
        scan_every: float = 1.0,
        roots: set[str] | None = None,
    ) -> None:
        self.s = services
        self.owner = owner or f"executor-{uuid.uuid4().hex[:8]}"
        self.lease_ttl = lease_ttl
        self.beat = beat
        self.scan_every = scan_every
        self.roots = roots
        self.running: dict[str, asyncio.Task[None]] = {}

    @property
    def store(self) -> StateStore:
        return self.s.store

    def now(self) -> float:
        return self.s.clock()

    # ---- the loop -----------------------------------------------------------------------------
    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                self.s.wakeup.clear()
                await self.tick()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.s.wakeup.wait(), self.scan_every)
        finally:
            await self.drain()

    async def drain(self) -> None:
        for task in list(self.running.values()):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def tick(self) -> bool:
        """One pass over every record with work left. True if anything changed or started."""
        moved = False
        for record in await self.store.scan_work():
            if self.roots is not None and record.id.split("/")[0] not in self.roots:
                continue
            if record.id in self.running:
                continue
            try:
                moved |= await self._process(record)
            except (Busy, Conflict):
                moved = True  # someone else moved it; look again next pass
        return moved

    async def _process(self, record: Record) -> bool:
        await self.s.prepare(record.id)
        lease = await self.store.lease(record.id)
        if lease is not None and lease.valid(self.now()):
            return False  # another executor runs its step
        before = record.revision
        record = await self._apply_inbox(record)
        record = await self._resolve(record)
        record = await self._carry_out(record)
        moved = record.revision != before
        if is_runnable(record, None, self.now()):
            self._start(record)
            moved = True
        return moved

    async def _control(
        self, record: Record, change: Callable[[Record], Record], kind: str, **detail: Any
    ) -> Record:
        saved = await self.store.commit_control(
            record.id, record.revision, change, self.now(), kind=kind, **detail
        )
        if saved.revision != record.revision:
            self.s.notify()
        return saved

    # ---- 1. inbox ---------------------------------------------------------------------------
    async def _apply_inbox(self, record: Record) -> Record:
        for item in await self.store.read_inbox(record.id, record.inbox_applied):
            message = item["message"]
            n = item["n"]

            def change(r: Record, message: dict[str, Any] = message, n: int = n) -> Record:
                if message["kind"] == "answer":
                    try:
                        r = apply_answer(
                            r,
                            message["question_id"],
                            message["answer"],
                            by=message.get("by", "person"),
                        )
                    except InvalidAnswer as exc:
                        log.warning("%s: answer dropped: %s", r.id, exc)
                else:
                    r = apply_signal(r, message["signal"])
                return r.but(inbox_applied=n)

            kind = "answer" if message["kind"] == "answer" else "signal"
            detail = (
                {"question_id": message["question_id"]}
                if kind == "answer"
                else {"signal": _signal_name(message["signal"])}
            )
            record = await self._control(record, change, kind, **detail)
        return record

    # ---- 2. waits ---------------------------------------------------------------------------
    async def _resolve(self, record: Record) -> Record:
        waits = [r for r in record.reasons if r["kind"] in ("join", "sleep", "answer")]
        if not waits or record.ended:
            return record
        children: dict[str, ChildView] = {}
        for reason in waits:
            for name in reason.get("children", []):
                child_id = record.children.get(name)
                child = await self.store.find(child_id) if child_id else None
                if child is not None:
                    children[name] = ChildView.of(child)
        now = self.now()
        resolved = resolve_wait(record, children, now)
        if resolved == record:
            return record
        return await self._control(
            record,
            lambda r: resolve_wait(r, children, now),
            "woke",
            wake=(resolved.wake or {}).get("kind"),
        )

    # ---- 3. intents -------------------------------------------------------------------------
    async def _carry_out(self, record: Record) -> Record:
        for intent in record.pending_intents:
            if intent["kind"] == "spawn":
                ref = self.s.catalog.child_ref(record.agent, intent["agent"])
                child = new_record(
                    self.s.catalog,
                    ref,
                    intent["id"],
                    parent=record.id,
                    name=intent["name"],
                    data_input=intent["input"],
                    workspace=intent.get("workspace"),
                )
                if await self.store.create(child):
                    await self.s.prepare(child.id)  # an attached child works elsewhere
                    await install_skills(self.s, child)
            else:
                target = await self.store.find(intent["target"])
                if target is not None and _signal_name(intent["signal"]) == "KILL":
                    await self.store.kill(target.id)
                elif target is not None:
                    await self.store.append_inbox(
                        target.id, intent["id"], {"kind": "signal", "signal": intent["signal"]}
                    )
            record = await self._control(
                record,
                lambda r, i=intent["id"]: mark_intent_done(r, i),
                "intent",
                intent=intent["id"],
            )
        return record

    # ---- 4. steps ---------------------------------------------------------------------------
    def _start(self, record: Record) -> None:
        task = asyncio.create_task(self._step(record.id))
        self.running[record.id] = task
        task.add_done_callback(lambda _t, i=record.id: self._done(i))

    def _done(self, agent_id: str) -> None:
        self.running.pop(agent_id, None)
        self.s.notify()

    async def _step(self, agent_id: str) -> None:
        lease = await self.store.try_acquire(agent_id, self.owner, self.lease_ttl, self.now())
        if lease is None:
            return
        record = await self.store.load(agent_id)
        if not is_runnable(record, None, self.now()):
            await self.store.release(agent_id, lease)
            return
        heart = asyncio.create_task(self._heartbeat(agent_id, lease))
        try:
            await self.s.prepare(agent_id)
            await self.s.refresh_views(agent_id)
            outcome = await self._run(record, lease)
            current = await self.store.lease(agent_id)
            lease = current if current and current.epoch == lease.epoch else lease
            try:
                await self.store.commit_step(agent_id, lease, record.step, outcome, self.now())
            except InvalidTransition as exc:
                await self.store.commit_step(
                    agent_id, lease, record.step, Fault(f"invalid transition: {exc}"), self.now()
                )
        except (Fenced, Conflict) as exc:
            log.warning("%s: step %d not committed: %s", agent_id, record.step, exc)
        finally:
            heart.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heart

    async def _heartbeat(self, agent_id: str, lease: Lease) -> None:
        while True:
            await asyncio.sleep(self.beat)
            renewed = await self.store.renew(agent_id, lease, self.lease_ttl, self.now())
            if renewed is None:
                return
            lease = renewed

    async def _run(self, record: Record, lease: Lease) -> Transition | Fault:
        try:
            cls = self.s.catalog.agent_class(record.agent)
        except Exception as exc:
            return Fault(f"cannot load {record.agent}: {exc}")
        schema_version = None
        if record.schema_version != cls.schema_version:
            try:
                data = cls().migrate_data(record.schema_version, dict(record.data))
            except Exception as exc:
                return Fault(f"data migration failed: {exc}")
            record = record.but(data=data)
            schema_version = cls.schema_version
        root = record.id.split("/")[0]
        root_input = (await self.store.load(root)).input if root != record.id else {}
        spec = self.s.catalog_spec(record.agent)
        ctx = Context(self.s, record, lease, spec, root_input)
        ctx.effects.schema_version = schema_version
        outcome = await run_state(cls, record, ctx)
        if isinstance(outcome, Transition):
            problem = self._check_spawns(record, spec, outcome)
            if problem:
                return Fault(problem)
        return outcome

    def _check_spawns(self, record: Record, spec: AgentSpec | None, t: Transition) -> str | None:
        for name, (agent, _input) in t.spawns.items():
            if name in HOME_FOLDERS or "/" in name or not name:
                return f"a child may not be named {name!r}"
            if agent.startswith(AGENT_PREFIX) and spec is not None and not spec.may_call(agent):
                return f"agent {spec.id} did not declare {agent}"
            try:
                self.s.catalog.agent_class(self.s.catalog.child_ref(record.agent, agent))
            except Exception as exc:
                return f"cannot spawn {agent}: {exc}"
        return None


def _signal_name(signal: Any) -> str:
    return signal[0] if isinstance(signal, (list, tuple)) else signal


__all__ = ["Executor", "Services", "install_skills", "new_record"]
