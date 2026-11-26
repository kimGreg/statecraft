"""The engine's interface for applications. Non-blocking: it records requests; executors act.

    engine = Engine(store=SqliteStore(path), files=LocalFiles(folder), tools=tools,
                    catalog=Catalog({"notes": "my_agents_notes"}))
    root = await engine.spawn("notes:digest", input={...})
    await engine.signal(agent_id, "STOP" | "CONT" | "TERM" | "KILL" | ("USR", message))
    await engine.answer(question_id, answer)
    async for event in engine.events(root, since=seq): ...
    await engine.run(root)        # an in-process executor until the tree is quiet (CLI, tests)

The application keeps only the root's id. Everything else is read back from the store (records,
questions, events) and the workspace (artifacts).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

from .artifacts import Artifact
from .catalog import Catalog
from .context import dumps
from .executor import Executor, Services, install_skills, new_record
from .process import Lease, Record
from .store import StateStore
from .transitions import InvalidAnswer, apply_answer, view
from .workspace import Files


def agent_of_question(question_id: str) -> str:
    """`<agent>/<step>/<state>/question/<name>` -> `<agent>`."""
    head, sep, _name = question_id.rpartition("/question/")
    if not sep:
        raise InvalidAnswer(f"{question_id} is not a question id")
    return head.rsplit("/", 2)[0]


class Engine:
    def __init__(
        self,
        *,
        store: StateStore,
        files: Files,
        tools: Any,
        catalog: Catalog,
        model: Any = None,
        contracts: Any = None,
        clock: Callable[[], float] = time.time,
        views: list[Any] | None = None,
        envelope: type[Artifact] = Artifact,
        state: Callable[..., Any] | None = None,
    ) -> None:
        self.services = Services(
            store=store,
            files=files,
            tools=tools,
            catalog=catalog,
            model=model,
            contracts=contracts,
            clock=clock,
            views=list(views or []),
            envelope=envelope,
            state=state,
        )

    @property
    def store(self) -> StateStore:
        return self.services.store

    def workspace(self, root: str) -> Any:
        return self.services.files_of(root)

    # ---- control ------------------------------------------------------------------------------
    async def spawn(
        self,
        ref: str,
        input: dict[str, Any] | None = None,
        *,
        root_id: str | None = None,
        workspace: str | None = None,
        inherit: dict[str, Any] | None = None,
    ) -> str:
        """A new root and its workspace; its input is also written to `shared/input/`.

        With `workspace`, the root is attached to that root's workspace instead: its tools and
        files work in that session (it reads the run there, as a conversation about a finished
        run does), its own objects stay under its home, and it never rewrites that run's views.

        With `inherit` ({"root": <run>, "agents": [...]}), the root is a fork: it reads that
        run's publications of the listed agents (by name or agent id; all of them without a
        list) as its own, unless it runs an agent of that name itself. Nothing is copied.
        """
        root_id = root_id or f"{ref.partition(':')[2]}-{uuid.uuid4().hex[:8]}"
        if "/" in root_id:
            raise ValueError("a root id has no '/'")
        if workspace is not None and await self.store.find(workspace) is None:
            raise ValueError(f"no root {workspace} to attach to")
        if inherit is not None and await self.store.find(str(inherit.get("root"))) is None:
            raise ValueError(f"no root {inherit.get('root')} to inherit from")
        record = new_record(
            self.services.catalog, ref, root_id, data_input=input, workspace=workspace
        ).but(inherits=dict(inherit) if inherit else None)
        self.services._sessions[root_id] = workspace or root_id
        given = f"shared/input/{root_id}.json" if workspace else "shared/input/input.json"
        await self.workspace(root_id).put(given, dumps(record.input))
        if not await self.store.create(record):
            raise ValueError(f"{root_id} exists")
        await install_skills(self.services, record)
        await self.services.refresh_views(root_id)
        self.services.notify()
        return root_id

    async def signal(self, agent_id: str, signal: Any, *, message_id: str | None = None) -> None:
        if (signal[0] if isinstance(signal, (list, tuple)) else signal) == "KILL":
            await self.store.kill(agent_id)
        else:
            await self.store.append_inbox(
                agent_id,
                message_id or f"user/{uuid.uuid4().hex}",
                {"kind": "signal", "signal": signal},
            )
        self.services.notify()

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
        self.services.notify()

    # ---- reading back ---------------------------------------------------------------------------
    async def tree(self, root: str) -> list[Record]:
        return await self.store.tree(root)

    async def table(self, root: str) -> list[dict[str, Any]]:
        now = self.services.clock()
        rows = []
        for record in await self.tree(root):
            lease: Lease | None = await self.store.lease(record.id)
            rows.append(
                {
                    "id": record.id,
                    "agent": record.agent,
                    "state": record.state,
                    "step": record.step,
                    "view": view(record, lease, now),
                    "reasons": record.reasons,
                    "error": record.error,
                }
            )
        return rows

    async def questions(self, root: str) -> list[dict[str, Any]]:
        return [
            {"agent": r.id, **reason}
            for r in await self.tree(root)
            for reason in r.reasons
            if reason["kind"] == "answer"
        ]

    async def artifact(self, root: str, kind: str, by: str | None = None) -> dict[str, Any]:
        """A published artifact of the tree; `by` names its publisher: an agent id or name
        first, else an agent kind (`<feature>:<by>`)."""
        await self.services.prepare(root)
        published = [r for r in await self.tree(root) if kind in r.published]
        found = published
        if by is not None:
            found = [r for r in published if by in (r.id, r.name)] or [
                r for r in published if r.agent.endswith(":" + by)
            ]
        if len(found) != 1:
            raise LookupError(f"{kind}: published by {[r.id for r in found]}")
        return json.loads(await self.workspace(root).get(found[0].published[kind]["path"]))

    async def events(
        self, root: str, since: int = 0, *, follow: bool = True, poll: float = 0.5
    ) -> AsyncIterator[dict[str, Any]]:
        """Durable events after `since`. Subscribes first, then reads the log, and reads it again
        after the last sequence on every notification or poll, so nothing falls in between."""
        changed = asyncio.Event()

        def listener(event: dict[str, Any]) -> None:
            if not event.get("diagnostic"):
                changed.set()

        self.services.listeners.append(listener)
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
            self.services.listeners.remove(listener)

    # ---- running in-process --------------------------------------------------------------------
    def executor(self, **options: Any) -> Executor:
        return Executor(self.services, **options)

    async def run(
        self, root: str | None = None, *, timeout: float | None = None, **options: Any
    ) -> list[dict[str, Any]]:
        """Run an executor until nothing can move without someone outside: every agent of the
        tree ended or suspended on an answer, a stop or a fault (sleeps are waited out)."""
        executor = self.executor(roots={root} if root else None, **options)

        async def until_quiet() -> None:
            while True:
                self.services.wakeup.clear()
                moved = await executor.tick()
                if not moved and not executor.running and not await self._sleeping(root):
                    return
                if not moved:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.services.wakeup.wait(), executor.scan_every)

        try:
            await asyncio.wait_for(until_quiet(), timeout)
        finally:
            await executor.drain()
            if root:
                await self.services.refresh_views(root)
        return await self.table(root) if root else []

    async def _sleeping(self, root: str | None) -> bool:
        """Time alone will move something: a sleep or answer deadline, or a lease held by
        another (perhaps dead) executor that will expire."""
        records = await self.tree(root) if root else await self.store.scan_work()
        now = self.services.clock()
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


__all__ = ["Engine", "agent_of_question"]
