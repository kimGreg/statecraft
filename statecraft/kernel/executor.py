"""The executor: a loop over the store. It holds no state that matters after a crash.

For each record with work left (found by `scan_work`, periodically; notifications only make it
sooner), in order:
    1. apply its inbox (signals, answers) with `commit_control`
    2. resolve its waits (children's records, deadlines)
    3. carry out its pending intents (create children, send signals) and mark them done
    4. if runnable: wait for a slot, take a lease, run one state, `commit_step` (which releases
       the lease)

Concurrency (architecture §5): at most one valid step per agent (the lease); different agents run
concurrently up to `concurrency` slots; a suspended agent holds no slot. A step is cancelled when
its lease is lost (renewal fails) or it is killed: it then commits nothing. `stop` lets running
steps finish for `drain` seconds, then cancels the rest and releases their leases, so another
executor (or this runtime's next start) need not wait for them to expire.

Failures stay where they happen (one agent's failure never stops the others):
    an agent's code fails (its state, construction, context)   the step commits a Fault
    a cancellation the executor did not make                    the step commits a Fault
    an inbox message that is not valid                          rejected: recorded, skipped
    a spawn whose agent cannot be created                       the parent faults (CONT retries)
    the store is busy, or the record moved on                   tried again next pass
    the lease is lost, or the agent is killed                   the step stops, commits nothing
    anything else while handling one record                     logged in `failures`; the
                                                                executor goes on with the rest
A step that commits nothing always releases its lease.

What a step needs that is not the kernel's (the agent's code, its context, its pinned effects,
creating a child) comes from an `Environment` the runtime implements.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Callable
from typing import Any, Protocol

from .process import ChildView, Fault, Lease, Record, Transition
from .store import Busy, Conflict, Ended, Fenced, StateStore, StoreError, Unread, _payload
from .transitions import (
    InvalidAnswer,
    InvalidTransition,
    apply_answer,
    apply_fault,
    apply_signal,
    is_runnable,
    mark_intent_done,
    resolve_wait,
    signal_problem,
    undeliver,
)

#: How many failures an executor keeps for inspection (`Executor.failures`).
FAILURES_KEPT = 100

log = logging.getLogger("statecraft.executor")


class Environment(Protocol):
    """What the executor needs from the runtime."""

    store: StateStore
    clock: Callable[[], float]

    def notify(self) -> None: ...

    async def prepare(self, agent_id: str) -> None: ...

    async def initialize(self, record: Record, intent: dict[str, Any]) -> None:
        """A new root's workspace (session, input); safe to repeat."""
        ...

    async def run_step(
        self, record: Record, lease: Lease, root_input: dict[str, Any]
    ) -> Transition | Fault:
        """Check the record's pins, build its context, run its state, settle its effects."""
        ...

    async def create_child(self, parent: Record, intent: dict[str, Any]) -> Record: ...

    async def child_created(self, child: Record) -> None: ...


class Executor:
    def __init__(
        self,
        env: Environment,
        *,
        owner: str | None = None,
        lease_ttl: float = 120,
        beat: float = 30,
        scan_every: float = 1.0,
        roots: set[str] | None = None,
        concurrency: int = 16,
    ) -> None:
        self.env = env
        self.owner = owner or f"executor-{uuid.uuid4().hex[:8]}"
        self.lease_ttl = lease_ttl
        self.beat = beat
        self.scan_every = scan_every
        self.roots = roots
        self.slots = asyncio.Semaphore(concurrency)
        self.concurrency = concurrency
        #: agent id -> the task that waits for a slot and runs its step
        self.running: dict[str, asyncio.Task[None]] = {}
        #: agent id -> (the state's work task, its lease) while a step holds a lease
        self._work: dict[str, tuple[asyncio.Task[Any], Lease]] = {}
        self.stopping = False
        self.peak = 0  # the most steps that ran at once (observability, tests)
        #: agents whose step this executor cancelled on purpose (KILL, lease loss, stop): any
        #: other cancellation of a step is the state's own, and a fault
        self._cancelled: set[str] = set()
        #: failures while handling a record that were not the agent's (newest last)
        self.failures: list[dict[str, Any]] = []
        adopt = getattr(env, "adopt", None)
        if adopt is not None:  # so a KILL carried out anywhere reaches this executor's steps
            adopt(self)

    @property
    def store(self) -> StateStore:
        return self.env.store

    def now(self) -> float:
        return self.env.clock()

    # ---- the loop -----------------------------------------------------------------------------
    async def run(self, stop: asyncio.Event, wakeup: asyncio.Event, drain: float = 30) -> None:
        try:
            while not stop.is_set():
                wakeup.clear()
                try:
                    await self.tick()
                except Exception as exc:  # the scan itself failed (store unavailable): retry
                    self._failed("", "scan", exc)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(wakeup.wait(), self.scan_every)
        finally:
            await self.stop(drain)

    async def stop(self, drain: float = 30) -> None:
        """Start nothing new; give running steps `drain` seconds; cancel the rest and release
        their leases (their states run again later, as after a crash)."""
        self.stopping = True
        tasks = list(self.running.values())
        if tasks:
            _done, pending = await asyncio.wait(tasks, timeout=drain)
            for agent_id, (work, _lease) in list(self._work.items()):
                self._cancelled.add(agent_id)
                work.cancel()  # the step's own task then releases its lease
            if pending:
                _done, stuck = await asyncio.wait(pending, timeout=5)
                for task in stuck:
                    task.cancel()
                for task in stuck:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task

    async def drain(self) -> None:
        for task in list(self.running.values()):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    def cancel(self, agent_id: str, reason: str = "cancelled") -> bool:
        """Cancel the step this executor runs for the agent (KILL): it commits nothing."""
        held = self._work.get(agent_id)
        if held is None:
            return False
        log.info("%s: step cancelled (%s)", agent_id, reason)
        self._cancelled.add(agent_id)
        held[0].cancel()
        return True

    def _failed(self, agent_id: str, phase: str, exc: BaseException) -> None:
        log.error("%s: %s failed: %s: %s", agent_id or "-", phase, type(exc).__name__, exc)
        self.failures.append(
            {"agent": agent_id, "phase": phase, "error": f"{type(exc).__name__}: {exc}"[:500]}
        )
        del self.failures[:-FAILURES_KEPT]

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
            except StoreError as exc:  # the store is busy or unavailable: next pass
                self._failed(record.id, "store", exc)
            except Exception as exc:  # isolated: the other records go on
                self._failed(record.id, "process", exc)
        return moved

    async def _process(self, record: Record) -> bool:
        await self.env.prepare(record.id)
        lease = await self.store.lease(record.id)
        if lease is not None and lease.valid(self.now()):
            return False  # another executor runs its step
        before = record.revision
        record = await self._apply_inbox(record)
        record = await self._end_stranded(record)
        record = await self._resolve(record)
        record = await self._carry_out(record)
        moved = record.revision != before
        if not self.stopping and is_runnable(record, None, self.now()):
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
            self.env.notify()
        return saved

    # ---- 1. inbox ---------------------------------------------------------------------------
    async def _apply_inbox(self, record: Record) -> Record:
        for item in await self.store.read_inbox(record.id, record.inbox_applied):
            message = item["message"]
            n = item["n"]
            problem = message_problem(message, record)
            if problem is None and record.ended:
                problem = "arrived as it ended"
            if problem is not None:  # accepted, never received: accounted as undelivered
                log.warning("%s: message %s undelivered: %s", record.id, item.get("id"), problem)
                record = await self._control(
                    record,
                    lambda r, n=n, m=message, p=problem: undeliver(r, _payload(m), p, n),
                    "undelivered",
                    message_id=item.get("id"),
                    reason=problem,
                )
                continue

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
                elif message["kind"] == "repin":
                    r = r.but(definition=message["definition"], bindings=message["bindings"])
                else:
                    r = apply_signal(r, message["signal"])
                return r.but(inbox_applied=n)

            kind = message["kind"] if message["kind"] in ("answer", "repin") else "signal"
            detail: dict[str, Any] = {}
            if kind == "answer":
                detail = {"question_id": message["question_id"]}
            elif kind == "signal":
                detail = {"signal": signal_name(message["signal"])}
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
                    children[name] = await self._child_view(child, record)
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

    async def _end_stranded(self, record: Record) -> Record:
        """An agent whose cleanup (`on_term`) faulted after its parent ended has no one left to
        CONT or end it: it ends as terminated, its error kept, instead of waiting forever."""
        if not (record.terminating and record.parent and record.reason("fault")):
            return record
        parent = await self.store.find(record.parent)
        if parent is not None and not parent.ended:
            return record  # its parent decides
        log.warning("%s: cleanup faulted after its parent ended; ending it", record.id)
        return await self.store.kill(record.id)

    async def _child_view(self, child: Record, parent: Record) -> ChildView:
        """A faulted child that is being dealt with is not faulted for a join: a CONT, TERM or
        KILL already in its inbox, or still a pending intent of the parent (a parent that CONTs
        a child and waits again, in one step or two, is not woken by the same fault)."""
        view = ChildView.of(child)
        if view.fault is None:
            return view
        for intent in parent.pending_intents:
            if (
                intent["kind"] == "signal"
                and intent.get("target") == child.id
                and signal_name(intent["signal"]) in ("CONT", "TERM", "KILL")
            ):
                return ChildView(child.status)
        for item in await self.store.read_inbox(child.id, child.inbox_applied):
            message = item["message"]
            if (
                isinstance(message, dict)
                and message.get("kind") == "signal"
                and signal_name(message.get("signal")) in ("CONT", "TERM", "KILL")
            ):
                return ChildView(child.status)
        return view

    # ---- 3. intents -------------------------------------------------------------------------
    async def _carry_out(self, record: Record) -> Record:
        for intent in record.pending_intents:
            fault = record.reason("fault")
            if fault is not None and fault.get("intent") == intent["id"]:
                continue  # this intent failed; it is tried again after CONT
            if intent["kind"] == "init":  # a root's workspace (a crash after its creation)
                try:
                    await self.env.initialize(record, intent)
                except StoreError:
                    raise  # transient: the next pass tries again
                except Exception as exc:  # a workspace that cannot be prepared: the root's fault
                    error = f"cannot prepare the workspace: {type(exc).__name__}: {exc}"
                    return await self._control(
                        record,
                        lambda r, e=error, i=intent["id"]: _spawn_fault(r, e, i),
                        "fault",
                        intent=intent["id"],
                    )
            elif intent["kind"] == "spawn":
                try:
                    child = await self.env.create_child(record, intent)
                except Exception as exc:  # the parent's problem, not the executor's
                    error = f"cannot spawn {intent['name']} ({intent['agent']}): {exc}"
                    return await self._control(
                        record,
                        lambda r, e=error, i=intent["id"]: _spawn_fault(r, e, i),
                        "fault",
                        intent=intent["id"],
                    )
                if await self.store.create(child):
                    await self.env.child_created(child)
            else:
                target = await self.store.find(intent["target"])
                if target is not None and signal_name(intent["signal"]) == "KILL":
                    await self.store.kill(target.id)
                    killed = getattr(self.env, "killed", None)  # every executor of the runtime
                    if killed is not None:
                        killed(target.id)
                    else:
                        self.cancel(target.id, "killed")
                elif target is not None:
                    with contextlib.suppress(Ended):  # a signal to an ended child is a no-op
                        await self.store.append_inbox(
                            target.id, intent["id"], {"kind": "signal", "signal": intent["signal"]}
                        )
            detail: dict[str, Any] = {"intent": intent["id"]}
            if intent["kind"] == "signal" and target is None:
                detail["outcome"] = f"no agent {intent['target']}: the signal was not delivered"
                log.warning("%s: %s", record.id, detail["outcome"])
            record = await self._control(
                record, lambda r, i=intent["id"]: mark_intent_done(r, i), "intent", **detail
            )
        return record

    # ---- 4. steps ---------------------------------------------------------------------------
    def _start(self, record: Record) -> None:
        task = asyncio.create_task(self._step(record.id))
        self.running[record.id] = task
        task.add_done_callback(lambda _t, i=record.id: self._done(i))

    def _done(self, agent_id: str) -> None:
        self.running.pop(agent_id, None)
        self.env.notify()

    async def _step(self, agent_id: str) -> None:
        async with self.slots:
            if self.stopping:
                return
            lease = await self.store.try_acquire(agent_id, self.owner, self.lease_ttl, self.now())
            if lease is None:
                return
            self.peak = max(self.peak, self.concurrency - self.slots._value)
            try:
                record = await self.store.load(agent_id)
                unread = await self.store.read_inbox(agent_id, record.inbox_applied)
                if unread or not is_runnable(record, None, self.now()):
                    # a STOP, TERM or answer that arrived while it waited for a slot is applied
                    # before any step runs: the next pass applies the inbox first
                    await self.store.release(agent_id, lease)
                    if unread:
                        self.env.notify()
                    return
                await self._run_and_commit(record, lease)
            finally:
                self._work.pop(agent_id, None)

    async def _run_and_commit(self, record: Record, lease: Lease) -> None:
        agent_id = record.id
        self._cancelled.discard(agent_id)
        committed = False
        heart: asyncio.Task[None] | None = None
        try:
            outcome: Transition | Fault
            try:
                await self.env.prepare(agent_id)
                root = agent_id.split("/")[0]
                root_input = (await self.store.load(root)).input if root != agent_id else {}
                work = asyncio.create_task(self.env.run_step(record, lease, root_input))
                self._work[agent_id] = (work, lease)
                heart = asyncio.create_task(self._heartbeat(agent_id, lease, work))
                outcome = await work
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():  # this executor is being cancelled
                    raise
                if agent_id in self._cancelled:  # KILL, lease loss or stop: nothing committed
                    log.warning("%s: step %d cancelled; nothing committed", agent_id, record.step)
                    return
                outcome = Fault("the state was cancelled from inside (CancelledError)")
            except StoreError:
                raise  # the store, not the agent: the step runs again later
            except Exception as exc:  # the runtime failed preparing or running the step
                outcome = Fault(f"the step could not run: {type(exc).__name__}: {exc}")
            current = await self.store.lease(agent_id)
            lease = current if current and current.epoch == lease.epoch else lease
            try:
                await self.store.commit_step(agent_id, lease, record.step, outcome, self.now())
                committed = True
            except Unread:
                log.info("%s: messages arrived as it ended; running the state again", agent_id)
                self.env.notify()
            except (Fenced, Conflict, StoreError):
                raise
            except Exception as exc:  # the outcome cannot be stored (invalid transition, data)
                kind = (
                    "invalid transition"
                    if isinstance(exc, InvalidTransition)
                    else (f"the step's result could not be stored ({type(exc).__name__})")
                )
                await self.store.commit_step(
                    agent_id, lease, record.step, Fault(f"{kind}: {exc}"), self.now()
                )
                committed = True
        except (Fenced, Conflict) as exc:
            log.warning("%s: step %d not committed: %s", agent_id, record.step, exc)
        except StoreError as exc:
            self._failed(agent_id, "step", exc)
        finally:
            if heart is not None:
                heart.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heart
            if not committed:  # never leave the lease to expire (stop, cancel, store errors)
                with contextlib.suppress(Exception):
                    await asyncio.shield(self.store.release(agent_id, lease))
            self._cancelled.discard(agent_id)

    async def _heartbeat(self, agent_id: str, lease: Lease, work: asyncio.Task[Any]) -> None:
        """Renew the lease; when renewal fails the step has lost it: cancel the step (it could
        not commit anyway, and must stop calling effects)."""
        while True:
            await asyncio.sleep(self.beat)
            try:
                renewed = await self.store.renew(agent_id, lease, self.lease_ttl, self.now())
            except StoreError as exc:  # busy: try again until the lease would have expired
                if lease.valid(self.now()):
                    log.warning("%s: lease renewal failed (%s); trying again", agent_id, exc)
                    continue
                renewed = None
            if renewed is None:
                log.warning("%s: lease lost; cancelling the step", agent_id)
                self._cancelled.add(agent_id)
                work.cancel()
                return
            lease = renewed
            if agent_id in self._work:
                self._work[agent_id] = (work, lease)


def message_problem(message: Any, record: Record) -> str | None:
    """Why an inbox message cannot be applied, or None."""
    if not isinstance(message, dict) or message.get("kind") not in ("signal", "answer", "repin"):
        return f"not a message: {str(message)[:200]}"
    if message["kind"] == "signal":
        return signal_problem(message.get("signal"))
    if message["kind"] == "answer":
        try:
            apply_answer(
                record, message["question_id"], message["answer"], by=message.get("by", "person")
            )
        except (InvalidAnswer, KeyError) as exc:
            return f"answer not applied: {exc}"
    if message["kind"] == "repin" and not (
        isinstance(message.get("definition"), dict) and isinstance(message.get("bindings"), dict)
    ):
        return "repin without a definition and bindings"
    return None


def _spawn_fault(record: Record, error: str, intent_id: str) -> Record:
    faulted = apply_fault(record, error) if not record.ended else record
    return faulted.but(
        reasons=[{**r, "intent": intent_id} if r["kind"] == "fault" else r for r in faulted.reasons]
    )


def signal_name(signal: Any) -> str:
    return signal[0] if isinstance(signal, (list, tuple)) else signal


__all__ = ["Environment", "Executor", "message_problem", "signal_name"]
