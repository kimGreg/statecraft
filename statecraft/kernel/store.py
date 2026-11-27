"""The state store: agent records, leases, inboxes, history and the durable event log.

`StateStore` holds every rule about fencing and atomicity once. An adapter supplies only a
transaction over its tables (`Tables`); the memory and SQLite stores pass the same contract tests.

What each operation checks:
    commit_step     a current, unexpired lease (owner, epoch) and `expected_step`; it applies a
                    transition or a fault and releases the lease, in one transaction
    commit_control  `expected_revision`, and no other executor's valid lease; the change is a pure
                    rule from `transitions` and raises when it is not valid for the record
    renew, release  owner and epoch (`renew` also not expired)
    kill            nothing: ends the agent and bumps the epoch, so a running step cannot commit
Every change bumps `revision` and writes its history entry and durable event in the same
transaction. Times are passed in, so the store never reads a clock of its own.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, Protocol, TypeVar

from .process import Fault, Lease, Record, Transition
from .transitions import apply_outcome, apply_signal, undeliver

T = TypeVar("T")


class StoreError(RuntimeError):
    pass


class Fenced(StoreError):
    """The caller's lease is not current, or has expired: its commit is refused."""


class Conflict(StoreError):
    """The record moved on (step or revision) since the caller read it."""


class Busy(StoreError):
    """Another executor holds a valid lease on the record."""


class Ended(StoreError):
    """The agent has ended: it takes no more signals or answers."""


class Unavailable(StoreError):
    """The store could not be reached or stayed locked: nothing was done; try again."""


class NotJSON(ValueError):
    """A record or message holds a value that is not JSON (a set, bytes, an object): stores
    keep JSON only, the same in every store. A step whose data is not JSON faults."""


def jsonable(value: Any) -> Any:
    """The value as every store keeps it: through JSON (tuples become lists, keys strings)."""
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise NotJSON(f"not JSON: {exc}") from exc


class Unread(StoreError):
    """A step would end the agent while messages it has not read are waiting: not committed.
    The executor releases the lease; the next pass applies the messages and runs the state
    again, so the agent sees them before it ends."""


class Tables(Protocol):
    """One transaction's view of the adapter's tables."""

    def get(self, agent_id: str) -> Record | None: ...
    def put(self, record: Record) -> None: ...
    def get_lease(self, agent_id: str) -> tuple[Lease | None, int]: ...  # (lease, last epoch)
    def put_lease(self, agent_id: str, lease: Lease | None, epoch: int) -> None: ...
    def inbox_append(self, agent_id: str, message_id: str, message: Any) -> bool: ...
    def inbox_read(self, agent_id: str, after: int) -> list[dict[str, Any]]: ...
    def event_append(self, root: str, event_id: str, event: dict[str, Any]) -> int: ...
    def event_read(self, root: str, after: int) -> list[dict[str, Any]]: ...
    def history_append(self, agent_id: str, revision: int, entry: dict[str, Any]) -> None: ...
    def history_read(self, agent_id: str) -> list[dict[str, Any]]: ...
    def work_ids(self) -> list[str]: ...
    def tree_ids(self, root: str) -> list[str]: ...
    def root_ids(self) -> list[str]: ...


def _payload(message: Any) -> Any:
    """What a message carries: a USR signal's message, else the message itself."""
    if isinstance(message, dict) and message.get("kind") == "signal":
        signal = message.get("signal")
        if isinstance(signal, list) and len(signal) == 2 and signal[0] == "USR":
            return signal[1]
    return message


def root_of(agent_id: str) -> str:
    return agent_id.split("/", 1)[0]


class StateStore:
    """Fencing and atomicity on top of an adapter's `transaction()`."""

    def transaction(self, write: bool = True) -> AbstractContextManager[Tables]:
        raise NotImplementedError

    # ---- running a transaction ---------------------------------------------------------------

    async def _txn(self, body: Callable[[Tables], T], *, write: bool = True) -> T:
        """Run `body` in one transaction. Where it runs is the adapter's: inline (memory), or on
        the thread that owns the store's connection (SQLite), so the event loop never waits on a
        lock. `write=False` is a read: it may see a snapshot and never blocks writers."""
        return await self._run(lambda: self._in_transaction(body, write))

    def _in_transaction(self, body: Callable[[Tables], T], write: bool) -> T:
        with self.transaction(write=write) as t:
            return body(t)

    async def _run(self, fn: Callable[[], T]) -> T:
        return fn()

    # ---- reads -------------------------------------------------------------------------------

    async def load(self, agent_id: str) -> Record:
        record = await self.find(agent_id)
        if record is None:
            raise KeyError(agent_id)
        return record

    async def find(self, agent_id: str) -> Record | None:
        return await self._txn(lambda t: t.get(agent_id), write=False)

    async def lease(self, agent_id: str) -> Lease | None:
        return await self._txn(lambda t: t.get_lease(agent_id)[0], write=False)

    async def scan_work(self) -> list[Record]:
        """Every record with work left: active, or ended with intents pending."""
        return await self._txn(
            lambda t: [r for r in (t.get(i) for i in t.work_ids()) if r is not None], write=False
        )

    async def roots(self) -> list[Record]:
        """Every root (a run), in id order."""
        return await self._txn(
            lambda t: [r for r in (t.get(i) for i in t.root_ids()) if r is not None], write=False
        )

    async def tree(self, root: str) -> list[Record]:
        """The root and every agent under it, in id order."""
        return await self._txn(
            lambda t: [r for r in (t.get(i) for i in t.tree_ids(root)) if r is not None],
            write=False,
        )

    async def read_inbox(self, agent_id: str, after: int) -> list[dict[str, Any]]:
        return await self._txn(lambda t: t.inbox_read(agent_id, after), write=False)

    async def read_events(self, root: str, after_seq: int = 0) -> list[dict[str, Any]]:
        return await self._txn(lambda t: t.event_read(root, after_seq), write=False)

    async def history(self, agent_id: str) -> list[dict[str, Any]]:
        return await self._txn(lambda t: t.history_read(agent_id), write=False)

    # ---- writes ------------------------------------------------------------------------------

    def _save(
        self,
        t: Tables,
        before: Record | None,
        after: Record,
        kind: str,
        crossings: list[dict[str, Any]] | None = None,
        **detail: Any,
    ) -> Record:
        """Bump the revision; write the record, its history entry and its durable event in the
        caller's transaction. New publications are stamped with that event's sequence, so views
        can order them by when they were published."""
        after = Record.from_dict(
            jsonable(after.but(revision=(before.revision if before else 0) + 1).to_dict())
        )
        event = {
            "id": f"{after.id}@{after.revision}",
            "agent": after.id,
            "kind": kind,
            "revision": after.revision,
            "step": after.step,
            "state": after.state,
            "status": after.status,
            "reasons": [r["kind"] for r in after.reasons],
            **detail,
        }
        seq = t.event_append(root_of(after.id), event["id"], jsonable(event))
        if any("seq" not in ref for ref in after.published.values()):
            stamped = {
                k: ref if "seq" in ref else {**ref, "seq": seq}
                for k, ref in after.published.items()
            }
            after = after.but(published=stamped)
        t.put(after)
        entry: dict[str, Any] = {"kind": kind, "record": after.to_dict()}
        if detail:  # why (a rejected message, the signal applied, the intent carried out)
            entry["detail"] = dict(detail)
        if crossings:
            entry["crossings"] = crossings
        t.history_append(after.id, after.revision, jsonable(entry))
        return after

    async def create(self, record: Record) -> bool:
        """Create-only: a spawn carried out twice creates one child."""

        def body(t: Tables) -> bool:
            if t.get(record.id) is not None:
                return False
            self._save(t, None, record.but(revision=0), "created")
            return True

        return await self._txn(body)

    async def try_acquire(self, agent_id: str, owner: str, ttl: float, now: float) -> Lease | None:
        def body(t: Tables) -> Lease | None:
            record = t.get(agent_id)
            lease, epoch = t.get_lease(agent_id)
            if record is None or record.ended or (lease is not None and lease.valid(now)):
                return None
            new = Lease(owner, epoch + 1, now + ttl)
            t.put_lease(agent_id, new, new.epoch)
            return new

        return await self._txn(body)

    async def renew(self, agent_id: str, lease: Lease, ttl: float, now: float) -> Lease | None:
        def body(t: Tables) -> Lease | None:
            current, epoch = t.get_lease(agent_id)
            if current != lease or not lease.valid(now):
                return None
            new = Lease(lease.owner, lease.epoch, now + ttl)
            t.put_lease(agent_id, new, epoch)
            return new

        return await self._txn(body)

    async def release(self, agent_id: str, lease: Lease) -> None:
        def body(t: Tables) -> None:
            current, epoch = t.get_lease(agent_id)
            if current is not None and (current.owner, current.epoch) == (lease.owner, lease.epoch):
                t.put_lease(agent_id, None, epoch)

        await self._txn(body)

    async def commit_step(
        self,
        agent_id: str,
        lease: Lease,
        expected_step: int,
        outcome: Transition | Fault,
        now: float,
    ) -> Record:
        def body(t: Tables) -> Record:
            current, epoch = t.get_lease(agent_id)
            if (
                current is None
                or (current.owner, current.epoch) != (lease.owner, lease.epoch)
                or not current.valid(now)
            ):
                raise Fenced(f"{agent_id}: lease {lease.owner}#{lease.epoch} is not current")
            before = t.get(agent_id)
            if before is None or before.step != expected_step:
                raise Conflict(f"{agent_id}: expected step {expected_step}")
            after = apply_outcome(before, outcome, now=now)
            if after.ended and t.inbox_read(agent_id, before.inbox_applied):
                raise Unread(f"{agent_id}: messages arrived before it could end")
            kind = "fault" if isinstance(outcome, Fault) else "step"
            saved = self._save(
                t, before, after, kind, crossings=list(outcome.crossings), from_state=before.state
            )
            t.put_lease(agent_id, None, epoch)
            return saved

        return await self._txn(body)

    async def commit_control(
        self,
        agent_id: str,
        expected_revision: int,
        change: Callable[[Record], Record],
        now: float,
        kind: str = "control",
        **detail: Any,
    ) -> Record:
        def body(t: Tables) -> Record:
            before = t.get(agent_id)
            if before is None:
                raise KeyError(agent_id)
            if before.revision != expected_revision:
                raise Conflict(f"{agent_id}: expected revision {expected_revision}")
            lease, _ = t.get_lease(agent_id)
            if lease is not None and lease.valid(now):
                raise Busy(f"{agent_id} is running")
            after = change(before)
            if after == before:
                return before
            return self._save(t, before, after, kind, **detail)

        return await self._txn(body)

    async def kill(self, agent_id: str) -> Record:
        def body(t: Tables) -> Record:
            before = t.get(agent_id)
            if before is None:
                raise KeyError(agent_id)
            _, epoch = t.get_lease(agent_id)
            t.put_lease(agent_id, None, epoch + 1)
            if before.ended:
                return before
            after = apply_signal(before, "KILL")
            # messages accepted but not applied yet will never be: account for them
            for item in t.inbox_read(agent_id, before.inbox_applied):
                after = undeliver(after, _payload(item["message"]), "arrived as it was killed")
                after = after.but(inbox_applied=item["n"])
            return self._save(t, before, after, "signal", signal="KILL")

        return await self._txn(body)

    async def append_inbox(
        self,
        agent_id: str,
        message_id: str,
        message: Any,
        check: Callable[[Record], Any] | None = None,
    ) -> bool:
        """Append if that id is not there yet; signals and answers are never applied twice.
        `check(record)` runs in the same transaction and refuses by raising (an answer is
        checked against the question as it is when it is stored, not before)."""
        message = jsonable(message)

        def body(t: Tables) -> bool:
            record = t.get(agent_id)
            if record is None:
                raise KeyError(agent_id)
            if record.ended:
                raise Ended(f"{agent_id} has ended ({record.status}); it takes no more messages")
            if check is not None:
                check(record)
            return t.inbox_append(agent_id, message_id, message)

        return await self._txn(body)


__all__ = [
    "Busy",
    "Conflict",
    "Ended",
    "Fenced",
    "NotJSON",
    "StateStore",
    "StoreError",
    "Tables",
    "Unavailable",
    "Unread",
    "jsonable",
    "root_of",
]
