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

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, Protocol

from .process import Fault, Lease, Record, Transition
from .transitions import apply_outcome, apply_signal


class StoreError(RuntimeError):
    pass


class Fenced(StoreError):
    """The caller's lease is not current, or has expired: its commit is refused."""


class Conflict(StoreError):
    """The record moved on (step or revision) since the caller read it."""


class Busy(StoreError):
    """Another executor holds a valid lease on the record."""


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


def root_of(agent_id: str) -> str:
    return agent_id.split("/", 1)[0]


class StateStore:
    """Fencing and atomicity on top of an adapter's `transaction()`."""

    def transaction(self) -> AbstractContextManager[Tables]:
        raise NotImplementedError

    # ---- reads -------------------------------------------------------------------------------

    async def load(self, agent_id: str) -> Record:
        with self.transaction() as t:
            record = t.get(agent_id)
        if record is None:
            raise KeyError(agent_id)
        return record

    async def find(self, agent_id: str) -> Record | None:
        with self.transaction() as t:
            return t.get(agent_id)

    async def lease(self, agent_id: str) -> Lease | None:
        with self.transaction() as t:
            return t.get_lease(agent_id)[0]

    async def scan_work(self) -> list[Record]:
        """Every record with work left: active, or ended with intents pending."""
        with self.transaction() as t:
            return [r for r in (t.get(i) for i in t.work_ids()) if r is not None]

    async def roots(self) -> list[Record]:
        """Every root (a run), in id order."""
        with self.transaction() as t:
            return [r for r in (t.get(i) for i in t.root_ids()) if r is not None]

    async def tree(self, root: str) -> list[Record]:
        """The root and every agent under it, in id order."""
        with self.transaction() as t:
            return [r for r in (t.get(i) for i in t.tree_ids(root)) if r is not None]

    async def read_inbox(self, agent_id: str, after: int) -> list[dict[str, Any]]:
        with self.transaction() as t:
            return t.inbox_read(agent_id, after)

    async def read_events(self, root: str, after_seq: int = 0) -> list[dict[str, Any]]:
        with self.transaction() as t:
            return t.event_read(root, after_seq)

    async def history(self, agent_id: str) -> list[dict[str, Any]]:
        with self.transaction() as t:
            return t.history_read(agent_id)

    # ---- writes ------------------------------------------------------------------------------

    def _save(
        self, t: Tables, before: Record | None, after: Record, kind: str, **detail: Any
    ) -> Record:
        """Bump the revision; write the record, its history entry and its durable event in the
        caller's transaction. New publications are stamped with that event's sequence, so views
        can order them by when they were published."""
        after = after.but(revision=(before.revision if before else 0) + 1)
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
        seq = t.event_append(root_of(after.id), event["id"], event)
        if any("seq" not in ref for ref in after.published.values()):
            stamped = {
                k: ref if "seq" in ref else {**ref, "seq": seq}
                for k, ref in after.published.items()
            }
            after = after.but(published=stamped)
        t.put(after)
        t.history_append(after.id, after.revision, {"kind": kind, "record": after.to_dict()})
        return after

    async def create(self, record: Record) -> bool:
        """Create-only: a spawn carried out twice creates one child."""
        with self.transaction() as t:
            if t.get(record.id) is not None:
                return False
            self._save(t, None, record.but(revision=0), "created")
            return True

    async def try_acquire(self, agent_id: str, owner: str, ttl: float, now: float) -> Lease | None:
        with self.transaction() as t:
            record = t.get(agent_id)
            lease, epoch = t.get_lease(agent_id)
            if record is None or record.ended or (lease is not None and lease.valid(now)):
                return None
            new = Lease(owner, epoch + 1, now + ttl)
            t.put_lease(agent_id, new, new.epoch)
            return new

    async def renew(self, agent_id: str, lease: Lease, ttl: float, now: float) -> Lease | None:
        with self.transaction() as t:
            current, epoch = t.get_lease(agent_id)
            if current != lease or not lease.valid(now):
                return None
            new = Lease(lease.owner, lease.epoch, now + ttl)
            t.put_lease(agent_id, new, epoch)
            return new

    async def release(self, agent_id: str, lease: Lease) -> None:
        with self.transaction() as t:
            current, epoch = t.get_lease(agent_id)
            if current is not None and (current.owner, current.epoch) == (lease.owner, lease.epoch):
                t.put_lease(agent_id, None, epoch)

    async def commit_step(
        self,
        agent_id: str,
        lease: Lease,
        expected_step: int,
        outcome: Transition | Fault,
        now: float,
    ) -> Record:
        with self.transaction() as t:
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
            kind = "fault" if isinstance(outcome, Fault) else "step"
            saved = self._save(t, before, after, kind, from_state=before.state)
            t.put_lease(agent_id, None, epoch)
            return saved

    async def commit_control(
        self,
        agent_id: str,
        expected_revision: int,
        change: Callable[[Record], Record],
        now: float,
        kind: str = "control",
        **detail: Any,
    ) -> Record:
        with self.transaction() as t:
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

    async def kill(self, agent_id: str) -> Record:
        with self.transaction() as t:
            before = t.get(agent_id)
            if before is None:
                raise KeyError(agent_id)
            _, epoch = t.get_lease(agent_id)
            t.put_lease(agent_id, None, epoch + 1)
            if before.ended:
                return before
            return self._save(t, before, apply_signal(before, "KILL"), "signal", signal="KILL")

    async def append_inbox(self, agent_id: str, message_id: str, message: Any) -> bool:
        """Append if that id is not there yet; signals and answers are never applied twice."""
        with self.transaction() as t:
            if t.get(agent_id) is None:
                raise KeyError(agent_id)
            return t.inbox_append(agent_id, message_id, message)


__all__ = ["Busy", "Conflict", "Fenced", "StateStore", "StoreError", "Tables", "root_of"]
