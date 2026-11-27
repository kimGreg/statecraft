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
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, Protocol, TypeVar

from .delta import diff, patch
from .process import Fault, Lease, Record, Transition
from .transitions import apply_outcome, apply_signal, has_work, undeliver

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


class FormatError(StoreError):
    """A store whose data format this statecraft does not read: newer (upgrade statecraft), or
    older (migrate it first: `statecraft store migrate`)."""


class LimitReached(Exception):
    """A step's spawns would take its tree past a limit (`max_total_agents`, `max_live_agents`),
    counted in the commit's own transaction: the step faults instead of committing them."""


class OverBudget(Exception):
    """A metered call would take its tree past its budget: refused before it is sent."""


class CallRefused(Exception):
    """A keyed call refused before anything was sent: its identity names another request or
    another agent, or the agent has an unresolved call to that effect."""


#: The states of a journaled call. `sent`: sent at least once, no outcome yet. `completed` and
#: `failed`: settled. `indeterminate`: it may have been applied and its outcome is unknown;
#: `clear_by` says how it may be settled: `resend` (the provider applies a key once, so sending
#: the same key again is its reconciliation), `reconcile` (the provider can tell the outcome) or
#: `operator` (only a person, through `resolve_call`). `replay`: a person allowed one new send.
CALL_STATES = ("sent", "completed", "failed", "indeterminate", "replay")


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
    def call_get(self, key: str) -> dict[str, Any] | None: ...
    def call_put(self, key: str, agent_id: str, entry: dict[str, Any]) -> None: ...
    def calls_of(self, agent_id: str) -> list[dict[str, Any]]: ...
    # storage upkeep (C2)
    def history_trim(self, agent_id: str, revision: int, entry: dict[str, Any]) -> int: ...
    def event_drop(self, root: str, seqs: list[int]) -> None: ...
    def inbox_compact(self, agent_id: str, upto: int) -> int: ...
    def forget(self, agent_ids: list[str], root: str) -> None: ...
    # store-wide settings that every executor reads (maintenance mode)
    def setting_get(self, name: str) -> Any: ...
    def setting_put(self, name: str, value: Any) -> None: ...


#: History keeps a full record every CHECKPOINT revisions and a delta from the previous revision
#: in between (`delta.py`); reading reconstructs every entry's record.
CHECKPOINT = 32
#: What an applied inbox message is compacted to: its id and position stay (a message sent again
#: with the same id is still refused), its body goes.
COMPACTED = {"kind": "compacted"}


def expand(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """History entries with each entry's full record, from checkpoints and deltas."""
    record: Any = None
    found = []
    for entry in entries:
        if "record" in entry:
            record = entry["record"]
        else:
            entry = dict(entry)
            record = patch(record, entry.pop("delta"))
            entry["record"] = record
        found.append(entry)
    return found


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
        return expand(await self._txn(lambda t: t.history_read(agent_id), write=False))

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
        if after.parent and _child_news(before, after):
            # CHLD: the parent learns that a child faulted or ended (its wait is still decided
            # from the child's record; the notification is for whoever follows the parent)
            news = {
                "id": f"{after.id}@{after.revision}/CHLD",
                "kind": "CHLD",
                "agent": after.parent,
                "child": after.id,
                "status": after.status,
                "fault": after.reason("fault") is not None,
                "revision": after.revision,
            }
            t.event_append(root_of(after.id), news["id"], jsonable(news))
        if any("seq" not in ref for ref in after.published.values()):
            stamped = {
                k: ref if "seq" in ref else {**ref, "seq": seq}
                for k, ref in after.published.items()
            }
            after = after.but(published=stamped)
        t.put(after)
        entry: dict[str, Any] = {"kind": kind}
        if before is None or after.revision % CHECKPOINT == 1:
            entry["record"] = after.to_dict()
        else:  # `before` is what the previous entry recorded: every record write goes through here
            entry["delta"] = diff(before.to_dict(), after.to_dict())
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
        limits: dict[str, int] | None = None,
    ) -> Record:
        """`limits`: tree limits checked against the tree as it is in this transaction, its
        children not created yet included (a spawn reserves its place when it is committed)."""

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
            if limits and isinstance(outcome, Transition) and outcome.spawns:
                _check_tree_limits(t, after, limits)
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

    # ---- the call journal: identities of keyed calls, their digests and outcomes ------------
    async def begin_call(
        self, agent_id: str, key: str, effect: str, digest: str, now: float
    ) -> tuple[dict[str, Any], bool]:
        """Record a keyed call before it is sent, or return its existing entry; and whether it
        was created now (never sent before). Refused
        (`CallRefused`) when the identity names a different request or another agent, or when
        the agent has an unresolved (`indeterminate`) call to the same effect under another
        identity: a new identity is never a way around an unknown outcome."""

        def body(t: Tables) -> tuple[dict[str, Any], bool]:
            existing = t.call_get(key)
            if existing is not None:
                if existing["agent"] != agent_id:
                    raise CallRefused(f"call {key} belongs to {existing['agent']}")
                if existing["digest"] != digest:
                    raise CallRefused(
                        f"call {key} was made with a different request; an identity names one "
                        "request (effect, operation, arguments, workspace and scope)"
                    )
            for other in t.calls_of(agent_id):
                if (
                    other["key"] != key
                    and other["effect"] == effect
                    and other["status"] == "indeterminate"
                ):
                    raise CallRefused(
                        f"an earlier call to {effect} ({other['key']}) has an unknown outcome; "
                        "it must be settled first (CONT reconciles it, or an operator resolves it)"
                    )
            if existing is not None:
                return existing, False
            entry = {
                "key": key,
                "agent": agent_id,
                "effect": effect,
                "digest": digest,
                "status": "sent",
                "since": now,
            }
            t.call_put(key, agent_id, entry)
            return entry, True

        return await self._txn(body)

    async def end_call(
        self, key: str, status: str, *, expected: str | None = None, **detail: Any
    ) -> dict[str, Any]:
        """Settle or mark a journaled call (`completed`, `failed`, `indeterminate` with
        `clear_by`, or back to `sent` when a replay starts). With `expected`, only if the call
        is in that state now (`CallRefused` otherwise): an operator's decision never overwrites
        what a running step settled, nor the other way round."""
        if status not in CALL_STATES:
            raise ValueError(f"call state must be one of {CALL_STATES}, not {status!r}")
        detail = jsonable(detail)

        def body(t: Tables) -> dict[str, Any]:
            entry = t.call_get(key)
            if entry is None:
                raise KeyError(key)
            if expected is not None and entry["status"] != expected:
                raise CallRefused(f"call {key} is {entry['status']}, not {expected}")
            entry = {k: v for k, v in entry.items() if k not in ("clear_by", "error", "result")}
            entry.update(detail, status=status)
            t.call_put(key, entry["agent"], entry)
            return entry

        return await self._txn(body)

    async def call(self, key: str) -> dict[str, Any] | None:
        return await self._txn(lambda t: t.call_get(key), write=False)

    async def calls(self, agent_id: str) -> list[dict[str, Any]]:
        """The journaled calls of an agent, oldest first."""
        return await self._txn(lambda t: t.calls_of(agent_id), write=False)

    async def record_attempt(
        self, agent_id: str, step: int, epoch: int, entry: dict[str, Any]
    ) -> int:
        """Keep what an attempt that did not commit did (why, and its effect calls) as an
        `attempt` event in the run's durable log: recovery tooling reads it. Once per attempt."""
        event_id = f"{agent_id}/attempt/{step}/{epoch}"
        event = jsonable(
            {"id": event_id, "kind": "attempt", "agent": agent_id, "step": step, "epoch": epoch}
            | entry
        )
        return await self._txn(lambda t: t.event_append(root_of(agent_id), event_id, event))

    async def audit(self, root: str, entry: dict[str, Any]) -> int:
        """Record who did what to a run (a control operation, an operator's decision)
        in the run's durable event log, as an `audit` event."""
        event_id = f"audit/{uuid.uuid4().hex}"
        event = jsonable({"id": event_id, "kind": "audit", **entry})
        return await self._txn(lambda t: t.event_append(root, event_id, event))

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

    # ---- store-wide settings ------------------------------------------------------------------
    async def setting(self, name: str) -> Any:
        """A store-wide setting (None when unset), seen by every process on this store."""
        return await self._txn(lambda t: t.setting_get(name), write=False)

    async def set_setting(self, name: str, value: Any) -> None:
        value = jsonable(value)
        await self._txn(lambda t: t.setting_put(name, value))

    # ---- tree budgets (E3): reservations before metered calls, settled with what they used ----
    # A budget is limits per unit ({"usd": 2.0, "units": 100000}); `spent` and `held` count
    # every unit that calls report. The units are the extensions': the kernel only adds.
    async def budget(self, root: str) -> dict[str, Any] | None:
        return await self._txn(lambda t: t.setting_get(_budget_name(root)), write=False)

    async def set_budget(self, root: str, limit: dict[str, float] | None) -> dict[str, Any]:
        """Set (or, with None, remove) a tree's limits; what was spent and held is kept."""
        limit = jsonable(limit)

        def body(t: Tables) -> dict[str, Any]:
            found = t.setting_get(_budget_name(root)) or {"spent": {}, "held": {}}
            found["limit"] = dict(limit or {})
            t.setting_put(_budget_name(root), found)
            return found

        return await self._txn(body)

    async def reserve(
        self, root: str, key: str, amounts: dict[str, float], *, agent: str, epoch: int
    ) -> bool:
        """Hold `amounts` for one send of a metered call, or refuse (`OverBudget`) when what was
        spent, what is held and this would pass a limit. False when the tree has no budget.
        Holds of the agent's earlier attempts (lower epochs: those attempts are gone, and their
        calls may have been charged) are counted as spent first. Holding a key twice holds once."""
        amounts = jsonable(amounts)

        def body(t: Tables) -> bool:
            found = t.setting_get(_budget_name(root))
            if found is None:
                return False
            spent, held = found["spent"], found["held"]
            for other, hold in list(held.items()):
                if hold["agent"] == agent and hold["epoch"] < epoch:
                    _add(spent, hold["amounts"])
                    del held[other]
            if key not in held:
                for unit, most in (found.get("limit") or {}).items():
                    total = (
                        spent.get(unit, 0)
                        + sum(h["amounts"].get(unit, 0) for h in held.values())
                        + amounts.get(unit, 0)
                    )
                    if total > most:
                        raise OverBudget(
                            f"budget {unit}={most}: this call would bring {root} to {total:g}"
                        )
                held[key] = {"agent": agent, "epoch": epoch, "amounts": amounts}
            t.setting_put(_budget_name(root), found)
            return True

        return await self._txn(body)

    async def settle(self, root: str, key: str, used: dict[str, float] | None) -> None:
        """End a hold: `used` is what the call reported (or its hold, when it may have been sent
        and reported nothing); None releases it unspent (the call was not sent)."""
        used = jsonable(used)

        def body(t: Tables) -> None:
            found = t.setting_get(_budget_name(root))
            if found is None or key not in found["held"]:
                return
            hold = found["held"].pop(key)
            if used is not None:
                _add(found["spent"], used or hold["amounts"])
            t.setting_put(_budget_name(root), found)

        await self._txn(body)

    # ---- storage upkeep (C2): compacting and forgetting ended runs -----------------------------
    async def compact(self, root: str, *, dry_run: bool = False) -> dict[str, int]:
        """Make a run smaller without changing what it is. Always: applied inbox messages keep
        only their id. Once every agent of the run has ended with no work left: each agent's
        history becomes its last entry (a full record), and its events become the last event of
        each agent and every `audit` event. Returns what was (or, dry, would be) removed."""

        def body(t: Tables) -> dict[str, int]:
            ids = t.tree_ids(root)
            if not ids:
                raise KeyError(root)
            records = [r for r in (t.get(i) for i in ids) if r is not None]
            done = {"inbox": 0, "history": 0, "events": 0}
            for record in records:
                applied = [
                    m
                    for m in t.inbox_read(record.id, 0)
                    if m["n"] <= record.inbox_applied and m["message"] != COMPACTED
                ]
                done["inbox"] += len(applied)
                if applied and not dry_run:
                    t.inbox_compact(record.id, record.inbox_applied)
            if not all(r.ended and not has_work(r) for r in records):
                return done
            for record in records:
                entries = expand(t.history_read(record.id))
                if len(entries) > 1:
                    done["history"] += len(entries) - 1
                    if not dry_run:
                        last = entries[-1]
                        t.history_trim(record.id, last["revision"], _without_revision(last))
            events = t.event_read(root, 0)
            last_of = {e.get("agent"): e["seq"] for e in events if e.get("kind") != "audit"}
            keep = set(last_of.values()) | {e["seq"] for e in events if e.get("kind") == "audit"}
            dropped = [e["seq"] for e in events if e["seq"] not in keep]
            done["events"] = len(dropped)
            if dropped and not dry_run:
                t.event_drop(root, dropped)
            return done

        return await self._txn(body, write=not dry_run)

    async def forget(self, root: str, *, dry_run: bool = False) -> dict[str, int]:
        """Delete a run whose agents have all ended with no work left: records, leases, inbox,
        history, calls and events. Refused (`Busy`) for a run still at work, and for a run another
        run inherits from or works attached to (it still reads it)."""

        def body(t: Tables) -> dict[str, int]:
            ids = t.tree_ids(root)
            if not ids:
                raise KeyError(root)
            records = [r for r in (t.get(i) for i in ids) if r is not None]
            if not all(r.ended and not has_work(r) for r in records):
                raise Busy(f"{root} is still at work; it can be forgotten once it has ended")
            for other_id in t.root_ids():
                other = t.get(other_id)
                if other is None or other_id == root:
                    continue
                if other.workspace == root or (other.inherits or {}).get("root") == root:
                    raise Busy(f"{other_id} reads {root}; forget {other_id} first")
            if not dry_run:
                t.forget(ids, root)
                if t.setting_get(_budget_name(root)) is not None:
                    t.setting_put(_budget_name(root), None)
            return {"agents": len(ids)}

        return await self._txn(body, write=not dry_run)


def _budget_name(root: str) -> str:
    return f"budget/{root}"


def _add(into: dict[str, float], amounts: dict[str, float]) -> None:
    for unit, n in amounts.items():
        if isinstance(n, int | float) and not isinstance(n, bool):
            into[unit] = into.get(unit, 0) + n


def _check_tree_limits(t: Tables, after: Record, limits: dict[str, int]) -> None:
    records = {r.id: r for r in (t.get(i) for i in t.tree_ids(root_of(after.id))) if r}
    records[after.id] = after
    pending = {
        i["id"]
        for r in records.values()
        for i in r.intents
        if i["kind"] == "spawn" and not i["done"] and i["id"] not in records
    }
    total = len(records) + len(pending)
    most = limits.get("max_total_agents")
    if most is not None and total > most:
        raise LimitReached(f"limit max_total_agents={most}: the tree would have {total}")
    live = sum(1 for r in records.values() if not r.ended) + len(pending)
    most = limits.get("max_live_agents")
    if most is not None and live > most:
        raise LimitReached(f"limit max_live_agents={most}: {live} agents would be live")


def _child_news(before: Record | None, after: Record) -> bool:
    """A child ended, or faulted, in this change."""
    if before is None:
        return False
    ended = after.ended and not before.ended
    faulted = after.reason("fault") is not None and before.reason("fault") is None
    return ended or faulted


def _without_revision(entry: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in entry.items() if k != "revision"}


__all__ = [
    "CALL_STATES",
    "CHECKPOINT",
    "COMPACTED",
    "Busy",
    "CallRefused",
    "Conflict",
    "Ended",
    "Fenced",
    "FormatError",
    "LimitReached",
    "NotJSON",
    "OverBudget",
    "StateStore",
    "StoreError",
    "Tables",
    "Unavailable",
    "Unread",
    "expand",
    "jsonable",
    "root_of",
]
