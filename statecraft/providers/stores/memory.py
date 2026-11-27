"""An in-memory state store for tests: every transaction works on a copy and swaps it in."""

from __future__ import annotations

import copy
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from ...kernel.process import Lease, Record
from ...kernel.store import COMPACTED, StateStore
from ...kernel.transitions import has_work


class _Tables:
    def __init__(self, state: dict[str, Any]) -> None:
        self.s = state

    def get(self, agent_id: str) -> Record | None:
        raw = self.s["agents"].get(agent_id)
        return Record.from_dict(copy.deepcopy(raw)) if raw is not None else None

    def put(self, record: Record) -> None:
        self.s["agents"][record.id] = copy.deepcopy(record.to_dict())

    def get_lease(self, agent_id: str) -> tuple[Lease | None, int]:
        lease, epoch = self.s["leases"].get(agent_id, (None, 0))
        return lease, epoch

    def put_lease(self, agent_id: str, lease: Lease | None, epoch: int) -> None:
        self.s["leases"][agent_id] = (lease, epoch)

    def inbox_append(self, agent_id: str, message_id: str, message: Any) -> bool:
        box = self.s["inbox"].setdefault(agent_id, [])
        if any(m["id"] == message_id for m in box):
            return False
        box.append({"n": len(box) + 1, "id": message_id, "message": copy.deepcopy(message)})
        return True

    def inbox_read(self, agent_id: str, after: int) -> list[dict[str, Any]]:
        return copy.deepcopy([m for m in self.s["inbox"].get(agent_id, []) if m["n"] > after])

    def event_append(self, root: str, event_id: str, event: dict[str, Any]) -> int:
        log = self.s["events"].setdefault(root, [])
        for e in log:
            if e["id"] == event_id:
                return e["seq"]
        log.append({**copy.deepcopy(event), "seq": len(log) + 1})
        return len(log)

    def event_read(self, root: str, after: int) -> list[dict[str, Any]]:
        return copy.deepcopy([e for e in self.s["events"].get(root, []) if e["seq"] > after])

    def history_append(self, agent_id: str, revision: int, entry: dict[str, Any]) -> None:
        self.s["history"].setdefault(agent_id, []).append(
            {"revision": revision, **copy.deepcopy(entry)}
        )

    def history_read(self, agent_id: str) -> list[dict[str, Any]]:
        return copy.deepcopy(self.s["history"].get(agent_id, []))

    def history_trim(self, agent_id: str, revision: int, entry: dict[str, Any]) -> int:
        entries = self.s["history"].get(agent_id, [])
        self.s["history"][agent_id] = [{"revision": revision, **copy.deepcopy(entry)}]
        return len(entries) - 1

    def event_drop(self, root: str, seqs: list[int]) -> None:
        gone = set(seqs)
        self.s["events"][root] = [e for e in self.s["events"].get(root, []) if e["seq"] not in gone]

    def inbox_compact(self, agent_id: str, upto: int) -> int:
        n = 0
        for m in self.s["inbox"].get(agent_id, []):
            if m["n"] <= upto and m["message"] != COMPACTED:
                m["message"] = dict(COMPACTED)
                n += 1
        return n

    def forget(self, agent_ids: list[str], root: str) -> None:
        for table in ("agents", "leases", "inbox", "history"):
            for agent_id in agent_ids:
                self.s[table].pop(agent_id, None)
        gone = set(agent_ids)
        self.s["calls"] = {k: e for k, e in self.s["calls"].items() if e["agent"] not in gone}
        self.s["events"].pop(root, None)

    def setting_get(self, name: str) -> Any:
        return copy.deepcopy(self.s["settings"].get(name))

    def setting_put(self, name: str, value: Any) -> None:
        self.s["settings"][name] = copy.deepcopy(value)

    def tree_ids(self, root: str) -> list[str]:
        return sorted(i for i in self.s["agents"] if i == root or i.startswith(root + "/"))

    def root_ids(self) -> list[str]:
        return sorted(i for i in self.s["agents"] if "/" not in i)

    def call_get(self, key: str) -> dict[str, Any] | None:
        entry = self.s["calls"].get(key)
        return copy.deepcopy(entry) if entry is not None else None

    def call_put(self, key: str, agent_id: str, entry: dict[str, Any]) -> None:
        self.s["calls"][key] = {**copy.deepcopy(entry), "agent": agent_id}

    def calls_of(self, agent_id: str) -> list[dict[str, Any]]:
        return copy.deepcopy([e for e in self.s["calls"].values() if e["agent"] == agent_id])

    def work_ids(self) -> list[str]:
        return sorted(
            i
            for i, raw in self.s["agents"].items()
            if has_work(Record.from_dict(copy.deepcopy(raw)))
        )


class MemoryStore(StateStore):
    def __init__(self) -> None:
        self._state: dict[str, Any] = {
            "agents": {},
            "leases": {},
            "inbox": {},
            "events": {},
            "history": {},
            "calls": {},
            "settings": {},
        }
        self._lock = threading.RLock()

    @contextmanager
    def transaction(self, write: bool = True) -> Iterator[_Tables]:
        with self._lock:
            working = copy.deepcopy(self._state)
            yield _Tables(working)
            self._state = working
