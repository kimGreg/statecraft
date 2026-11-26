"""The local state store: one SQLite file, one `BEGIN IMMEDIATE` transaction per operation.

Several executor processes may share the file; SQLite serializes the writers and each operation
reads and writes inside its own transaction, so fencing and CAS checks hold across processes.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..process import Lease, Record
from ..store import StateStore
from ..transitions import has_work

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents  (id TEXT PRIMARY KEY, work INTEGER NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS agents_work ON agents(work);
CREATE TABLE IF NOT EXISTS leases  (id TEXT PRIMARY KEY, owner TEXT, epoch INTEGER NOT NULL,
                                    expires REAL);
CREATE TABLE IF NOT EXISTS inbox   (agent TEXT NOT NULL, n INTEGER NOT NULL, id TEXT NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (agent, id));
CREATE TABLE IF NOT EXISTS events  (root TEXT NOT NULL, seq INTEGER NOT NULL, id TEXT NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (root, seq), UNIQUE (root, id));
CREATE TABLE IF NOT EXISTS history (agent TEXT NOT NULL, revision INTEGER NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (agent, revision));
"""


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class _Tables:
    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def _one(self, sql: str, *args: Any) -> Any:
        return self.db.execute(sql, args).fetchone()

    def get(self, agent_id: str) -> Record | None:
        row = self._one("SELECT body FROM agents WHERE id = ?", agent_id)
        return Record.from_dict(json.loads(row[0])) if row else None

    def put(self, record: Record) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO agents (id, work, body) VALUES (?, ?, ?)",
            (record.id, int(has_work(record)), _dump(record.to_dict())),
        )

    def get_lease(self, agent_id: str) -> tuple[Lease | None, int]:
        row = self._one("SELECT owner, epoch, expires FROM leases WHERE id = ?", agent_id)
        if row is None:
            return None, 0
        owner, epoch, expires = row
        return (Lease(owner, epoch, expires) if owner is not None else None), epoch

    def put_lease(self, agent_id: str, lease: Lease | None, epoch: int) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO leases (id, owner, epoch, expires) VALUES (?, ?, ?, ?)",
            (agent_id, lease.owner if lease else None, epoch, lease.expires if lease else None),
        )

    def inbox_append(self, agent_id: str, message_id: str, message: Any) -> bool:
        if self._one("SELECT 1 FROM inbox WHERE agent = ? AND id = ?", agent_id, message_id):
            return False
        n = self._one("SELECT COALESCE(MAX(n), 0) + 1 FROM inbox WHERE agent = ?", agent_id)[0]
        self.db.execute(
            "INSERT INTO inbox (agent, n, id, body) VALUES (?, ?, ?, ?)",
            (agent_id, n, message_id, _dump(message)),
        )
        return True

    def inbox_read(self, agent_id: str, after: int) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT n, id, body FROM inbox WHERE agent = ? AND n > ? ORDER BY n", (agent_id, after)
        ).fetchall()
        return [{"n": n, "id": i, "message": json.loads(b)} for n, i, b in rows]

    def event_append(self, root: str, event_id: str, event: dict[str, Any]) -> int:
        row = self._one("SELECT seq FROM events WHERE root = ? AND id = ?", root, event_id)
        if row:
            return row[0]
        seq = self._one("SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE root = ?", root)[0]
        self.db.execute(
            "INSERT INTO events (root, seq, id, body) VALUES (?, ?, ?, ?)",
            (root, seq, event_id, _dump({**event, "seq": seq})),
        )
        return seq

    def event_read(self, root: str, after: int) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT body FROM events WHERE root = ? AND seq > ? ORDER BY seq", (root, after)
        ).fetchall()
        return [json.loads(b) for (b,) in rows]

    def history_append(self, agent_id: str, revision: int, entry: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO history (agent, revision, body) VALUES (?, ?, ?)",
            (agent_id, revision, _dump(entry)),
        )

    def history_read(self, agent_id: str) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT revision, body FROM history WHERE agent = ? ORDER BY revision", (agent_id,)
        ).fetchall()
        return [{"revision": r, **json.loads(b)} for r, b in rows]

    def tree_ids(self, root: str) -> list[str]:
        rows = self.db.execute(
            "SELECT id FROM agents WHERE id = ? OR substr(id, 1, ?) = ? ORDER BY id",
            (root, len(root) + 1, root + "/"),
        )
        return [i for (i,) in rows]

    def root_ids(self) -> list[str]:
        return [
            i
            for (i,) in self.db.execute(
                "SELECT id FROM agents WHERE instr(id, '/') = 0 ORDER BY id"
            )
        ]

    def work_ids(self) -> list[str]:
        return [i for (i,) in self.db.execute("SELECT id FROM agents WHERE work = 1 ORDER BY id")]


class SqliteStore(StateStore):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(
            self.path, timeout=30, isolation_level=None, check_same_thread=False
        )
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(SCHEMA)
        self._lock = threading.RLock()

    @contextmanager
    def transaction(self) -> Iterator[_Tables]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield _Tables(self._db)
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")

    def close(self) -> None:
        self._db.close()
