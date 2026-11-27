"""The local state store: one SQLite file, one transaction per operation.

Several executor processes may share the file; SQLite serializes the writers and each operation
reads and writes inside its own transaction, so fencing and CAS checks hold across processes.

Connection ownership: each store has one connection, owned by one thread of its own; every
operation runs there, one at a time, so the event loop never waits on a database lock (heartbeats
keep moving while another process writes). Writes take `BEGIN IMMEDIATE`; reads take a deferred
`BEGIN` and see a snapshot (WAL). A lock held longer than `busy_timeout` raises `Unavailable`:
nothing was done, and the executor tries again next pass. Fencing does not depend on any of this:
the lease (owner, epoch) is checked in the commit's own write transaction, and acquiring a lease
bumps the epoch in a write transaction too, so a commit can never land after another executor
took the lease. (Expiry is judged by the time taken before the lock wait, so a commit may land a
little after its lease expired in wall-clock time, while no one else holds it.) A process that
forks gets a connection of its own.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any, TypeVar

from ...kernel.process import Lease, Record
from ...kernel.store import StateStore, Unavailable
from ...kernel.transitions import has_work

T = TypeVar("T")
#: How long one operation waits for another process's lock before `Unavailable` (seconds).
BUSY_TIMEOUT = 10.0
#: How long opening the file waits for others creating it at the same moment (seconds).
OPEN_TIMEOUT = 30.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents  (id TEXT PRIMARY KEY, work INTEGER NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS agents_work ON agents(work);
CREATE TABLE IF NOT EXISTS leases  (id TEXT PRIMARY KEY, owner TEXT, epoch INTEGER NOT NULL,
                                    expires REAL);
CREATE TABLE IF NOT EXISTS inbox   (agent TEXT NOT NULL, n INTEGER NOT NULL, id TEXT NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (agent, id));
CREATE INDEX IF NOT EXISTS inbox_n ON inbox(agent, n);
CREATE TABLE IF NOT EXISTS events  (root TEXT NOT NULL, seq INTEGER NOT NULL, id TEXT NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (root, seq), UNIQUE (root, id));
CREATE TABLE IF NOT EXISTS history (agent TEXT NOT NULL, revision INTEGER NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (agent, revision));
"""


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)  # key order kept, as the memory store keeps it


def _locked(exc: sqlite3.OperationalError) -> bool:
    text = str(exc).lower()
    return "locked" in text or "busy" in text


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
    def __init__(self, path: str | Path, *, busy_timeout: float = BUSY_TIMEOUT) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout = busy_timeout
        self._db: sqlite3.Connection | None = None
        self._pid = 0
        self._thread: ThreadPoolExecutor | None = None
        self._lock = threading.RLock()
        self._run_sync(self._connect)  # open (and create) now: errors show at construction

    # ---- the connection, owned by the store's thread -----------------------------------------
    def _connect(self) -> None:
        deadline = time.monotonic() + OPEN_TIMEOUT
        while True:
            db = sqlite3.connect(
                self.path,
                timeout=self.busy_timeout,
                isolation_level=None,
                check_same_thread=False,
            )
            try:
                db.execute("PRAGMA journal_mode=WAL")  # needs an exclusive lock: may be busy
                db.execute("PRAGMA synchronous=FULL")
                db.executescript(SCHEMA)
                break
            except sqlite3.OperationalError as exc:
                db.close()
                if not _locked(exc) or time.monotonic() > deadline:
                    raise Unavailable(f"cannot open {self.path}: {exc}") from exc
                time.sleep(0.05)  # another process is creating the file: wait for it
        self._db, self._pid = db, os.getpid()

    def _executor(self) -> ThreadPoolExecutor:
        if self._thread is None or self._pid != os.getpid():
            self._thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="statecraft-db")
        return self._thread

    def _run_sync(self, fn: Callable[[], T]) -> T:
        with self._lock:
            stale = self._db is None or self._pid != os.getpid()  # first use, or a fork
            if stale and fn is not self._connect:
                self._connect()
            return fn()

    async def _run(self, fn: Callable[[], T]) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor(), lambda: self._run_sync(fn))

    @contextmanager
    def transaction(self, write: bool = True) -> Iterator[_Tables]:
        with self._lock:
            db = self._db
            assert db is not None
            try:
                db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            except sqlite3.OperationalError as exc:
                if _locked(exc):
                    raise Unavailable(f"{self.path} stayed locked: {exc}") from exc
                raise
            try:
                yield _Tables(db)
            except BaseException:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise
            try:
                db.execute("COMMIT")
            except sqlite3.OperationalError as exc:
                if db.in_transaction:  # never leave the connection inside a transaction
                    db.execute("ROLLBACK")
                if _locked(exc):
                    raise Unavailable(f"{self.path}: commit refused: {exc}") from exc
                raise

    def close(self) -> None:
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None
        if self._thread is not None:
            self._thread.shutdown(wait=False)
            self._thread = None
