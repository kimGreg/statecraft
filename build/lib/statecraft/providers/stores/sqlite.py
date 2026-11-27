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
from ...kernel.store import COMPACTED, FormatError, StateStore, Unavailable
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
CREATE TABLE IF NOT EXISTS calls   (key TEXT PRIMARY KEY, agent TEXT NOT NULL, n INTEGER NOT NULL,
                                    body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS calls_agent ON calls(agent, n);
CREATE TABLE IF NOT EXISTS meta    (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


#: The data format this statecraft reads and writes. 1: statecraft 0.2 (no `meta` table); 2: the
#: call journal (`calls`); 3: history entries may be deltas between checkpoints, applied inbox
#: messages may be compacted (an older statecraft would misread both). A store records its
#: format; an older one is migrated explicitly (`statecraft store migrate`, after a backup), a
#: newer one is refused.
FORMAT = 3


def _migrate_1(db: sqlite3.Connection) -> None:
    """1 -> 2: the call journal."""
    db.execute(
        "CREATE TABLE IF NOT EXISTS calls (key TEXT PRIMARY KEY, agent TEXT NOT NULL, "
        "n INTEGER NOT NULL, body TEXT NOT NULL)"
    )
    db.execute("CREATE INDEX IF NOT EXISTS calls_agent ON calls(agent, n)")


def _migrate_2(db: sqlite3.Connection) -> None:
    """2 -> 3: nothing to rewrite (full-record history entries are valid checkpoints). The format
    number keeps statecraft 0.3 and later from misreading a newer store; statecraft 0.2 never
    reads it, so do not run 0.2 on a migrated store (keep the backup to go back)."""


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {1: _migrate_1, 2: _migrate_2}


def _statements(script: str) -> list[str]:
    return [part.strip() for part in script.split(";") if part.strip()]


def format_of(db: sqlite3.Connection) -> int | None:
    """The format of an open database: None when it is empty (a new store)."""
    tables = {name for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "agents" not in tables:
        return None
    if "meta" not in tables:
        return 1
    row = db.execute("SELECT value FROM meta WHERE key = 'format'").fetchone()
    return int(row[0]) if row else 1


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

    def call_get(self, key: str) -> dict[str, Any] | None:
        row = self._one("SELECT body FROM calls WHERE key = ?", key)
        return json.loads(row[0]) if row else None

    def call_put(self, key: str, agent_id: str, entry: dict[str, Any]) -> None:
        body = _dump({**entry, "agent": agent_id})
        if self._one("SELECT 1 FROM calls WHERE key = ?", key):
            self.db.execute("UPDATE calls SET body = ? WHERE key = ?", (body, key))
            return
        n = self._one("SELECT COALESCE(MAX(n), 0) + 1 FROM calls WHERE agent = ?", agent_id)[0]
        self.db.execute(
            "INSERT INTO calls (key, agent, n, body) VALUES (?, ?, ?, ?)", (key, agent_id, n, body)
        )

    def calls_of(self, agent_id: str) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT body FROM calls WHERE agent = ? ORDER BY n", (agent_id,)
        ).fetchall()
        return [json.loads(b) for (b,) in rows]

    def history_trim(self, agent_id: str, revision: int, entry: dict[str, Any]) -> int:
        dropped = self.db.execute(
            "DELETE FROM history WHERE agent = ? AND revision <> ?", (agent_id, revision)
        ).rowcount
        self.db.execute(
            "UPDATE history SET body = ? WHERE agent = ? AND revision = ?",
            (_dump(entry), agent_id, revision),
        )
        return dropped

    def event_drop(self, root: str, seqs: list[int]) -> None:
        self.db.executemany(
            "DELETE FROM events WHERE root = ? AND seq = ?", [(root, n) for n in seqs]
        )

    def inbox_compact(self, agent_id: str, upto: int) -> int:
        return self.db.execute(
            "UPDATE inbox SET body = ? WHERE agent = ? AND n <= ? AND body <> ?",
            (_dump(COMPACTED), agent_id, upto, _dump(COMPACTED)),
        ).rowcount

    def forget(self, agent_ids: list[str], root: str) -> None:
        for table, column in (
            ("agents", "id"),
            ("leases", "id"),
            ("inbox", "agent"),
            ("history", "agent"),
            ("calls", "agent"),
        ):
            self.db.executemany(
                f"DELETE FROM {table} WHERE {column} = ?", [(i,) for i in agent_ids]
            )
        self.db.execute("DELETE FROM events WHERE root = ?", (root,))

    def setting_get(self, name: str) -> Any:
        row = self._one("SELECT value FROM meta WHERE key = ?", f"setting:{name}")
        return json.loads(row[0]) if row else None

    def setting_put(self, name: str, value: Any) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            (f"setting:{name}", _dump(value)),
        )

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
    def __init__(
        self, path: str | Path, *, busy_timeout: float = BUSY_TIMEOUT, migrate: bool = False
    ) -> None:
        """`migrate`: upgrade an older data format on open (otherwise refused: FormatError)."""
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout = busy_timeout
        self.migrate = migrate
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
                self._prepare(db)
                break
            except FormatError:
                db.close()
                raise
            except sqlite3.OperationalError as exc:
                db.close()
                if not _locked(exc) or time.monotonic() > deadline:
                    raise Unavailable(f"cannot open {self.path}: {exc}") from exc
                time.sleep(0.05)  # another process is creating the file: wait for it
        self._db, self._pid = db, os.getpid()

    def _prepare(self, db: sqlite3.Connection) -> None:
        """Create a new store, or check (and with `migrate`, upgrade) an existing one's format,
        in one write transaction: a concurrent opener never sees half of it."""
        db.execute("BEGIN IMMEDIATE")
        try:
            found = format_of(db)
            if found is None:
                for statement in _statements(SCHEMA):
                    db.execute(statement)
                db.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('format', ?)", (str(FORMAT),)
                )
            elif found > FORMAT:
                raise FormatError(
                    f"{self.path} has data format {found}; this statecraft reads {FORMAT}: "
                    "upgrade statecraft"
                )
            elif found < FORMAT:
                if not self.migrate:
                    raise FormatError(
                        f"{self.path} has data format {found}; this statecraft reads {FORMAT}: "
                        "back it up and run `statecraft store migrate --config ...`"
                    )
                for version in range(found, FORMAT):
                    MIGRATIONS[version](db)
                db.execute(
                    "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                db.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('format', ?)", (str(FORMAT),)
                )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

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


def migrate(path: str | Path) -> tuple[int | None, int]:
    """Back the store up (`<path>.format-<n>.bak`) and upgrade it to this statecraft's format.
    Returns (the format it had, the format it has). Stop every process using the store first:
    one that is writing makes this refuse (`Unavailable`). The backup is SQLite's online backup,
    so it holds what is still in the write-ahead log too."""
    path = Path(path)
    db = sqlite3.connect(path, timeout=1.0, isolation_level=None)
    try:
        found = format_of(db)
        if found is not None and found < FORMAT:
            try:
                db.execute("BEGIN EXCLUSIVE")  # no other writer now
                db.execute("ROLLBACK")
            except sqlite3.OperationalError as exc:
                raise Unavailable(f"{path} is in use; stop its processes, then migrate") from exc
            backup = sqlite3.connect(path.with_name(f"{path.name}.format-{found}.bak"))
            try:
                db.backup(backup)
            finally:
                backup.close()
    finally:
        db.close()
    SqliteStore(path, migrate=True).close()
    return found, FORMAT


def check(path: str | Path) -> dict[str, Any]:
    """The store's data format and whether this statecraft reads it."""
    db = sqlite3.connect(Path(path))
    try:
        found = format_of(db)
    finally:
        db.close()
    return {
        "path": str(path),
        "format": found,
        "reads": FORMAT,
        "ok": found in (None, FORMAT),
        "action": "none"
        if found in (None, FORMAT)
        else "migrate"
        if found < FORMAT
        else "upgrade statecraft",
    }
