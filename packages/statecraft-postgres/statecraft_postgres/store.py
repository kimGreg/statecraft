"""A state store on PostgreSQL (roadmap C3): executors on several hosts against one database.

    store = PostgresStore("postgresql://user:pw@db/statecraft", schema="statecraft")

The same transaction semantics as the SQLite store, which the store contract suite checks: every
operation is one transaction; writes run `SERIALIZABLE` and are retried from the start when
PostgreSQL reports a serialization conflict or a deadlock (the operation's body reads and writes
only through its transaction, so running it again is safe); reads run `REPEATABLE READ READ
ONLY` and see a snapshot. Fencing does not depend on the isolation level alone: the lease (owner,
epoch) is read and checked in the commit's own write transaction, and a concurrent acquisition of
that lease conflicts with it, so one of the two is retried and sees the other.

Connection ownership: one connection per store and process, used by one thread of the store's
own (the event loop never waits on the database); a process that forks gets its own; a broken
connection is replaced and the operation reports `Unavailable` (nothing was done).

The data format is the core's (`meta.format`, 3); a newer one is refused, an older one does not
exist for this store yet.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, TypeVar

import psycopg
from psycopg import errors, sql

from statecraft.kernel.process import Lease, Record
from statecraft.kernel.store import COMPACTED, FormatError, StateStore, Tables, Unavailable
from statecraft.kernel.transitions import has_work

T = TypeVar("T")
FORMAT = 3
#: How many times a write is run again after a serialization conflict before `Unavailable`.
RETRIES = 40

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents  (id TEXT PRIMARY KEY, work BOOLEAN NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS agents_work ON agents(work) WHERE work;
CREATE TABLE IF NOT EXISTS leases  (id TEXT PRIMARY KEY, owner TEXT, epoch BIGINT NOT NULL,
                                    expires DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS inbox   (agent TEXT NOT NULL, n BIGINT NOT NULL, id TEXT NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (agent, id));
CREATE INDEX IF NOT EXISTS inbox_n ON inbox(agent, n);
CREATE TABLE IF NOT EXISTS events  (root TEXT NOT NULL, seq BIGINT NOT NULL, id TEXT NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (root, seq), UNIQUE (root, id));
CREATE TABLE IF NOT EXISTS history (agent TEXT NOT NULL, revision BIGINT NOT NULL,
                                    body TEXT NOT NULL, PRIMARY KEY (agent, revision));
CREATE TABLE IF NOT EXISTS calls   (key TEXT PRIMARY KEY, agent TEXT NOT NULL, n BIGINT NOT NULL,
                                    body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS calls_agent ON calls(agent, n);
CREATE TABLE IF NOT EXISTS meta    (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

RETRY = (errors.SerializationFailure, errors.DeadlockDetected)


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class _Tables:
    def __init__(self, cur: psycopg.Cursor) -> None:
        self.cur = cur

    def _one(self, query: str, *args: Any) -> Any:
        return self.cur.execute(query, args).fetchone()

    def _all(self, query: str, *args: Any) -> list[Any]:
        return self.cur.execute(query, args).fetchall()

    def get(self, agent_id: str) -> Record | None:
        row = self._one("SELECT body FROM agents WHERE id = %s", agent_id)
        return Record.from_dict(json.loads(row[0])) if row else None

    def put(self, record: Record) -> None:
        self.cur.execute(
            "INSERT INTO agents (id, work, body) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET work = EXCLUDED.work, body = EXCLUDED.body",
            (record.id, has_work(record), _dump(record.to_dict())),
        )

    def get_lease(self, agent_id: str) -> tuple[Lease | None, int]:
        row = self._one("SELECT owner, epoch, expires FROM leases WHERE id = %s", agent_id)
        if row is None:
            return None, 0
        owner, epoch, expires = row
        return (Lease(owner, epoch, expires) if owner is not None else None), epoch

    def put_lease(self, agent_id: str, lease: Lease | None, epoch: int) -> None:
        self.cur.execute(
            "INSERT INTO leases (id, owner, epoch, expires) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, epoch = EXCLUDED.epoch, "
            "expires = EXCLUDED.expires",
            (agent_id, lease.owner if lease else None, epoch, lease.expires if lease else None),
        )

    def inbox_append(self, agent_id: str, message_id: str, message: Any) -> bool:
        if self._one("SELECT 1 FROM inbox WHERE agent = %s AND id = %s", agent_id, message_id):
            return False
        n = self._one("SELECT COALESCE(MAX(n), 0) + 1 FROM inbox WHERE agent = %s", agent_id)[0]
        self.cur.execute(
            "INSERT INTO inbox (agent, n, id, body) VALUES (%s, %s, %s, %s)",
            (agent_id, n, message_id, _dump(message)),
        )
        return True

    def inbox_read(self, agent_id: str, after: int) -> list[dict[str, Any]]:
        rows = self._all(
            "SELECT n, id, body FROM inbox WHERE agent = %s AND n > %s ORDER BY n", agent_id, after
        )
        return [{"n": n, "id": i, "message": json.loads(b)} for n, i, b in rows]

    def event_append(self, root: str, event_id: str, event: dict[str, Any]) -> int:
        row = self._one("SELECT seq FROM events WHERE root = %s AND id = %s", root, event_id)
        if row:
            return int(row[0])
        seq = self._one("SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE root = %s", root)[0]
        self.cur.execute(
            "INSERT INTO events (root, seq, id, body) VALUES (%s, %s, %s, %s)",
            (root, seq, event_id, _dump({**event, "seq": seq})),
        )
        return int(seq)

    def event_read(self, root: str, after: int) -> list[dict[str, Any]]:
        rows = self._all(
            "SELECT body FROM events WHERE root = %s AND seq > %s ORDER BY seq", root, after
        )
        return [json.loads(b) for (b,) in rows]

    def history_append(self, agent_id: str, revision: int, entry: dict[str, Any]) -> None:
        self.cur.execute(
            "INSERT INTO history (agent, revision, body) VALUES (%s, %s, %s)",
            (agent_id, revision, _dump(entry)),
        )

    def history_read(self, agent_id: str) -> list[dict[str, Any]]:
        rows = self._all(
            "SELECT revision, body FROM history WHERE agent = %s ORDER BY revision", agent_id
        )
        return [{"revision": int(r), **json.loads(b)} for r, b in rows]

    def history_trim(self, agent_id: str, revision: int, entry: dict[str, Any]) -> int:
        dropped = self.cur.execute(
            "DELETE FROM history WHERE agent = %s AND revision <> %s", (agent_id, revision)
        ).rowcount
        self.cur.execute(
            "UPDATE history SET body = %s WHERE agent = %s AND revision = %s",
            (_dump(entry), agent_id, revision),
        )
        return dropped

    def event_drop(self, root: str, seqs: list[int]) -> None:
        self.cur.execute("DELETE FROM events WHERE root = %s AND seq = ANY(%s)", (root, seqs))

    def inbox_compact(self, agent_id: str, upto: int) -> int:
        return self.cur.execute(
            "UPDATE inbox SET body = %s WHERE agent = %s AND n <= %s AND body <> %s",
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
            self.cur.execute(
                sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
                    sql.Identifier(table), sql.Identifier(column)
                ),
                (agent_ids,),
            )
        self.cur.execute("DELETE FROM events WHERE root = %s", (root,))

    def setting_get(self, name: str) -> Any:
        row = self._one("SELECT value FROM meta WHERE key = %s", f"setting:{name}")
        return json.loads(row[0]) if row else None

    def setting_put(self, name: str, value: Any) -> None:
        self.cur.execute(
            "INSERT INTO meta (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (f"setting:{name}", _dump(value)),
        )

    def call_get(self, key: str) -> dict[str, Any] | None:
        row = self._one("SELECT body FROM calls WHERE key = %s", key)
        return json.loads(row[0]) if row else None

    def call_put(self, key: str, agent_id: str, entry: dict[str, Any]) -> None:
        body = _dump({**entry, "agent": agent_id})
        if self._one("SELECT 1 FROM calls WHERE key = %s", key):
            self.cur.execute("UPDATE calls SET body = %s WHERE key = %s", (body, key))
            return
        n = self._one("SELECT COALESCE(MAX(n), 0) + 1 FROM calls WHERE agent = %s", agent_id)[0]
        self.cur.execute(
            "INSERT INTO calls (key, agent, n, body) VALUES (%s, %s, %s, %s)",
            (key, agent_id, n, body),
        )

    def calls_of(self, agent_id: str) -> list[dict[str, Any]]:
        rows = self._all("SELECT body FROM calls WHERE agent = %s ORDER BY n", agent_id)
        return [json.loads(b) for (b,) in rows]

    def tree_ids(self, root: str) -> list[str]:
        rows = self._all(
            "SELECT id FROM agents WHERE id = %s OR left(id, %s) = %s ORDER BY id",
            root,
            len(root) + 1,
            root + "/",
        )
        return [i for (i,) in rows]

    def root_ids(self) -> list[str]:
        return [
            i for (i,) in self._all("SELECT id FROM agents WHERE strpos(id, '/') = 0 ORDER BY id")
        ]

    def work_ids(self) -> list[str]:
        return [i for (i,) in self._all("SELECT id FROM agents WHERE work ORDER BY id")]


class PostgresStore(StateStore):
    def __init__(
        self, dsn: str, *, schema: str = "statecraft", connect_timeout: float = 10
    ) -> None:
        self.dsn = dsn
        self.schema = schema
        self.connect_timeout = connect_timeout
        self._db: psycopg.Connection | None = None
        self._pid = 0
        self._thread: ThreadPoolExecutor | None = None
        self._lock = threading.RLock()
        self.conflicts = 0  # serialization conflicts retried (observability, tests)
        self._run_sync(self._connect)

    # ---- the connection ----------------------------------------------------------------------
    def _connect(self) -> None:
        try:
            db = psycopg.connect(
                self.dsn, autocommit=True, connect_timeout=int(self.connect_timeout)
            )
        except psycopg.OperationalError as exc:
            raise Unavailable(f"cannot connect to PostgreSQL: {exc}") from exc
        db.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
        db.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema)))
        self._prepare(db)
        self._db, self._pid = db, os.getpid()

    def _prepare(self, db: psycopg.Connection) -> None:
        """Create the tables, or check the format, under an advisory lock: concurrent openers
        never see half a schema."""
        with db.transaction():
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"statecraft:{self.schema}",))
            for statement in [p.strip() for p in SCHEMA.split(";") if p.strip()]:
                db.execute(statement)
            row = db.execute("SELECT value FROM meta WHERE key = 'format'").fetchone()
            if row is None:
                db.execute("INSERT INTO meta (key, value) VALUES ('format', %s)", (str(FORMAT),))
            elif int(row[0]) != FORMAT:
                raise FormatError(
                    f"schema {self.schema} has data format {row[0]}; this store reads {FORMAT}"
                )

    def _executor(self) -> ThreadPoolExecutor:
        if self._thread is None or self._pid != os.getpid():
            self._thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="statecraft-pg")
        return self._thread

    def _run_sync(self, fn: Callable[[], T]) -> T:
        with self._lock:
            stale = self._db is None or self._pid != os.getpid() or self._db.closed
            if stale and fn is not self._connect:
                self._connect()
            return fn()

    async def _run(self, fn: Callable[[], T]) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor(), lambda: self._run_sync(fn))

    @contextmanager
    def transaction(self, write: bool = True) -> Iterator[Tables]:
        db = self._db
        assert db is not None
        level = "SERIALIZABLE" if write else "REPEATABLE READ READ ONLY"
        with db.transaction():
            db.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
            with db.cursor() as cur:
                yield _Tables(cur)

    def _in_transaction(self, body: Callable[[Tables], T], write: bool) -> T:
        """Run the body in its own transaction; again from the start on a serialization
        conflict (its reads and writes go only through the transaction)."""
        for attempt in range(RETRIES):
            try:
                with self.transaction(write=write) as t:
                    return body(t)
            except RETRY:
                self.conflicts += 1
                time.sleep(min(0.002 * 2**attempt, 0.1) * random.random())
            except psycopg.OperationalError as exc:  # the connection broke: nothing was done
                self._reset()
                raise Unavailable(f"PostgreSQL: {exc}") from exc
        raise Unavailable(f"PostgreSQL: still conflicting after {RETRIES} tries")

    def _reset(self) -> None:
        if self._db is not None:
            try:
                self._db.close()
            finally:
                self._db = None

    def close(self) -> None:
        with self._lock:
            self._reset()
        if self._thread is not None:
            self._thread.shutdown(wait=True)
            self._thread = None

    def drop(self) -> None:
        """Remove this store's schema and everything in it (tests)."""
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self.schema))
            )


def from_config(cfg: dict[str, Any], base: Any = None) -> PostgresStore:
    """`store: {provider: postgres, dsn: ..., schema: statecraft}` (the `dsn` may come from the
    environment through `${VAR}`)."""
    unknown = set(cfg) - {"dsn", "schema", "connect_timeout"}
    if unknown or "dsn" not in cfg:
        raise TypeError(f"postgres store takes dsn, schema, connect_timeout; got {sorted(cfg)}")
    return PostgresStore(**cfg)


__all__ = ["FORMAT", "PostgresStore", "from_config"]
