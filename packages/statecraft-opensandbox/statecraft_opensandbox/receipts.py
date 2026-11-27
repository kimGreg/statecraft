"""Durable receipts: what the provider sent, for which call identity, and what became of it.

A SQLite file the provider owns, kept outside every workspace and every sandbox mount, shared by
every executor process on the machine. Reservations and start claims are single statements, so
two executors recovering the same call never start it twice.

    receipts   key, digest, agent, sandbox, execution, state, result, reason, updated
               state: reserved > starting > submitted > completed | cancelled | unknown
    sandboxes  scope, label, sandbox, agent, state (creating | ready)
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (key TEXT PRIMARY KEY, digest TEXT NOT NULL,
    agent TEXT NOT NULL, sandbox TEXT, execution TEXT, state TEXT NOT NULL, result TEXT,
    reason TEXT, updated REAL);
CREATE TABLE IF NOT EXISTS sandboxes (scope TEXT PRIMARY KEY, label TEXT NOT NULL, sandbox TEXT,
    agent TEXT, state TEXT NOT NULL, updated REAL);
"""
FIELDS = ("key", "digest", "agent", "sandbox", "execution", "state", "result", "reason", "updated")
SANDBOX_FIELDS = ("scope", "label", "sandbox", "agent", "state", "updated")


class Receipts:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=10000")
        self._db.executescript(SCHEMA)
        self._lock = threading.Lock()

    async def _run(self, sql: str, *args: Any) -> list[tuple]:
        def run() -> list[tuple]:
            with self._lock:
                cursor = self._db.execute(sql, args)
                return cursor.fetchall() if cursor.description else [(cursor.rowcount,)]

        return await asyncio.to_thread(run)

    # ---- calls --------------------------------------------------------------------------------
    async def reserve(self, key: str, digest: str, agent: str) -> dict[str, Any]:
        """The receipt of a call identity, created `reserved` if new."""
        await self._run(
            "INSERT OR IGNORE INTO receipts (key, digest, agent, state, updated) "
            "VALUES (?, ?, ?, 'reserved', ?)",
            key,
            digest,
            agent,
            time.time(),
        )
        found = await self.get(key)
        assert found is not None
        return found

    async def claim(self, key: str) -> bool:
        """Claim the start of a reserved call: exactly one caller wins."""
        [(count,)] = await self._run(
            "UPDATE receipts SET state = 'starting', updated = ? "
            "WHERE key = ? AND state = 'reserved'",
            time.time(),
            key,
        )
        return count == 1

    async def get(self, key: str) -> dict[str, Any] | None:
        rows = await self._run(f"SELECT {', '.join(FIELDS)} FROM receipts WHERE key = ?", key)
        if not rows:
            return None
        row = dict(zip(FIELDS, rows[0], strict=True))
        row["result"] = json.loads(row["result"]) if row["result"] else None
        return row

    async def set(self, key: str, **fields: Any) -> None:
        if "result" in fields:
            fields["result"] = json.dumps(fields["result"])
        fields["updated"] = time.time()
        assign = ", ".join(f"{k} = ?" for k in fields)
        await self._run(f"UPDATE receipts SET {assign} WHERE key = ?", *fields.values(), key)

    # ---- sandboxes ----------------------------------------------------------------------------
    async def sandbox(self, scope: str) -> dict[str, Any] | None:
        rows = await self._run(
            f"SELECT {', '.join(SANDBOX_FIELDS)} FROM sandboxes WHERE scope = ?", scope
        )
        return dict(zip(SANDBOX_FIELDS, rows[0], strict=True)) if rows else None

    async def begin_sandbox(self, scope: str, label: str, agent: str) -> dict[str, Any]:
        """The sandbox row of a scope, created `creating` with this label if new (the label is
        chosen before the create request, so a lost answer can be found again)."""
        await self._run(
            "INSERT OR IGNORE INTO sandboxes (scope, label, agent, state, updated) "
            "VALUES (?, ?, ?, 'creating', ?)",
            scope,
            label,
            agent,
            time.time(),
        )
        found = await self.sandbox(scope)
        assert found is not None
        return found

    async def set_sandbox(self, scope: str, **fields: Any) -> None:
        fields["updated"] = time.time()
        assign = ", ".join(f"{k} = ?" for k in fields)
        await self._run(f"UPDATE sandboxes SET {assign} WHERE scope = ?", *fields.values(), scope)

    async def drop_sandbox(self, scope: str) -> None:
        await self._run("DELETE FROM sandboxes WHERE scope = ?", scope)

    async def sandboxes(self) -> list[dict[str, Any]]:
        rows = await self._run(f"SELECT {', '.join(SANDBOX_FIELDS)} FROM sandboxes")
        return [dict(zip(SANDBOX_FIELDS, r, strict=True)) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._db.close()


__all__ = ["Receipts"]
