"""The SQLite store shared by several processes.

What must hold
  * several processes opening a new database file at the same moment all succeed (the WAL
    switch and the schema wait for one another instead of failing at once);
  * a store call never blocks the event loop, even while another process holds the write lock:
    the event loop keeps running (heartbeats keep moving);
  * a lock held longer than `busy_timeout` is `Unavailable` (a store error the executor tries
    again), never a raw sqlite3 error, and nothing was done;
  * a process forked from one that already used a store opens its own connection: its writes
    land in the file and the parent's connection keeps working.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import sqlite3
import time
from pathlib import Path

import pytest

from statecraft.kernel.process import Record
from statecraft.kernel.store import StoreError, Unavailable
from statecraft.providers.stores import SqliteStore


def _open(path: str, barrier, results) -> None:
    barrier.wait()
    try:
        SqliteStore(Path(path)).close()
        results.put("ok")
    except Exception as exc:  # pragma: no cover - the failure being tested
        results.put(f"{type(exc).__name__}: {exc}")


@pytest.mark.parametrize("trial", range(3))
def test_processes_opening_a_new_file_at_once_all_succeed(tmp_path, trial) -> None:
    ctx = multiprocessing.get_context("spawn")
    barrier, results = ctx.Barrier(6), ctx.Queue()
    path = str(tmp_path / f"new-{trial}.db")
    procs = [ctx.Process(target=_open, args=(path, barrier, results)) for _ in range(6)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
    outcomes = [results.get(timeout=5) for _ in procs]
    assert outcomes == ["ok"] * 6, outcomes


def _hold_write_lock(path: str, ready, seconds: float) -> None:
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("BEGIN IMMEDIATE")
    ready.set()
    time.sleep(seconds)
    db.execute("ROLLBACK")
    db.close()


async def test_a_locked_database_neither_blocks_the_loop_nor_leaks_sqlite_errors(tmp_path) -> None:
    path = tmp_path / "s.db"
    store = SqliteStore(path, busy_timeout=0.5)
    await store.create(Record(id="r", agent="a", state="s"))
    ctx = multiprocessing.get_context("spawn")
    ready = ctx.Event()
    holder = ctx.Process(target=_hold_write_lock, args=(str(path), ready, 1.5))
    holder.start()
    await asyncio.to_thread(ready.wait, 30)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.02)
            ticks += 1

    beat = asyncio.create_task(ticker())
    with pytest.raises(Unavailable) as raised:  # a write waits busy_timeout, then gives up
        await store.try_acquire("r", "me", 10, now=0)
    beat.cancel()
    assert isinstance(raised.value, StoreError)
    assert ticks >= 10  # the loop kept running while the store call waited
    holder.join(10)
    assert await store.lease("r") is None  # nothing was done
    assert await store.try_acquire("r", "me", 10, now=0) is not None  # and it works again
    store.close()


def _forked_write(store: SqliteStore, results) -> None:
    async def write() -> None:
        await store.create(Record(id="from-child", agent="x", state="go"))
        results.put((await store.load("parent")).id)

    try:
        asyncio.run(write())
    except Exception as exc:  # pragma: no cover - the failure being tested
        results.put(f"{type(exc).__name__}: {exc}")


async def test_a_forked_process_gets_its_own_connection(tmp_path) -> None:
    store = SqliteStore(tmp_path / "db")
    await store.create(Record(id="parent", agent="x", state="go"))
    context = multiprocessing.get_context("fork")
    results = context.Queue()
    child = context.Process(target=_forked_write, args=(store, results))
    child.start()
    child.join(30)
    assert results.get(timeout=5) == "parent"
    assert (await store.load("from-child")).id == "from-child"
    await store.create(Record(id="after", agent="x", state="go"))
    store.close()
