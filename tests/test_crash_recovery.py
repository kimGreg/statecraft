"""Crash recovery with real process deaths: a worker process is killed (SIGKILL, or `os._exit` at a
store boundary), then a new process recovers from the SQLite file and the workspace alone.

What must hold at every crash point: the tree finishes; each child is created once and publishes
once; an unfinished state runs again with the same effect key; committed spawns are carried out
even when the process died right after the commit.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from statecraft.providers.stores import SqliteStore

HERE = Path(__file__).parent
WORKER = HERE / "crash_worker.py"


def worker(tmp: Path, point: str, *extra: str) -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(HERE), *sys.path])}
    return subprocess.Popen(
        [sys.executable, str(WORKER), str(tmp / "s.db"), str(tmp / "ws"), point, *extra], env=env
    )


def check_finished(tmp: Path) -> None:
    store = SqliteStore(tmp / "s.db")
    tree = asyncio.run(store.tree("crashroot"))
    store.close()
    assert {r.id: r.status for r in tree} == {
        "crashroot": "succeeded",
        "crashroot/a": "succeeded",
        "crashroot/b": "succeeded",
    }
    root = next(r for r in tree if r.id == "crashroot")
    total = json.loads((tmp / "ws" / "crashroot" / root.published["sum"]["path"]).read_text())
    assert total["payload"]["value"] == 3
    logs = sorted((tmp / "ws" / "crashroot" / "shared" / "log").iterdir())
    runs: dict[str, int] = {}
    for p in logs:  # one file per run (key and process): a duplicate run cannot hide
        runs[p.read_text()] = runs.get(p.read_text(), 0) + 1
    assert set(runs) == {"crashroot/a/0/work/log", "crashroot/b/0/work/log"}
    # at least once; at most once per process (the crashed one may have run it uncommitted)
    assert all(1 <= n <= 2 for n in runs.values()), runs


POINTS = [
    "commit_step:before:1",  # the root's first state ran; nothing committed
    "commit_step:after:1",  # the spawn intents are committed, no child exists yet
    "create:after:1",  # one child created, the intent not marked done
    "commit_step:before:2",  # a child's state ran; its commit never happened
    "commit_step:after:3",  # both children done; the parent not woken yet
    "commit_control:after:2",  # both spawn intents marked done, children not run yet
    "commit_control:after:3",  # the parent's join resolved (its wake stored), not run yet
]


@pytest.mark.parametrize("point", POINTS)
def test_a_process_death_at_a_store_boundary_is_recovered(tmp_path, point) -> None:
    first = worker(tmp_path, point, "spawn")
    assert first.wait(60) == 9
    second = worker(tmp_path, "-")
    assert second.wait(60) == 0
    check_finished(tmp_path)


def test_sigkill_in_the_middle_of_a_state_reruns_it_after_the_lease_expires(tmp_path) -> None:
    first = worker(tmp_path, "-", "spawn", "slow")
    marker = tmp_path / "ws" / "crashroot" / "home" / "crashroot" / "a" / "private" / "started"
    deadline = time.time() + 30
    while not marker.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert marker.exists()
    first.send_signal(signal.SIGKILL)
    assert first.wait(10) == -signal.SIGKILL
    second = worker(tmp_path, "-")
    assert second.wait(60) == 0
    check_finished(tmp_path)
    runs = [
        p.name
        for p in (tmp_path / "ws" / "crashroot" / "shared" / "log").iterdir()
        if p.name.startswith("crashroot_a_")
    ]
    assert len(runs) == 2  # the same state, the same key, two processes
