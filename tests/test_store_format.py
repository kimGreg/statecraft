"""The store's data format is versioned, and an old one is migrated explicitly (roadmap C1).

What must hold
  * a new SQLite store records its format; this statecraft reads format 2;
  * a statecraft 0.2 store (format 1, written by its schema) is refused with what to do; after
    `statecraft store migrate` (which backs it up first) it opens, its records load as they were,
    and the call journal works;
  * a store with a newer format is refused (upgrade statecraft);
  * `statecraft store check` says which and exits non-zero when the store cannot be read.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from statecraft.cli.main import main as cli
from statecraft.kernel.process import Record
from statecraft.kernel.store import FormatError
from statecraft.providers.stores import SqliteStore
from statecraft.providers.stores.sqlite import FORMAT, check, migrate

#: statecraft 0.2's schema, as it wrote stores (format 1)
SCHEMA_0_2 = """
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


def store_0_2(path: Path) -> Record:
    record = Record(id="old", agent="t:a", state="go", data={"kept": [1, 2]})
    db = sqlite3.connect(path)
    db.executescript(SCHEMA_0_2)
    db.execute(
        "INSERT INTO agents (id, work, body) VALUES (?, 1, ?)",
        ("old", json.dumps(record.to_dict())),
    )
    db.commit()
    db.close()
    return record


async def test_a_new_store_records_its_format(tmp_path: Path) -> None:
    SqliteStore(tmp_path / "new.db").close()
    assert FORMAT == 2 and check(tmp_path / "new.db")["format"] == 2


async def test_an_old_store_is_refused_then_migrated(tmp_path: Path) -> None:
    path = tmp_path / "old.db"
    record = store_0_2(path)
    with pytest.raises(FormatError, match="store migrate"):
        SqliteStore(path)
    assert check(path) == {
        "path": str(path),
        "format": 1,
        "reads": 2,
        "ok": False,
        "action": "migrate",
    }
    assert migrate(path) == (1, 2)
    assert (tmp_path / "old.db.format-1.bak").exists()
    store = SqliteStore(path)
    loaded = await store.load("old")
    assert loaded.data == record.data and loaded.state == "go"
    entry, created = await store.begin_call("old", "old/0/go/call/x", "e", "d", 1.0)
    assert created and entry["status"] == "sent"
    store.close()


def test_a_newer_store_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "newer.db"
    SqliteStore(path).close()
    db = sqlite3.connect(path)
    db.execute("UPDATE meta SET value = '99' WHERE key = 'format'")
    db.commit()
    db.close()
    with pytest.raises(FormatError, match="upgrade statecraft"):
        SqliteStore(path)


def test_the_cli_checks_and_migrates(tmp_path: Path, capsys) -> None:
    store_0_2(tmp_path / "state.db")
    config = tmp_path / "statecraft.yaml"
    config.write_text(f"store: {{provider: sqlite, path: '{tmp_path}/state.db'}}\n")
    assert cli(["store", "check", "--config", str(config)]) == 1
    assert cli(["store", "migrate", "--config", str(config)]) == 0
    assert cli(["store", "check", "--config", str(config)]) == 0
    assert '"format": 2' in capsys.readouterr().out
