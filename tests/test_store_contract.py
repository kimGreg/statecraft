"""The memory and SQLite stores pass the state store conformance suite."""

from __future__ import annotations

import pytest

from statecraft.providers.stores import MemoryStore, SqliteStore
from statecraft.testing.store_contract import *  # noqa: F403


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    if request.param == "memory":
        yield MemoryStore()
        return
    s = SqliteStore(tmp_path / "state.db")
    yield s
    s.close()
