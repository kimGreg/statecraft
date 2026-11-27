"""A PostgreSQL server for the tests: `STATECRAFT_PG_DSN` (skipped without it). Each test gets
a schema of its own, dropped after."""

from __future__ import annotations

import os
import uuid

import pytest

DSN = os.environ.get("STATECRAFT_PG_DSN")


def schema_name() -> str:
    return f"t_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def dsn() -> str:
    if not DSN:
        pytest.skip("set STATECRAFT_PG_DSN to a PostgreSQL server to run these")
    return DSN


@pytest.fixture
def store(dsn):
    from statecraft_postgres import PostgresStore

    made = PostgresStore(dsn, schema=schema_name())
    yield made
    made.close()
    made.drop()
