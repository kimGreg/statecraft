"""Several processes on one PostgreSQL store: one lease at a time, every step committed once.

What must hold
  * processes racing to take a record's lease and commit its next step never commit one step
    twice or skip one: the record's step is the number of successful commits, its history has
    every revision once, and each commit was made under the lease of the epoch it held;
  * a configuration naming `provider: postgres` builds this store.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
import uuid


def schema_name() -> str:
    return f"t_{uuid.uuid4().hex[:12]}"


RACER = textwrap.dedent(
    """
    import asyncio, sys
    from statecraft.kernel.process import Transition
    from statecraft.kernel.store import Busy, Conflict, Fenced
    from statecraft_postgres import PostgresStore

    async def main(dsn, schema, rounds):
        store = PostgresStore(dsn, schema=schema)
        won = 0
        for n in range(rounds):
            lease = await store.try_acquire("r", f"p{sys.argv[4]}", 30, now=n)
            if lease is None:
                continue
            record = await store.load("r")
            try:
                await store.commit_step("r", lease, record.step, Transition(state="s"), now=n)
                won += 1
            except (Fenced, Conflict, Busy):
                pass
        print(won)

    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3])))
    """
)


def test_processes_commit_every_step_once(dsn) -> None:
    from statecraft_postgres import PostgresStore

    from statecraft.kernel.process import Record

    schema = schema_name()
    store = PostgresStore(dsn, schema=schema)
    try:
        asyncio.run(store.create(Record(id="r", agent="t:a", state="s")))
        racers = [
            subprocess.Popen(
                [sys.executable, "-c", RACER, dsn, schema, "40", str(i)],
                stdout=subprocess.PIPE,
                text=True,
            )
            for i in range(4)
        ]
        won = sum(int(p.communicate(timeout=120)[0].strip()) for p in racers)
        record = asyncio.run(store.load("r"))
        history = asyncio.run(store.history("r"))
        assert won > 0 and record.step == won
        assert [h["revision"] for h in history] == list(range(1, len(history) + 1))
        assert sum(1 for h in history if h["kind"] == "step") == won
    finally:
        store.close()
        store.drop()


def test_the_configuration_builds_a_postgres_store(dsn, tmp_path) -> None:
    from statecraft_postgres import PostgresStore

    from statecraft import Runtime

    schema = schema_name()
    config = tmp_path / "statecraft.yaml"
    config.write_text(f"store: {{provider: postgres, dsn: '{dsn}', schema: {schema}}}\n")
    runtime = Runtime.from_config(config)
    try:
        assert isinstance(runtime.store, PostgresStore) and runtime.store.schema == schema
    finally:
        runtime.store.close()
        runtime.store.drop()
