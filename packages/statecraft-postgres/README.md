# statecraft-postgres

The statecraft state store on PostgreSQL, for executors on several hosts against one database.

```yaml
store: {provider: postgres, dsn: "${STATECRAFT_PG_DSN}", schema: statecraft}
```

```python
from statecraft_postgres import PostgresStore

runtime = Runtime(store=PostgresStore(dsn, schema="statecraft"), workspace=...)
```

## Guarantees

The same as the SQLite store; the core's store contract suite runs unchanged against it
(`tests/test_contract.py`).

- Every operation is one transaction. Writes run `SERIALIZABLE` and are run again from the start
  when PostgreSQL reports a serialization conflict or a deadlock (up to 40 times, then
  `Unavailable`: nothing was done, the executor tries again). Reads run `REPEATABLE READ READ
  ONLY`.
- Fencing: a step commits only under the lease (owner, epoch) it read in its own transaction; a
  concurrent acquisition of that lease writes the same row, so one of the two is retried and
  sees the other.
- A broken connection is replaced; the operation that saw it reports `Unavailable`. For a write
  that broke while committing the outcome is unknown; the kernel looks again.
- The data format is the core's (3), recorded in the schema's `meta` table; another is refused.

## Tested

- the store contract suite (21 cases);
- four processes racing for one record's lease: every step committed once, history without gaps;
- the chaos harness (`chaos/chaos.py run --pg DSN`): worker processes killed (SIGKILL), paused
  past their lease (SIGSTOP) and restarted, STOP/CONT, messages, answers; every invariant checked.
  The workers are processes on one machine talking to the database over TCP; separate hosts
  were not tested.

Run the tests with a server: `STATECRAFT_PG_DSN=postgresql://... pytest packages/statecraft-postgres`.
Each test uses a schema of its own and drops it.

## Not done

- `LISTEN/NOTIFY` wake-ups: executors find work by polling (`scan_every`), as with SQLite.
- One connection per store and process: a process's operations run one at a time (as SQLite's);
  more executor processes give more concurrency.
- Migrations: this store starts at format 3; the next format change must add one.
