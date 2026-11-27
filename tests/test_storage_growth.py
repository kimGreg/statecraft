"""Storage grows with what a run did, not with its square (roadmap C2).

What must hold
  * a delta patches its old value into the new one, for any JSON values;
  * a run whose data grows by a turn every step (a conversation) keeps a history linear in its
    length: measured on SQLite, the history holds a few times the final record, where full
    snapshots would hold about steps/2 times it;
  * `scan_work` reads only agents with work (an index), not every record.
"""

from __future__ import annotations

import json
import sqlite3

from hypothesis import given, settings
from hypothesis import strategies as st

from statecraft.kernel.delta import diff, patch
from statecraft.kernel.process import Record, Transition
from statecraft.kernel.store import CHECKPOINT
from statecraft.providers.stores.sqlite import SqliteStore

json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=5),
    lambda inner: (
        st.lists(inner, max_size=4) | st.dictionaries(st.text(max_size=3), inner, max_size=4)
    ),
    max_leaves=20,
)


@settings(max_examples=400, deadline=None)
@given(json_values, json_values)
def test_a_delta_patches_old_into_new(old, new) -> None:
    patched = patch(old, diff(old, new))
    assert json.dumps(patched, sort_keys=True) == json.dumps(new, sort_keys=True)


@settings(max_examples=200, deadline=None)
@given(st.lists(json_values, max_size=6), st.lists(json_values, max_size=3))
def test_an_append_costs_the_appended_items(start, more) -> None:
    delta = diff(start, [*start, *more])
    if start and more:
        assert delta == {"+": more}
    assert patch(start, delta) == [*start, *more]


async def test_a_growing_conversation_keeps_a_linear_history(tmp_path) -> None:
    steps = 300
    store = SqliteStore(tmp_path / "state.db")
    await store.create(Record(id="r", agent="t:a", state="s", data={"conversation": []}))
    for n in range(steps):
        lease = await store.try_acquire("r", "a", 10, now=n)
        conversation = [
            *(await store.load("r")).data["conversation"],
            {"turn": n, "text": "x" * 400},
        ]
        data = {"conversation": conversation}
        await store.commit_step("r", lease, n, Transition(state="s", data=data), now=n)
    store.close()
    db = sqlite3.connect(tmp_path / "state.db")
    history = db.execute("SELECT SUM(LENGTH(body)) FROM history").fetchone()[0]
    record = db.execute("SELECT LENGTH(body) FROM agents").fetchone()[0]
    db.close()
    checkpoints = steps // CHECKPOINT + 1
    # each checkpoint is a full record (at most the final size), each delta about one turn
    assert history < record * (checkpoints + 2), (history, record)
    assert history < record * steps / 8  # full snapshots would be about record * steps / 2


async def test_scan_work_reads_only_agents_with_work(tmp_path) -> None:
    store = SqliteStore(tmp_path / "state.db")
    for n in range(50):
        await store.create(Record(id=f"done{n}", agent="t:a", state="s", status="succeeded"))
    await store.create(Record(id="busy", agent="t:a", state="s"))
    assert [r.id for r in await store.scan_work()] == ["busy"]
    plan = store._run_sync(
        lambda: store._db.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM agents WHERE work = 1 ORDER BY id"
        ).fetchall()
    )
    assert "agents_work" in str(plan)
    store.close()
