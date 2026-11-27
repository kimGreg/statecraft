"""Property-based tests of the kernel's transition rules (pure functions, no store).

A stateful machine drives one agent's record through random sequences of what can happen to it:
committed steps (next state, waits, spawns, signals to children, done), faults, signals (STOP,
CONT, TERM, KILL, USR), answers, wait resolutions with random child states, and intents being
carried out. Steps are applied only when the executor would run one (`is_runnable`). After every
operation the invariants below are checked.

What must hold
  * an ended agent has no reasons, no mailbox, no wake, is not terminating, and takes nothing
    more: lifecycle signals change nothing, a message is kept in `undelivered`, steps and faults
    are refused, answers are refused;
  * a running agent waits on at most one thing (join, answer or sleep) and each reason kind
    appears at most once;
  * a committed step advances `step` by exactly one; a fault or a control change never does;
  * children and spawn intents match one to one (id = <agent>/<name>); intent ids are unique and
    an intent once done stays done;
  * a terminating agent runs its `on_term` state; committing it ends the agent (terminated);
  * every USR message is accounted for exactly once: in the mailbox, consumed by a committed
    step (`signals_seen`), or kept in `undelivered`;
  * a join clears only when every child it names has ended, or one faulted with `on_fault`;
  * `is_runnable` holds exactly when the agent is active, has no reasons and no pending intents.
"""

from __future__ import annotations

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from statecraft.kernel.process import ENDED, ChildView, Fault, Record, Transition
from statecraft.kernel.transitions import (
    InvalidAnswer,
    InvalidTransition,
    apply_answer,
    apply_outcome,
    apply_signal,
    is_runnable,
    mark_intent_done,
    resolve_wait,
)

NOW = 1000.0
STATES = ("a", "b", "c")
CHILD_STATUS = st.sampled_from(["active", "succeeded", "terminated", "faulted"])


class Lifecycle(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.record = Record(id="r", agent="x", state="a", term_state="cleanup")
        self.usr_sent = 0
        self.usr_consumed = 0
        self.done_intents: set[str] = set()
        self.ended_by: str | None = None

    # ---- steps the executor would run ------------------------------------------------------
    def runnable(self) -> bool:
        return is_runnable(self.record, None, NOW)

    def commit(self, t: Transition) -> None:
        before = self.record
        seen = min(t.signals_seen, len(before.mailbox))
        t = Transition(**{**t.__dict__, "signals_seen": seen})
        after = apply_outcome(before, t, now=NOW)
        assert after.step == before.step + 1, "a committed step advances by one"
        self.usr_consumed += sum(1 for m in before.mailbox[:seen])
        if before.terminating:
            assert after.status == "terminated", "on_term committed ends the agent"
            self.ended_by = "term"
        elif t.done:
            assert after.status == "succeeded"
            self.ended_by = "done"
        self.record = after

    @precondition(lambda self: self.runnable())
    @rule(state=st.sampled_from(STATES), seen=st.integers(0, 3))
    def next_state(self, state, seen):
        self.commit(Transition(state=state, signals_seen=seen))

    @precondition(lambda self: self.runnable())
    @rule(names=st.lists(st.sampled_from(["c1", "c2", "c3"]), min_size=1, max_size=3, unique=True))
    def spawn(self, names):
        fresh = [n for n in names if n not in self.record.children]
        if not fresh:
            return
        self.commit(Transition(state="a", spawns={n: ("x", {}) for n in fresh}))

    @precondition(lambda self: self.runnable() and self.record.children)
    @rule(data=st.data(), on_fault=st.booleans())
    def wait_join(self, data, on_fault):
        names = data.draw(
            st.lists(st.sampled_from(sorted(self.record.children)), min_size=1, unique=True)
        )
        wait = {"kind": "join", "children": names, "on_fault": "b" if on_fault else None}
        self.commit(Transition(wait=wait, then="c"))

    @precondition(lambda self: self.runnable())
    @rule(name=st.sampled_from(["q1", "q2"]))
    def wait_answer(self, name):
        self.commit(Transition(wait={"kind": "answer", "name": name, "question": "?"}, then="b"))

    @precondition(lambda self: self.runnable())
    @rule(seconds=st.floats(0, 5))
    def wait_sleep(self, seconds):
        self.commit(Transition(wait={"kind": "sleep", "seconds": seconds}, then="a"))

    @precondition(lambda self: self.runnable() and self.record.children)
    @rule(data=st.data(), how=st.sampled_from(["TERM", "KILL", "STOP"]))
    def signal_child(self, data, how):
        name = data.draw(st.sampled_from(sorted(self.record.children)))
        self.commit(Transition(state="a", signals=[(self.record.children[name], how)]))

    @precondition(lambda self: self.runnable())
    @rule(seen=st.integers(0, 3))
    def done(self, seen):
        self.commit(Transition(done=True, signals_seen=seen))

    @precondition(lambda self: self.runnable())
    @rule()
    def fault(self):
        before = self.record
        after = apply_outcome(before, Fault("boom"), now=NOW)
        assert after.step == before.step and after.state == before.state
        assert after.reason("fault") is not None
        self.record = after

    # ---- control from outside ----------------------------------------------------------------
    @rule(name=st.sampled_from(["STOP", "CONT", "TERM", "KILL"]))
    def signal(self, name):
        before = self.record
        after = apply_signal(before, name)
        if before.ended:
            assert after == before, "a signal to an ended agent changes nothing"
        else:
            assert after.step == before.step, "a signal never commits a step"
            if name == "KILL":
                assert after.status == "terminated"
                self.ended_by = self.ended_by or "kill"
        self.record = after

    @rule()
    def usr(self):
        before = self.record
        after = apply_signal(before, ("USR", {"n": self.usr_sent}))
        self.usr_sent += 1
        if not before.ended:
            assert len(after.mailbox) == len(before.mailbox) + 1
        else:  # kept, never dropped
            assert after.undelivered[-1]["reason"] == "arrived as it ended"
        self.record = after

    @rule(answer=st.sampled_from(["yes", None]))
    def answer(self, answer):
        reason = self.record.reason("answer")
        if reason is None or self.record.ended:
            try:
                apply_answer(self.record, "r/0/a/question/q1", answer)
            except InvalidAnswer:
                return
            raise AssertionError("an answer with no open question was accepted")
        self.record = apply_answer(self.record, reason["question_id"], answer)
        assert self.record.reason("answer") is None
        assert self.record.wake and self.record.wake["kind"] == "answer"

    @rule(data=st.data())
    def resolve(self, data):
        views = {}
        for name in self.record.children:
            status = data.draw(CHILD_STATUS)
            views[name] = (
                ChildView("active", "child broke") if status == "faulted" else ChildView(status)
            )
        before = self.record
        after = resolve_wait(before, views, NOW + data.draw(st.floats(0, 10)))
        join = before.reason("join")
        if join is not None and after.reason("join") is None:
            named = {n: views.get(n) for n in join["children"]}
            all_ended = all(v is not None and v.status in ENDED for v in named.values())
            faulted = any(
                v is not None and v.status == "active" and v.fault for v in named.values()
            )
            assert all_ended or (faulted and join.get("on_fault")), (join, named)
            assert after.wake is not None and after.wake["kind"] == "join"
        self.record = after

    @precondition(lambda self: any(not i["done"] for i in self.record.intents))
    @rule(data=st.data())
    def carry_out(self, data):
        pending = [i["id"] for i in self.record.intents if not i["done"]]
        intent = data.draw(st.sampled_from(pending))
        self.record = mark_intent_done(self.record, intent)
        self.done_intents.add(intent)

    # ---- an ended agent takes nothing more ----------------------------------------------------
    @precondition(lambda self: self.record.ended)
    @rule()
    def nothing_after_the_end(self):
        for outcome in (Transition(state="a"), Transition(done=True), Fault("late")):
            try:
                apply_outcome(self.record, outcome, now=NOW)
            except InvalidTransition:
                continue
            raise AssertionError(f"an ended agent took {outcome}")

    # ---- invariants ----------------------------------------------------------------------------
    @invariant()
    def ended_is_final(self):
        r = self.record
        if r.ended:
            assert r.status in ENDED
            assert r.reasons == [] and r.mailbox == [] and r.wake is None and not r.terminating

    @invariant()
    def at_most_one_wait(self):
        kinds = [x["kind"] for x in self.record.reasons]
        assert len(kinds) == len(set(kinds)), kinds
        assert sum(k in ("join", "answer", "sleep") for k in kinds) <= 1, kinds

    @invariant()
    def children_match_spawn_intents(self):
        r = self.record
        spawns = {i["id"]: i for i in r.intents if i["kind"] == "spawn"}
        assert set(spawns) == set(r.children.values())
        for name, child in r.children.items():
            assert child == f"{r.id}/{name}" and spawns[child]["name"] == name

    @invariant()
    def intents_unique_and_done_stays_done(self):
        ids = [i["id"] for i in self.record.intents]
        assert len(ids) == len(set(ids)), ids
        done = {i["id"] for i in self.record.intents if i["done"]}
        assert self.done_intents <= done

    @invariant()
    def terminating_runs_on_term(self):
        r = self.record
        if r.terminating and not r.ended:
            assert r.state == r.term_state

    @invariant()
    def every_message_is_accounted_for(self):
        r = self.record
        held = len(r.mailbox) + len(r.undelivered)
        assert self.usr_sent == held + self.usr_consumed, (
            self.usr_sent,
            len(r.mailbox),
            len(r.undelivered),
            self.usr_consumed,
        )

    @invariant()
    def runnable_means_free(self):
        r = self.record
        expected = r.status == "active" and not r.reasons and not r.pending_intents
        assert is_runnable(r, None, NOW) == expected


Lifecycle.TestCase.settings = settings(max_examples=400, stateful_step_count=60, deadline=None)
TestLifecycle = Lifecycle.TestCase
