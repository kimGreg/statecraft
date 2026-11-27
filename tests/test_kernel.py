"""The pure lifecycle rules: record in, record out.

What must hold: a transition advances the step and consumes the wake; a fault keeps step, state,
data and wake; keys carry the step and the state; waits clear only on decided facts; signals
follow the lifecycle table; an ending agent records TERM intents for its children; answers are
checked; a join is end synchronization (a terminated child clears it, not as success).
"""

from __future__ import annotations

import pytest

from statecraft.kernel.process import ChildView, Fault, Record, Transition
from statecraft.kernel.transitions import (
    InvalidAnswer,
    InvalidTransition,
    apply_answer,
    apply_outcome,
    apply_signal,
    apply_transition,
    has_work,
    is_runnable,
    mark_intent_done,
    resolve_wait,
)


def rec(**kw) -> Record:
    return Record(**{"id": "root", "agent": "t:a", "state": "s0", **kw})


def test_a_transition_advances_the_step_and_consumes_the_wake() -> None:
    r = rec(wake={"kind": "sleep"}, data={"x": 1})
    after = apply_transition(r, Transition(state="s1", data={"x": 2}), now=0)
    assert (after.step, after.state, after.data, after.wake) == (1, "s1", {"x": 2}, None)


def test_a_fault_keeps_step_state_data_and_wake() -> None:
    r = rec(step=4, wake={"kind": "answer", "answer": "yes"}, data={"x": 1})
    after = apply_outcome(r, Fault("boom"), now=0)
    assert (after.step, after.state, after.data, after.wake) == (4, "s0", {"x": 1}, r.wake)
    assert after.reason("fault") == {"kind": "fault", "error": "boom", "state": "s0"}
    assert not is_runnable(after, None, 0)
    assert apply_signal(after, "CONT").reasons == []


def test_keys_carry_the_step_and_the_state() -> None:
    r = rec(step=7, state="publish", term_state="cleanup")
    termed = apply_signal(r, "TERM")
    assert r.key("write") == "root/7/publish/write"
    assert termed.key("write") == "root/7/cleanup/write"
    assert r.staging(3) == "home/root/staging/7/3"


def test_spawn_and_signal_become_intents_with_deterministic_ids() -> None:
    t = Transition(
        state="wait", spawns={"c": ("agent.c", {"n": 1})}, signals=[("root/other", "STOP")]
    )
    after = apply_transition(rec(step=2), t, now=0)
    assert after.children == {"c": "root/c"}
    assert [i["id"] for i in after.intents] == ["root/c", "root/2/s0/signal/0"]
    assert not is_runnable(after, None, 0)  # intents first
    done = mark_intent_done(mark_intent_done(after, "root/c"), "root/2/s0/signal/0")
    assert is_runnable(done, None, 0)


def test_a_join_clears_when_every_child_ended_and_reports_how() -> None:
    r = apply_transition(
        rec(),
        Transition(
            wait={"kind": "join", "children": ["a", "b"]},
            then="next",
            spawns={"a": ("agent.x", {}), "b": ("agent.x", {})},
        ),
        now=0,
    )
    assert resolve_wait(r, {"a": ChildView("succeeded")}, 0) == r
    woke = resolve_wait(r, {"a": ChildView("succeeded"), "b": ChildView("terminated")}, 0)
    assert woke.reasons == [] and woke.state == "next"
    assert woke.wake["children"] == {"a": "succeeded", "b": "terminated"}


def test_a_child_fault_clears_the_join_only_with_on_fault() -> None:
    t = Transition(
        wait={"kind": "join", "children": ["a", "b"], "on_fault": "handle"},
        then="next",
        spawns={"a": ("agent.x", {}), "b": ("agent.x", {})},
    )
    r = apply_transition(rec(), t, now=0)
    views = {"a": ChildView("active", "e1"), "b": ChildView("active", "e2")}
    woke = resolve_wait(r, views, 0)
    assert woke.state == "handle"
    assert [f["child"] for f in woke.wake["faulted"]] == ["a", "b"]
    plain = apply_transition(
        rec(),
        Transition(
            wait={"kind": "join", "children": ["a"]}, then="next", spawns={"a": ("agent.x", {})}
        ),
        now=0,
    )
    assert resolve_wait(plain, {"a": ChildView("active", "e1")}, 0) == plain


def test_sleep_and_answer_deadlines_clear_by_the_clock() -> None:
    slept = apply_transition(
        rec(), Transition(wait={"kind": "sleep", "seconds": 5}, then="next"), now=100
    )
    assert resolve_wait(slept, {}, 104.9) == slept
    assert resolve_wait(slept, {}, 105).wake == {"kind": "sleep"}
    asked = apply_transition(
        rec(),
        Transition(
            wait={"kind": "answer", "name": "q", "default": "no", "timeout": 10}, then="next"
        ),
        now=0,
    )
    woke = resolve_wait(asked, {}, 10)
    assert (woke.wake["answer"], woke.wake["by"]) == ("no", "timeout")


def test_answers_are_checked_and_stopped_remains() -> None:
    asked = apply_transition(
        rec(step=3),
        Transition(
            wait={"kind": "answer", "name": "q", "schema": {"enum": ["a", "b"]}}, then="next"
        ),
        now=0,
    )
    qid = "root/3/s0/question/q"
    assert asked.reason("answer")["question_id"] == qid
    with pytest.raises(InvalidAnswer):
        apply_answer(asked, qid, "c")
    with pytest.raises(InvalidAnswer):
        apply_answer(asked, "root/3/s0/question/other", "a")
    stopped = apply_signal(asked, "STOP")
    answered = apply_answer(stopped, qid, "a")
    assert [r["kind"] for r in answered.reasons] == ["stopped"]
    assert answered.wake["answer"] == "a"
    with pytest.raises(InvalidAnswer):
        apply_answer(answered, qid, "b")


def test_signals_follow_the_lifecycle() -> None:
    r = rec(term_state="cleanup")
    faulted = apply_signal(apply_outcome(r, Fault("x"), now=0), "STOP")
    assert apply_signal(faulted, "CONT").reasons == []
    termed = apply_signal(faulted, "TERM")
    assert (termed.state, termed.terminating, termed.reasons) == ("cleanup", True, [])
    ended = apply_transition(termed, Transition(state="whatever"), now=0)
    assert ended.status == "terminated"
    assert apply_signal(rec(), "TERM").status == "terminated"  # no on_term
    assert apply_signal(ended, "CONT") == ended  # ended: no-op
    assert apply_signal(rec(), ("USR", "hi")).mailbox == [{"message": "hi"}]


def test_an_ending_agent_terms_its_children_and_keeps_work() -> None:
    r = apply_transition(rec(), Transition(state="s1", spawns={"c": ("agent.x", {})}), now=0)
    r = mark_intent_done(r, "root/c")
    for ended in (apply_transition(r, Transition(done=True), now=0), apply_signal(r, "KILL")):
        terms = [i for i in ended.pending_intents if i["signal"] == "TERM"]
        assert [i["target"] for i in terms] == ["root/c"]
        assert has_work(ended)
    with pytest.raises(InvalidTransition):
        apply_transition(apply_signal(r, "KILL"), Transition(state="x"), now=0)


def test_a_terminated_parent_with_a_pending_spawn_still_creates_and_terms_it() -> None:
    r = apply_transition(rec(), Transition(state="s1", spawns={"c": ("agent.x", {})}), now=0)
    killed = apply_signal(r, "KILL")
    kinds = [(i["kind"], i.get("target", i["id"])) for i in killed.pending_intents]
    assert kinds == [("spawn", "root/c"), ("signal", "root/c")]  # create first, then TERM


def test_the_default_is_always_an_acceptable_answer() -> None:
    asked = apply_transition(
        rec(),
        Transition(
            wait={
                "kind": "answer",
                "name": "q",
                "default": {},
                "schema": {"type": "object", "required": ["x"]},
            },
            then="next",
        ),
        now=0,
    )
    qid = asked.reason("answer")["question_id"]
    assert apply_answer(asked, qid, {}).wake["answer"] == {}
