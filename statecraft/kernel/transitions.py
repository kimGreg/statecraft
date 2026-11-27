"""The pure lifecycle rules. Record in, record out; no I/O, no clock reads (time is passed in).

The store persists what these functions return and owns `revision`; the executor decides when
to call them. Every rule the design states about lifecycle lives here, so it is tested once.
"""

from __future__ import annotations

import copy
from typing import Any

from .process import ENDED, ChildView, Fault, Lease, Record, Transition


class InvalidTransition(ValueError):
    """The change is not valid for the record's lifecycle point."""


class InvalidAnswer(ValueError):
    """No such open question, already answered, or the answer does not match its schema."""


CONTROL_SIGNALS = frozenset({"STOP", "CONT", "TERM", "KILL"})


def _signal_name(signal: Any) -> str:
    return signal[0] if isinstance(signal, (tuple, list)) else signal


def _term_children(record: Record) -> list[dict[str, Any]]:
    """`TERM` intents for every child; ending ones that already ended is a no-op later."""
    existing = {i["id"] for i in record.intents}
    out = []
    for name, child in record.children.items():
        intent_id = f"{record.id}/end/term/{name}"
        if intent_id not in existing:
            out.append(
                {
                    "id": intent_id,
                    "kind": "signal",
                    "target": child,
                    "signal": "TERM",
                    "done": False,
                }
            )
    return out


def _end(record: Record, status: str) -> Record:
    ended = record.but(
        status=status,
        reasons=[],
        wake=None,
        terminating=False,
        mailbox=[],
        undelivered=[*record.undelivered, *record.mailbox],
    )
    return ended.but(intents=[*ended.intents, *_term_children(ended)])


# ---- a state's outcome ------------------------------------------------------------------------


def apply_outcome(record: Record, outcome: Transition | Fault, *, now: float) -> Record:
    if isinstance(outcome, Fault):
        return apply_fault(record, outcome.error)
    return apply_transition(record, outcome, now=now)


def apply_fault(record: Record, error: str) -> Record:
    """The state raised: suspended with a fault; step, state, data and wake stay as they were."""
    if record.ended:
        raise InvalidTransition(f"{record.id} has ended")
    reasons = [r for r in record.reasons if r["kind"] != "fault"]
    return record.but(
        reasons=[*reasons, {"kind": "fault", "error": error, "state": record.state}], error=error
    )


def _attached(target: Any) -> dict[str, Any]:
    """A spawn's attachment: a root id (read-only) or {"root": id, "mode": "ro"|"rw"}."""
    if target is None:
        return {"workspace": None}
    if isinstance(target, dict):
        return {"workspace": target["root"], "workspace_mode": target.get("mode", "ro")}
    return {"workspace": target, "workspace_mode": "ro"}


def apply_transition(record: Record, t: Transition, *, now: float) -> Record:
    if record.ended:
        raise InvalidTransition(f"{record.id} has ended")
    if record.reasons:
        raise InvalidTransition(f"{record.id} is suspended: {record.reasons}")
    if sum((t.state is not None, t.wait is not None, t.done)) != 1:
        raise InvalidTransition("a transition has exactly one of state, wait, done")

    intents = list(record.intents)
    children = dict(record.children)
    for name, (agent, child_input) in t.spawns.items():
        child_id = f"{record.id}/{name}"
        if name in children:
            raise InvalidTransition(f"{record.id} already has a child named {name}")
        children[name] = child_id
        intents.append(
            {
                "id": child_id,
                "kind": "spawn",
                "done": False,
                "agent": agent,
                "name": name,
                **_attached(t.attach.get(name)),
                "parent": record.id,
                "input": copy.deepcopy(child_input),
            }
        )
    for n, (target, signal) in enumerate(t.signals):
        intents.append(
            {
                "id": record.key("signal", str(n)),
                "kind": "signal",
                "done": False,
                "target": target,
                "signal": copy.deepcopy(signal),
            }
        )

    base = record.but(
        step=record.step + 1,
        data=copy.deepcopy(t.data),
        wake=None,
        intents=intents,
        children=children,
        published={**record.published, **copy.deepcopy(t.published)},
        mailbox=record.mailbox[t.signals_seen :],
        notes={**record.notes, **copy.deepcopy(t.notes)},
        schema_version=t.schema_version or record.schema_version,
        error=None,
    )
    if record.terminating:  # `on_term` committed: the agent has ended
        return _end(base, "terminated")
    if t.done:
        return _end(base, "succeeded")
    if t.state is not None:
        return base.but(state=t.state)
    return base.but(state=t.then, reasons=[_wait_reason(record, t, now)])


def _wait_reason(record: Record, t: Transition, now: float) -> dict[str, Any]:
    wait = dict(t.wait or {})
    kind = wait.get("kind")
    if t.then is None:
        raise InvalidTransition("a wait names the state that runs when it clears")
    if kind == "join":
        missing = [c for c in wait["children"] if c not in record.children and c not in t.spawns]
        if missing:
            raise InvalidTransition(f"join names children that were never spawned: {missing}")
        return {
            "kind": "join",
            "children": list(wait["children"]),
            "on_fault": wait.get("on_fault"),
        }
    if kind == "answer":
        timeout = wait.get("timeout")
        return {
            "kind": "answer",
            "question_id": record.key("question", wait["name"]),
            "name": wait["name"],
            "question": wait.get("question", ""),
            "schema": wait.get("schema") or {},
            "default": wait.get("default"),
            "deadline": None if timeout is None else now + timeout,
        }
    if kind == "sleep":
        return {"kind": "sleep", "until": now + float(wait["seconds"])}
    raise InvalidTransition(f"unknown wait: {kind}")


# ---- control: signals, answers, waits, intents ------------------------------------------------


SIGNALS = ("STOP", "CONT", "TERM", "KILL", "USR")


def signal_problem(signal: Any) -> str | None:
    """Why a value is not a signal, or None: STOP, CONT, TERM, KILL, or ("USR", message)."""
    if isinstance(signal, str):
        if signal in ("STOP", "CONT", "TERM", "KILL"):
            return None
        return f"unknown signal {signal!r}; signals are {', '.join(SIGNALS)}"
    if isinstance(signal, (list, tuple)) and len(signal) == 2 and signal[0] == "USR":
        return None
    return f'not a signal: {signal!r} (a USR signal is ("USR", message))'


def apply_signal(record: Record, signal: Any) -> Record:
    """STOP, CONT, TERM, KILL change the lifecycle; ("USR", message) goes to the mailbox."""
    name = _signal_name(signal)
    if record.ended:
        return record  # signals to an ended agent are no-ops
    if name == "STOP":
        if record.reason("stopped"):
            return record
        return record.but(reasons=[*record.reasons, {"kind": "stopped"}])
    if name == "CONT":
        return record.but(
            reasons=[r for r in record.reasons if r["kind"] not in ("stopped", "fault")]
        )
    if name == "TERM":
        if record.terminating:
            return record
        if record.term_state is None:
            return _end(record, "terminated")
        return record.but(
            state=record.term_state,
            terminating=True,
            reasons=[],
            wake={"kind": "term"},
            mailbox=[],
            undelivered=[*record.undelivered, *record.mailbox],
        )
    if name == "KILL":
        return _end(record, "terminated")  # the store also bumps the lease epoch
    if name == "USR":
        return record.but(mailbox=[*record.mailbox, {"message": signal[1]}])
    raise InvalidTransition(f"unknown signal: {signal!r}")


def _check_schema(schema: dict[str, Any], answer: Any) -> None:
    if "enum" in schema and answer not in schema["enum"]:
        raise InvalidAnswer(f"{answer!r} is not one of {schema['enum']}")
    kinds = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "object": dict,
        "array": list,
    }
    expected = schema.get("type")
    if expected in kinds and not isinstance(answer, kinds[expected]):
        raise InvalidAnswer(f"{answer!r} is not of type {expected}")


def apply_answer(record: Record, question_id: str, answer: Any, *, by: str = "person") -> Record:
    """Checked atomically with the store's CAS: open, unanswered, valid. `stopped` remains."""
    reason = record.reason("answer")
    if record.ended or reason is None or reason["question_id"] != question_id:
        raise InvalidAnswer(f"{question_id} is not open on {record.id}")
    if answer != reason.get("default"):  # the default is always acceptable
        _check_schema(reason["schema"], answer)
    return record.but(
        reasons=[r for r in record.reasons if r is not reason],
        wake={
            "kind": "answer",
            "question_id": question_id,
            "name": reason["name"],
            "answer": copy.deepcopy(answer),
            "by": by,
        },
    )


def resolve_wait(record: Record, children: dict[str, ChildView], now: float) -> Record:
    """Clear a join, sleep or answer deadline that is decided; otherwise return it unchanged.

    `children` maps child names to their views; a join over a child not created yet waits.
    """
    if record.ended:
        return record
    for reason in record.reasons:
        kind = reason["kind"]
        if kind == "join":
            views = {n: children.get(n) for n in reason["children"]}
            faulted = [
                {"child": n, "id": record.children.get(n), "error": v.fault}
                for n, v in views.items()
                if v and v.status == "active" and v.fault
            ]
            if faulted and reason.get("on_fault"):
                return _clear(
                    record,
                    reason,
                    state=reason["on_fault"],
                    wake={
                        "kind": "join",
                        "faulted": faulted,
                        "children": {n: (v.status if v else "active") for n, v in views.items()},
                    },
                )
            if all(v is not None and v.status in ENDED for v in views.values()):
                return _clear(
                    record,
                    reason,
                    wake={
                        "kind": "join",
                        "faulted": [],
                        "children": {n: v.status for n, v in views.items()},
                    },
                )
        elif kind == "sleep" and now >= reason["until"]:
            return _clear(record, reason, wake={"kind": "sleep"})
        elif kind == "answer" and reason["deadline"] is not None and now >= reason["deadline"]:
            return _clear(
                record,
                reason,
                wake={
                    "kind": "answer",
                    "question_id": reason["question_id"],
                    "name": reason["name"],
                    "answer": copy.deepcopy(reason["default"]),
                    "by": "timeout",
                },
            )
    return record


def _clear(
    record: Record, reason: dict[str, Any], *, wake: dict[str, Any], state: str | None = None
) -> Record:
    return record.but(
        reasons=[r for r in record.reasons if r is not reason],
        wake=wake,
        state=state or record.state,
    )


def mark_intent_done(record: Record, intent_id: str) -> Record:
    if not any(i["id"] == intent_id for i in record.intents):
        raise InvalidTransition(f"{record.id} has no intent {intent_id}")
    return record.but(
        intents=[{**i, "done": True} if i["id"] == intent_id else i for i in record.intents]
    )


# ---- derived views ----------------------------------------------------------------------------


def is_runnable(record: Record, lease: Lease | None, now: float) -> bool:
    return (
        record.status == "active"
        and not record.reasons
        and not record.pending_intents
        and (lease is None or not lease.valid(now))
    )


def has_work(record: Record) -> bool:
    """What the executor's scan visits: active agents, and ended ones with intents left."""
    return record.status == "active" or bool(record.pending_intents)


def view(record: Record, lease: Lease | None, now: float) -> str:
    if record.ended:
        return record.status
    if lease is not None and lease.valid(now):
        return "running"
    if record.reasons:
        return "suspended"
    return "runnable" if not record.pending_intents else "pending"
