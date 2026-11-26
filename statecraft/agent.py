"""Writing an agent: a class of `@state` methods, each returning what happens next.

    class Example(Agent):
        start = "measure"

        @state
        async def measure(self, ctx, data):
            data["facts"] = await ctx.call("maps.measure")
            return Spawn({"paragraph": ("agent.decider", {...})}, then="wait")

        @state
        async def wait(self, ctx, data):
            return Wait(join=["paragraph"], then="write")

A state returns a state name, `Spawn`, `Wait`, or `Done`; raising is a fault. Nothing durable
lives on `self`: later states see only `data` (and `ctx.woke` in the state a wait woke).
"""

from __future__ import annotations

import copy
import traceback
from dataclasses import dataclass, field
from typing import Any, ClassVar

from .process import Fault, Record, Transition


class AgentDefinitionError(TypeError):
    """An agent class or a state's result does not follow the rules."""


class ChildFailed(RuntimeError):
    """Raised by an agent whose required child did not succeed (a join does not promise it)."""


def state(fn):
    """Register a method as a state."""
    fn.__agent_state__ = True
    return fn


@dataclass(frozen=True)
class Spawn:
    children: dict[str, tuple[str, dict[str, Any]]]
    then: str
    #: Children that work in another root's workspace: {child name: root} (see `attach`).
    attach: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Wait:
    then: str
    join: list[str] | None = None
    answer: dict[str, Any] | None = None  # {name, question, schema, default, timeout?}
    sleep: float | None = None
    on_fault: str | None = None


class _DoneType:
    _instance = None

    def __new__(cls):
        cls._instance = cls._instance or super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "Done"


Done = _DoneType()


class Agent:
    """A state machine. Subclasses set `start` and may set `on_term`, `schema_version`."""

    start: ClassVar[str]
    on_term: ClassVar[str | None] = None
    schema_version: ClassVar[int] = 1

    @classmethod
    def agent_states(cls) -> dict[str, Any]:
        found = {}
        for klass in reversed(cls.__mro__):
            for name, value in vars(klass).items():
                if getattr(value, "__agent_state__", False):
                    found[name] = value
        return found

    @classmethod
    def check_definition(cls) -> None:
        states = cls.agent_states()
        start = getattr(cls, "start", None)
        if start not in states:
            raise AgentDefinitionError(f"{cls.__name__}.start names no state: {start!r}")
        if cls.on_term is not None and cls.on_term not in states:
            raise AgentDefinitionError(f"{cls.__name__}.on_term names no state: {cls.on_term!r}")

    def migrate_data(self, old_version: int, data: dict[str, Any]) -> dict[str, Any]:
        raise AgentDefinitionError(
            f"{type(self).__name__} has no data migration from version {old_version}"
        )


@dataclass
class StepEffects:
    """What a running state recorded through its context besides its return value."""

    signals: list[tuple[str, Any]] = field(default_factory=list)
    published: dict[str, dict[str, Any]] = field(default_factory=dict)
    signals_seen: int = 0
    notes: dict[str, Any] = field(default_factory=dict)
    schema_version: int | None = None


def to_transition(
    agent: type[Agent], result: Any, data: dict[str, Any], effects: StepEffects
) -> Transition:
    states = agent.agent_states()

    def known(name: str | None, what: str) -> str | None:
        if name is not None and name not in states:
            raise AgentDefinitionError(f"{agent.__name__}: {what} names no state: {name!r}")
        return name

    common = {
        "data": data,
        "signals": list(effects.signals),
        "published": dict(effects.published),
        "signals_seen": effects.signals_seen,
        "notes": dict(effects.notes),
        "schema_version": effects.schema_version,
    }
    if result is Done:
        return Transition(done=True, **common)
    if isinstance(result, str):
        return Transition(state=known(result, "the next state"), **common)
    if isinstance(result, Spawn):
        return Transition(
            state=known(result.then, "Spawn.then"),
            spawns=dict(result.children),
            attach=dict(result.attach),
            **common,
        )
    if isinstance(result, Wait):
        chosen = [k for k in ("join", "answer", "sleep") if getattr(result, k) is not None]
        if len(chosen) != 1:
            raise AgentDefinitionError(f"{agent.__name__}: a Wait has one of join/answer/sleep")
        known(result.then, "Wait.then")
        known(result.on_fault, "Wait.on_fault")
        if chosen[0] == "join":
            wait = {"kind": "join", "children": list(result.join), "on_fault": result.on_fault}
        elif chosen[0] == "answer":
            if "name" not in result.answer:
                raise AgentDefinitionError(f"{agent.__name__}: a question has a name")
            wait = {"kind": "answer", **result.answer}
        else:
            wait = {"kind": "sleep", "seconds": result.sleep}
        return Transition(wait=wait, then=result.then, **common)
    raise AgentDefinitionError(f"{agent.__name__}: a state returned {result!r}")


async def run_state(agent: type[Agent], record: Record, ctx: Any) -> Transition | Fault:
    """Run the record's current state once. `ctx.effects` collects signals and publications."""
    handler = agent.agent_states().get(record.state)
    if handler is None:
        return Fault(f"{agent.__name__} has no state {record.state!r}")
    data = copy.deepcopy(record.data)
    try:
        result = await handler(agent(), ctx, data)
        return to_transition(agent, result, data, ctx.effects)
    except Exception as exc:
        lines = traceback.format_exception_only(type(exc), exc)
        return Fault("".join(lines).strip())
