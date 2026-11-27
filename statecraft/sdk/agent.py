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

The kernel runs anything that implements `ExecutableAgent` (one async `step`); `Agent` with
`@state` methods is the convenient way to write one.
"""

from __future__ import annotations

import asyncio
import copy
import traceback
from dataclasses import dataclass, field
from typing import Any, ClassVar, Protocol, runtime_checkable

from ..kernel.process import Fault, Record, Transition


@runtime_checkable
class ExecutableAgent(Protocol):
    """What the kernel executes: one asynchronous step from a state, returning what happens next
    (a state name, `Spawn`, `Wait` or `Done`; raising is a fault). Classes that are not `Agent`s
    also declare `start` (and may declare `on_term`, `schema_version`, `states`)."""

    async def step(self, state: str, ctx: Any, data: dict[str, Any]) -> Any: ...


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
    #: A join may also clear at a deadline (seconds) or when a message waits in the mailbox; its
    #: wake says which (`by`: children, timeout, message) and which children are still `live`.
    timeout: float | None = None
    messages: bool = False


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
    crossings: list[dict[str, Any]] = field(default_factory=list)


def states_of(agent: type) -> set[str]:
    """The states an agent class declares: its `@state` methods, or a `states` attribute."""
    if isinstance(agent, type) and issubclass(agent, Agent):
        return set(agent.agent_states())
    return set(getattr(agent, "states", ()) or ())


def to_transition(
    agent: type, result: Any, data: dict[str, Any], effects: StepEffects
) -> Transition:
    states = states_of(agent)

    def known(name: str | None, what: str) -> str | None:
        if name is not None and states and name not in states:
            raise AgentDefinitionError(f"{agent.__name__}: {what} names no state: {name!r}")
        return name

    common = {
        "data": data,
        "signals": list(effects.signals),
        "published": dict(effects.published),
        "signals_seen": effects.signals_seen,
        "notes": dict(effects.notes),
        "schema_version": effects.schema_version,
        "crossings": list(effects.crossings),
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
        if chosen[0] != "join" and (result.timeout is not None or result.messages):
            raise AgentDefinitionError(
                f"{agent.__name__}: Wait timeout and messages are for joins (an answer has its "
                "own timeout; a sleep is a timeout)"
            )
        if chosen[0] == "join":
            wait = {
                "kind": "join",
                "children": list(result.join),
                "on_fault": result.on_fault,
                "timeout": result.timeout,
                "messages": result.messages,
            }
        elif chosen[0] == "answer":
            if "name" not in result.answer:
                raise AgentDefinitionError(f"{agent.__name__}: a question has a name")
            wait = {"kind": "answer", **result.answer}
        else:
            wait = {"kind": "sleep", "seconds": result.sleep}
        return Transition(wait=wait, then=result.then, **common)
    raise AgentDefinitionError(f"{agent.__name__}: a state returned {result!r}")


async def run_state(agent: type, record: Record, ctx: Any) -> Transition | Fault:
    """Run the record's current state once. `ctx.effects` collects signals, publications and
    crossings. An `Agent` dispatches to its `@state` method; any other `ExecutableAgent` gets
    `step(state, ctx, data)`."""
    data = copy.deepcopy(record.data)
    try:  # constructing the agent is part of the state: its errors are the state's fault
        if isinstance(agent, type) and issubclass(agent, Agent):
            handler = agent.agent_states().get(record.state)
            if handler is None:
                return Fault(f"{agent.__name__} has no state {record.state!r}")
            call = handler(agent(), ctx, data)
        else:
            call = agent().step(record.state, ctx, data)
        result = await call
        return to_transition(agent, result, data, ctx.effects)
    except asyncio.CancelledError:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise  # the executor cancels the step (KILL, lease loss, stop)
        # raised by the state itself: its fault, after its calls are settled like any other
        return Fault(
            "the state was cancelled from inside (CancelledError)",
            crossings=list(ctx.effects.crossings),
        )
    except Exception as exc:
        lines = traceback.format_exception_only(type(exc), exc)
        return Fault("".join(lines).strip(), crossings=list(ctx.effects.crossings))
