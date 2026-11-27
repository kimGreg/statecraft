"""An agent process as data: its record, its lease, and what one step can result in.

Nothing here performs I/O. The store keeps records; the executor runs steps; the pure rules that
move a record from one lifecycle point to the next are in `transitions`.

Keys (effects, questions, signal intents, staging paths) are built from the step and the state's
name, never from the revision: a retried state keeps its keys, and a state entered by a control
operation at the same step (`TERM` moving to `on_term`) gets different ones.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal

Status = Literal["active", "succeeded", "terminated"]
ENDED: frozenset[str] = frozenset({"succeeded", "terminated"})
WAIT_KINDS: frozenset[str] = frozenset({"join", "answer", "sleep"})


@dataclass(frozen=True)
class Lease:
    """Execution ownership. Only the holder of the current epoch, before `expires`, may commit."""

    owner: str
    epoch: int
    expires: float

    def valid(self, now: float) -> bool:
        return now < self.expires


@dataclass(frozen=True)
class Record:
    """The authoritative execution context of one agent.

    `reasons` are dicts with a `kind`: join {children, on_fault}, answer {question_id, name,
    question, schema, default, deadline}, sleep {until}, stopped, fault {error, state}.
    `intents` are dicts {id, kind: spawn|signal, done, ...}. `mailbox` holds USR messages the
    current state has not committed past yet.
    """

    id: str
    agent: str
    state: str
    parent: str | None = None
    name: str | None = None
    input: dict[str, Any] = field(default_factory=dict)
    code: str = ""
    schema_version: int = 1
    term_state: str | None = None
    revision: int = 0
    step: int = 0
    status: Status = "active"
    terminating: bool = False
    data: dict[str, Any] = field(default_factory=dict)
    reasons: list[dict[str, Any]] = field(default_factory=list)
    wake: dict[str, Any] | None = None
    intents: list[dict[str, Any]] = field(default_factory=list)
    children: dict[str, str] = field(default_factory=dict)
    published: dict[str, dict[str, Any]] = field(default_factory=dict)
    mailbox: list[dict[str, Any]] = field(default_factory=list)
    inbox_applied: int = 0
    notes: dict[str, Any] = field(default_factory=dict)
    #: A root attached to another root's workspace works in that one (None: its own).
    workspace: str | None = None
    #: How it works there: "ro" (reads the other run, publishes only under its own home) or "rw".
    workspace_mode: str = "ro"
    #: What was resolved when it was created, pinned for its whole life: {name, version, code}.
    definition: dict[str, Any] = field(default_factory=dict)
    #: The effects it may call, each pinned: {effect: {provider, operation, version,
    #: schema_digest, policy_digest}}.
    bindings: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: A forked root reads an earlier run's publications as its own: {"root": <run>,
    #: "agents": [<names or agent ids kept>]} (all of that run's agents when no list).
    inherits: dict[str, Any] | None = None
    error: str | None = None

    @property
    def ended(self) -> bool:
        return self.status in ENDED

    def reason(self, kind: str) -> dict[str, Any] | None:
        return next((r for r in self.reasons if r["kind"] == kind), None)

    @property
    def pending_intents(self) -> list[dict[str, Any]]:
        return [i for i in self.intents if not i["done"]]

    def key(self, *parts: str) -> str:
        """`<agent>/<step>/<state>/<parts...>`: stable across retries of this state."""
        return "/".join([self.id, str(self.step), self.state, *parts])

    def staging(self, epoch: int) -> str:
        """The current attempt's staging folder, relative to the workspace."""
        return f"home/{self.id}/staging/{self.step}/{epoch}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Record:
        known = cls.__dataclass_fields__
        return cls(**{k: v for k, v in raw.items() if k in known})

    def but(self, **changes: Any) -> Record:
        return replace(self, **changes)


@dataclass(frozen=True)
class Transition:
    """What a state that finished asks for. Exactly one of: `state`, `wait`, `done`.

    `wait` is a reason dict (join/answer/sleep) without engine-made fields; `then` is the state
    that runs when it clears. `spawns` maps a child name to (agent, input). `signals` are
    (target id, signal) pairs sent after the commit. `published` maps an artifact type to its
    reference. `signals_seen` is how many mailbox messages this state consumed.
    """

    state: str | None = None
    wait: dict[str, Any] | None = None
    then: str | None = None
    done: bool = False
    data: dict[str, Any] = field(default_factory=dict)
    spawns: dict[str, tuple[str, dict[str, Any]]] = field(default_factory=dict)
    #: Spawned children that work in another root's workspace: {child name: root}.
    attach: dict[str, str] = field(default_factory=dict)
    signals: list[tuple[str, Any]] = field(default_factory=list)
    published: dict[str, dict[str, Any]] = field(default_factory=dict)
    signals_seen: int = 0
    notes: dict[str, Any] = field(default_factory=dict)
    schema_version: int | None = None
    #: The step's effect calls (observability): committed in the step's history entry.
    crossings: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class Fault:
    """A state raised. Committed with the same fencing as a transition."""

    error: str
    crossings: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ChildView:
    """What a parent's join needs to know about one child."""

    status: Status
    fault: str | None = None

    @classmethod
    def of(cls, record: Record) -> ChildView:
        fault = record.reason("fault")
        return cls(record.status, fault["error"] if fault else None)
