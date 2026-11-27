"""`StepContext`: the runtime's `ctx`, built for one step and dropped after it (the contract is
`statecraft.sdk.context.Context`).

Everything here may run again when the state is retried, so writes are idempotent: artifacts are
content-addressed objects, staging is per attempt, and `effect_key` is stable across retries.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import TYPE_CHECKING, Any

from ..kernel.process import Lease, Record
from ..sdk.agent import StepEffects
from ..sdk.contracts import Artifact
from .workspace import READABLE, AgentFiles, Files, home

if TYPE_CHECKING:
    from .registry import AgentDefinition
    from .runtime import Runtime


class NotDeclared(PermissionError):
    """An agent read or wrote an artifact type its definition does not declare."""


class ContractBroken(ValueError):
    """An artifact does not meet its type's contract; it was not staged."""


class Ambiguous(LookupError):
    """More than one agent of the tree published that artifact type; name the one (`by=`)."""


def dumps(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")


def short_id(record: Record) -> str:
    return record.agent.rpartition(":")[2] or record.agent


class StepContext:
    def __init__(
        self,
        runtime: Runtime,
        record: Record,
        lease: Lease,
        definition: AgentDefinition,
        root_input: dict[str, Any],
        files: Files,
        workspace: tuple[str, str],
    ) -> None:
        self._runtime = runtime
        self._files = files
        self._workspace_id, self._mode = workspace
        self.record = record
        self.lease = lease
        self.definition = definition
        self.effects = StepEffects()
        self.files = AgentFiles(self)
        self.parameters = {**root_input, **record.input}
        self._tree: list[Record] | None = None
        self._inflight: set[asyncio.Future[Any]] = set()
        self._readable = (*READABLE, *runtime.view_paths)

    # ---- what the step starts with -----------------------------------------------------------
    @property
    def id(self) -> str:
        return self.record.id

    @property
    def name(self) -> str:
        """The name its parent gave it (the root's id for a root)."""
        return self.record.name or self.record.id

    @property
    def children(self) -> dict[str, str]:
        return dict(self.record.children)

    @property
    def input(self) -> dict[str, Any]:
        return self.record.input

    @property
    def step(self) -> int:
        return self.record.step

    @property
    def state_name(self) -> str:
        return self.record.state

    @property
    def woke(self) -> dict[str, Any]:
        """The input of the state a wait woke (or `TERM`); `{}` otherwise."""
        return self.record.wake or {}

    @property
    def signals(self) -> list[Any]:
        """USR messages; reading them consumes them when this state commits."""
        self.effects.signals_seen = len(self.record.mailbox)
        return [m["message"] for m in self.record.mailbox]

    @property
    def staging(self) -> str:
        return self.record.staging(self.lease.epoch)

    def effect_key(self, name: str) -> str:
        return self.record.key(name)

    def service(self, name: str) -> Any:
        """An object an extension provided (`registrar.provide`); None when absent."""
        return self._runtime.registry.services.get(name)

    def extension(self, name: str) -> dict[str, Any]:
        """The definition's `extensions.<name>` settings (opaque to the runtime)."""
        return self.definition.extension(name)

    # ---- tools --------------------------------------------------------------------------------
    async def call(
        self, effect: str, /, *, idempotency_key: str | None = None, **arguments: Any
    ) -> Any:
        """Call a permitted tool through the effect manager. `idempotency_key` is for
        deduplicated tools (`ctx.effect_key(...)`)."""
        return await self._runtime.effects.call(self, effect, arguments, idempotency_key)

    def tools(self) -> list[str]:
        """The tools this instance may call (its pinned bindings)."""
        return sorted(self.record.bindings)

    def tool_spec(self, effect: str) -> dict[str, Any] | None:
        """A permitted tool's description and schemas (for agents that offer tools onward)."""
        binding = self.record.bindings.get(effect)
        found = self._runtime.registry.effect(effect, binding) if binding else None
        if found is None:
            return None
        return {
            "name": found.name,
            "description": found.description,
            "input_schema": found.input_schema,
            "output_schema": found.output_schema,
        }

    # ---- artifacts ----------------------------------------------------------------------------
    async def write(self, artifact: Artifact | str, payload: Any = None, **envelope: Any) -> str:
        """Store the artifact as a create-only object; this step's commit publishes it."""
        if isinstance(artifact, str):
            artifact = self._runtime.envelope(
                artifact_type=artifact, payload=payload or {}, **envelope
            )
        kind = artifact.artifact_type
        if not self.definition.may_write(kind):
            raise NotDeclared(f"agent {self.definition.name} does not declare writing {kind}")
        contracts = self._runtime.contracts
        if contracts is not None:
            errors = await contracts.check(kind, artifact.payload)
            if errors:
                raise ContractBroken(f"{kind} does not meet its contract: " + "; ".join(errors))
        body = dumps(artifact.model_dump())
        sha = hashlib.sha256(body).hexdigest()
        path = f"{home(self.id)}/objects/{sha}.json"
        await self._files.create(path, body)
        n = len(self.effects.published)
        self.effects.published[kind] = {"path": path, "sha256": sha, "step": self.step, "n": n}
        return path

    async def _records(self) -> list[Record]:
        if self._tree is None:
            self._tree = await self._runtime.workspaces.visible_tree(self.record.id.split("/")[0])
        return self._tree

    async def _published_paths(self) -> set[str]:
        own = {ref["path"] for ref in self.effects.published.values()}
        return own | {ref["path"] for r in await self._records() for ref in r.published.values()}

    def _check_read(self, kind: str) -> None:
        if not self.definition.may_read(kind):
            raise NotDeclared(f"agent {self.definition.name} does not declare reading {kind}")

    async def _ref(self, kind: str, by: str | None) -> dict[str, Any] | None:
        if kind in self.effects.published and by in (None, self.id):
            return self.effects.published[kind]
        records = await self._records()
        chosen = records if by is None else self._named(records, by)
        found = [r for r in chosen if kind in r.published]
        if not found:
            return None
        if len(found) > 1:
            raise Ambiguous(f"{kind} is published by {[r.id for r in found]}; pass by=")
        return found[0].published[kind]

    def _named(self, records: list[Record], by: str) -> list[Record]:
        """`by`: my child's name, a sibling's name, a name in the tree, an agent id, in that
        order; the first rule that matches decides."""
        rules = (
            lambda r: r.id == self.record.children.get(by),
            lambda r: r.name == by and r.parent == self.record.parent,
            lambda r: r.name == by,
            lambda r: r.id == by or short_id(r) == by,
        )
        for rule in rules:
            found = [r for r in records if rule(r)]
            if found:
                return found
        return []

    async def read(self, kind: str, by: str | None = None) -> Artifact:
        """A published artifact of the tree: `read("documents")`, or `by=` a child's name or an
        agent's id when several agents publish the type."""
        self._check_read(kind)
        ref = await self._ref(kind, by)
        if ref is None:
            raise KeyError(f"no agent of this tree has published {kind}")
        found = json.loads(await self._runtime.workspaces.read_object(ref["path"]))
        return self._runtime.envelope.model_validate(found)

    async def has(self, kind: str, by: str | None = None) -> bool:
        self._check_read(kind)
        return await self._ref(kind, by) is not None

    async def state(self) -> dict[str, Any]:
        """The tree's cumulative state (notes and publications per agent, or the application's)."""
        return await self._runtime.state_of(await self._records())

    async def describe(self, root: str) -> dict[str, Any]:
        """Another run's progress, read only."""
        tree = await self._runtime.workspaces.visible_tree(root)
        if not tree:
            raise KeyError(f"no root {root}")
        head = next(r for r in tree if r.id == root)
        return {
            "id": root,
            "agent": head.agent,
            "status": head.status,
            "succeeded": [r.name for r in tree if r.parent and r.status == "succeeded"],
            "faulted": [r.name for r in tree if r.reason("fault") is not None],
            "published": {r.name or r.id: sorted(r.published) for r in tree if r.published},
            "inherits": head.inherits,
        }

    def discard(self) -> None:
        """Drop what this step staged (artifacts, notes)."""
        self.effects.published.clear()
        self.effects.notes.clear()

    def note(self, **entry: Any) -> None:
        """Small facts kept on the record, committed with the step."""
        self.effects.notes.update(entry)

    def signal(self, target: str, signal: Any) -> None:
        """`target`: a child's name or an agent id; sent after the commit."""
        self.effects.signals.append((self.record.children.get(target, target), signal))


__all__ = ["Ambiguous", "ContractBroken", "NotDeclared", "StepContext", "dumps", "short_id"]
