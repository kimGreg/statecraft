"""What a running state gets: `ctx`. Built for one step and dropped after it.

    await ctx.call("maps.measure", **arguments)   # an MCP tool the agent declares
    await ctx.write("measurement", payload, summary=...)  # stage an artifact; the step's
                                                             # commit publishes it
    await ctx.read("operational_area")                       # a published artifact of the tree
    await ctx.state()                                        # what earlier agents recorded
    await ctx.files.read("shared/input.json")                # workspace files
    ctx.signal("child", "STOP")                              # sent after the commit
    ctx.woke, ctx.signals, ctx.input, ctx.effect_key("send")

Spawning, joining, asking a person and sleeping are what a state returns, not ctx calls.
Everything here may run again when the state is retried, so writes are idempotent: artifacts are
content-addressed objects, staging is per attempt, and `effect_key` is stable across retries.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from .agent import StepEffects
from .artifacts import Artifact
from .process import Lease, Record
from .spec import AGENT_PREFIX, AgentSpec
from .workspace import HOME_FOLDERS, Files, clean, home

if TYPE_CHECKING:
    from .executor import Services


class NotDeclared(PermissionError):
    """An agent read, wrote or called something its `agent.yaml` does not declare."""


class NotAllowed(PermissionError):
    """A workspace path the agent may not read or write."""


class ContractBroken(ValueError):
    """An artifact does not meet its type's contract; it was not staged."""


class Ambiguous(LookupError):
    """More than one agent of the tree published that artifact type; name the one (`by=`)."""


def dumps(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")


def short_id(record: Record) -> str:
    return record.agent.partition(":")[2] or record.agent


class AgentFiles:
    """Workspace files, with the permissions of one agent."""

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def _mine(self, path: str) -> bool:
        return path.startswith(home(self._ctx.id) + "/") and not any(
            path.startswith(home(c) + "/") for c in self._ctx.record.children.values()
        )

    async def _check_read(self, path: str) -> None:
        if path.startswith(self._ctx._services.readable) or self._mine(path):
            return
        if path.startswith("home/"):
            rest = path.removeprefix("home/").split("/")
            folder = next((i for i, part in enumerate(rest) if part in HOME_FOLDERS), None)
            if folder is not None and rest[folder] == "skills":
                return
            if folder is not None and rest[folder] == "objects":
                if path in await self._ctx._published_paths():
                    return
                raise NotAllowed(f"{path} is not a published object")
        raise NotAllowed(f"{self._ctx.id} may not read {path}")

    def _check_write(self, path: str) -> None:
        mine = home(self._ctx.id) + "/"
        if path.startswith("shared/"):
            return
        if path.startswith(mine + "private/") and self._mine(path):
            return
        if path.startswith(self._ctx.staging + "/"):
            return
        raise NotAllowed(f"{self._ctx.id} may not write {path}")

    async def read(self, path: str) -> bytes:
        path = clean(path)
        await self._check_read(path)
        parts = path.split("/")
        if parts[0] == "home" and len(parts) > 2 and parts[1] != self._ctx.id.split("/")[0]:
            return await self._ctx._services.read_object(path)  # another root's (inherited)
        return await self._ctx._files.get(path)

    async def read_text(self, path: str) -> str:
        return (await self.read(path)).decode("utf-8")

    async def exists(self, path: str) -> bool:
        path = clean(path)
        await self._check_read(path)
        return await self._ctx._files.exists(path)

    async def write(self, path: str, data: bytes | str) -> None:
        path = clean(path)
        self._check_write(path)
        await self._ctx._files.put(path, data.encode("utf-8") if isinstance(data, str) else data)

    async def list(self, prefix: str) -> list[str]:
        found = await self._ctx._files.list(prefix)
        allowed = []
        for path in found:
            try:
                await self._check_read(path)
                allowed.append(path)
            except NotAllowed:
                continue
        return allowed


class Context:
    def __init__(
        self,
        services: Services,
        record: Record,
        lease: Lease,
        spec: AgentSpec | None,
        root_input: dict[str, Any],
    ) -> None:
        self._services = services
        self._files: Files = services.files_of(record.id)
        self.record = record
        self.lease = lease
        self.spec = spec
        self.effects = StepEffects()
        self.files = AgentFiles(self)
        self.parameters = {**root_input, **record.input}
        self._tree: list[Record] | None = None

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
        """Its children: name -> agent id."""
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
        """The input of the state a wait woke (or `TERM`); `{}` otherwise. Durable until the
        state commits."""
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

    @property
    def model(self) -> Any:
        return self._services.model

    # ---- tools ----------------------------------------------------------------------------
    async def call(self, name: str, /, **arguments: Any) -> Any:
        """Call an MCP tool (`server.tool`). Agents are spawned by returning `Spawn`."""
        if name.startswith(AGENT_PREFIX):
            raise NotDeclared(f"{name}: spawn agents by returning Spawn, not ctx.call")
        if self.spec is not None and not self.spec.may_call(name):
            raise NotDeclared(f"agent {self.spec.id} did not declare {name}")
        server, _, tool = name.partition(".")
        self._services.diagnostic(self.record, "tool.called", tool=name)
        result = await self._services.tools_of(self.id).call(server, tool, arguments)
        self._services.diagnostic(self.record, "tool.returned", tool=name)
        return result

    async def tool(self, server: str, name: str, arguments: dict[str, Any] | None = None) -> Any:
        return await self.call(f"{server}.{name}", **(arguments or {}))

    # ---- artifacts --------------------------------------------------------------------------
    async def write(self, artifact: Artifact | str, payload: Any = None, **envelope: Any) -> str:
        """Store the artifact as a create-only object; this step's commit publishes it."""
        if isinstance(artifact, str):
            artifact = self._services.envelope(
                artifact_type=artifact, payload=payload or {}, **envelope
            )
        kind = artifact.artifact_type
        if self.spec is not None and not self.spec.may_write(kind):
            raise NotDeclared(f"agent {self.spec.id} did not declare writing {kind}")
        contracts = self._services.contracts
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
            self._tree = await self._services.visible_tree(self.record.id.split("/")[0])
        return self._tree

    async def _published_paths(self) -> set[str]:
        own = {ref["path"] for ref in self.effects.published.values()}
        return own | {ref["path"] for r in await self._records() for ref in r.published.values()}

    def _check_read(self, kind: str) -> None:
        if self.spec is not None and not self.spec.may_read(kind):
            raise NotDeclared(f"agent {self.spec.id} did not declare reading {kind}")

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
        """A published artifact of the tree: `read("documents")`, or `by=` a child's name or
        an agent's id when several agents publish the type."""
        self._check_read(kind)
        ref = await self._ref(kind, by)
        if ref is None:
            raise KeyError(f"no agent of this tree has published {kind}")
        found = json.loads(await self._services.read_object(ref["path"]))
        return self._services.envelope.model_validate(found)

    async def has(self, kind: str, by: str | None = None) -> bool:
        self._check_read(kind)
        return await self._ref(kind, by) is not None

    # ---- the tree's cumulative state (a projection of publications and notes) ---------------
    async def state(self) -> dict[str, Any]:
        """The tree's cumulative state: by default each agent's notes and what it published
        (`views.agents_state`); the application's own when it gave the engine one."""
        return await self._services.state_of(await self._records())

    async def describe(self, root: str) -> dict[str, Any]:
        """Another run's progress, read only: its agent and status, which of its agents (by
        name) succeeded, which are suspended on a fault, and what each published."""
        tree = await self._services.visible_tree(root)  # a fork's inherited agents count too
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
        """Drop what this step staged (artifacts, notes): it ends at a boundary before its work
        is finished, and runs again later. Staged objects stay as unreferenced files."""
        self.effects.published.clear()
        self.effects.notes.clear()

    def note(self, **entry: Any) -> None:
        """Add names and counts to this agent's entry in `state()`, committed with the step."""
        self.effects.notes.update(entry)

    # ---- control, sent after the commit ------------------------------------------------------
    def signal(self, target: str, signal: Any) -> None:
        """`target`: a child's name or an agent id."""
        self.effects.signals.append((self.record.children.get(target, target), signal))

    async def skill(self, name: str) -> str:
        """The text of an installed skill's SKILL.md."""
        return await self.files.read_text(f"{home(self.id)}/skills/{name}/SKILL.md")


__all__ = [
    "AgentFiles",
    "Ambiguous",
    "Context",
    "ContractBroken",
    "NotAllowed",
    "NotDeclared",
    "dumps",
    "short_id",
]
