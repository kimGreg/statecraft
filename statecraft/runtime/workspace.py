"""The workspace service: business files of a root's tree, with the rules the kernel's guarantees
depend on (architecture §8). Bytes are kept by a workspace provider (`statecraft.providers`).

Layout (relative to a workspace):

    shared/                         read-write for every agent of the tree (not for attached roots)
    shared/input/                   the root's input
    home/<agent-id>/private/        the agent's scratch: may be lost or rewritten by a stale worker
    home/<agent-id>/staging/<step>/<epoch>/   one attempt's work in progress
    home/<agent-id>/objects/<sha256>.json     content-addressed, create-only artifacts
    run/                            the runtime's projections; agents never write it

A root gets a workspace three ways: **create** (its own), **attach** (it works against another
root's workspace: read-only by default, it publishes only under its own home; `rw` must be granted
explicitly), **fork** (its own workspace; it reads an earlier run's committed outputs for the
agents it keeps, by reference, nothing copied).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..kernel.files import HOME_FOLDERS, Files, WorkspaceProvider, clean, home
from ..kernel.process import Record
from ..kernel.store import StateStore

if TYPE_CHECKING:
    from .context import StepContext

#: Paths every agent may read besides the views': shared files and the runtime's files.
READABLE = ("shared/", "run/")


class NotAllowed(PermissionError):
    """A workspace path the agent may not read or write."""


class WorkspaceService:
    """Which workspace each agent works in, opened through the provider, and the fork view."""

    def __init__(self, provider: WorkspaceProvider, store: StateStore) -> None:
        self.provider = provider
        self.store = store
        #: agent id -> (workspace id, mode) for every agent whose record was seen; "" inherits
        self._sessions: dict[str, tuple[str, str]] = {}
        self._open: dict[str, Files] = {}

    # ---- sessions -----------------------------------------------------------------------------
    def remember(self, record: Record) -> None:
        if record.workspace:
            self._sessions[record.id] = (record.workspace, record.workspace_mode or "ro")
        elif "/" not in record.id:
            self._sessions[record.id] = (record.id, "own")
        else:
            self._sessions[record.id] = ("", "")

    async def prepare(self, agent_id: str) -> None:
        """Learn the workspace of the agent and its ancestors from their records."""
        parts = agent_id.split("/")
        for depth in range(1, len(parts) + 1):
            key = "/".join(parts[:depth])
            if key not in self._sessions:
                record = await self.store.find(key)
                if record is not None:
                    self.remember(record)

    def session_of(self, agent_id: str) -> tuple[str, str]:
        """(workspace id, mode) of its nearest ancestor (itself included) with its own workspace
        or an attachment: "own" for a root's own workspace, "ro" or "rw" when attached."""
        parts = agent_id.split("/")
        for depth in range(len(parts), 0, -1):
            found = self._sessions.get("/".join(parts[:depth]))
            if found and found[0]:
                return found
        return (parts[0], "own")

    async def files_of(self, agent_id: str) -> Files:
        await self.prepare(agent_id)
        workspace_id, _mode = self.session_of(agent_id)
        return await self.open(workspace_id)

    async def open(self, workspace_id: str) -> Files:
        if workspace_id not in self._open:
            self._open[workspace_id] = await self.provider.open(workspace_id)
        return self._open[workspace_id]

    async def close(self) -> None:
        self._open.clear()
        await self.provider.close()

    # ---- fork view ----------------------------------------------------------------------------
    async def visible_tree(self, root: str) -> list[Record]:
        """The root's tree, and what it inherits from earlier runs: their records for the agents
        it keeps (by name or agent id), unless its own tree has an agent of that name."""
        own = await self.store.tree(root)
        record = next((r for r in own if r.id == root), None)
        inherits = record.inherits if record is not None else None
        if not inherits:
            return own
        mine = {r.name for r in own if r.parent}
        kept = set(inherits.get("agents") or [])
        found = []
        for other in await self.visible_tree(str(inherits["root"])):
            if other.parent is None or other.name in mine:
                continue
            short = other.agent.rpartition(":")[2]
            if not kept or other.name in kept or short in kept:
                found.append(other)
        return [*own, *found]

    async def read_object(self, path: str) -> bytes:
        """An object or file of any agent, read in the workspace of the agent whose home the path
        names (`home/<agent id>/<objects|private|staging|skills>/...`)."""
        parts = path.split("/")
        owner = ""
        if parts[0] == "home" and len(parts) > 1:
            folder = next((i for i, p in enumerate(parts) if i > 1 and p in HOME_FOLDERS), None)
            owner = "/".join(parts[1:folder]) if folder else parts[1]
        if not owner:
            raise FileNotFoundError(path)
        return await (await self.files_of(owner)).get(path)


class AgentFiles:
    """Workspace files, with the permissions of one agent (`ctx.files`)."""

    def __init__(self, ctx: StepContext) -> None:
        self._ctx = ctx

    def _mine(self, path: str) -> bool:
        return path.startswith(home(self._ctx.id) + "/") and not any(
            path.startswith(home(c) + "/") for c in self._ctx.record.children.values()
        )

    async def _check_read(self, path: str) -> None:
        if path.startswith(self._ctx._readable) or self._mine(path):
            return
        if path.startswith("home/"):
            rest = path.removeprefix("home/").split("/")
            # rest[0] is the root's id, never a home folder (a root may not be named like one)
            folder = next(
                (i for i, part in enumerate(rest) if i > 0 and part in HOME_FOLDERS), None
            )
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
            if self._ctx._mode == "ro":
                raise NotAllowed(
                    f"{self._ctx.id} works attached (read-only) to another run's workspace; "
                    "it publishes under its own home and may not write shared/"
                )
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
            return await self._ctx._runtime.workspaces.read_object(path)  # another root's
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


def input_path(record: Record) -> str:
    """Where a root's input is written: `shared/input/input.json` in its own workspace; under its
    own home when it is attached to another run's (it may not write that run's shared/)."""
    return f"{home(record.id)}/input.json" if record.workspace else "shared/input/input.json"


__all__ = [
    "HOME_FOLDERS",
    "READABLE",
    "AgentFiles",
    "Files",
    "NotAllowed",
    "WorkspaceProvider",
    "WorkspaceService",
    "clean",
    "home",
    "input_path",
]
