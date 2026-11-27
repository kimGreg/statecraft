"""The workspace port: what the runtime needs from file storage, and the path rules every
provider shares. Paths are relative and use `/`."""

from __future__ import annotations

from posixpath import normpath
from typing import Protocol

#: Reserved folder names in an agent's home (a child may not be named after one).
HOME_FOLDERS = frozenset({"private", "staging", "objects", "skills"})


class Files(Protocol):
    async def get(self, path: str) -> bytes: ...
    async def put(self, path: str, data: bytes) -> None: ...  # overwrite
    async def create(self, path: str, data: bytes) -> bool: ...  # create-only
    async def exists(self, path: str) -> bool: ...
    async def list(self, prefix: str) -> list[str]: ...


class WorkspaceProvider(Protocol):
    """A provider may declare `capabilities`: `mountable` (it has `host_path(workspace_id,
    relative) -> str`, a host folder a sandbox can mount) and `internal_links` (reads follow
    links that stay within their area)."""

    async def open(self, workspace_id: str) -> Files: ...
    async def close(self) -> None: ...


#: Where an agent's areas appear inside a sandbox (a provider maps them; agents see these).
MOUNT_TARGETS = {"shared": "/workspace", "private": "/home/agent"}


def mount_plan(scope: dict) -> list[dict]:
    """What a sandbox may mount for an agent, from its authorized scope: the workspace's
    `shared` folder with the granted mode, and the agent's own private folder read-write.
    Never staging, objects, skills, the runtime's `run/` folder or any receipt storage."""
    return [
        {"source": "shared", "target": MOUNT_TARGETS["shared"], "mode": scope["shared"]},
        {"source": scope["private"], "target": MOUNT_TARGETS["private"], "mode": "rw"},
    ]


def clean(path: str) -> str:
    cleaned = normpath(str(path).lstrip("/"))
    if cleaned in (".", "") or cleaned.startswith(".."):
        raise ValueError(f"{path!r} is not a path inside the workspace")
    return cleaned


def home(agent_id: str) -> str:
    return f"home/{agent_id}"


def link_anchor(parts: list[str]) -> int | None:
    """How many leading components of a path form the area a symbolic link inside it may stay
    within: the `shared` folder, or one folder of an agent's home (`home/<agent>/private`, ...).
    None for paths outside such an area (links are not followed there)."""
    for i, part in enumerate(parts):
        if part == "shared":
            return i + 1
        if part == "home":
            for j in range(i + 2, len(parts)):
                if parts[j] in HOME_FOLDERS:
                    return j + 1
            return None
    return None


__all__ = [
    "HOME_FOLDERS",
    "MOUNT_TARGETS",
    "Files",
    "WorkspaceProvider",
    "clean",
    "home",
    "link_anchor",
    "mount_plan",
]
