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
    async def open(self, workspace_id: str) -> Files: ...
    async def close(self) -> None: ...


def clean(path: str) -> str:
    cleaned = normpath(str(path).lstrip("/"))
    if cleaned in (".", "") or cleaned.startswith(".."):
        raise ValueError(f"{path!r} is not a path inside the workspace")
    return cleaned


def home(agent_id: str) -> str:
    return f"home/{agent_id}"


__all__ = ["HOME_FOLDERS", "Files", "WorkspaceProvider", "clean", "home"]
