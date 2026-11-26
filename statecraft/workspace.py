"""The workspace port: business files of one root's tree (inputs, artifacts, scratch, skills).

Layout (logical, relative to the root's workspace):

    shared/                         read-write for every agent of the tree
    home/<agent-id>/private/        the agent's scratch: may be lost or rewritten by a stale worker
    home/<agent-id>/staging/<step>/<epoch>/   one attempt's work in progress
    home/<agent-id>/objects/<sha256>.json     content-addressed, create-only artifacts
    home/<agent-id>/skills/<name>/  its installed skills
    run/                            the engine's projections; agents never write it

A child's id is `<parent id>/<name>`, so homes nest; a child name may not be one of the reserved
folder names of a home.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from posixpath import normpath
from typing import Protocol

HOME_FOLDERS = frozenset({"private", "staging", "objects", "skills"})


class Files(Protocol):
    """What the engine needs from file storage. Paths are relative and use `/`."""

    async def get(self, path: str) -> bytes: ...
    async def put(self, path: str, data: bytes) -> None: ...  # overwrite
    async def create(self, path: str, data: bytes) -> bool: ...  # create-only
    async def exists(self, path: str) -> bool: ...
    async def list(self, prefix: str) -> list[str]: ...


def clean(path: str) -> str:
    cleaned = normpath(str(path).lstrip("/"))
    if cleaned in (".", "") or cleaned.startswith(".."):
        raise ValueError(f"{path!r} is not a path inside the workspace")
    return cleaned


def home(agent_id: str) -> str:
    return f"home/{agent_id}"


class LocalFiles:
    """A folder on disk. Writes go through a temporary file and a rename, so a reader never sees
    half a file; `create` uses an exclusive link, so two writers of the same object make one."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, path: str) -> Path:
        return self.root / clean(path)

    def _temp(self, target: Path, data: bytes) -> str:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=target.parent, prefix=".tmp-")
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        return temp

    async def get(self, path: str) -> bytes:
        try:
            return self._path(path).read_bytes()
        except FileNotFoundError:
            raise FileNotFoundError(path) from None

    async def put(self, path: str, data: bytes) -> None:
        target = self._path(path)
        os.replace(self._temp(target, data), target)

    async def create(self, path: str, data: bytes) -> bool:
        target = self._path(path)
        if target.exists():
            return False
        temp = self._temp(target, data)
        try:
            os.link(temp, target)
            return True
        except FileExistsError:
            return False
        finally:
            os.unlink(temp)

    async def exists(self, path: str) -> bool:
        return self._path(path).exists()

    async def list(self, prefix: str) -> list[str]:
        base = self.root / clean(prefix) if prefix.strip("/") else self.root
        if not base.is_dir():
            return []
        return sorted(
            str(p.relative_to(self.root))
            for p in base.rglob("*")
            if p.is_file() and not p.name.startswith(".tmp-")
        )


class ScopedFiles:
    """One root's workspace inside a shared `Files` (`<root id>/...`)."""

    def __init__(self, files: Files, root_id: str) -> None:
        self.files = files
        self.prefix = clean(root_id) + "/"

    def _p(self, path: str) -> str:
        return self.prefix + clean(path)

    async def get(self, path: str) -> bytes:
        return await self.files.get(self._p(path))

    async def put(self, path: str, data: bytes) -> None:
        await self.files.put(self._p(path), data)

    async def create(self, path: str, data: bytes) -> bool:
        return await self.files.create(self._p(path), data)

    async def exists(self, path: str) -> bool:
        return await self.files.exists(self._p(path))

    async def list(self, prefix: str) -> list[str]:
        found = await self.files.list(self.prefix + prefix.lstrip("/"))
        return [f.removeprefix(self.prefix) for f in found]
