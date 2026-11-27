"""Workspace providers: where a workspace's bytes are kept. The rules inside a workspace (homes,
staging per attempt, create-only objects, who may read and write what) are the runtime's
(`statecraft.runtime.workspace`) and hold on every provider.

    class WorkspaceProvider(Protocol):
        async def open(self, workspace_id: str) -> Files   # get, put, create-only, exists, list
        async def close(self) -> None

`LocalWorkspace(root)` keeps each workspace in `<root>/<workspace id>/`; `MemoryWorkspace` keeps
them in memory (tests).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from ..kernel.files import Files, clean


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
        except (FileNotFoundError, IsADirectoryError):
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


class MemoryFiles:
    """Files in a dict (tests)."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    async def get(self, path: str) -> bytes:
        try:
            return self.files[clean(path)]
        except KeyError:
            raise FileNotFoundError(path) from None

    async def put(self, path: str, data: bytes) -> None:
        self.files[clean(path)] = bytes(data)

    async def create(self, path: str, data: bytes) -> bool:
        key = clean(path)
        if key in self.files:
            return False
        self.files[key] = bytes(data)
        return True

    async def exists(self, path: str) -> bool:
        return clean(path) in self.files

    async def list(self, prefix: str) -> list[str]:
        start = prefix.lstrip("/")
        return sorted(p for p in self.files if p.startswith(start))


class ScopedFiles:
    """One workspace inside a shared `Files` (`<workspace id>/...`)."""

    def __init__(self, files: Any, workspace_id: str) -> None:
        self.files = files
        self.prefix = clean(workspace_id) + "/"

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


class LocalWorkspace:
    """Each workspace is a folder under `root`."""

    def __init__(self, root: str | Path) -> None:
        self.files = LocalFiles(root)

    async def open(self, workspace_id: str) -> Files:
        return ScopedFiles(self.files, workspace_id)

    async def close(self) -> None:
        return None


class MemoryWorkspace:
    def __init__(self) -> None:
        self.files = MemoryFiles()

    async def open(self, workspace_id: str) -> Files:
        return ScopedFiles(self.files, workspace_id)

    async def close(self) -> None:
        return None


__all__ = ["LocalFiles", "LocalWorkspace", "MemoryFiles", "MemoryWorkspace", "ScopedFiles"]
