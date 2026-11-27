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

import contextlib
import errno
import os
import secrets
import stat
from pathlib import Path
from typing import Any

from ..kernel.files import Files, clean


class Escape(PermissionError):
    """A path that would leave the workspace (a symbolic link in it): refused."""


_DIR = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


class LocalFiles:
    """A folder on disk, contained: every path is opened one component at a time, relative to
    an open directory and without following symbolic links (`O_NOFOLLOW`), so no path, and no
    link created or swapped in while it is opened, leads outside the folder. Links inside the
    workspace are refused, not followed; listings skip them.

    Writes go through a temporary file and a rename, so a reader never sees half a file; the
    directory is synced after, so a published file survives a power loss with its commit.
    `create` uses an exclusive link, so two writers of the same object make one.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        if not (_NOFOLLOW and os.open in os.supports_dir_fd):
            raise OSError("LocalFiles needs a platform with O_NOFOLLOW and dir_fd (POSIX)")

    # ---- contained opening -------------------------------------------------------------------
    def _dir(self, parts: list[str], *, create: bool) -> int:
        """An open descriptor of the folder `parts` (relative to the root); the caller closes
        it. Raises FileNotFoundError, or Escape for a component that is a link."""
        fd = os.open(self.root, _DIR)
        try:
            for part in parts:
                try:
                    nxt = os.open(part, _DIR | _NOFOLLOW, dir_fd=fd)
                except FileNotFoundError:
                    if not create:
                        raise
                    with contextlib.suppress(FileExistsError):
                        os.mkdir(part, dir_fd=fd)
                    nxt = os.open(part, _DIR | _NOFOLLOW, dir_fd=fd)
                except OSError as exc:
                    if exc.errno in (errno.ELOOP, errno.ENOTDIR, errno.EMLINK):
                        raise Escape(f"{part!r} is a link or not a folder: not followed") from None
                    raise
                os.close(fd)
                fd = nxt
            return fd
        except BaseException:
            os.close(fd)
            raise

    def _split(self, path: str) -> tuple[list[str], str]:
        parts = clean(path).split("/")
        return parts[:-1], parts[-1]

    async def get(self, path: str) -> bytes:
        folders, name = self._split(path)
        try:
            dfd = self._dir(folders, create=False)
        except (FileNotFoundError, NotADirectoryError):
            raise FileNotFoundError(path) from None
        try:
            fd = os.open(name, os.O_RDONLY | _NOFOLLOW, dir_fd=dfd)
        except FileNotFoundError:
            raise FileNotFoundError(path) from None
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise Escape(f"{path!r} is a link: not followed") from None
            if exc.errno == errno.EISDIR:
                raise FileNotFoundError(path) from None
            raise
        finally:
            os.close(dfd)
        with os.fdopen(fd, "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise FileNotFoundError(path)
            return handle.read()

    def _temp(self, dfd: int, data: bytes) -> str:
        name = f".tmp-{secrets.token_hex(8)}"
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW, 0o644, dir_fd=dfd)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        return name

    async def put(self, path: str, data: bytes) -> None:
        folders, name = self._split(path)
        dfd = self._dir(folders, create=True)
        try:
            temp = self._temp(dfd, data)
            os.replace(temp, name, src_dir_fd=dfd, dst_dir_fd=dfd)  # replaces a link, never follows
            os.fsync(dfd)
        finally:
            os.close(dfd)

    async def create(self, path: str, data: bytes) -> bool:
        folders, name = self._split(path)
        dfd = self._dir(folders, create=True)
        try:
            try:
                os.stat(name, dir_fd=dfd, follow_symlinks=False)
                return False
            except FileNotFoundError:
                pass
            temp = self._temp(dfd, data)
            try:
                os.link(temp, name, src_dir_fd=dfd, dst_dir_fd=dfd, follow_symlinks=False)
                os.fsync(dfd)
                return True
            except FileExistsError:
                return False
            finally:
                os.unlink(temp, dir_fd=dfd)
        finally:
            os.close(dfd)

    async def exists(self, path: str) -> bool:
        folders, name = self._split(path)
        try:
            dfd = self._dir(folders, create=False)
        except (FileNotFoundError, NotADirectoryError, Escape):
            return False  # through a link: not a file of the workspace
        try:
            st = os.stat(name, dir_fd=dfd, follow_symlinks=False)
            return stat.S_ISREG(st.st_mode)  # a link is not a file of the workspace
        except FileNotFoundError:
            return False
        finally:
            os.close(dfd)

    async def list(self, prefix: str) -> list[str]:
        """Files under the folder `prefix` (all files for an empty prefix); links skipped."""
        start = prefix.strip("/")
        parts = clean(start).split("/") if start else []
        try:
            top = self._dir(parts, create=False)
        except (FileNotFoundError, NotADirectoryError, Escape):
            return []
        found: list[str] = []

        def walk(fd: int, base: str) -> None:
            with os.scandir(fd) as entries:
                for entry in entries:
                    if entry.name.startswith(".tmp-") or entry.is_symlink():
                        continue
                    rel = f"{base}/{entry.name}" if base else entry.name
                    if entry.is_file(follow_symlinks=False):
                        found.append(rel)
                    elif entry.is_dir(follow_symlinks=False):
                        try:
                            sub = os.open(entry.name, _DIR | _NOFOLLOW, dir_fd=fd)
                        except OSError:
                            continue  # swapped for a link meanwhile: skipped
                        try:
                            walk(sub, rel)
                        finally:
                            os.close(sub)

        try:
            walk(top, "/".join(parts))
        finally:
            os.close(top)
        return sorted(found)


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
        """Files under the folder `prefix` (as LocalFiles)."""
        start = prefix.strip("/")
        if not start:
            return sorted(self.files)
        folder = clean(start) + "/"
        return sorted(p for p in self.files if p.startswith(folder))


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
        start = prefix.strip("/")
        inner = self.prefix + clean(start) if start else self.prefix  # never outside the scope
        found = await self.files.list(inner)
        return [f.removeprefix(self.prefix) for f in found if f.startswith(self.prefix)]


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


__all__ = [
    "Escape",
    "LocalFiles",
    "LocalWorkspace",
    "MemoryFiles",
    "MemoryWorkspace",
    "ScopedFiles",
]
