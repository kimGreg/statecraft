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
import ctypes
import errno
import functools
import os
import secrets
import stat
import sys
import time
from pathlib import Path
from typing import Any, ClassVar

from ..ports.workspace import Files, clean, link_anchor


class Escape(PermissionError):
    """A path that would leave the workspace (a symbolic link in it): refused."""


_DIR = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
#: a FIFO or device opened for reading must not block: only regular files are read
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)
_RESOLVE_BENEATH, _RESOLVE_NO_MAGICLINKS, _OPENAT2 = 0x08, 0x02, 437


class _OpenHow(ctypes.Structure):
    _fields_: ClassVar = [
        ("flags", ctypes.c_uint64),
        ("mode", ctypes.c_uint64),
        ("resolve", ctypes.c_uint64),
    ]


def _openat2(dir_fd: int, path: str, flags: int) -> int:
    """`openat2` resolving `path` beneath `dir_fd`: links are followed only while they stay
    beneath it (EXDEV otherwise), checked by the kernel, so no swap race can lead outside."""
    how = _OpenHow(flags, 0, _RESOLVE_BENEATH | _RESOLVE_NO_MAGICLINKS)
    fd = _libc().syscall(_OPENAT2, dir_fd, path.encode(), ctypes.byref(how), ctypes.sizeof(how))
    if fd < 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err), path)
    return fd


@functools.cache
def _libc() -> Any:
    return ctypes.CDLL(None, use_errno=True)


@functools.cache
def beneath_supported() -> bool:
    """Whether this platform resolves paths beneath a folder in the kernel (Linux 5.6+)."""
    if not sys.platform.startswith("linux"):
        return False
    try:
        os.close(_openat2(-100, ".", os.O_RDONLY | _DIR))
    except (OSError, AttributeError):
        return False
    return True


class LocalFiles:
    """A folder on disk, contained: every path is opened one component at a time, relative to
    an open directory and without following symbolic links (`O_NOFOLLOW`), so no path, and no
    link created or swapped in while it is opened, leads outside the folder. Links inside the
    workspace are refused, not followed; listings skip them.

    Writes go through a temporary file and a rename, so a reader never sees half a file; the
    directory is synced after, so a published file survives a power loss with its commit.
    `create` uses an exclusive link, so two writers of the same object make one.
    """

    def __init__(self, root: str | Path, *, internal_links: bool | None = None) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        if not (_NOFOLLOW and os.open in os.supports_dir_fd):
            raise OSError("LocalFiles needs a platform with O_NOFOLLOW and dir_fd (POSIX)")
        #: follow links that stay within their area (`shared/`, one home folder) when reading;
        #: only where the kernel resolves beneath a folder, refused elsewhere
        supported = beneath_supported()
        self.internal_links = supported if internal_links is None else internal_links
        if self.internal_links and not supported:
            raise OSError("internal links need openat2 (Linux 5.6 or later)")

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
                        os.fsync(fd)  # the new folder's entry survives a power loss
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

    def _anchor(self, path: str) -> int | None:
        if not self.internal_links:
            return None
        parts = clean(path).split("/")
        n = link_anchor(parts)
        return n if n is not None and n < len(parts) else None

    def _open_beneath(self, path: str, n: int) -> int:
        """Open a file for reading, following links only while they stay within its area."""
        parts = clean(path).split("/")
        try:
            dfd = self._dir(parts[:n], create=False)  # the area itself: no links on the way
        except (FileNotFoundError, NotADirectoryError):
            raise FileNotFoundError(path) from None
        try:
            return _openat2(dfd, "/".join(parts[n:]), os.O_RDONLY | _NONBLOCK)
        except OSError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.EISDIR):
                raise FileNotFoundError(path) from None
            if exc.errno in (errno.EXDEV, errno.ELOOP):
                raise Escape(f"{path!r} leads outside its area: not followed") from None
            raise
        finally:
            os.close(dfd)

    @staticmethod
    def _read(fd: int, path: str) -> bytes:
        with os.fdopen(fd, "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise FileNotFoundError(path)
            return handle.read()

    async def get(self, path: str) -> bytes:
        anchor = self._anchor(path)
        if anchor is not None:
            return self._read(self._open_beneath(path, anchor), path)
        folders, name = self._split(path)
        try:
            dfd = self._dir(folders, create=False)
        except (FileNotFoundError, NotADirectoryError):
            raise FileNotFoundError(path) from None
        try:
            fd = os.open(name, os.O_RDONLY | _NOFOLLOW | _NONBLOCK, dir_fd=dfd)
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
        return self._read(fd, path)

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
        anchor = self._anchor(path)
        if anchor is not None:
            try:
                fd = self._open_beneath(path, anchor)
            except (FileNotFoundError, Escape):
                return False
            try:
                return stat.S_ISREG(os.fstat(fd).st_mode)
            finally:
                os.close(fd)
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
        """Files under the folder `prefix` (all files for an empty prefix). A link is listed
        when it is a file that stays within its area (internal links); other links are skipped,
        and linked folders are not walked into."""
        start = prefix.strip("/")
        parts = clean(start).split("/") if start else []
        try:
            top = self._dir(parts, create=False)
        except (FileNotFoundError, NotADirectoryError, Escape):
            return []
        found: list[str] = []

        def linked_file(rel: str) -> bool:
            anchor = self._anchor(rel)
            if anchor is None:
                return False
            try:
                fd = self._open_beneath(rel, anchor)
            except (FileNotFoundError, Escape, OSError):
                return False
            try:
                return stat.S_ISREG(os.fstat(fd).st_mode)
            finally:
                os.close(fd)

        def walk(fd: int, base: str) -> None:
            with os.scandir(fd) as entries:
                for entry in entries:
                    if entry.name.startswith(".tmp-"):
                        continue
                    rel = f"{base}/{entry.name}" if base else entry.name
                    if entry.is_symlink():
                        if linked_file(rel):
                            found.append(rel)
                        continue
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

    # ---- upkeep (collectable): what garbage collection needs --------------------------------
    async def remove(self, path: str) -> bool:
        """Delete a file (a link is deleted, never followed); False when it is not there."""
        folders, name = self._split(path)
        try:
            dfd = self._dir(folders, create=False)
        except (FileNotFoundError, NotADirectoryError):
            return False
        try:
            os.unlink(name, dir_fd=dfd)
            os.fsync(dfd)
            return True
        except FileNotFoundError:
            return False
        finally:
            os.close(dfd)

    async def modified(self, path: str) -> float:
        """When the file was last written (seconds since the epoch), without following links."""
        folders, name = self._split(path)
        dfd = self._dir(folders, create=False)
        try:
            return os.stat(name, dir_fd=dfd, follow_symlinks=False).st_mtime
        finally:
            os.close(dfd)

    async def prune(self, prefix: str) -> int:
        """Remove the empty folders under the folder `prefix` (not `prefix` itself)."""
        parts = clean(prefix).split("/")
        try:
            top = self._dir(parts, create=False)
        except (FileNotFoundError, NotADirectoryError):
            return 0
        removed = 0

        def walk(fd: int) -> bool:  # True when the folder is empty afterwards
            nonlocal removed
            empty = True
            with os.scandir(fd) as entries:
                listed = list(entries)
            for entry in listed:
                if entry.is_dir(follow_symlinks=False):
                    sub = os.open(entry.name, _DIR | _NOFOLLOW, dir_fd=fd)
                    try:
                        inner_empty = walk(sub)
                    finally:
                        os.close(sub)
                    if inner_empty:
                        with contextlib.suppress(OSError):
                            os.rmdir(entry.name, dir_fd=fd)
                            removed += 1
                            continue
                empty = False
            return empty

        try:
            walk(top)
        finally:
            os.close(top)
        return removed


class MemoryFiles:
    """Files in a dict (tests)."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.times: dict[str, float] = {}

    async def get(self, path: str) -> bytes:
        try:
            return self.files[clean(path)]
        except KeyError:
            raise FileNotFoundError(path) from None

    async def put(self, path: str, data: bytes) -> None:
        self.files[clean(path)] = bytes(data)
        self.times[clean(path)] = time.time()

    async def create(self, path: str, data: bytes) -> bool:
        key = clean(path)
        if key in self.files:
            return False
        self.files[key] = bytes(data)
        self.times[key] = time.time()
        return True

    async def remove(self, path: str) -> bool:
        self.times.pop(clean(path), None)
        return self.files.pop(clean(path), None) is not None

    async def modified(self, path: str) -> float:
        if clean(path) not in self.files:
            raise FileNotFoundError(path)
        return self.times[clean(path)]

    async def prune(self, prefix: str) -> int:
        return 0  # no folders

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

    @property
    def collectable(self) -> bool:
        return all(hasattr(self.files, m) for m in ("remove", "modified", "prune"))

    async def remove(self, path: str) -> bool:
        return bool(await self.files.remove(self._p(path)))

    async def modified(self, path: str) -> float:
        return float(await self.files.modified(self._p(path)))

    async def prune(self, prefix: str) -> int:
        return int(await self.files.prune(self._p(prefix)))


class LocalWorkspace:
    """Each workspace is a folder under `root`. Mountable: a sandbox provider gets the host
    folder of an area it may expose (`host_path`). Collectable: its files can be removed, aged
    (`modified`) and their empty folders pruned, which garbage collection needs."""

    def __init__(self, root: str | Path, *, internal_links: bool | None = None) -> None:
        self.files = LocalFiles(root, internal_links=internal_links)
        self.capabilities = frozenset(
            {
                "mountable",
                "collectable",
                *(["internal_links"] if self.files.internal_links else []),
            }
        )

    def host_path(self, workspace_id: str, relative: str) -> str:
        """The host folder of `relative` in a workspace, created if needed, with no link on the
        way (Escape otherwise): what a sandbox mounts."""
        parts = clean(f"{workspace_id}/{relative}").split("/")
        os.close(self.files._dir(parts, create=True))
        return str(self.files.root.resolve().joinpath(*parts))

    async def open(self, workspace_id: str) -> Files:
        return ScopedFiles(self.files, workspace_id)

    async def close(self) -> None:
        return None


class MemoryWorkspace:
    capabilities = frozenset({"collectable"})

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
