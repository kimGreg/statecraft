"""Workspaces stay contained, and agents see only what they may.

What must hold
  * LocalFiles never leaves its folder: a symbolic link to outside, as a file or as a folder on
    the way, is refused on read, write, create and exists, and skipped by listings; nothing is
    written outside; a link swapped in while a path is being opened is refused as well;
  * a listing prefix is a folder inside the workspace: `../other` is refused on every store, and
    the memory store and LocalFiles mean the same by a prefix;
  * an agent reads neither another agent's private or staging files nor an object no committed
    reference names; a stale attempt cannot write the current attempt's staging folder.
"""

from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path

import pytest

from statecraft import Agent, Done, Spawn, Wait, state
from statecraft.providers.workspaces import (
    Escape,
    LocalFiles,
    LocalWorkspace,
    MemoryFiles,
    ScopedFiles,
)
from statecraft.runtime.workspace import NotAllowed
from statecraft.testing import memory_runtime


@pytest.fixture
def jail(tmp_path: Path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("host secret")
    files = LocalFiles(tmp_path / "ws")
    (tmp_path / "ws" / "shared").mkdir(parents=True)
    os.symlink(outside / "secret.txt", tmp_path / "ws" / "shared" / "link.txt")
    os.symlink(outside, tmp_path / "ws" / "shared" / "dir")
    return files, outside


async def test_links_to_outside_are_refused_on_every_operation(jail) -> None:
    files, outside = jail
    for attempt in (
        files.get("shared/link.txt"),
        files.get("shared/dir/secret.txt"),
        files.put("shared/dir/planted.txt", b"x"),
        files.create("shared/dir/created.txt", b"x"),
    ):
        with pytest.raises(Escape):
            await attempt
    assert not (outside / "planted.txt").exists() and not (outside / "created.txt").exists()
    assert await files.exists("shared/link.txt") is False
    assert await files.exists("shared/dir/secret.txt") is False
    assert await files.list("shared") == [] and await files.list("") == []
    await files.put("shared/link.txt", b"mine")  # replaces the link itself, never its target
    assert (outside / "secret.txt").read_text() == "host secret"
    assert await files.get("shared/link.txt") == b"mine"


async def test_a_link_swapped_in_while_reading_is_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    (outside / "d").mkdir(parents=True)
    (outside / "d" / "f.txt").write_text("OUTSIDE")
    files = LocalFiles(tmp_path / "ws")
    await files.put("shared/d/f.txt", b"inside")
    real, link = tmp_path / "ws" / "shared" / "d", tmp_path / "ws" / "shared" / "swap"
    stop = threading.Event()

    def swapper() -> None:  # keep replacing the folder with a link to outside and back
        while not stop.is_set():
            os.symlink(outside / "d", link)
            os.replace(link, tmp_path / "ws" / "shared" / "d.link")
            os.rename(real, tmp_path / "ws" / "shared" / "d.real")
            os.rename(tmp_path / "ws" / "shared" / "d.link", real)
            os.unlink(real)
            os.rename(tmp_path / "ws" / "shared" / "d.real", real)

    thread = threading.Thread(target=swapper)
    thread.start()
    seen = set()
    try:
        for _ in range(2000):
            try:
                seen.add(await files.get("shared/d/f.txt"))
            except (Escape, FileNotFoundError):
                seen.add("refused")
    finally:
        stop.set()
        thread.join()
    assert b"OUTSIDE" not in seen and b"inside" in seen


async def test_listing_prefixes_stay_inside_the_workspace(tmp_path: Path) -> None:
    for shared in (MemoryFiles(), LocalFiles(tmp_path / "ws")):
        a, b = ScopedFiles(shared, "a"), ScopedFiles(shared, "b")
        await a.put("home/a/x.txt", b"1")
        await a.put("home/ab/y.txt", b"2")
        await b.put("shared/b.txt", b"3")
        for bad in ("../b", "../b/", "x/../../b"):
            with pytest.raises(ValueError):
                await a.list(bad)
        assert await a.list("home/a") == ["home/a/x.txt"]  # a folder, not a string prefix
        assert await a.list("") == ["home/a/x.txt", "home/ab/y.txt"]
    workspace = LocalWorkspace(tmp_path / "sessions")
    await (await workspace.open("b")).put("shared/secret.txt", b"b only")
    with pytest.raises(ValueError):
        await (await workspace.open("a")).list("../b")


class Reader(Agent):
    """Tries to read what it may not: a sibling's private and staging files, an object no
    committed reference names; publishes what each attempt got."""

    start = "go"

    @state
    async def go(self, ctx, data):
        got = {}
        for path in ctx.input["paths"]:
            try:
                got[path] = (await ctx.files.read(path)).decode()
            except (NotAllowed, FileNotFoundError) as exc:
                got[path] = type(exc).__name__
        await ctx.write("got", got)
        return Done


class Writer(Agent):
    """Writes its private file and a staged object it never publishes, then waits."""

    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.files.write(f"home/{ctx.id}/private/secret.txt", "SECRET")
        await ctx.files.write(f"{ctx.staging}/draft.txt", "DRAFT")
        await ctx.files._ctx._files.put(f"home/{ctx.id}/objects/unpublished.json", b"{}")
        return Wait(answer={"name": "hold", "question": "?"}, then="done")

    @state
    async def done(self, ctx, data):
        return Done


class Pair(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Spawn({"w": ("writer", {})}, then="read")

    @state
    async def read(self, ctx, data):
        return Wait(sleep=0.05, then="reader")

    @state
    async def reader(self, ctx, data):
        w = ctx.children["w"]
        paths = [
            f"home/{w}/private/secret.txt",
            f"home/{w}/staging/0/1/draft.txt",
            f"home/{w}/objects/unpublished.json",
        ]
        return Spawn({"r": ("reader", {"paths": paths})}, then="end")

    @state
    async def end(self, ctx, data):
        ctx.signal("w", "TERM")
        return Wait(join=["r"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


async def test_agents_cannot_read_private_staging_or_unpublished_files() -> None:
    rt = memory_runtime()
    rt.registrar.agents.register("pair", Pair, spawns=["writer", "reader"])
    rt.registrar.agents.register("writer", Writer)
    rt.registrar.agents.register("reader", Reader, writes=["got"])
    async with rt:
        root = await rt.spawn("pair", root_id="p")
        await rt.run(root, timeout=10)
        got = (await rt.artifact(root, "got"))["payload"]
    assert set(got.values()) == {"NotAllowed"}, got


class Stale(Agent):
    """Writes into a staging folder of another attempt of itself."""

    start = "go"

    @state
    async def go(self, ctx, data):
        other = f"home/{ctx.id}/staging/{ctx.step}/999/x.txt"
        try:
            await ctx.files.write(other, "stale")
            data["wrote"] = True
        except NotAllowed:
            data["wrote"] = False
        return Done


async def test_an_attempt_writes_only_its_own_staging_folder() -> None:
    rt = memory_runtime()
    rt.registrar.agents.register("stale", Stale)
    async with rt:
        root = await rt.spawn("stale", root_id="s")
        await rt.run(root, timeout=10)
        assert (await rt.get(root))["data"]["wrote"] is False


async def test_listing_never_follows_a_link_into_a_loop(tmp_path: Path) -> None:
    files = LocalFiles(tmp_path / "ws")
    await files.put("shared/a/f.txt", b"1")
    os.symlink(tmp_path / "ws" / "shared", tmp_path / "ws" / "shared" / "a" / "loop")
    assert await asyncio.wait_for(files.list(""), 5) == ["shared/a/f.txt"]
