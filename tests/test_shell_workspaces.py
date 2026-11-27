"""Workspaces a shell can use (roadmap A3).

What must hold
  * reads follow a symbolic link only while it stays within its area (`shared/`, or one folder of
    an agent's home), checked by the kernel at open time; a link from `shared/` into an agent's
    private folder, or out of the workspace, is refused; listings show internal links to files;
    with internal links off, every link is refused, as before;
  * the mount plan comes from the agent's authorized scope: `shared` read-only for an agent
    attached without a write grant, its own private folder read-write, nothing else;
  * a shell provider that needs a mountable workspace is refused at start with a memory
    workspace; the local shell runs in the host folder the workspace gives it;
  * an agent attached to a run reads the run's publications, and `exists` agrees with `read`;
  * no runtime module opens workspace paths itself: only the workspace provider touches them.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

import pytest

from statecraft import Agent, Done, EffectDefinition, EffectPolicy, state
from statecraft.ports.workspace import mount_plan
from statecraft.providers.workspaces import (
    Escape,
    LocalFiles,
    LocalWorkspace,
    MemoryWorkspace,
    beneath_supported,
)
from statecraft.runtime.registry import RegistryError
from statecraft.runtime.runtime import Runtime
from statecraft.testing import memory_runtime, permissive_runtime
from statecraft.testing.local_shell import LocalShell

linux_only = pytest.mark.skipif(not beneath_supported(), reason="needs openat2")


@linux_only
async def test_links_are_followed_only_within_their_area(tmp_path: Path) -> None:
    files = LocalFiles(tmp_path / "ws")
    await files.put("w/shared/src/real.txt", b"code")
    await files.put("w/home/a/private/secret.txt", b"secret")
    await files.put("w/home/a/private/notes.txt", b"mine")
    shared = tmp_path / "ws" / "w" / "shared"
    os.symlink("src/real.txt", shared / "alias.txt")  # inside shared
    os.symlink("src", shared / "lib")  # a linked folder inside shared
    os.symlink("../home/a/private/secret.txt", shared / "peek.txt")  # into a private folder
    os.symlink("/etc/hostname", shared / "host.txt")  # out of the workspace
    private = tmp_path / "ws" / "w" / "home" / "a" / "private"
    os.symlink("notes.txt", private / "today.txt")  # inside the private folder
    assert await files.get("w/shared/alias.txt") == b"code"
    assert await files.get("w/shared/lib/real.txt") == b"code"
    assert await files.get("w/home/a/private/today.txt") == b"mine"
    for escaping in ("w/shared/peek.txt", "w/shared/host.txt"):
        with pytest.raises(Escape):
            await files.get(escaping)
        assert await files.exists(escaping) is False
    listed = await files.list("w/shared")
    assert "w/shared/alias.txt" in listed and "w/shared/peek.txt" not in listed
    strict = LocalFiles(tmp_path / "ws", internal_links=False)
    with pytest.raises(Escape):
        await strict.get("w/shared/alias.txt")


def test_the_mount_plan_follows_the_scope() -> None:
    attached = {"workspace": "run1", "shared": "ro", "private": "home/c1/private"}
    assert mount_plan(attached) == [
        {"source": "shared", "target": "/workspace", "mode": "ro"},
        {"source": "home/c1/private", "target": "/home/agent", "mode": "rw"},
    ]


class Runs(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        data["scope"] = ctx.scope()
        data["out"] = await ctx.call("sh.exec", call_name="pwd", command="pwd; echo hi > made.txt")
        return Done


def shell_runtime(tmp_path: Path, workspace) -> Runtime:
    rt = Runtime(store=memory_runtime().store, workspace=workspace, scan_every=0.01)
    rt.registrar.providers.register(
        "sh", LocalShell(receipts=tmp_path / "receipts", workspaces=tmp_path / "ws")
    )
    rt.registrar.tools.register(
        EffectDefinition(
            name="sh.exec",
            provider="sh",
            operation="exec",
            policy=EffectPolicy(kind="deduplicated", timeout=30),
        )
    )
    rt.registrar.agents.register("runs", Runs, tools=["sh.*"])
    return rt


async def test_a_shell_needs_a_mountable_workspace(tmp_path: Path) -> None:
    rt = shell_runtime(tmp_path, MemoryWorkspace())
    with pytest.raises(RegistryError, match="mountable"):
        await rt.start()


async def test_the_shell_runs_in_the_workspace_it_was_given(tmp_path: Path) -> None:
    async with shell_runtime(tmp_path, LocalWorkspace(tmp_path / "ws")) as rt:
        root = await rt.spawn("runs", root_id="r")
        await rt.run(root, timeout=10)
        record = await rt.store.load(root)
        assert record.status == "succeeded", record.error
        made = (await rt.read_file(root, "shared/made.txt")).decode()
    host = (tmp_path / "ws" / "r" / "shared").resolve()
    assert record.data["out"]["stdout"].strip() == str(host) and made == "hi\n"
    assert [m["mode"] for m in record.data["scope"]["mounts"]] == ["rw", "rw"]


class Writes(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("note", {"text": "the run's note"})
        return Done


class Reads(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        data["note"] = (await ctx.read("note")).payload
        [path] = await ctx.files.list(f"home/{ctx.input['run']}/objects")
        data["exists"] = await ctx.files.exists(path)
        data["size"] = len(await ctx.files.read(path))
        data["scope"] = ctx.scope()["shared"]
        return Done


async def test_an_attached_agent_reads_the_runs_publications() -> None:
    rt = permissive_runtime({"t:writes": Writes, "t:reads": Reads})
    async with rt:
        await rt.spawn("t:writes", {}, root_id="run1")
        await rt.run("run1", timeout=5)
        await rt.spawn("t:reads", {"run": "run1"}, root_id="c1", attach="run1")
        await rt.run("c1", timeout=5)
        data = (await rt.store.load("c1")).data
    assert data["note"] == {"text": "the run's note"}
    assert data["exists"] is True and data["size"] > 0 and data["scope"] == "ro"


#: modules allowed to open files by path: configuration files, agent packages' code (for its
#: digest), the stores and the workspace provider; none of them opens a workspace path
OPENS_FILES = {
    "runtime/config.py",
    "runtime/registry.py",
    "providers/workspaces.py",
    "providers/stores/sqlite.py",
}
FS_MODULES = {"os", "shutil"}
FS_CALLS = {"open", "mkdir", "makedirs", "remove", "unlink", "rename", "replace", "symlink"}
PATH_METHODS = {"read_text", "read_bytes", "write_text", "write_bytes", "open", "unlink"}


def test_no_runtime_code_opens_workspace_paths_itself() -> None:
    """Only the workspace provider touches workspace folders, contained (a sandbox mounts what it
    hands out): a runtime module that opened paths itself would follow links planted by a shell."""
    package = Path(__file__).resolve().parents[1] / "statecraft"
    found = []
    for path in sorted([*package.glob("runtime/*.py"), *package.glob("providers/**/*.py")]):
        rel = str(path.relative_to(package))
        if rel in OPENS_FILES:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Name) and func.id == "open":
                found.append(f"{rel}:{node.lineno}: open")
            elif isinstance(func, ast.Attribute):
                owner = ast.unparse(func.value)
                if (owner in FS_MODULES and func.attr in FS_CALLS) or (
                    func.attr in PATH_METHODS and "path" in owner.lower()
                ):
                    found.append(f"{rel}:{node.lineno}: {owner}.{func.attr}")
    assert found == []
