"""Storage upkeep (roadmap C2): collecting what no commit names, compacting and forgetting runs.

    report = await runtime.collect(dry_run=True)      # what would go; nothing removed
    await runtime.collect(grace=3600)                 # remove it
    await runtime.compact("run-1")                    # smaller history, events and inbox
    await runtime.forget("run-1", files=True)         # an ended run, entirely

What must hold
  * staging folders of attempts that did not commit go once their step committed (a later step)
    or the agent ended; the current step's staging stays while the agent may still run it;
  * an object (`home/<agent>/objects/...`) goes only when no committed reference names it: the
    agent's record and every entry of its retained history (forks read those records, so their
    references count), and it is older than `grace`;
  * collection never races a commit: an agent's home is swept while collection holds that
    agent's lease (no attempt runs, so nothing is published meanwhile), and its references are
    read after the lease was taken; an agent with a running step is skipped, an ended agent
    needs no lease (it never runs again);
  * a dry run reports and removes nothing; a workspace that is not `collectable` is reported;
  * forgetting a run is refused while it works or another run reads it (`StateStore.forget`).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from ..kernel.process import Record
from ..ports.workspace import home

if TYPE_CHECKING:
    from .runtime import Runtime

OWNER = "statecraft-upkeep"
LEASE_SECONDS = 120.0


def _references(records: list[dict[str, Any]]) -> set[str]:
    return {
        ref["path"]
        for record in records
        for ref in (record.get("published") or {}).values()
        if isinstance(ref, dict) and "path" in ref
    }


def _staging_step(path: str, prefix: str) -> int | None:
    """The step of `<prefix><step>/<epoch>/...`, or None when the path is not shaped so."""
    head = path.removeprefix(prefix).split("/", 1)[0]
    return int(head) if head.isdigit() else None


async def _sweep(
    runtime: Runtime, record: Record, files: Any, *, dry_run: bool, grace: float, now: float
) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {"staging": [], "objects": []}
    staging = f"{home(record.id)}/staging/"
    for path in await files.list(staging):
        step = _staging_step(path, staging)
        if step is not None and (record.ended or step < record.step):
            found["staging"].append(path)
    history = await runtime.store.history(record.id)
    named = _references([record.to_dict(), *(entry["record"] for entry in history)])
    for path in await files.list(f"{home(record.id)}/objects/"):
        if path in named:
            continue
        try:
            age = now - await files.modified(path)
        except FileNotFoundError:
            continue
        if age >= grace:
            found["objects"].append(path)
    if not dry_run:
        for path in [*found["staging"], *found["objects"]]:
            await files.remove(path)
        if found["staging"]:
            await files.prune(staging)
    return found


async def collect(runtime: Runtime, *, dry_run: bool = True, grace: float = 3600.0) -> dict:
    """Remove (or, dry, report) staging of finished attempts and objects no commit names."""
    report: dict[str, Any] = {"dry_run": dry_run, "staging": [], "objects": [], "skipped": []}
    for root in await runtime.store.roots():
        for record in await runtime.store.tree(root.id):
            files = await runtime.workspaces.files_of(record.id)
            if not getattr(files, "collectable", False):
                report["skipped"].append({"agent": record.id, "why": "workspace not collectable"})
                continue
            lease = None
            if not record.ended:
                lease = await runtime.store.try_acquire(
                    record.id, OWNER, LEASE_SECONDS, runtime.clock()
                )
                if lease is None:
                    report["skipped"].append({"agent": record.id, "why": "a step is running"})
                    continue
            try:
                current = await runtime.store.load(record.id)  # read after the lease was taken
                found = await _sweep(
                    runtime,
                    current,
                    files,
                    dry_run=dry_run,
                    grace=grace,
                    now=time.time(),  # file times are wall-clock times
                )
            finally:
                if lease is not None:
                    await runtime.store.release(record.id, lease)
            report["staging"] += found["staging"]
            report["objects"] += found["objects"]
    if not dry_run:
        runtime.notify()  # agents whose lease collection held may run again
    return report


async def forget(
    runtime: Runtime, root: str, *, files: bool = False, dry_run: bool = False
) -> dict:
    """Delete an ended run from the store; with `files`, its files too: its own workspace, or,
    when it worked attached to another run's, its homes there."""
    record = await runtime.store.load(root)
    found = await runtime.store.forget(root, dry_run=True)  # refused here, before any file goes
    if files:
        workspace = await runtime.workspaces.open(record.workspace or root)
        if not getattr(workspace, "collectable", False):
            raise ValueError(f"the workspace of {root} is not collectable; forget it without files")
        prefix = home(root) if record.workspace else ""
        paths = await workspace.list(prefix)
        found = {**found, "files": len(paths)}
        if not dry_run:
            for path in paths:
                await workspace.remove(path)
            for folder in [prefix] if prefix else ["home", "shared", "run"]:
                await workspace.prune(folder)
    if not dry_run:
        await runtime.store.forget(root)
    return found


__all__ = ["collect", "forget"]
