"""A sandbox service on this machine, for deterministic tests: each sandbox maps its mounts to host
folders and runs commands with `/bin/sh` in its own process group, in the host folder mounted at
`/workspace`. It behaves like OpenSandbox where it matters, and can be told to fail like it:

    lose_submit_answer   the next submit starts the command, then the answer is lost
    lose_records(id)     `execd` restarted: the sandbox's execution records are gone (404)
    lose_create_answer   the next create makes the sandbox, then the answer is lost

No isolation: it is a test double, not a sandbox.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import uuid
from typing import Any


class FakeBackend:
    def __init__(self) -> None:
        self.sandboxes: dict[str, dict[str, Any]] = {}
        self.creates = 0
        self.submits = 0
        self.lose_submit_answer = False
        self.lose_create_answer = False

    async def create(self, labels: dict[str, str], mounts: list[dict[str, Any]]) -> str:
        self.creates += 1
        sandbox = uuid.uuid4().hex[:12]
        self.sandboxes[sandbox] = {
            "labels": dict(labels),
            "mounts": list(mounts),
            "state": "Running",
            "runs": {},
        }
        if self.lose_create_answer:
            self.lose_create_answer = False
            raise ConnectionError("the create answer was lost")
        return sandbox

    async def find(self, labels: dict[str, str]) -> list[dict[str, Any]]:
        return [
            {"id": sid, "state": box["state"]}
            for sid, box in self.sandboxes.items()
            if all(box["labels"].get(k) == v for k, v in labels.items())
        ]

    async def state(self, sandbox: str) -> str | None:
        box = self.sandboxes.get(sandbox)
        return None if box is None else box["state"]

    async def delete(self, sandbox: str) -> None:
        box = self.sandboxes.pop(sandbox, None)
        if box is None:
            return
        for run in box["runs"].values():  # destroying the sandbox ends everything in it
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(run["proc"].pid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(run["proc"].wait(), 5)

    def _workdir(self, box: dict[str, Any]) -> str:
        for mount in box["mounts"]:
            if mount["target"] == "/workspace":
                return mount["host"]
        raise RuntimeError("no /workspace mount")

    async def submit(self, sandbox: str, command: str) -> str:
        box = self.sandboxes[sandbox]
        self.submits += 1
        execution = uuid.uuid4().hex
        read, write = os.pipe()
        cwd = self._workdir(box)
        script = command.replace("cd /workspace &&", "", 1)
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh",
            "-c",
            script,
            cwd=cwd,
            stdout=write,
            stderr=write,
            start_new_session=True,
        )
        os.close(write)
        run = {"proc": proc, "output": b"", "reader": None}
        box["runs"][execution] = run

        async def collect() -> None:
            loop = asyncio.get_running_loop()
            with os.fdopen(read, "rb", buffering=0) as handle:
                while True:
                    chunk = await loop.run_in_executor(None, handle.read, 65536)
                    if not chunk:
                        break
                    run["output"] += chunk

        run["reader"] = asyncio.ensure_future(collect())
        if self.lose_submit_answer:
            self.lose_submit_answer = False
            raise ConnectionError("the submit answer was lost")
        return execution

    async def status(self, sandbox: str, execution: str) -> dict[str, Any] | None:
        box = self.sandboxes.get(sandbox)
        run = None if box is None else box["runs"].get(execution)
        if run is None:
            return None
        proc = run["proc"]
        return {"running": proc.returncode is None, "exit": proc.returncode}

    async def output(self, sandbox: str, execution: str) -> str:
        run = self.sandboxes[sandbox]["runs"][execution]
        if run["reader"] is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.shield(run["reader"]), 5)
        return run["output"].decode(errors="replace")

    async def interrupt(self, sandbox: str, execution: str) -> None:
        run = self.sandboxes[sandbox]["runs"].get(execution)
        if run is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(run["proc"].pid, signal.SIGKILL)

    def lose_records(self, sandbox: str) -> None:
        """`execd` restarted: its execution records are gone; the commands may still run."""
        self.sandboxes[sandbox]["lost"] = self.sandboxes[sandbox]["runs"]
        self.sandboxes[sandbox]["runs"] = {}

    async def close(self) -> None:
        for sandbox in list(self.sandboxes):
            await self.delete(sandbox)


__all__ = ["FakeBackend"]
