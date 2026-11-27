"""A local shell provider with durable receipts: the reference for the shell effect contract.

It runs commands on this machine, in the agent's workspace `shared/` folder, with no isolation:
for tests, examples and trusted local use only. A sandbox backend (an extension package) keeps
the same contract and adds isolation.

    provider = LocalShell(receipts=Path("/var/lib/app/receipts"), workspaces=Path("./ws"))
    registrar.providers.register("sh", provider)
    registrar.tools.register(EffectDefinition(name="sh.exec", provider="sh", operation="exec",
        policy=EffectPolicy(kind="deduplicated", timeout=600)))

Contract (declared capabilities: deduplicated, reconcile, remote_work):
  * one key, one execution: the receipt folder of a key is created atomically (`mkdir`), so two
    executors, or a call and its retry, never start a key twice; the same key with another
    command is refused;
  * a receipt survives the provider, the runtime and the process: the command's exit code and
    output are written to files by the command's own shell, so any later process can read them;
  * `reconcile` reports `completed` (with the result), `running` (with a future that ends when
    the command does), `absent` (never received) or `unknown` (it was cancelled, or its process
    is gone without an exit code: it may have done part of its work);
  * cancelling a call kills the command's whole process group, and the call's work future ends
    only when no process of the group is left: capacity is never returned early;
  * receipts are kept outside the workspaces (refused otherwise), where no agent and no command
    can write them.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import signal
import time
from pathlib import Path
from typing import Any, ClassVar

from ..kernel.effects import EffectError
from ..ports.effects import Reconciled

POLL = 0.02
#: How long a claimed start may take before a receipt without a process counts as unknown.
START_GRACE = 5.0
#: $1 the receipt folder, $2 the workspace folder, $3 the command
WRAPPER = (
    'cd "$2" && /bin/sh -c "$3" >"$1/out" 2>"$1/err"; '
    'echo $? >"$1/code.tmp" && mv "$1/code.tmp" "$1/code"'
)


def _alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class LocalShell:
    capabilities: ClassVar[frozenset[str]] = frozenset({"deduplicated", "reconcile", "remote_work"})
    capacity_scope = "process"

    def __init__(
        self, *, receipts: str | Path, workspaces: str | Path, concurrency: int | None = None
    ) -> None:
        self.receipts = Path(receipts).resolve()
        self.workspaces = Path(workspaces).resolve()
        if self.receipts == self.workspaces or self.workspaces in self.receipts.parents:
            raise ValueError("receipts must be kept outside the workspaces")
        self.receipts.mkdir(parents=True, exist_ok=True)
        self.concurrency = concurrency
        self._children: dict[int, asyncio.subprocess.Process] = {}

    # ---- receipts -----------------------------------------------------------------------------
    def _folder(self, key: str) -> Path:
        return self.receipts / hashlib.sha256(key.encode()).hexdigest()[:32]

    @staticmethod
    def _digest(arguments: dict[str, Any]) -> str:
        return hashlib.sha256(json.dumps(arguments, sort_keys=True).encode()).hexdigest()

    def _result(self, folder: Path) -> dict[str, Any]:
        return {
            "exit": int((folder / "code").read_text().strip() or "-1"),
            "stdout": (folder / "out").read_text(errors="replace"),
            "stderr": (folder / "err").read_text(errors="replace"),
        }

    def _pgid(self, folder: Path) -> int | None:
        try:
            return int((folder / "pgid").read_text())
        except (FileNotFoundError, ValueError):
            return None

    def _state(self, folder: Path) -> str:
        if not (folder / "started").exists():
            return "absent"  # never reserved, or reserved by a caller that died before starting
        if (folder / "code").exists():
            return "completed"
        if (folder / "cancelled").exists():
            return "unknown"
        pgid = self._pgid(folder)
        if pgid is None:  # claimed; its process is being started, or the starter died
            age = time.time() - (folder / "started").stat().st_mtime
            return "running" if age < START_GRACE else "unknown"
        return "running" if _alive(pgid) else "unknown"

    @staticmethod
    def _claim(folder: Path) -> bool:
        """Claim the start of a reserved receipt: exactly one caller wins."""
        try:
            os.close(os.open(folder / "started", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
        except FileExistsError:
            return False
        return True

    async def _ended(self, folder: Path) -> None:
        """Until the command's process group is gone (its exit code written, or killed)."""
        pgid = self._pgid(folder)
        child = self._children.get(pgid) if pgid else None
        while pgid is not None and _alive(pgid):
            if child is not None and child.returncode is None:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(asyncio.shield(child.wait()), POLL)
                continue
            await asyncio.sleep(POLL)
        if pgid is not None:
            self._children.pop(pgid, None)

    # ---- the provider -------------------------------------------------------------------------
    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any:
        if operation != "exec":
            raise EffectError("invalid", f"no operation {operation!r}", sent=False)
        if not context.key:
            raise EffectError("refused", "a shell command needs its call identity", sent=False)
        if context.scope.get("shared", "rw") != "rw":
            raise EffectError(
                "refused", "a local shell cannot enforce a read-only workspace", sent=False
            )
        command = arguments.get("command")
        if not isinstance(command, str) or not command:
            raise EffectError("invalid", "give a command", sent=False)
        folder = self._folder(context.key)
        digest = self._digest(arguments)
        try:
            folder.mkdir()  # the reservation: exactly one caller creates it
            (folder / "request").write_text(digest)
        except FileExistsError:
            request = folder / "request"
            if request.exists() and request.read_text() != digest:
                raise EffectError(
                    "invalid", "this call identity was used for another command", sent=False
                ) from None
        if not self._claim(folder):
            return await self._attach(folder, context)
        cwd = self.workspaces / context.workspace / "shared"
        cwd.mkdir(parents=True, exist_ok=True)
        for name in ("out", "err"):
            (folder / name).write_text("")
        # the wrapper writes the receipt; the command itself runs in the workspace and is not
        # told where the receipt is
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh",
            "-c",
            WRAPPER,
            "wrapper",
            str(folder),
            str(cwd),
            command,
            cwd=str(cwd),
            start_new_session=True,  # its own process group: cancelling ends all of it
        )
        self._children[proc.pid] = proc
        (folder / "pgid").write_text(str(proc.pid))
        return await self._attach(folder, context)

    async def _attach(self, folder: Path, context: Any) -> Any:
        """Wait for the command of this receipt; on cancellation, kill its process group and
        keep the call's capacity until the group is gone."""
        ended = asyncio.ensure_future(self._ended(folder))
        context.work.append(ended)
        try:
            while True:
                state = self._state(folder)
                if state == "completed":
                    self._kill(folder)  # what it left running in the background ends with it
                    return self._result(folder)
                if state == "unknown":
                    raise EffectError("indeterminate", "the command ended without an exit code")
                await asyncio.sleep(POLL)
        except asyncio.CancelledError:
            self._cancel(folder)
            raise

    def _cancel(self, folder: Path) -> None:
        if not (folder / "code").exists():
            (folder / "cancelled").write_text("")
        self._kill(folder)

    def _kill(self, folder: Path) -> None:
        pgid = self._pgid(folder)
        if pgid is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pgid, signal.SIGKILL)

    async def reconcile(self, operation: str, key: str, context: Any) -> Reconciled:
        folder = self._folder(key)
        state = self._state(folder)
        if state == "completed":
            return Reconciled("completed", result=self._result(folder))
        if state == "absent":
            return Reconciled("absent")
        if state == "running":
            return Reconciled("running", work=asyncio.ensure_future(self._ended(folder)))
        return Reconciled("unknown")

    async def close(self) -> None:
        for proc in list(self._children.values()):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(proc.wait(), 0.1)


__all__ = ["LocalShell"]
