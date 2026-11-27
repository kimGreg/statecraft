"""Shell commands in sandboxes, as a durable statecraft effect (`sh.exec`).

    effect_providers:
      sh: {provider: opensandbox, endpoint: http://127.0.0.1:8080, api_key: ${OSB_KEY},
           image: python:3.12-slim, scope: agent, receipts: /var/lib/app/sandbox-receipts.db}

Contract (declared capabilities: deduplicated, reconcile, remote_work; tree_termination only
with `destroy_on_cancel`, since cancelling a command ends its process group but not a process
that left it, and only destroying the sandbox ends everything):
  * one call identity, one command: a receipt (provider-owned, outside every workspace and every
    sandbox) is reserved and claimed before the command is submitted, so a retry, a CONT or a
    second executor never submits it twice; the same identity with another command is refused;
  * the execution id is kept the moment the sandbox gives it; when that answer is lost the
    outcome is unknown (`indeterminate`, reported, never resubmitted);
  * `reconcile` reads the receipt and the sandbox: completed (the result), running (reattach),
    absent, or unknown (cancelled midway, the sandbox lost its record, the submit answer lost);
  * a cancelled call interrupts the command; its capacity is held until the sandbox says it
    ended (or the sandbox is destroyed, with `destroy_on_cancel`);
  * sandboxes are per agent by default (`scope: run` or `call`), with the mount plan the runtime
    computed (`shared` with the granted mode, the agent's private folder), labelled before they
    are created so a lost create answer is found again; a sandbox whose agent ended is destroyed.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, ClassVar

from statecraft import EffectDefinition, EffectError, EffectPolicy
from statecraft.ports.effects import Reconciled

from .backend import Backend
from .receipts import Receipts

#: how long a start may go without a heartbeat before a receipt without an execution id is
#: unknown (its starter died: it may have submitted)
START_GRACE = 30.0
#: how long a sandbox creation may go without finishing before another process takes it over
CREATE_GRACE = 300.0
SCOPES = ("agent", "run", "call")
#: sandbox states in which nothing runs any more (None: deleted)
GONE = (None, "Terminated", "Failed", "Stopped", "Deleted")


def _label(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "-", text)[:60]


class OpenSandboxProvider:
    capacity_scope = "process"
    workspace_requires: ClassVar[frozenset[str]] = frozenset({"mountable"})

    def __init__(
        self,
        backend: Backend,
        *,
        receipts: str | Path,
        name: str = "sh",
        timeout: float = 600,
        scope: str = "agent",
        destroy_on_cancel: bool = False,
        concurrency: int | None = None,
        poll: float = 0.2,
        cleanup_every: int = 20,
        start_grace: float = START_GRACE,
    ) -> None:
        if scope not in SCOPES:
            raise ValueError(f"scope must be one of {SCOPES}, not {scope!r}")
        if destroy_on_cancel and scope != "call":
            raise ValueError(
                "destroy_on_cancel needs scope: call (destroying a shared sandbox would kill the "
                "other calls running in it)"
            )
        self.backend = backend
        self.name = name
        self.timeout = timeout
        self.receipts = Receipts(receipts)
        self.scope = scope
        self.destroy_on_cancel = destroy_on_cancel
        self.concurrency = concurrency
        self.poll = poll
        self.cleanup_every = cleanup_every
        self.start_grace = start_grace
        self.capabilities = frozenset(
            {"deduplicated", "reconcile", "remote_work"}
            | ({"tree_termination"} if destroy_on_cancel else set())
        )
        self.owner = uuid.uuid4().hex[:12]
        self._view: Any = None
        self._calls = 0
        self._creating: dict[str, asyncio.Lock] = {}
        self._watchers: set[asyncio.Future[Any]] = set()

    def bind(self, view: Any) -> None:
        self._view = view

    async def start(self) -> None:
        await self.cleanup()

    async def discover(self) -> list[EffectDefinition]:
        """`<name>.exec`: run a shell command in the agent's sandbox, in `/workspace`."""
        return [
            EffectDefinition(
                name=f"{self.name}.exec",
                operation="exec",
                description=(
                    "Run a shell command in your sandbox. The working folder is /workspace "
                    "(the shared workspace); /home/agent is your private folder."
                ),
                input_schema={
                    "type": "object",
                    "properties": {"command": {"type": "string", "minLength": 1}},
                    "required": ["command"],
                    "additionalProperties": False,
                },
                output_schema={
                    "type": "object",
                    "properties": {"exit": {}, "output": {"type": "string"}},
                },
                policy=EffectPolicy(kind="deduplicated", timeout=self.timeout, max_attempts=2),
            )
        ]

    async def close(self) -> None:
        for watcher in list(self._watchers):  # nothing polls the receipts after they close
            watcher.cancel()
        await asyncio.gather(*self._watchers, return_exceptions=True)
        self.receipts.close()

    def _watch(self, key: str) -> asyncio.Future[Any]:
        watcher = asyncio.ensure_future(self._ended(key))
        self._watchers.add(watcher)
        watcher.add_done_callback(self._watchers.discard)
        return watcher

    # ---- sandboxes ----------------------------------------------------------------------------
    def _scope_key(self, context: Any) -> str:
        """The sandbox a call runs in: per agent, run or call, for this provider and this mount
        plan (a changed plan, a grant changed, gets another sandbox)."""
        plan = hashlib.sha256(
            json.dumps(context.scope.get("mounts"), sort_keys=True).encode()
        ).hexdigest()[:12]
        owner = {"agent": context.agent, "run": context.root, "call": context.key}[self.scope]
        return f"{self.name}:{self.scope}:{owner}:{plan}"

    def _mounts(self, context: Any) -> list[dict[str, Any]]:
        if self._view is None:
            raise EffectError("refused", "the provider was not bound to a runtime", sent=False)
        return [
            {
                "host": self._view.host_path(context.workspace, mount["source"]),
                "target": mount["target"],
                "mode": mount["mode"],
            }
            for mount in context.scope.get("mounts", [])
        ]

    async def _sandbox(self, context: Any) -> str:
        """The sandbox of this call's scope: found, or created once. One creator per scope
        across processes (a claim in the receipts); the others wait for it; the label chosen
        before the create finds a lost answer; a dead sandbox is replaced only if it is still
        the one recorded."""
        scope = self._scope_key(context)
        lock = self._creating.setdefault(scope, asyncio.Lock())
        async with lock:
            deadline = time.monotonic() + CREATE_GRACE + 120
            while time.monotonic() < deadline:
                row = await self.receipts.begin_sandbox(scope, uuid.uuid4().hex, context.agent)
                if row["sandbox"]:
                    state = await self.backend.state(row["sandbox"])
                    if state == "Running":
                        return row["sandbox"]
                    if state == "Pending":
                        await self._until_running(row["sandbox"])
                        return row["sandbox"]
                    with contextlib.suppress(Exception):  # whatever it is, it is not used again
                        await self.backend.delete(row["sandbox"])
                    await self.receipts.replace_sandbox(scope, row["sandbox"], uuid.uuid4().hex)
                    continue
                if not await self.receipts.claim_creation(scope, self.owner, CREATE_GRACE):
                    if row["state"] == f"creating:{self.owner}":
                        pass  # ours from an earlier attempt in this process
                    else:
                        await asyncio.sleep(self.poll)  # another process creates it
                        continue
                sandbox = await self._create(row, context)
                await self.receipts.set_sandbox(scope, sandbox=sandbox, state="ready")
                await self._until_running(sandbox)
                return sandbox
            raise EffectError("unavailable", "no sandbox could be made in time", sent=False)

    async def _create(self, row: dict[str, Any], context: Any) -> str:
        label = {"statecraft-scope": row["label"]}
        found = await self.backend.find(label)
        if found:
            return found[0]["id"]
        labels = {**label, "statecraft-agent": _label(context.agent)}
        try:
            return await self.backend.create(labels, self._mounts(context))
        except EffectError:
            raise
        except Exception:  # the answer may be lost: the label finds it again
            found = await self.backend.find(label)
            if not found:
                raise
            return found[0]["id"]

    async def _until_running(self, sandbox: str, limit: float = 120) -> None:
        end = time.monotonic() + limit
        while time.monotonic() < end:
            state = await self.backend.state(sandbox)
            if state == "Running":
                return
            if state in GONE:
                raise EffectError("unavailable", f"sandbox {sandbox} is {state}", sent=False)
            await asyncio.sleep(self.poll)
        raise EffectError("unavailable", f"sandbox {sandbox} did not start", sent=False)

    async def cleanup(self) -> None:
        """Destroy the sandboxes nothing will use again: an ended agent's (scope agent), an
        ended run's (scope run). A row goes only once its sandbox is gone."""
        if self._view is None:
            return
        rows = await self.receipts.sandboxes()
        owners = {}
        for row in rows:
            parts = row["scope"].split(":", 2)  # name:kind:owner:plan (owner may hold ":")
            if len(parts) == 3 and parts[1] in ("agent", "run"):
                owners[row["scope"]] = parts[2].rsplit(":", 1)[0]
        if not owners:
            return
        ended = await self._view.ended(sorted(set(owners.values())))
        for row in rows:
            if owners.get(row["scope"]) in ended:
                await self._destroy_scope(row)

    async def _destroy_scope(self, row: dict[str, Any]) -> None:
        try:
            for found in await self.backend.find({"statecraft-scope": row["label"]}):
                await self.backend.delete(found["id"])
            if row["sandbox"]:
                await self.backend.delete(row["sandbox"])
        except Exception:
            return  # kept: tried again at the next cleanup
        await self.receipts.drop_sandbox(row["scope"])

    # ---- the effect ---------------------------------------------------------------------------
    @staticmethod
    def _digest(arguments: dict[str, Any], context: Any) -> str:
        request = {"arguments": arguments, "mounts": context.scope.get("mounts")}
        return hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()

    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any:
        if operation != "exec":
            raise EffectError("invalid", f"no operation {operation!r}", sent=False)
        if not context.key:
            raise EffectError("refused", "a shell command needs its call identity", sent=False)
        command = arguments.get("command")
        if not isinstance(command, str) or not command:
            raise EffectError("invalid", "give a command", sent=False)
        self._calls += 1
        if self.cleanup_every and self._calls % self.cleanup_every == 0:
            with contextlib.suppress(Exception):
                await self.cleanup()
        key = context.key
        digest = self._digest(arguments, context)
        receipt = await self.receipts.reserve(key, digest, context.agent)
        if receipt["digest"] != digest:
            raise EffectError(
                "invalid", "this call identity was used for another command", sent=False
            )
        if not await self.receipts.claim(key):
            return await self._follow(key, context)
        beat = asyncio.ensure_future(self._beat(key))
        try:
            try:
                sandbox = await self._sandbox(context)
                await self.receipts.set(key, sandbox=sandbox)
            except BaseException as exc:  # nothing was submitted: the call may be sent again
                await asyncio.shield(self.receipts.transition(key, ("starting",), state="reserved"))
                if isinstance(exc, (asyncio.CancelledError, EffectError)):
                    raise
                raise EffectError(
                    "unavailable", f"no sandbox: {type(exc).__name__}: {exc}", sent=False
                ) from exc
            try:
                execution = await self.backend.submit(sandbox, f"cd /workspace && {command}")
            except Exception as exc:  # it may have started: the id never came back
                reason = f"the submit answer was lost before its id was kept: {exc}"[:300]
                await self.receipts.transition(key, ("starting",), state="unknown", reason=reason)
                raise EffectError("indeterminate", reason) from None
        finally:
            beat.cancel()
        recorded = await self.receipts.transition(
            key, ("starting",), execution=execution, state="submitted"
        )
        if not recorded:  # cancelled or given up meanwhile: what was just submitted must stop
            with contextlib.suppress(Exception):
                await self.backend.interrupt(sandbox, execution)
            await self.receipts.set(key, execution=execution)
        return await self._follow(key, context)

    async def _beat(self, key: str) -> None:
        while True:
            await asyncio.sleep(self.start_grace / 3)
            with contextlib.suppress(Exception):
                await self.receipts.heartbeat(key)

    async def _settled(self, key: str) -> tuple[str, Any]:
        """What the receipt and the sandbox say now: (state, result or reason)."""
        row = await self.receipts.get(key)
        if row is None or row["state"] == "reserved":
            return "absent", None
        if row["state"] == "completed":
            return "completed", row["result"]
        if row["state"] in ("unknown", "cancelled"):
            return "unknown", row["reason"] or "cancelled midway; it may have done part of it"
        if not row["execution"]:
            reason = "its start stopped before the submit answer was kept: it may have run"
            if await self.receipts.expire_start(key, self.start_grace, reason):
                return "unknown", reason
            return await self._settled(key) if not await self._starting(key) else ("running", None)
        try:
            status = await self.backend.status(row["sandbox"], row["execution"])
        except Exception as exc:
            with contextlib.suppress(Exception):
                if await self.backend.state(row["sandbox"]) in GONE:
                    return await self._unknown(key, "the sandbox is gone; it may have run")
            return "running", f"cannot read the sandbox now: {exc}"[:200]
        if status is None:
            return await self._unknown(
                key, "the sandbox lost the execution's record (its command daemon restarted)"
            )
        if status["running"]:
            return "running", None
        output = await self.backend.output(row["sandbox"], row["execution"])
        result = {"exit": status["exit"], "output": output}
        if await self.receipts.transition(key, ("submitted",), state="completed", result=result):
            await self._settled_call(row)
            return "completed", result
        return await self._settled(key)  # someone else settled it first

    async def _starting(self, key: str) -> bool:
        row = await self.receipts.get(key)
        return row is not None and row["state"] == "starting" and not row["execution"]

    async def _unknown(self, key: str, reason: str) -> tuple[str, Any]:
        if await self.receipts.transition(key, ("submitted",), state="unknown", reason=reason):
            row = await self.receipts.get(key)
            if row is not None:
                await self._settled_call(row)
            return "unknown", reason
        return await self._settled(key)

    async def _settled_call(self, row: dict[str, Any]) -> None:
        """A call-scope sandbox is not used again once its call settled."""
        if self.scope != "call" or not row.get("sandbox"):
            return
        sandbox_row = next(
            (r for r in await self.receipts.sandboxes() if r["sandbox"] == row["sandbox"]), None
        )
        if sandbox_row is not None:
            await self._destroy_scope(sandbox_row)

    async def _ended(self, key: str) -> None:
        """Until the sandbox says the execution stopped (or it, or its sandbox, is gone): what
        capacity waits for, also after a cancel (asking is not stopping)."""
        while True:
            row = await self.receipts.get(key)
            if row is None or row["state"] in ("reserved", "completed"):
                return
            if not row["execution"]:
                if row["state"] != "starting":
                    return
                if time.time() - (row["updated"] or 0) > self.start_grace:
                    return
            else:
                try:
                    status = await self.backend.status(row["sandbox"], row["execution"])
                except Exception:
                    status = {"running": True}  # cannot tell now: keep holding, unless the
                    with contextlib.suppress(Exception):  # sandbox itself is gone
                        if await self.backend.state(row["sandbox"]) in GONE:
                            status = None
                if status is None or not status["running"]:
                    if row["state"] == "submitted":
                        await self._settled(key)  # keep the result in the receipt
                    return
            await asyncio.sleep(self.poll)

    async def _follow(self, key: str, context: Any) -> Any:
        context.work.append(self._watch(key))
        try:
            while True:
                state, found = await self._settled(key)
                if state == "completed":
                    return found
                if state == "unknown":
                    raise EffectError("indeterminate", str(found))
                await asyncio.sleep(self.poll)
        except asyncio.CancelledError:
            await asyncio.shield(self._cancel(key))
            raise

    async def _cancel(self, key: str) -> None:
        reason = "cancelled midway; it may have done part of it"
        if not await self.receipts.transition(
            key, ("starting", "submitted"), state="cancelled", reason=reason
        ):
            return  # settled, or never started
        row = await self.receipts.get(key)
        if row is None:
            return
        if row["sandbox"] and row["execution"]:
            with contextlib.suppress(Exception):
                await self.backend.interrupt(row["sandbox"], row["execution"])
        if self.destroy_on_cancel and row["sandbox"]:
            with contextlib.suppress(Exception):
                await self.backend.delete(row["sandbox"])

    async def cancel(self, operation: str, key: str, context: Any) -> None:
        await self._cancel(key)

    async def reconcile(self, operation: str, key: str, context: Any) -> Reconciled:
        state, found = await self._settled(key)
        if state == "completed":
            return Reconciled("completed", result=found)
        if state == "absent":
            return Reconciled("absent")
        if state == "running":
            return Reconciled("running", work=self._watch(key))
        return Reconciled("unknown", reason=str(found or ""))


__all__ = ["OpenSandboxProvider"]
