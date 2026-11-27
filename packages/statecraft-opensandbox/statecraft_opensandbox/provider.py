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

#: how long a claimed start may take before a receipt without an execution id is unknown
START_GRACE = 30.0
SCOPES = ("agent", "run", "call")


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
    ) -> None:
        if scope not in SCOPES:
            raise ValueError(f"scope must be one of {SCOPES}, not {scope!r}")
        self.backend = backend
        self.name = name
        self.timeout = timeout
        self.receipts = Receipts(receipts)
        self.scope = scope
        self.destroy_on_cancel = destroy_on_cancel
        self.concurrency = concurrency
        self.poll = poll
        self.cleanup_every = cleanup_every
        self.capabilities = frozenset(
            {"deduplicated", "reconcile", "remote_work"}
            | ({"tree_termination"} if destroy_on_cancel else set())
        )
        self._view: Any = None
        self._calls = 0
        self._creating: dict[str, asyncio.Lock] = {}

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
        self.receipts.close()

    # ---- sandboxes ----------------------------------------------------------------------------
    def _scope_key(self, context: Any) -> str:
        if self.scope == "agent":
            return f"agent:{context.agent}"
        if self.scope == "run":
            return f"run:{context.root}"
        return f"call:{context.key}"

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
        """The sandbox of this call's scope: found, or created once (single flight in this
        process; across processes, the label chosen before the create finds a lost answer)."""
        scope = self._scope_key(context)
        lock = self._creating.setdefault(scope, asyncio.Lock())
        async with lock:
            row = await self.receipts.sandbox(scope)
            if row is not None and row["sandbox"]:
                state = await self.backend.state(row["sandbox"])
                if state in ("Running", "Pending"):
                    if state == "Pending":
                        await self._until_running(row["sandbox"])
                    return row["sandbox"]
                await self.receipts.drop_sandbox(scope)  # gone or failed: make another
                row = None
            if row is None:
                row = await self.receipts.begin_sandbox(scope, uuid.uuid4().hex, context.agent)
            labels = {"statecraft-scope": row["label"], "statecraft-agent": _label(context.agent)}
            found = await self.backend.find({"statecraft-scope": row["label"]})
            if found:
                sandbox = found[0]["id"]
            else:
                try:
                    sandbox = await self.backend.create(labels, self._mounts(context))
                except EffectError:
                    raise
                except Exception:  # the answer may be lost: the label finds it again
                    found = await self.backend.find({"statecraft-scope": row["label"]})
                    if not found:
                        raise
                    sandbox = found[0]["id"]
            await self.receipts.set_sandbox(scope, sandbox=sandbox, state="ready")
            await self._until_running(sandbox)
            return sandbox

    async def _until_running(self, sandbox: str, limit: float = 120) -> None:
        end = time.monotonic() + limit
        while time.monotonic() < end:
            state = await self.backend.state(sandbox)
            if state == "Running":
                return
            if state is None or state in ("Failed", "Terminated", "Stopped"):
                raise EffectError("unavailable", f"sandbox {sandbox} is {state}", sent=False)
            await asyncio.sleep(self.poll)
        raise EffectError("unavailable", f"sandbox {sandbox} did not start", sent=False)

    async def cleanup(self) -> None:
        """Destroy the sandboxes of agents that ended (scope `agent`)."""
        if self._view is None:
            return
        rows = [r for r in await self.receipts.sandboxes() if r["scope"].startswith("agent:")]
        if not rows:
            return
        ended = await self._view.ended([r["agent"] for r in rows])
        for row in rows:
            if row["agent"] in ended:
                if row["sandbox"]:
                    with contextlib.suppress(Exception):
                        await self.backend.delete(row["sandbox"])
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
        receipt = await self.receipts.reserve(
            context.key, self._digest(arguments, context), context.agent
        )
        if receipt["digest"] != self._digest(arguments, context):
            raise EffectError(
                "invalid", "this call identity was used for another command", sent=False
            )
        if not await self.receipts.claim(context.key):
            return await self._follow(context.key, context)
        sandbox = await self._sandbox(context)
        await self.receipts.set(context.key, sandbox=sandbox)
        try:
            execution = await self.backend.submit(sandbox, f"cd /workspace && {command}")
        except Exception as exc:  # it may have started: the id never came back
            reason = f"the submit answer was lost before its id was kept: {exc}"[:300]
            await self.receipts.set(context.key, state="unknown", reason=reason)
            raise EffectError("indeterminate", reason) from None
        await self.receipts.set(context.key, execution=execution, state="submitted")
        return await self._follow(context.key, context)

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
            if time.time() - (row["updated"] or 0) < START_GRACE:
                return "running", None
            return "unknown", "the submit answer was lost before its id was kept"
        try:
            status = await self.backend.status(row["sandbox"], row["execution"])
        except Exception as exc:
            return "running", f"cannot read the sandbox now: {exc}"[:200]
        if status is None:
            reason = "the sandbox lost the execution's record (its command daemon restarted)"
            await self.receipts.set(key, state="unknown", reason=reason)
            return "unknown", reason
        if status["running"]:
            return "running", None
        output = await self.backend.output(row["sandbox"], row["execution"])
        result = {"exit": status["exit"], "output": output}
        await self.receipts.set(key, state="completed", result=result)
        return "completed", result

    async def _ended(self, key: str) -> None:
        """Until the sandbox says the execution stopped (or it, or its sandbox, is gone): what
        capacity waits for, also after a cancel (asking is not stopping)."""
        while True:
            row = await self.receipts.get(key)
            if row is None or row["state"] in ("reserved", "completed"):
                return
            if not row["execution"]:
                if row["state"] != "starting" or time.time() - (row["updated"] or 0) > START_GRACE:
                    return
            else:
                try:
                    status = await self.backend.status(row["sandbox"], row["execution"])
                except Exception:
                    status = {"running": True}  # cannot tell now: keep holding
                if status is None or not status["running"]:
                    if row["state"] == "submitted":
                        await self._settled(key)  # keep the result in the receipt
                    return
            await asyncio.sleep(self.poll)

    async def _follow(self, key: str, context: Any) -> Any:
        ended = asyncio.ensure_future(self._ended(key))
        context.work.append(ended)
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
        row = await self.receipts.get(key)
        if row is None or row["state"] in ("completed", "unknown", "cancelled"):
            return
        if row["state"] == "reserved":
            return
        await self.receipts.set(
            key, state="cancelled", reason="cancelled midway; it may have done part of it"
        )
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
            return Reconciled("running", work=asyncio.ensure_future(self._ended(key)))
        return Reconciled("unknown", reason=str(found or ""))


__all__ = ["OpenSandboxProvider"]
