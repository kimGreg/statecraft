"""The effect provider port: what a provider gets with each call, what it may declare, and
helpers to classify the failures of network calls. Providers import this, never the runtime.

A provider is any object with `invoke(operation, arguments, context)`; `start()`, `discover()`,
`close()` and `reconcile(operation, key, context)` are optional. It may declare:

    capabilities: frozenset of CAPABILITIES       optional contracts it implements (tested by
                                                  `statecraft.testing.contracts` when declared)
    capacity_scope: one of CAPACITY_SCOPES        where its `concurrency` limit holds
    concurrency: int | None                       calls at once (in this runtime process)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..kernel.effects import EffectError

#: Optional contracts a provider may declare. `deduplicated`: a request carrying a key is
#: applied once per key. `reconcile`: `reconcile(operation, key, context)` tells the outcome of
#: a keyed request sent before (a restart, a lost answer) without sending it again.
#: `remote_work`: work that outlives a cancelled call is registered in `context.work` until it
#: is confirmed ended. `shared_capacity`: `concurrency` holds across processes and hosts.
CAPABILITIES = frozenset({"deduplicated", "reconcile", "remote_work", "shared_capacity"})

#: `process`: the runtime's slots, per runtime process (the default). `shared`: the provider
#: enforces its limit across processes (its own store). `backend`: the backend enforces it.
CAPACITY_SCOPES = ("process", "shared", "backend")


@dataclass
class EffectContext:
    """What a provider gets with each call."""

    root: str
    agent: str
    step: int
    state: str
    workspace: str
    key: str | None
    deadline: float | None
    attempt: int = 1
    #: What this agent may expose to the provider: {"workspace", "shared": "ro"|"rw",
    #: "private"} (computed by the runtime from grants and attachment; never from the model).
    scope: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)
    #: Work the provider started that cancelling the call cannot stop (a thread, a process, a
    #: remote execution): the call's capacity stays taken, and no other attempt starts, until
    #: it ends. A remote execution's future ends when its end is confirmed, not when asked.
    work: list[asyncio.Future[Any]] = field(default_factory=list)

    def running(self) -> list[asyncio.Future[Any]]:
        return [w for w in self.work if not w.done()]


@dataclass(frozen=True)
class Reconciled:
    """The outcome of a keyed request sent before. `status`: `completed` (with `result`),
    `failed` (with `error`, an EffectError that was definitely not applied or definitely
    failed), `running` (`work` ends when it does), `absent` (never received: safe to send),
    `unknown` (cannot be established)."""

    status: str
    result: Any = None
    error: EffectError | None = None
    work: asyncio.Future[Any] | None = None


class EffectProvider(Protocol):
    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any: ...


def classify_status(status: int, body: str = "", *, effect: str = "") -> EffectError:
    """An HTTP error answer as a failure: 429 and 5xx `transient`, 408 `timeout`, 401/403
    `refused` (code `auth`), other 4xx `invalid` (code: the status)."""
    text = body[:300]
    if status == 429 or status >= 500:
        return EffectError("transient", f"answered {status}", effect=effect, code=str(status))
    if status == 408:
        return EffectError("timeout", f"answered {status}", effect=effect, code="408")
    if status in (401, 403):
        return EffectError("refused", f"answered {status}: {text}", effect=effect, code="auth")
    return EffectError("invalid", f"answered {status}: {text}", effect=effect, code=str(status))


def classify(exc: BaseException, *, effect: str = "") -> EffectError:
    """An exception as a classified failure, for tools that call networks:

        except Exception as exc:
            raise classify(exc) from exc

    `EffectError` as it is; refused connections `unavailable` (never sent); timeouts `timeout`;
    other connection and transport errors `transient`; an HTTP error answer by its status
    (`classify_status`); anything else `failed` (not retried). Exceptions are recognized by
    type name too, so HTTP libraries need not be imported here."""
    if isinstance(exc, EffectError):
        return exc
    names = {k.__name__ for k in type(exc).__mro__}
    message = f"{type(exc).__name__}: {exc}"[:300]
    if names & {"ConnectionRefusedError", "ConnectError", "ConnectTimeout"}:
        return EffectError("unavailable", message, effect=effect, sent=False)
    if names & {"TimeoutError", "TimeoutException", "ReadTimeout", "WriteTimeout", "PoolTimeout"}:
        return EffectError("timeout", message, effect=effect)
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if isinstance(status, int):
        body = str(getattr(exc.response, "text", "") or "")  # type: ignore[attr-defined]
        return classify_status(status, body, effect=effect)
    if names & {"ConnectionError", "TransportError", "RemoteProtocolError", "BrokenPipeError"}:
        return EffectError("transient", message, effect=effect)
    return EffectError("failed", message, effect=effect)


__all__ = [
    "CAPABILITIES",
    "CAPACITY_SCOPES",
    "EffectContext",
    "EffectProvider",
    "Reconciled",
    "classify",
    "classify_status",
]
