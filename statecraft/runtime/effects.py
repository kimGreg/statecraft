"""The effect manager: every `ctx.call` goes through it (architecture §7.3).

    permission (the instance's pinned bindings) ▸ policy allows the call at all ▸ input schema
    ▸ provider capacity ▸ invoke with a deadline ▸ classify a failure ▸ retry by policy
    ▸ output schema ▸ record a crossing in the step

Calls are tracked per step: when the state ends, a call still running is cancelled and the step
faults (work started in a state ends before it commits, architecture §5.1).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import jsonschema

from ..kernel.effects import EffectError, may_retry

if TYPE_CHECKING:
    from .context import StepContext
    from .registry import Registry


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
    extra: dict[str, Any] = field(default_factory=dict)


def _size(value: Any) -> int:
    try:
        return len(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return -1


class EffectManager:
    def __init__(self, registry: Registry, *, backoff: float = 0.05) -> None:
        self.registry = registry
        self.backoff = backoff
        self._capacity: dict[str, asyncio.Semaphore] = {}

    def _slots(self, provider_name: str, provider: Any) -> asyncio.Semaphore | None:
        limit = getattr(provider, "concurrency", None)
        if not limit:
            return None
        if provider_name not in self._capacity:
            self._capacity[provider_name] = asyncio.Semaphore(int(limit))
        return self._capacity[provider_name]

    async def call(
        self, ctx: StepContext, name: str, arguments: dict[str, Any], key: str | None = None
    ) -> Any:
        task = asyncio.ensure_future(self._call(ctx, name, arguments, key))
        ctx._inflight.add(task)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # the caller gave up on this call (its own wait_for, a lost race, KILL): end it
            # before going on, so it is not counted as still running when the state ends
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
            raise
        finally:
            if task.done():
                ctx._inflight.discard(task)

    async def settle(self, ctx: StepContext) -> str | None:
        """Cancel calls still running when the state ended; a problem when there were some."""
        pending = [t for t in ctx._inflight if not t.done()]
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(BaseException):  # cancelled or failed: no longer matters
                await task
        ctx._inflight.clear()
        if pending:
            return f"{len(pending)} tool call(s) were still running when the state ended"
        return None

    async def _call(
        self, ctx: StepContext, name: str, arguments: dict[str, Any], key: str | None
    ) -> Any:
        binding = ctx.record.bindings.get(name)
        crossing: dict[str, Any] = {"effect": name, "attempts": 0, "ok": False}
        started = time.monotonic()
        try:
            if binding is None:
                raise EffectError("refused", "this agent may not call it", effect=name, sent=False)
            definition = self.registry.effect(name, binding)
            if definition is None:
                raise EffectError(
                    "refused",
                    "changed or gone since this instance started",
                    effect=name,
                    sent=False,
                )
            provider = self.registry.providers.get(definition.provider)
            if provider is None:
                raise EffectError(
                    "unavailable", f"no provider {definition.provider!r}", effect=name, sent=False
                )
            crossing.update(
                provider=definition.provider,
                operation=definition.operation,
                request_size=_size(arguments),
            )
            policy = definition.policy
            if policy.kind == "non_repeatable":
                raise EffectError(
                    "refused",
                    "non_repeatable tools cannot be called from a state that may run again",
                    effect=name,
                    sent=False,
                )
            if policy.kind == "deduplicated" and not key:
                raise EffectError(
                    "refused",
                    "a deduplicated tool needs idempotency_key=ctx.effect_key(...)",
                    effect=name,
                    sent=False,
                )
            try:
                jsonschema.validate(arguments, definition.input_schema)
            except jsonschema.ValidationError as exc:
                raise EffectError("invalid", exc.message[:300], effect=name, sent=False) from exc
            slots = self._slots(definition.provider, provider)
            attempt = 0
            while True:
                attempt += 1
                crossing["attempts"] = attempt
                context = EffectContext(
                    root=ctx.id.split("/")[0],
                    agent=ctx.id,
                    step=ctx.step,
                    state=ctx.state_name,
                    workspace=ctx._workspace_id,
                    key=key,
                    deadline=policy.timeout,
                    attempt=attempt,
                )
                try:
                    waited = time.monotonic()
                    if slots is not None:
                        await slots.acquire()
                    crossing["waited"] = round(time.monotonic() - waited, 4)
                    try:
                        result = await asyncio.wait_for(
                            provider.invoke(definition.operation or name, arguments, context),
                            policy.timeout,
                        )
                    finally:
                        if slots is not None:
                            slots.release()
                except TimeoutError as exc:
                    failure = EffectError("timeout", f"no result in {policy.timeout}s", effect=name)
                    failure.__cause__ = exc
                except EffectError as exc:
                    failure = exc
                    failure.effect = failure.effect or name
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # a provider bug: whether it was sent is unknown
                    failure = EffectError("transient", f"{type(exc).__name__}: {exc}", effect=name)
                else:
                    break
                if not may_retry(policy, failure, keyed=bool(key), attempt=attempt):
                    raise failure
                await asyncio.sleep(min(self.backoff * 2 ** (attempt - 1), 2.0))
            if definition.output_schema is not None:
                try:
                    jsonschema.validate(_structured(result), definition.output_schema)
                except jsonschema.ValidationError as exc:
                    raise EffectError(
                        "failed", f"result breaks its schema: {exc.message[:200]}", effect=name
                    ) from exc
            crossing.update(ok=True, response_size=_size(result))
            usage = getattr(provider, "usage_of", None)
            if usage is not None:
                crossing["usage"] = usage(result)
            return result
        except EffectError as exc:
            crossing.update(failure=exc.cls, error=exc.message[:300])
            raise
        finally:
            crossing["duration"] = round(time.monotonic() - started, 4)
            ctx.effects.crossings.append(crossing)


def _structured(result: Any) -> Any:
    """Output schemas of plain values are validated as the value; MCP-style `{"result": v}`
    wrappers are accepted as they are."""
    return result


__all__ = ["EffectContext", "EffectManager"]
