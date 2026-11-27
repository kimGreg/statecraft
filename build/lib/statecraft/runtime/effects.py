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
import uuid
from typing import TYPE_CHECKING, Any

import jsonschema

from ..kernel.effects import EffectError, digest_of, may_retry
from ..kernel.store import CallRefused, OverBudget, StateStore
from ..ports.effects import EffectContext, Reconciled
from . import telemetry

if TYPE_CHECKING:
    from .context import StepContext
    from .registry import Registry


#: The value `_settle_earlier` returns when the request is to be sent.
_SEND = object()
#: How many times a running earlier execution is waited for before its outcome is unknown.
RECONCILE_ROUNDS = 3


def capabilities_of(provider: Any) -> frozenset[str]:
    return frozenset(getattr(provider, "capabilities", None) or ())


def _size(value: Any) -> int:
    try:
        return len(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return -1


class EffectManager:
    def __init__(
        self, registry: Registry, *, backoff: float = 0.05, store: StateStore | None = None
    ) -> None:
        self.registry = registry
        #: the runtime's store, where keyed calls are journaled (identity, digest, outcome)
        self.store = store
        self.backoff = backoff
        self._capacity: dict[str, asyncio.Semaphore] = {}
        #: waiters that release capacity once work a cancelled call could not stop has ended
        self._lingering: set[asyncio.Task[None]] = set()
        #: an OpenTelemetry tracer (set by the runtime) for a span per call
        self.tracer: Any = None
        #: process counters since start (metrics): calls, failures, retries, capacity waits
        self.counters = {"calls": 0, "failures": 0, "retries": 0, "capacity_waits": 0}
        self._limits: dict[str, int] = {}

    def capacity(self) -> dict[str, dict[str, int]]:
        """Each limited provider's slots: its limit and how many are held (by calls, and by
        work a cancelled call could not stop); and how many such lingering waits there are."""
        return {
            name: {
                "limit": self._limits[name],
                "held": self._limits[name] - slots._value,
                "lingering": len(self._lingering),
            }
            for name, slots in self._capacity.items()
        }

    def _release_after(self, context: EffectContext, slots: asyncio.Semaphore | None) -> None:
        """Release the call's capacity now, or when the work it started ends."""
        if slots is None:
            return
        running = context.running()
        if not running:
            slots.release()
            return

        async def wait_then_release() -> None:
            try:
                await asyncio.gather(*running, return_exceptions=True)
            finally:
                slots.release()

        task = asyncio.ensure_future(wait_then_release())
        self._lingering.add(task)
        task.add_done_callback(self._lingering.discard)

    def _slots(self, provider_name: str, provider: Any) -> asyncio.Semaphore | None:
        limit = getattr(provider, "concurrency", None)
        if not limit:
            return None
        if provider_name not in self._capacity:
            self._capacity[provider_name] = asyncio.Semaphore(int(limit))
            self._limits[provider_name] = int(limit)
        return self._capacity[provider_name]

    async def call(
        self, ctx: StepContext, name: str, arguments: dict[str, Any], key: str | None = None
    ) -> Any:
        if ctx.effects.blocked:
            first = ctx.effects.blocked[0]
            raise EffectError(
                "refused",
                f"no further calls in this step: {first['reason']}",
                effect=name,
                sent=False,
            )
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
        problems = []
        if ctx.effects.blocked:
            problems += [f"indeterminate: {b['reason']}" for b in ctx.effects.blocked]
        if pending:
            problems.append(f"{len(pending)} tool call(s) were still running when the state ended")
        return "; ".join(problems) or None

    def _journal(self) -> Any:
        if self.store is None:
            raise RuntimeError("the effect manager needs the runtime's store for keyed calls")
        return self.store

    @staticmethod
    def _block(ctx: StepContext, name: str, key: str | None, reason: str) -> None:
        ctx.effects.blocked.append({"effect": name, "key": key, "reason": reason[:300]})

    def _context(
        self,
        ctx: StepContext,
        definition: Any,
        key: str | None,
        scope: dict[str, Any],
        attempt: int,
    ) -> EffectContext:
        return EffectContext(
            root=ctx.id.split("/")[0],
            agent=ctx.id,
            step=ctx.step,
            state=ctx.state_name,
            workspace=ctx._workspace_id,
            key=key,
            deadline=definition.policy.timeout,
            attempt=attempt,
            scope=dict(scope),
        )

    async def _send(
        self,
        ctx: StepContext,
        provider: Any,
        definition: Any,
        name: str,
        arguments: dict[str, Any],
        key: str | None,
        scope: dict[str, Any],
        slots: asyncio.Semaphore | None,
        crossing: dict[str, Any],
    ) -> Any:
        """Send the request, again as its policy allows; the last failure is raised."""
        policy = definition.policy
        attempt = 0
        while True:
            attempt += 1
            crossing["attempts"] = attempt
            context = self._context(ctx, definition, key, scope, attempt)
            try:
                waited = time.monotonic()
                if slots is not None:
                    if slots.locked():
                        self.counters["capacity_waits"] += 1
                    await slots.acquire()
                crossing["waited"] = round(crossing.get("waited", 0) + time.monotonic() - waited, 4)
                try:
                    return await self._metered(
                        ctx,
                        provider,
                        definition,
                        name,
                        arguments,
                        lambda context=context: asyncio.wait_for(
                            provider.invoke(definition.operation or name, arguments, context),
                            policy.timeout,
                        ),
                    )
                finally:
                    self._release_after(context, slots)
            except TimeoutError as exc:
                failure = EffectError("timeout", f"no result in {policy.timeout}s", effect=name)
                failure.__cause__ = exc
                # work that cannot be stopped is still running: no other attempt overlaps it
                if context.running():
                    crossing["lingered"] = True
                    await asyncio.gather(*context.running(), return_exceptions=True)
            except EffectError as exc:
                failure = exc
                failure.effect = failure.effect or name
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # a provider bug: whether it was sent is unknown
                failure = EffectError("transient", f"{type(exc).__name__}: {exc}", effect=name)
            if not may_retry(policy, failure, keyed=bool(key), attempt=attempt):
                raise failure
            self.counters["retries"] += 1
            await asyncio.sleep(min(self.backoff * 2 ** (attempt - 1), 2.0))

    async def _metered(
        self,
        ctx: StepContext,
        provider: Any,
        definition: Any,
        name: str,
        arguments: dict[str, Any],
        send: Any,
    ) -> Any:
        """Send, holding the tree's budget first when the call is metered and its provider says
        what a send may cost (`estimate_of(operation, arguments)`); settle with what it used
        (`charge_of(operation, result)`), with its hold when it may have been sent and said
        nothing, or release the hold when it provably was not sent. Over budget: refused, not
        sent. A tree without a budget holds nothing."""
        estimate = getattr(provider, "estimate_of", None)
        if definition.policy.cost != "metered" or estimate is None or self.store is None:
            return await send()
        operation = definition.operation or name
        root, hold = ctx.record.id.split("/")[0], f"{ctx.id}@{ctx.lease.epoch}/{uuid.uuid4().hex}"
        try:
            held = await self.store.reserve(
                root,
                hold,
                dict(estimate(operation, arguments) or {}),
                agent=ctx.id,
                epoch=ctx.lease.epoch,
            )
        except OverBudget as exc:
            raise EffectError("refused", str(exc), effect=name, sent=False) from None
        if not held:
            return await send()
        try:
            result = await send()
        except EffectError as exc:
            await self.store.settle(root, hold, None if not exc.sent else {})
            raise
        except BaseException:
            await asyncio.shield(self.store.settle(root, hold, {}))  # it may have been sent
            raise
        charge = getattr(provider, "charge_of", None)
        used = dict(charge(operation, result) or {}) if charge is not None else {}
        await self.store.settle(root, hold, used)
        return result

    async def _settle_earlier(
        self,
        ctx: StepContext,
        provider: Any,
        definition: Any,
        name: str,
        arguments: dict[str, Any],
        key: str | None,
        scope: dict[str, Any],
        slots: asyncio.Semaphore | None,
        crossing: dict[str, Any],
    ) -> Any:
        """Journal a keyed call before it is sent, and settle an earlier send of it (a rerun after
        a crash, a CONT after an unknown outcome): its established result, `_SEND` when sending
        is safe, or an error. Never sends a request whose earlier outcome is unknown, except
        under the provider's own key contract (`resend`) or an operator's `replay`."""
        digest = digest_of(
            {
                # the request, not how it is run: a policy change (repin) keeps the identity
                "binding": {
                    k: v
                    for k, v in (ctx.record.bindings.get(name) or {}).items()
                    if k != "policy_digest"
                },
                "operation": definition.operation or name,
                "arguments": arguments,
                "workspace": ctx._workspace_id,
                "scope": scope,
            }
        )
        journal = self._journal()
        try:
            entry, created = await journal.begin_call(ctx.id, key, name, digest, time.time())
        except CallRefused as exc:
            self._block(ctx, name, key, str(exc))
            raise EffectError("refused", str(exc), effect=name, sent=False) from None
        # an operator's replay is a new request to the provider: a new generation of the key
        generation = int(entry.get("generation") or 0)
        sent_key = key if not generation else f"{key}~g{generation}"
        if created:
            return _SEND, sent_key
        status = entry["status"]
        crossing["earlier"] = status
        if status == "completed" and "result" in entry:  # established by an operator
            return entry["result"], sent_key
        if status in ("replay", "failed"):  # allowed once by a person; or definitely not applied
            await journal.end_call(key, "sent", generation=generation)
            return _SEND, sent_key
        can_reconcile = "reconcile" in capabilities_of(provider)
        if status == "indeterminate" and entry.get("clear_by") == "operator":
            reason = f"{name}: the outcome of call {key} is unknown; an operator must resolve it"
            self._block(ctx, name, key, reason)
            raise EffectError("indeterminate", reason, effect=name, sent=False)
        if not can_reconcile:
            # a deduplicated provider applies a key once: sending it again settles it
            return _SEND, sent_key
        value = await self._reconcile(
            ctx, provider, definition, name, key, sent_key, scope, slots, crossing
        )
        return value, sent_key

    async def _reconcile(
        self,
        ctx: StepContext,
        provider: Any,
        definition: Any,
        name: str,
        key: str | None,
        sent_key: str | None,
        scope: dict[str, Any],
        slots: asyncio.Semaphore | None,
        crossing: dict[str, Any],
    ) -> Any:
        """Ask the provider what became of an earlier send of this key, without sending it."""
        journal = self._journal()
        operation = definition.operation or name
        for _ in range(RECONCILE_ROUNDS):
            context = self._context(ctx, definition, sent_key, scope, 0)
            try:
                found = await asyncio.wait_for(
                    provider.reconcile(operation, sent_key, context), definition.policy.timeout
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # cannot be established now: still unknown
                found = Reconciled("unknown", error=EffectError("transient", str(exc)[:300]))
            crossing["reconciled"] = found.status
            if found.status == "completed":
                await journal.end_call(key, "completed")
                return found.result
            if found.status == "failed":
                error = found.error or EffectError("failed", "failed earlier", effect=name)
                await journal.end_call(key, "failed", error=error.message[:300])
                raise error
            if found.status == "absent":
                await journal.end_call(key, "sent")
                return _SEND
            if found.status == "running" and found.work is not None:
                # a running execution holds capacity like a new one, until it really ended
                if slots is not None:
                    if slots.locked():
                        self.counters["capacity_waits"] += 1
                    await slots.acquire()
                try:
                    await asyncio.wait_for(asyncio.shield(found.work), definition.policy.timeout)
                except TimeoutError:
                    self._release_when(found.work, slots)
                    break  # still running past the deadline: unknown for now
                except asyncio.CancelledError:  # KILL or lease loss while reattached
                    await self._cancel_remote(provider, operation, sent_key, context)
                    self._release_when(found.work, slots)
                    raise
                if slots is not None:
                    slots.release()
                continue
            break
        reason = f"{name}: the outcome of call {key} cannot be established yet"
        if found.reason:
            reason += f" ({found.reason[:200]})"
        self._block(ctx, name, key, reason)  # first: blocked even if journaling fails
        await journal.end_call(key, "indeterminate", clear_by="reconcile", error=reason)
        raise EffectError("indeterminate", reason, effect=name, sent=False)

    async def _unsettled(
        self, ctx: StepContext, provider: Any, name: str, key: str | None, failure: EffectError
    ) -> EffectError:
        """A keyed call that failed: a definite failure is journaled `failed` and raised as it
        is; an unknown outcome is journaled `indeterminate`, blocks the step and is raised."""
        journal = self._journal()
        unknown = failure.cls in ("transient", "timeout") or (
            failure.cls == "unavailable" and failure.sent
        )
        if failure.cls != "indeterminate" and not unknown:
            await journal.end_call(key, "failed", error=failure.message[:300])
            return failure
        can_reconcile = "reconcile" in capabilities_of(provider)
        if failure.cls == "indeterminate":
            clear_by = "reconcile" if can_reconcile else "operator"
        else:
            clear_by = "reconcile" if can_reconcile else "resend"
        reason = f"{name}: outcome unknown ({failure.cls}: {failure.message[:200]})"
        self._block(ctx, name, key, reason)  # first: blocked even if journaling fails
        await journal.end_call(key, "indeterminate", clear_by=clear_by, error=reason)
        return EffectError("indeterminate", reason, effect=name, code=failure.code)

    async def _cancelled(self, ctx: StepContext, provider: Any, name: str, key: str | None) -> None:
        """A keyed call cancelled after it was sent (the state's own timeout, KILL, lease loss):
        it may have been applied. The step is blocked and the call journaled unknown, so a new
        identity cannot go around it and a later run settles it first."""
        reason = f"{name}: call {key} was cancelled after it was sent; its outcome is unknown"
        self._block(ctx, name, key, reason)
        clear_by = "reconcile" if "reconcile" in capabilities_of(provider) else "resend"
        with contextlib.suppress(Exception):
            await asyncio.shield(
                self._journal().end_call(key, "indeterminate", clear_by=clear_by, error=reason)
            )

    @staticmethod
    async def _cancel_remote(provider: Any, operation: str, key: str | None, context: Any) -> None:
        cancel = getattr(provider, "cancel", None)
        if cancel is not None:
            with contextlib.suppress(Exception):
                await asyncio.shield(cancel(operation, key, context))

    def _release_when(self, work: asyncio.Future[Any], slots: asyncio.Semaphore | None) -> None:
        """Give a slot back once `work` ended (now, if it already did)."""
        if slots is None:
            return
        if work.done():
            slots.release()
            return

        async def wait_then_release() -> None:
            try:
                await asyncio.gather(work, return_exceptions=True)
            finally:
                slots.release()

        task = asyncio.ensure_future(wait_then_release())
        self._lingering.add(task)
        task.add_done_callback(self._lingering.discard)

    async def _call(
        self, ctx: StepContext, name: str, arguments: dict[str, Any], key: str | None
    ) -> Any:
        binding = ctx.record.bindings.get(name)
        crossing: dict[str, Any] = {"effect": name, "attempts": 0, "ok": False}
        started = time.monotonic()
        try:
            if binding is None:
                provider_name = name.split(".", 1)[0]
                if provider_name in self.registry.unreachable:
                    raise EffectError(
                        "unavailable",
                        f"its provider {provider_name} could not be reached when the runtime "
                        "started, so this agent was not bound to it; restart when it is "
                        "reachable, then repin the agent",
                        effect=name,
                        sent=False,
                    )
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
                    "a deduplicated tool needs the call's identity: ctx.call(..., "
                    "call_name=...) or idempotency_key=...",
                    effect=name,
                    sent=False,
                )
            try:
                jsonschema.validate(arguments, definition.input_schema)
            except jsonschema.ValidationError as exc:
                raise EffectError("invalid", exc.message[:300], effect=name, sent=False) from exc
            slots = self._slots(definition.provider, provider)
            scope = ctx.scope()
            result: Any = _SEND
            sent_key = key
            if policy.kind == "deduplicated":
                result, sent_key = await self._settle_earlier(
                    ctx, provider, definition, name, arguments, key, scope, slots, crossing
                )
            if result is _SEND:
                try:
                    result = await self._send(
                        ctx, provider, definition, name, arguments, sent_key, scope, slots, crossing
                    )
                except EffectError as failure:
                    if policy.kind == "deduplicated":
                        raise await self._unsettled(ctx, provider, name, key, failure) from None
                    raise
                except asyncio.CancelledError:
                    if policy.kind == "deduplicated" and crossing.get("attempts"):
                        await self._cancelled(ctx, provider, name, key)
                    raise
                if policy.kind == "deduplicated":
                    await self._journal().end_call(key, "completed")
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
            self.counters["failures"] += 1
            raise
        finally:
            crossing["duration"] = round(time.monotonic() - started, 4)
            ctx.effects.crossings.append(crossing)
            self.counters["calls"] += 1
            telemetry.effect_span(self.tracer, crossing)


def _structured(result: Any) -> Any:
    """Output schemas of plain values are validated as the value; MCP-style `{"result": v}`
    wrappers are accepted as they are."""
    return result


__all__ = ["EffectContext", "EffectManager"]
