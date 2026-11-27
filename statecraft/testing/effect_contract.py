"""The effect provider conformance suite (public testing API).

A provider package runs it by defining an `effect_harness` fixture and importing the tests:

    from statecraft.testing.effect_contract import *  # noqa: F403
    from statecraft.testing.effect_contract import EffectHarness

    @pytest.fixture
    def effect_harness(tmp_path):
        return EffectHarness(provider=..., operation="exec", request=..., applied=...,
                             slow_request=...)

Baseline tests run for every provider. Each optional capability's tests run only when the
provider declares it (`provider.capabilities`), so a simple provider need not implement remote
execution or reconciliation:

  baseline        a request is answered with JSON; failures are classified EffectErrors; a
                  cancelled call returns at once
  deduplicated    one key, one effect: repeated, concurrent and retried sends of a key apply it
                  once; the same key with another request is refused and applies nothing
  reconcile       a completed key reconciles to its result without a new effect; an unknown key
                  is `absent`; a key still running is `running` with a future that ends with it
  remote_work     a cancelled call registers work in `context.work` that ends only when the work
                  has really ended; nothing of it runs afterwards
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest

from ..kernel.effects import FAILURES, EffectError
from ..ports.effects import EffectContext, Reconciled


@dataclass
class EffectHarness:
    """How the suite drives and observes one provider."""

    provider: Any
    operation: str
    #: a request whose effect can be counted by its tag
    request: Callable[[str], dict[str, Any]]
    #: how many times the tagged request took effect
    applied: Callable[[str], int]
    #: a tagged request that takes effect at once and then runs `seconds`
    slow_request: Callable[[str, float], dict[str, Any]]
    #: a request the provider refuses (classified), when it has one
    bad_request: dict[str, Any] | None = None
    workspace: str = "w"
    scope: dict[str, Any] = field(
        default_factory=lambda: {"workspace": "w", "shared": "rw", "private": "home/a/private"}
    )

    def context(self, key: str | None = None) -> EffectContext:
        return EffectContext(
            root="r",
            agent="r",
            step=0,
            state="s",
            workspace=self.workspace,
            key=key,
            deadline=None,
            scope=dict(self.scope),
        )

    def declares(self, capability: str) -> bool:
        return capability in (getattr(self.provider, "capabilities", None) or ())

    def needs(self, capability: str) -> None:
        if not self.declares(capability):
            pytest.skip(f"the provider does not declare {capability}")


def _tag() -> str:
    return "t" + uuid.uuid4().hex[:12]


async def _invoke(h: EffectHarness, request: dict[str, Any], key: str | None = None) -> Any:
    return await h.provider.invoke(h.operation, request, h.context(key))


# ---- baseline -----------------------------------------------------------------------------------
async def test_a_request_is_answered_with_json(effect_harness: EffectHarness) -> None:
    tag = _tag()
    result = await _invoke(effect_harness, effect_harness.request(tag), key=f"k-{tag}")
    json.dumps(result)
    assert effect_harness.applied(tag) == 1


async def test_failures_are_classified(effect_harness: EffectHarness) -> None:
    if effect_harness.bad_request is None:
        pytest.skip("no bad request given")
    with pytest.raises(EffectError) as raised:
        await _invoke(effect_harness, effect_harness.bad_request, key=f"k-{_tag()}")
    assert raised.value.cls in FAILURES


async def test_a_cancelled_call_returns_at_once(effect_harness: EffectHarness) -> None:
    tag = _tag()
    call = asyncio.ensure_future(
        _invoke(effect_harness, effect_harness.slow_request(tag, 30), key=f"k-{tag}")
    )
    await asyncio.sleep(0.2)
    call.cancel()
    await asyncio.wait_for(asyncio.gather(call, return_exceptions=True), 2)
    assert call.cancelled() or call.exception() is not None


# ---- deduplicated -------------------------------------------------------------------------------
async def test_one_key_one_effect(effect_harness: EffectHarness) -> None:
    effect_harness.needs("deduplicated")
    tag, key = _tag(), f"k-{_tag()}"
    first = await _invoke(effect_harness, effect_harness.request(tag), key)
    again = await _invoke(effect_harness, effect_harness.request(tag), key)
    assert again == first and effect_harness.applied(tag) == 1


async def test_concurrent_sends_of_a_key_apply_it_once(effect_harness: EffectHarness) -> None:
    effect_harness.needs("deduplicated")
    tag, key = _tag(), f"k-{_tag()}"
    results = await asyncio.gather(
        *(_invoke(effect_harness, effect_harness.request(tag), key) for _ in range(5))
    )
    assert all(r == results[0] for r in results) and effect_harness.applied(tag) == 1


async def test_a_key_names_one_request(effect_harness: EffectHarness) -> None:
    effect_harness.needs("deduplicated")
    tag, other, key = _tag(), _tag(), f"k-{_tag()}"
    await _invoke(effect_harness, effect_harness.request(tag), key)
    with pytest.raises(EffectError) as raised:
        await _invoke(effect_harness, effect_harness.request(other), key)
    assert raised.value.sent is False and effect_harness.applied(other) == 0


# ---- reconcile ----------------------------------------------------------------------------------
async def test_a_completed_key_reconciles_to_its_result(effect_harness: EffectHarness) -> None:
    effect_harness.needs("reconcile")
    tag, key = _tag(), f"k-{_tag()}"
    result = await _invoke(effect_harness, effect_harness.request(tag), key)
    found = await effect_harness.provider.reconcile(
        effect_harness.operation, key, effect_harness.context(key)
    )
    assert isinstance(found, Reconciled) and found.status == "completed"
    assert found.result == result and effect_harness.applied(tag) == 1


async def test_an_unknown_key_is_absent(effect_harness: EffectHarness) -> None:
    effect_harness.needs("reconcile")
    key = f"k-{_tag()}"
    found = await effect_harness.provider.reconcile(
        effect_harness.operation, key, effect_harness.context(key)
    )
    assert found.status == "absent"


async def test_a_running_key_reconciles_to_running(effect_harness: EffectHarness) -> None:
    effect_harness.needs("reconcile")
    tag, key = _tag(), f"k-{_tag()}"
    call = asyncio.ensure_future(
        _invoke(effect_harness, effect_harness.slow_request(tag, 0.5), key)
    )
    await asyncio.sleep(0.2)
    found = await effect_harness.provider.reconcile(
        effect_harness.operation, key, effect_harness.context(key)
    )
    assert found.status == "running" and found.work is not None
    await asyncio.wait_for(found.work, 5)
    await asyncio.wait_for(call, 5)
    done = await effect_harness.provider.reconcile(
        effect_harness.operation, key, effect_harness.context(key)
    )
    assert done.status == "completed" and effect_harness.applied(tag) == 1


# ---- remote work --------------------------------------------------------------------------------
async def test_a_cancelled_call_holds_its_work_until_it_ended(
    effect_harness: EffectHarness,
) -> None:
    effect_harness.needs("remote_work")
    tag, key = _tag(), f"k-{_tag()}"
    context = effect_harness.context(key)
    call = asyncio.ensure_future(
        effect_harness.provider.invoke(
            effect_harness.operation, effect_harness.slow_request(tag, 30), context
        )
    )
    await asyncio.sleep(0.2)
    call.cancel()
    await asyncio.gather(call, return_exceptions=True)
    assert context.work, "work that outlives the call must be registered"
    await asyncio.wait_for(asyncio.gather(*context.work, return_exceptions=True), 5)
    assert all(w.done() for w in context.work)
    if effect_harness.declares("reconcile"):
        found = await effect_harness.provider.reconcile(effect_harness.operation, key, context)
        assert found.status in ("unknown", "completed", "failed")  # never running, never absent


__all__ = [
    "EffectHarness",
    "test_a_cancelled_call_holds_its_work_until_it_ended",
    "test_a_cancelled_call_returns_at_once",
    "test_a_completed_key_reconciles_to_its_result",
    "test_a_key_names_one_request",
    "test_a_request_is_answered_with_json",
    "test_a_running_key_reconciles_to_running",
    "test_an_unknown_key_is_absent",
    "test_concurrent_sends_of_a_key_apply_it_once",
    "test_failures_are_classified",
    "test_one_key_one_effect",
]
