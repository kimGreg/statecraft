"""Keyed calls: one identity names one request, and an unknown outcome is never replayed.

What must hold
  * a deduplicated call needs a logical identity (`call_name` or `idempotency_key`); the same
    identity with a different request (arguments, binding, workspace or scope) is refused before
    anything is sent, also after a restart;
  * an unknown outcome (the provider cannot tell, or the request may have been applied and no
    answer came) is `indeterminate`: no further call is made in that step and the step faults,
    even if the state catches the error; the journal keeps it across restarts;
  * while a call is unresolved, a new identity for the same effect is refused (no escape by a
    new key);
  * CONT settles it without replay: a deduplicated provider gets the same key again (applied
    once), a provider that can reconcile is asked instead of sent to, and a provider that can do
    neither needs an operator (`resolve_call`: completed with a result, failed, or one replay).
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import pytest

from statecraft import Agent, Done, EffectDefinition, EffectError, EffectPolicy, state
from statecraft.ports.effects import Reconciled
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.runtime.runtime import Runtime
from statecraft.testing import memory_runtime


class Ledger:
    """A deduplicated service: applies each key once; can be told to lose answers."""

    capabilities: ClassVar[frozenset[str]] = frozenset({"deduplicated"})

    def __init__(self) -> None:
        self.applied: dict[str, Any] = {}
        self.sends = 0
        self.lose_answers = 0  # this many sends apply the request but never answer
        self.declare_unknown = False

    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any:
        self.sends += 1
        if self.declare_unknown:
            raise EffectError("indeterminate", "the backend lost track of it")
        if context.key not in self.applied:
            self.applied[context.key] = {"charged": arguments["amount"]}
        if self.lose_answers > 0:
            self.lose_answers -= 1
            await asyncio.sleep(10)  # the answer never comes: the call times out
        return self.applied[context.key]


class Reconciling(Ledger):
    """Can say what became of a key without sending it again."""

    capabilities: ClassVar[frozenset[str]] = frozenset({"deduplicated", "reconcile"})

    def __init__(self) -> None:
        super().__init__()
        self.reconciled = 0

    async def reconcile(self, operation: str, key: str, context: Any) -> Reconciled:
        self.reconciled += 1
        if key in self.applied:
            return Reconciled("completed", result=self.applied[key])
        return Reconciled("absent")


class Pays(Agent):
    """Charges once under `call_name`, catching any failure; tries a second call afterwards."""

    start = "go"
    seen: ClassVar[list[Any]] = []
    #: what a changed program would call instead (a new name, another amount)
    change: ClassVar[dict[str, Any]] = {}

    @state
    async def go(self, ctx, data):
        name = Pays.change.get("name", "charge")
        amount = Pays.change.get("amount", ctx.input["amount"])
        try:
            result = await ctx.call("pay.charge", call_name=name, amount=amount)
        except EffectError as exc:  # caught: the step must fault anyway when it is unknown
            Pays.seen.append(exc.cls)
            try:
                await ctx.call("pay.charge", call_name="other", amount=1)
            except EffectError as again:
                Pays.seen.append(f"then {again.cls}")
            return Done
        Pays.seen.append(result)
        data["result"] = result
        return Done


def runtime_with(provider: Ledger, **options) -> Runtime:
    rt = options.pop("rt", None) or memory_runtime(**options)
    rt.registrar.providers.register("pay", provider)
    rt.registrar.tools.register(
        EffectDefinition(
            name="pay.charge",
            provider="pay",
            operation="charge",
            policy=EffectPolicy(kind="deduplicated", timeout=0.1, max_attempts=2),
        )
    )
    rt.registrar.agents.register("pays", Pays, tools=["pay.*"])
    rt.effects.backoff = 0.001
    return rt


async def test_a_lost_answer_blocks_the_step_and_cont_settles_it_with_the_same_key() -> None:
    Pays.seen, Pays.change = [], {}
    ledger = Ledger()
    ledger.lose_answers = 2  # both attempts apply nothing new and never answer
    async with runtime_with(ledger) as rt:
        root = await rt.spawn("pays", {"amount": 10}, root_id="p")
        await rt.run(root, timeout=5)
        record = await rt.get(root)
        [entry] = await rt.calls(root)
        assert record["view"] == "suspended" and "indeterminate" in record["error"]
        assert Pays.seen == ["indeterminate", "then refused"]  # caught, still blocked
        assert (entry["status"], entry["clear_by"]) == ("indeterminate", "resend")
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        assert (await rt.get(root))["view"] == "succeeded"
        assert (await rt.calls(root))[0]["status"] == "completed"
    assert len(ledger.applied) == 1  # three sends, one charge


async def test_a_new_identity_cannot_go_around_an_unknown_outcome() -> None:
    Pays.seen, Pays.change = [], {}
    ledger = Ledger()
    ledger.declare_unknown = True
    async with runtime_with(ledger) as rt:
        root = await rt.spawn("pays", {"amount": 10}, root_id="p")
        await rt.run(root, timeout=5)
        [entry] = await rt.calls(root)
        assert entry["clear_by"] == "operator"  # the provider cannot reconcile
        # the agent is changed to use another name for the same charge: refused
        Pays.change = {"name": "renamed"}
        ledger.declare_unknown = False
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        record = await rt.get(root)
    assert record["view"] == "suspended" and "unknown outcome" in record["error"]
    assert ledger.applied == {}


async def test_cont_alone_never_replays_what_only_an_operator_can_settle() -> None:
    Pays.seen, Pays.change = [], {}
    ledger = Ledger()
    ledger.declare_unknown = True
    async with runtime_with(ledger) as rt:
        root = await rt.spawn("pays", {"amount": 10}, root_id="p")
        await rt.run(root, timeout=5)
        ledger.declare_unknown = False
        sends = ledger.sends
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        assert (await rt.get(root))["view"] == "suspended" and ledger.sends == sends
        [entry] = await rt.calls(root)
        await rt.resolve_call(entry["key"], "completed", result={"charged": 10, "by": "bank"})
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        record = await rt.store.load(root)
    assert record.status == "succeeded" and ledger.sends == sends  # not sent again
    assert record.data["result"] == {"charged": 10, "by": "bank"}


async def test_an_operator_may_allow_one_replay() -> None:
    Pays.seen, Pays.change = [], {}
    ledger = Ledger()
    ledger.declare_unknown = True
    async with runtime_with(ledger) as rt:
        root = await rt.spawn("pays", {"amount": 10}, root_id="p")
        await rt.run(root, timeout=5)
        [entry] = await rt.calls(root)
        ledger.declare_unknown = False
        await rt.resolve_call(entry["key"], "replay")
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        assert (await rt.get(root))["view"] == "succeeded"
        with pytest.raises(ValueError):  # settled: nothing left to resolve
            await rt.resolve_call(entry["key"], "replay")
    assert len(ledger.applied) == 1


async def test_a_reconciling_provider_is_asked_instead_of_sent_to() -> None:
    Pays.seen, Pays.change = [], {}
    ledger = Reconciling()
    ledger.lose_answers = 2
    async with runtime_with(ledger) as rt:
        root = await rt.spawn("pays", {"amount": 10}, root_id="p")
        await rt.run(root, timeout=5)
        assert (await rt.calls(root))[0]["clear_by"] == "reconcile"
        sends = ledger.sends
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        record = await rt.store.load(root)
    assert record.status == "succeeded" and record.data["result"] == {"charged": 10}
    assert ledger.sends == sends and ledger.reconciled == 1


async def test_one_identity_names_one_request_across_a_restart(tmp_path) -> None:
    def disk(provider: Ledger) -> Runtime:
        rt = Runtime(
            store=SqliteStore(tmp_path / "db"),
            workspace=LocalWorkspace(tmp_path / "ws"),
            executors=0,
            scan_every=0.01,
        )
        return runtime_with(provider, rt=rt)

    Pays.seen, Pays.change = [], {}
    ledger = Ledger()
    ledger.declare_unknown = True
    async with disk(ledger) as first:
        root = await first.spawn("pays", {"amount": 10}, root_id="p")
        await first.run(root, timeout=5)
    ledger.declare_unknown = False
    async with disk(ledger) as second:  # restarted: the journal is still there
        [entry] = await second.calls(root)
        assert entry["status"] == "indeterminate"
        await second.resolve_call(entry["key"], "replay")
        Pays.change = {"amount": 99}  # the same call, another amount
        await second.signal(root, "CONT")
        await second.run(root, timeout=5)
        record = await second.get(root)
    assert record["view"] == "suspended" and "different request" in record["error"]
    assert ledger.applied == {}


async def test_a_deduplicated_call_needs_an_identity() -> None:
    class NoName(Agent):
        start = "go"

        @state
        async def go(self, ctx, data):
            try:
                await ctx.call("pay.charge", amount=1)
            except EffectError as exc:
                data["error"] = exc.message
            return Done

    rt = runtime_with(Ledger())
    rt.registrar.agents.register("noname", NoName, tools=["pay.*"])
    async with rt:
        root = await rt.spawn("noname", root_id="n")
        await rt.run(root, timeout=5)
        assert "call_name" in (await rt.store.load(root)).data["error"]
