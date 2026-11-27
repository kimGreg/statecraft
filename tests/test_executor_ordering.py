"""Control is applied in order, and nothing is left stranded (roadmap B1).

What must hold
  * a STOP sent while an agent waits for a slot is applied before its next step: no step runs
    after the STOP was accepted;
  * a parent that CONTs a faulted child and waits again is not woken by that same fault while
    the CONT sits in the child's inbox;
  * TERM supersedes STOP: a stopped agent that is terminated runs its cleanup and ends (KILL
    ends it without cleanup);
  * a signal to an agent that does not exist is recorded with the parent's intent, not dropped;
  * an answer is checked in the same transaction that stores it;
  * a child whose cleanup faulted after its parent ended is ended, not left waiting forever;
  * after `stop()`, `start()` runs agents again.
"""

from __future__ import annotations

import asyncio
from typing import ClassVar

import pytest

from statecraft import Agent, Done, Spawn, Wait, state
from statecraft.kernel.executor import Executor
from statecraft.kernel.transitions import InvalidAnswer
from statecraft.testing import memory_runtime


class Holder(Agent):
    """Holds the only slot until released."""

    start = "go"
    gate: ClassVar[dict[str, asyncio.Event]] = {}

    @state
    async def go(self, ctx, data):
        Holder.gate["in"].set()
        await Holder.gate["out"].wait()
        return Done


class Stepper(Agent):
    start = "one"
    steps: ClassVar[list[str]] = []

    @state
    async def one(self, ctx, data):
        Stepper.steps.append("one")
        return "two"

    @state
    async def two(self, ctx, data):
        Stepper.steps.append("two")
        return Done


async def test_a_stop_sent_while_queued_for_a_slot_stops_the_next_step() -> None:
    Holder.gate = {"in": asyncio.Event(), "out": asyncio.Event()}
    Stepper.steps = []
    rt = memory_runtime(executors=1, concurrency=1, scan_every=0.01)
    rt.registrar.agents.register("holder", Holder)
    rt.registrar.agents.register("stepper", Stepper)
    async with rt:
        await rt.spawn("holder", root_id="h")
        await asyncio.wait_for(Holder.gate["in"].wait(), 5)
        stepper = await rt.spawn("stepper", root_id="s")
        for _ in range(100):  # until the stepper waits for the slot
            await asyncio.sleep(0.01)
            if "s" in rt.executors[0].running:
                break
        assert "s" in rt.executors[0].running
        await rt.signal(stepper, "STOP")
        Holder.gate["out"].set()
        await asyncio.sleep(0.3)
        record = await rt.store.load(stepper)
    assert Stepper.steps == []
    assert record.step == 0 and record.reason("stopped") is not None


class Flaky(Agent):
    start = "go"
    runs: ClassVar[list[int]] = []

    @state
    async def go(self, ctx, data):
        Flaky.runs.append(1)
        if len(Flaky.runs) == 1:
            raise RuntimeError("first try fails")
        return Done


class Boss(Agent):
    start = "spawn"
    handled: ClassVar[list[str]] = []

    @state
    async def spawn(self, ctx, data):
        return Spawn({"c": ("flaky", {})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["c"], then="done", on_fault="handle")

    @state
    async def handle(self, ctx, data):
        Boss.handled.append(ctx.woke["faulted"][0]["error"])
        ctx.signal("c", "CONT")
        return "wait"

    @state
    async def done(self, ctx, data):
        return Done


async def test_a_continued_child_does_not_wake_its_parent_with_the_same_fault() -> None:
    Flaky.runs, Boss.handled = [], []
    rt = memory_runtime(executors=0)
    rt.registrar.agents.register("boss", Boss, spawns=["flaky"])
    rt.registrar.agents.register("flaky", Flaky)
    async with rt:
        await rt.spawn("boss", root_id="p")
        executor = Executor(rt, scan_every=0.01)

        async def step(agent_id: str) -> None:
            await executor._process(await rt.store.load(agent_id))
            await executor.drain()

        await step("p")  # spawn
        await step("p")  # the child is created; `wait` runs
        await step("p/c")  # the child faults once
        await step("p")  # the join clears on the fault; `handle` runs and CONTs the child
        await step("p")  # the CONT is delivered to the child's inbox; `wait` runs again
        await step("p")  # the child has not applied its CONT yet: no second wake
        assert len(Boss.handled) == 1
        await rt.run("p", timeout=5)
        assert (await rt.get("p"))["view"] == "succeeded"
    assert len(Boss.handled) == 1 and len(Flaky.runs) == 2


class Cleans(Agent):
    start = "go"
    on_term = "cleanup"
    cleaned: ClassVar[list[bool]] = []

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "never", "question": "?"}, then="go")

    @state
    async def cleanup(self, ctx, data):
        Cleans.cleaned.append(True)
        return Done


async def test_term_supersedes_stop_and_runs_the_cleanup() -> None:
    Cleans.cleaned = []
    rt = memory_runtime()
    rt.registrar.agents.register("cleans", Cleans)
    async with rt:
        root = await rt.spawn("cleans", root_id="c")
        await rt.run(root, timeout=5)
        await rt.signal(root, "STOP")
        await rt.signal(root, "TERM")
        await rt.run(root, timeout=5)
        assert (await rt.get(root))["view"] == "terminated"
    assert Cleans.cleaned == [True]


class SignalsNobody(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(sleep=0.0, then="done")

    @state
    async def done(self, ctx, data):
        return Done


async def test_a_signal_to_a_missing_agent_is_recorded() -> None:
    from statecraft.kernel.process import Transition

    rt = memory_runtime(executors=0)
    rt.registrar.agents.register("nobody", SignalsNobody)
    async with rt:
        root = await rt.spawn("nobody", root_id="n")
        lease = await rt.store.try_acquire(root, "x", 30, rt.clock())
        await rt.store.commit_step(
            root, lease, 0, Transition(state="done", signals=[("n/ghost", "TERM")]), rt.clock()
        )
        await rt.run(root, timeout=5)
        intents = [h for h in await rt.history(root) if h["kind"] == "intent"]
    assert any("no agent n/ghost" in str(h["detail"].get("outcome")) for h in intents), intents


class Asks(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "q", "question": "?"}, then="done")

    @state
    async def done(self, ctx, data):
        return Done


async def test_an_answer_is_checked_where_it_is_stored() -> None:
    rt = memory_runtime(executors=0)
    rt.registrar.agents.register("asks", Asks)
    async with rt:
        root = await rt.spawn("asks", root_id="a")
        await rt.run(root, timeout=5)
        question = (await rt.questions())[0]["question_id"]
        checked = []
        original = rt.store.append_inbox

        async def spy(agent_id, message_id, message, check=None):
            checked.append(check is not None)
            return await original(agent_id, message_id, message, check)

        rt.store.append_inbox = spy
        await rt.answer(question, "yes")
        assert checked == [True]  # the open question is checked inside the append
        assert await rt.store.read_inbox(root, 0) != []

        def closed(record):  # the question closed between the caller's view and the append
            raise InvalidAnswer("closed meanwhile")

        with pytest.raises(InvalidAnswer):
            await original(root, "late", {"kind": "answer"}, closed)
        assert len(await rt.store.read_inbox(root, 0)) == 1  # nothing stored


class FaultyCleanup(Agent):
    start = "go"
    on_term = "cleanup"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "never", "question": "?"}, then="go")

    @state
    async def cleanup(self, ctx, data):
        raise RuntimeError("cleanup broke")


class EndsWithChild(Agent):
    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        return Spawn({"c": ("faulty_cleanup", {})}, then="end")

    @state
    async def end(self, ctx, data):
        ctx.signal("c", "TERM")
        return Done


async def test_a_child_whose_cleanup_faults_after_its_parent_ended_is_ended() -> None:
    rt = memory_runtime()
    rt.registrar.agents.register("ends", EndsWithChild, spawns=["faulty_cleanup"])
    rt.registrar.agents.register("faulty_cleanup", FaultyCleanup)
    async with rt:
        root = await rt.spawn("ends", root_id="e")
        await rt.run(root, timeout=10)
        child = await rt.get("e/c")
        history = [h["kind"] for h in await rt.history("e/c")]
    assert (await rt.store.load("e")).status == "succeeded"
    assert child["view"] == "terminated", child
    assert "fault" in history  # the cleanup's error is kept in its history


class Quick(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Done


async def test_start_after_stop_runs_agents_again() -> None:
    rt = memory_runtime(executors=1, scan_every=0.01)
    rt.registrar.agents.register("quick", Quick)
    await rt.start()
    await rt.stop()
    await rt.start()
    root = await rt.spawn("quick", root_id="q")
    for _ in range(200):
        if (await rt.get(root))["view"] == "succeeded":
            break
        await asyncio.sleep(0.01)
    assert (await rt.get(root))["view"] == "succeeded"
    assert rt.executors and all(not t.done() for t in rt._tasks)
    await rt.close()


class Slow(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(sleep=0.4, then="done")

    @state
    async def done(self, ctx, data):
        return Done


class Interruptible(Agent):
    """Joins a slow child with a deadline and messages; records each wake, then rejoins."""

    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        return Spawn({"a": ("slow", {}), "b": ("slow", {})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["a", "b"], then="woke", timeout=ctx.input.get("timeout"), messages=True)

    @state
    async def woke(self, ctx, data):
        woke = ctx.woke
        data.setdefault("wakes", []).append(
            {"by": woke["by"], "live": woke.get("live", []), "signals": list(ctx.signals)}
        )
        if woke["by"] == "children":
            return Done
        return Wait(join=woke["live"], then="woke")  # rejoin what is still running

    @state
    async def done(self, ctx, data):
        return Done


async def test_a_join_wakes_at_its_deadline_and_the_children_go_on() -> None:
    rt = memory_runtime(scan_every=0.01)
    rt.registrar.agents.register("interruptible", Interruptible, spawns=["slow"])
    rt.registrar.agents.register("slow", Slow)
    async with rt:
        root = await rt.spawn("interruptible", {"timeout": 0.1}, root_id="i")
        await rt.run(root, timeout=10)
        record = await rt.store.load(root)
    wakes = record.data["wakes"]
    assert wakes[0]["by"] == "timeout" and sorted(wakes[0]["live"]) == ["a", "b"]
    assert wakes[-1]["by"] == "children" and record.status == "succeeded"


async def test_a_join_wakes_on_a_message() -> None:
    rt = memory_runtime(scan_every=0.01)
    rt.registrar.agents.register("interruptible", Interruptible, spawns=["slow"])
    rt.registrar.agents.register("slow", Slow)
    async with rt:
        root = await rt.spawn("interruptible", {}, root_id="m")
        for _ in range(100):
            record = await rt.store.load(root)
            if record.reason("join"):
                break
            await asyncio.sleep(0.01)
        await rt.signal(root, ("USR", "look"))
        await rt.run(root, timeout=10)
        record = await rt.store.load(root)
    wakes = record.data["wakes"]
    assert wakes[0]["by"] == "message" and wakes[0]["signals"] == ["look"]
    assert wakes[-1]["by"] == "children" and record.status == "succeeded"
