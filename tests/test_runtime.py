"""The runtime's lifecycle and pinning (architecture §9.5, §11).

What must hold
  * an instance pins its definition's code digest and each tool binding (provider, operation,
    version, schema and policy digests); after a restart with a changed policy or changed code it
    is suspended with the reason, not run; `repin` then CONT accepts the current ones;
  * `stop()` starts no new steps, cancels running ones after `drain` and releases their leases;
    it never ends agents: the next start resumes them without waiting for leases to expire;
  * `async with` starts and closes; spawning before start is refused;
  * configuration builds the same runtime (environment substitution included).
"""

from __future__ import annotations

import asyncio
import os

import pytest

from statecraft import Agent, Done, EffectPolicy, Wait, state
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.runtime.runtime import NotStarted, Runtime
from statecraft.sdk.effect import EffectDefinition
from statecraft.testing import FunctionProvider


class Asker(Agent):
    start = "first"

    @state
    async def first(self, ctx, data):
        data["a"] = await ctx.call("svc.get", n=1)
        return Wait(answer={"name": "go", "question": "go on?"}, then="second")

    @state
    async def second(self, ctx, data):
        data["b"] = await ctx.call("svc.get", n=2)
        await ctx.write("done", {"a": data["a"], "b": data["b"]})
        return Done


class AskerV2(Asker):
    """The same agent, other code."""

    start = "first"


def runtime_at(folder, *, policy: str = "read_only", agent: type = Asker, **options) -> Runtime:
    runtime = Runtime(
        store=SqliteStore(folder / "state.db"),
        workspace=LocalWorkspace(folder / "ws"),
        **{"executors": 0, "scan_every": 0.01, **options},
    )
    runtime.registrar.providers.register("svc", FunctionProvider({"svc.get": lambda a: a["n"]}))
    runtime.registrar.tools.register(
        EffectDefinition(
            name="svc.get", policy=EffectPolicy(kind=policy), provider="svc", operation="svc.get"
        )
    )
    runtime.registrar.agents.register("asker", agent, tools=["svc.get"])
    return runtime


async def answer_and_run(runtime: Runtime, root: str) -> dict:
    [question] = await runtime.questions(root)
    await runtime.answer(question["question_id"], True)
    [row] = await runtime.run(root, timeout=10)
    return row


async def test_a_changed_policy_suspends_the_instance_until_repinned(tmp_path) -> None:
    async with runtime_at(tmp_path) as first:
        root = await first.spawn("asker", root_id="a1")
        await first.run(root, timeout=10)
        pinned = (await first.get(root))["bindings"]["svc.get"]
    async with runtime_at(tmp_path, policy="idempotent") as second:
        row = await answer_and_run(second, root)
        assert row["view"] == "suspended" and "tool svc.get changed" in row["error"]
        await second.repin(root)
        await second.signal(root, "CONT")
        [row] = await second.run(root, timeout=10)
        assert row["view"] == "succeeded"
        assert (await second.get(root))["bindings"]["svc.get"] != pinned


async def test_changed_code_suspends_the_instance(tmp_path) -> None:
    async with runtime_at(tmp_path) as first:
        root = await first.spawn("asker", root_id="a2")
        await first.run(root, timeout=10)
    async with runtime_at(tmp_path) as same:
        assert same.registry.agents["asker"].code == (await same.get(root))["definition"]["code"]
    import textwrap

    module = tmp_path / "changed_agent.py"
    module.write_text(
        textwrap.dedent("""
        from statecraft import Agent, Done, state

        class Asker(Agent):
            start = "first"

            @state
            async def first(self, ctx, data):
                return Done

            @state
            async def second(self, ctx, data):
                return Done
    """)
    )
    import importlib.util

    spec = importlib.util.spec_from_file_location("changed_agent", module)
    changed = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(changed)  # type: ignore[union-attr]
    import sys

    sys.modules["changed_agent"] = changed
    try:
        async with runtime_at(tmp_path, agent=changed.Asker) as second:
            row = await answer_and_run(second, root)
            assert row["view"] == "suspended" and "code changed" in row["error"]
    finally:
        sys.modules.pop("changed_agent", None)


ATTEMPTS: dict[str, int] = {}


class Slow(Agent):
    """The first attempt takes long; later attempts are quick."""

    start = "work"

    @state
    async def work(self, ctx, data):
        ATTEMPTS[ctx.id] = ATTEMPTS.get(ctx.id, 0) + 1
        await asyncio.sleep(30 if ATTEMPTS[ctx.id] == 1 else 0)
        await ctx.write("done", {"attempts": ATTEMPTS[ctx.id]})
        return Done


async def test_stop_releases_leases_and_never_ends_agents(tmp_path) -> None:
    def make() -> Runtime:
        runtime = Runtime(
            store=SqliteStore(tmp_path / "state.db"),
            workspace=LocalWorkspace(tmp_path / "ws"),
            executors=1,
            scan_every=0.01,
            lease_ttl=600,
            beat=0.1,
        )
        runtime.registrar.agents.register("slow", Slow)
        return runtime

    first = await make().start()
    root = await first.spawn("slow", root_id="s1")
    for _ in range(500):
        if await first.store.lease(root) is not None:
            break
        await asyncio.sleep(0.01)
    assert await first.store.lease(root) is not None  # the step runs, holding a 600 s lease
    await first.stop(drain=0.1)
    assert await first.store.lease(root) is None  # released, not left to expire
    assert (await first.get(root))["status"] == "active"  # not ended
    await first.close()
    second = await make().start()
    try:
        for _ in range(500):
            if (await second.get(root))["status"] == "succeeded":
                break
            await asyncio.sleep(0.01)
        assert (await second.artifact(root, "done"))["payload"] == {"attempts": 2}
    finally:
        await second.close()


async def test_spawning_before_start_is_refused(tmp_path) -> None:
    runtime = runtime_at(tmp_path)
    with pytest.raises(NotStarted):
        await runtime.spawn("asker")


async def test_configuration_builds_the_runtime(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("STATECRAFT_TEST_ROOT", str(tmp_path))
    config = tmp_path / "statecraft.yaml"
    config.write_text(
        "store: {provider: sqlite, path: '${STATECRAFT_TEST_ROOT}/state.db'}\n"
        "workspace: {provider: local, root: '${STATECRAFT_TEST_ROOT}/ws'}\n"
        "runtime: {executors: 0, scan_every: 0.01}\n"
        "effect_providers:\n"
        "  math: {provider: python, module: statecraft.testing.tools}\n"
        "agents:\n"
        "  summer: {entry: tests_support_summer:Summer, tools: [math.add]}\n"
    )
    (tmp_path / "tests_support_summer.py").write_text(
        "from statecraft import Agent, Done, state\n"
        "class Summer(Agent):\n"
        "    start = 'go'\n"
        "    @state\n"
        "    async def go(self, ctx, data):\n"
        "        await ctx.write('sum', {'v': await ctx.call('math.add', a=2, b=3)})\n"
        "        return Done\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    async with Runtime.from_config(config) as runtime:
        root = await runtime.spawn("summer")
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "sum"))["payload"] == {"v": 5}
        assert {t["name"] for t in runtime.tools()} >= {"math.add", "work.burn"}
    assert (tmp_path / "state.db").exists() and os.path.isdir(tmp_path / "ws")
