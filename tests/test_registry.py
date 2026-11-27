"""Registries and startup (architecture §9, §10).

What must hold
  * agents register three ways (Python API, manifest, package folders with agent.yaml) into the
    same definition; plugins register through a registrar and are enabled by name;
  * validation runs after every plugin and provider registered, so order never matters; errors
    refuse the start, all of them reported at once;
  * spawn cycles (recursion) and unwritten reads are lint, not errors;
  * the registry is frozen while the runtime runs;
  * a spawn outside the parent's grant, or with input breaking the child's schema, faults it.
"""

from __future__ import annotations

import textwrap

import pytest

from statecraft import Agent, Done, Spawn, Wait, state
from statecraft.runtime.registry import Frozen, RegistryError
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Summer(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.write("sum", {"value": await ctx.call("math.add", a=1, b=2)})
        return Done


class Recursive(Agent):
    """Spawns itself until depth 0: a spawn cycle that is a legitimate agent."""

    start = "expand"

    @state
    async def expand(self, ctx, data):
        if ctx.input["depth"] == 0:
            await ctx.write("leaf", {"ok": True})
            return Done
        return Spawn({"child": ("recursive", {"depth": ctx.input["depth"] - 1})}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["child"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


class Parent(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Spawn({"c": (ctx.input["target"], ctx.input.get("input", {}))}, then="wait")

    @state
    async def wait(self, ctx, data):
        return Wait(join=["c"], then="done")

    @state
    async def done(self, ctx, data):
        return Done


def plugin_b(registrar):
    registrar.tools.register(add)


def plugin_a(registrar):
    registrar.agents.register("summer", Summer, tools=["math.add"], writes=["sum"])


async def test_plugin_order_does_not_matter() -> None:
    runtime = memory_runtime()
    plugin_a(runtime.registrar)  # its agent needs a tool registered only later
    plugin_b(runtime.registrar)
    async with runtime:
        root = await runtime.spawn("summer")
        await runtime.run(root, timeout=10)
        assert (await runtime.artifact(root, "sum"))["payload"] == {"value": 3}


async def test_errors_refuse_the_start_all_at_once() -> None:
    runtime = memory_runtime()
    runtime.registrar.agents.register("a", Summer, tools=["math.nope"], spawns=["ghost"])
    with pytest.raises(RegistryError) as raised:
        await runtime.start()
    problems = "\n".join(raised.value.problems)
    assert "math.nope" in problems and "ghost" in problems


async def test_recursion_is_lint_not_an_error() -> None:
    runtime = memory_runtime()
    runtime.registrar.agents.register(
        "recursive", Recursive, spawns=["recursive"], reads=["outside"]
    )
    async with runtime:
        assert any("spawn cycle" in f for f in runtime.registry.lint())
        assert any("reads outside" in f for f in runtime.registry.lint())
        root = await runtime.spawn("recursive", {"depth": 3})
        rows = await runtime.run(root, timeout=10)
        assert len(rows) == 4 and {r["view"] for r in rows} == {"succeeded"}


async def test_the_registry_is_frozen_while_running() -> None:
    runtime = memory_runtime()
    async with runtime:
        with pytest.raises(Frozen):
            runtime.registrar.agents.register("late", Summer)


async def test_a_spawn_outside_the_grant_or_with_bad_input_faults_the_parent() -> None:
    runtime = memory_runtime()
    runtime.registrar.tools.register(add)
    runtime.registrar.agents.register(
        "summer", Summer, tools=["math.add"], input={"type": "object", "required": ["n"]}
    )
    runtime.registrar.agents.register("other", Summer, tools=["math.add"])
    runtime.registrar.agents.register("parent", Parent, spawns=["summer"])
    async with runtime:
        root = await runtime.spawn("parent", {"target": "other"})
        [row] = await runtime.run(root, timeout=10)
        assert row["view"] == "suspended" and "may not spawn other" in row["error"]
        root = await runtime.spawn("parent", {"target": "summer", "input": {}})
        [row] = await runtime.run(root, timeout=10)
        assert "input for summer" in row["error"]
        with pytest.raises(ValueError):
            await runtime.spawn("summer", {})  # a root's input is checked at spawn


async def test_manifest_and_package_registration(tmp_path, monkeypatch) -> None:
    package = tmp_path / "pkg" / "sample_agents"
    folder = package / "agents" / "double"
    folder.mkdir(parents=True)
    for d in (package, package / "agents", folder):
        (d / "__init__.py").write_text("")
    (folder / "agent.yaml").write_text(
        textwrap.dedent("""
        id: double
        description: Doubles a number with the add tool.
        tools: [math.add]
        writes: [double]
        x-owner: research
    """)
    )
    (folder / "agent.py").write_text(
        textwrap.dedent("""
        from statecraft import Agent, Done, state

        class Double(Agent):
            start = "go"

            @state
            async def go(self, ctx, data):
                n = ctx.input["n"]
                await ctx.write("double", {"value": await ctx.call("math.add", a=n, b=n)})
                return Done
    """)
    )
    monkeypatch.syspath_prepend(str(tmp_path / "pkg"))
    from statecraft.runtime.runtime import Runtime

    runtime = Runtime.from_config(
        {
            "store": {"provider": "memory"},
            "workspace": {"provider": "memory"},
            "runtime": {"executors": 0, "scan_every": 0.01},
            "effect_providers": {
                "math": {"provider": "python", "module": "statecraft.testing.tools"}
            },
            "packages": [{"package": "sample_agents", "prefix": "s"}],
            "agents": {
                "twice": {
                    "entry": "sample_agents.agents.double.agent:Double",
                    "tools": ["math.add"],
                }
            },
        }
    )
    async with runtime:
        names = [a["name"] for a in runtime.agents()]
        assert names == ["s:double", "twice"]
        described = runtime.registry.describe_agent("s:double")
        assert described["meta"] == {"x-owner": "research"} and described["states"] == ["go"]
        for name in names:
            root = await runtime.spawn(name, {"n": 4})
            await runtime.run(root, timeout=10)
            assert (await runtime.artifact(root, "double"))["payload"] == {"value": 8}


async def test_plugins_are_enabled_by_name_only() -> None:
    from statecraft.runtime import plugins
    from statecraft.testing import memory_runtime as expected

    assert plugins.load("statecraft.testing:memory_runtime") is expected  # module:function
    assert isinstance(plugins.installed(), dict)  # installed plugins are listed, not run
    with pytest.raises(LookupError):
        plugins.load("not-installed")
