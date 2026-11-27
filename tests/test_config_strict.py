"""Configuration says what it means, or is refused.

What must hold
  * an unknown section, runtime key, store or workspace provider, or provider option is refused
    with where it was found; nothing falls back to memory or is dropped;
  * a duration is a number of seconds or `<n>ms|s|m|h`; anything else is refused at load time,
    never accepted to break later;
  * a policy override sets only the fields it names: `policies: {query: read_only}` keeps the
    timeout, cost and determinism the provider declared; a misspelled field is refused;
  * the python provider entry serves its tools under its own name with its concurrency and
    execution mode;
  * an unset `${NAME}` is an error; `${NAME:-default}` names a default.
"""

from __future__ import annotations

import sys
import textwrap

import pytest

from statecraft import Runtime
from statecraft.kernel.effects import EffectPolicy
from statecraft.runtime.config import ConfigError, configure, duration, read
from statecraft.sdk.effect import EffectDefinition
from statecraft.testing import memory_runtime


class Discovering:
    concurrency = None

    def __init__(self, name, **cfg) -> None:
        self.cfg = cfg

    async def start(self) -> None:
        return None

    async def discover(self):
        policy = EffectPolicy(kind="read_only", timeout=120.0, cost="metered", deterministic=False)
        return [EffectDefinition(name="search.query", operation="query", policy=policy)]

    async def invoke(self, op, args, ctx):
        return {"ok": 1}

    async def close(self) -> None:
        return None


def with_policies(policies) -> Runtime:
    rt = memory_runtime()
    rt.registrar.providers.factory("disc", Discovering)
    configure(
        rt.registrar,
        {"effect_providers": {"search": {"provider": "disc", "policies": policies}}},
    )
    return rt


async def test_a_policy_override_keeps_what_it_does_not_name() -> None:
    async with with_policies({"query": "read_only"}) as rt:
        policy = rt.registry.effects["search.query"].policy
    assert (policy.timeout, policy.cost, policy.deterministic) == (120.0, "metered", False)
    async with with_policies({"query": {"timeout": "30s", "max_attempts": 2}}) as rt:
        policy = rt.registry.effects["search.query"].policy
    assert (policy.timeout, policy.max_attempts, policy.cost) == (30.0, 2, "metered")


@pytest.mark.parametrize(
    "policy",
    [
        {"kind": "read_only", "max_atempts": 9},
        {"timeout": "soon"},
        {"timeout": True},
        {"max_attempts": "3"},
        "sometimes",
        3,
    ],
)
def test_a_bad_policy_is_refused_when_loaded(policy) -> None:
    with pytest.raises(ConfigError, match=r"effect_providers\.search\.policies\.query"):
        with_policies({"query": policy})


@pytest.mark.parametrize(
    ("config", "where"),
    [
        ({"store": {"provider": "postgres", "url": "x"}}, "store.provider"),
        ({"workspace": {"provider": "s3", "bucket": "b"}}, "workspace.provider"),
        ({"store": {"provider": "sqlite", "pth": "x.db"}}, "store"),
        ({"runtime": {"concurency": 4}}, "runtime"),
        ({"runtime": {"drain": "soon"}}, "runtime.drain"),
        ({"runtime": {"executors": -1}}, "runtime.executors"),
        ({"runtime": {"limits": {"max_dpth": 2}}}, "runtime.limits"),
        ({"runtme": {}}, "configuration"),
        ({"effect_providers": {"r": {"provider": "python", "module": "m", "threads": 2}}}, "r"),
        ({"packages": [{"package": "p", "prefx": "x"}]}, "packages"),
    ],
)
def test_unknown_names_are_refused_with_their_place(config, where) -> None:
    with pytest.raises(ConfigError, match=where):
        Runtime.from_config(config)


def test_durations_read_their_unit() -> None:
    runtime = Runtime.from_config(
        {"runtime": {"drain": "30s", "lease_ttl": "2m", "beat": "500ms", "scan_every": 0.5}}
    )
    options = runtime.executor_options
    assert (runtime.drain, options["lease_ttl"], options["beat"]) == (30.0, 120.0, 0.5)
    assert duration(1, "x") == 1.0
    for bad in ("1d", "-3s", "", "fast", None, False):
        with pytest.raises(ConfigError):
            duration(bad, "x")


def test_a_python_entry_serves_its_tools_with_its_options(tmp_path, monkeypatch) -> None:
    (tmp_path / "strict_tools.py").write_text(
        textwrap.dedent(
            """
            from statecraft import tool

            @tool(name="reports.render", policy="idempotent")
            def render(x: int) -> int:
                return x
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("strict_tools", None)
    rt = Runtime.from_config(
        {
            "effect_providers": {
                "reports": {
                    "provider": "python",
                    "module": "strict_tools",
                    "concurrency": 2,
                    "execution": "process",
                }
            }
        }
    )
    effect = rt.registry.effects["reports.render"]
    assert effect.provider == "reports" and effect.execution == "process"
    assert rt.registry.providers["reports"].concurrency == 2
    with pytest.raises(ConfigError, match="execution"):
        Runtime.from_config(
            {
                "effect_providers": {
                    "r": {"provider": "python", "module": "strict_tools", "execution": "fiber"}
                }
            }
        )


def test_an_unset_environment_variable_is_an_error(monkeypatch) -> None:
    monkeypatch.delenv("STATECRAFT_NOPE", raising=False)
    with pytest.raises(ConfigError, match="STATECRAFT_NOPE"):
        read({"extensions": {"agentic": {"api_key": "${STATECRAFT_NOPE}"}}})
    data, _ = read({"extensions": {"agentic": {"api_key": "${STATECRAFT_NOPE:-}"}}})
    assert data["extensions"]["agentic"]["api_key"] == ""
    monkeypatch.setenv("STATECRAFT_NOPE", "set")
    data, _ = read({"extensions": {"agentic": {"api_key": "${STATECRAFT_NOPE:-x}"}}})
    assert data["extensions"]["agentic"]["api_key"] == "set"
