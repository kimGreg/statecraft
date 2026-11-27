"""One configuration builds a runtime (architecture §13).

    runtime: {concurrency: 16, executors: 1, drain: 30, lease_ttl: 120, beat: 30,
              limits: {max_depth: 3, max_total_agents: 32, max_live_agents: 8}}
    store: {provider: sqlite, path: ./statecraft.db}            # or memory; a package's kind as a
                                                                # mapping: {provider: postgres, dsn}
    workspace: {provider: local, root: ./workspaces}            # or memory
    plugins: [agentic, research]                                # enabled entry points
    effect_providers:
      search: {provider: mcp, url: http://localhost:8001/mcp, concurrency: 8,
               policies: {query: read_only}}
      reports: {provider: python, module: research_agents.reports, execution: process}
    packages: [{package: research_agents, prefix: research}]    # agents/<...>/agent.yaml
    agents:
      report: {entry: research_agents.agents:ReportAgent, tools: [reports.render]}
    extensions:
      agentic: {...}                                            # read by the extension only

`${NAME}` in a string is replaced by the environment variable (credentials stay out of files and
are never stored); an unset variable is an error, and `${NAME:-default}` names a default. Order:
settings, plugins (they register agents, tools and provider kinds), providers from
`effect_providers`, packages, manifest agents. The runtime's `start()` then discovers, freezes and
validates.

Configuration is strict: an unknown section, key, store or workspace provider is refused with the
place it was found, never dropped or replaced by a default. A duration is seconds as a number or
a string with a unit (`500ms`, `30s`, `5m`, `1h`). A policy names only the fields it sets; the
rest stay as the tool declared them.
"""

from __future__ import annotations

import importlib
import inspect
import os
import re
from pathlib import Path
from typing import Any

import yaml

from ..kernel.effects import EffectPolicy
from ..sdk.effect import EXECUTIONS
from . import plugins
from .registry import Registrar

ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
SECTIONS = (
    "runtime",
    "store",
    "workspace",
    "plugins",
    "effect_providers",
    "packages",
    "agents",
    "extensions",
)
DURATIONS = ("lease_ttl", "beat", "scan_every", "drain")
COUNTS = ("concurrency", "executors")
RUNTIME_KEYS = (*COUNTS, *DURATIONS, "limits")
STORES = {"sqlite": ("path", "busy_timeout"), "memory": ()}
WORKSPACES = {"local": ("root",), "memory": ()}
PYTHON_KEYS = ("module", "concurrency", "execution", "processes")
UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*$")


class ConfigError(ValueError):
    """A configuration that cannot be used as written."""


def _env(match: re.Match[str]) -> str:
    name, default = match.group(1), match.group(2)
    value = os.environ.get(name)
    if value is not None:
        return value
    if default is not None:
        return default
    raise ConfigError(f"environment variable {name} is not set (write ${{{name}:-}} if optional)")


def substitute(value: Any) -> Any:
    if isinstance(value, str):
        return ENV.sub(_env, value)
    if isinstance(value, dict):
        return {k: substitute(v) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v) for v in value]
    return value


def read(config: str | Path | dict[str, Any]) -> tuple[dict[str, Any], Path]:
    if isinstance(config, dict):
        data, base = substitute(config), Path.cwd()
    else:
        path = Path(config)
        data, base = substitute(yaml.safe_load(path.read_text(encoding="utf-8")) or {}), path.parent
    if not isinstance(data, dict):
        raise ConfigError("a configuration is a mapping of sections")
    _known(data, SECTIONS, "configuration")
    return data, base


def _known(cfg: Any, keys: tuple[str, ...] | list[str], where: str) -> dict[str, Any]:
    if cfg is None:
        return {}
    if not isinstance(cfg, dict):
        raise ConfigError(f"{where} must be a mapping, not {type(cfg).__name__}")
    unknown = sorted(set(cfg) - set(keys))
    if unknown:
        raise ConfigError(f"{where}: unknown keys {unknown}; known: {sorted(keys)}")
    return cfg


def duration(value: Any, where: str) -> float:
    """Seconds from a number or `<n>[ms|s|m|h]`."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value)
    elif isinstance(value, str) and (m := DURATION.match(value)):
        seconds = float(m.group(1)) * UNITS[m.group(2) or "s"]
    else:
        raise ConfigError(f"{where} must be a duration like 30, 30s, 500ms or 5m, not {value!r}")
    if seconds < 0:
        raise ConfigError(f"{where} must not be negative")
    return seconds


def _count(value: Any, where: str, least: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < least:
        raise ConfigError(f"{where} must be a whole number of at least {least}, not {value!r}")
    return value


def policy_fields(value: Any, where: str) -> dict[str, Any]:
    """Only the policy fields `value` sets, checked: a kind name, or a mapping of fields."""
    if isinstance(value, str):
        fields: dict[str, Any] = {"kind": value}
    elif isinstance(value, dict):
        fields = dict(value)
        if isinstance(fields.get("timeout"), str):
            fields["timeout"] = duration(fields["timeout"], f"{where}.timeout")
    else:
        raise ConfigError(f"{where} must be a policy kind or a mapping, not {value!r}")
    try:
        EffectPolicy.of(fields)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{where}: {exc}") from None
    return fields


def runtime_options(cfg: Any) -> dict[str, Any]:
    options = dict(_known(cfg, RUNTIME_KEYS, "runtime"))
    for key in COUNTS:
        if key in options:
            options[key] = _count(options[key], f"runtime.{key}", 1 if key == "concurrency" else 0)
    for key in DURATIONS:
        if key in options:
            options[key] = duration(options[key], f"runtime.{key}")
    if "limits" in options:
        limits = _known(options["limits"], _limit_names(), "runtime.limits")
        options["limits"] = {
            k: None if v is None else _count(v, f"runtime.limits.{k}") for k, v in limits.items()
        }
    return options


def _limit_names() -> tuple[str, ...]:
    from .runtime import LIMITS

    return tuple(LIMITS)


def _path(base: Path, value: Any, where: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{where} must be a path, not {value!r}")
    p = Path(value)
    return p if p.is_absolute() else base / p


def _provider(cfg: Any, kinds: dict[str, tuple[str, ...]], where: str) -> tuple[str, dict]:
    if cfg is None:
        return "memory", {}
    if not isinstance(cfg, dict):
        raise ConfigError(f"{where} must be a mapping, not {type(cfg).__name__}")
    kind = cfg.get("provider")
    if kind not in kinds:
        raise ConfigError(f"{where}.provider must be one of {sorted(kinds)}, not {kind!r}")
    return kind, _known(cfg, ("provider", *kinds[kind]), f"{where} ({kind})")


def runtime_from_config(config: str | Path | dict[str, Any], **overrides: Any) -> Any:
    from ..providers.stores import MemoryStore, SqliteStore
    from ..providers.workspaces import LocalWorkspace, MemoryWorkspace
    from .runtime import Runtime

    data, base = read(config)
    given = data.get("store")
    if isinstance(given, dict) and given.get("provider") not in (None, *STORES):
        # a store kind offered by a package (`statecraft.stores`), e.g. postgres
        settings = {k: v for k, v in given.items() if k != "provider"}
        try:
            factory = plugins.store_factory(str(given["provider"]))
        except (LookupError, ImportError, AttributeError) as exc:
            raise ConfigError(f"store.provider: {exc}") from exc
        try:
            store: Any = factory(settings, base)
        except TypeError as exc:
            raise ConfigError(f"store ({given['provider']}): {exc}") from exc
        kind = "plugin"
    else:
        kind, store_cfg = _provider(given, STORES, "store")
    if kind == "plugin":
        pass
    elif kind == "sqlite":
        extra = {}
        if "busy_timeout" in store_cfg:
            extra["busy_timeout"] = duration(store_cfg["busy_timeout"], "store.busy_timeout")
        path = _path(base, store_cfg.get("path", "statecraft.db"), "store.path")
        store = SqliteStore(path, **extra)
    else:
        store = MemoryStore()
    kind, ws_cfg = _provider(data.get("workspace"), WORKSPACES, "workspace")
    if kind == "local":
        workspace: Any = LocalWorkspace(
            _path(base, ws_cfg.get("root", "workspaces"), "workspace.root")
        )
    else:
        workspace = MemoryWorkspace()
    options = runtime_options(data.get("runtime"))
    runtime = Runtime(store=store, workspace=workspace, **{**options, **overrides})
    configure(runtime.registrar, data, base)
    return runtime


def configure(registrar: Registrar, data: dict[str, Any], base: Path | None = None) -> None:
    base = base or Path.cwd()
    _known(data, SECTIONS, "configuration")
    for name, settings in (data.get("extensions") or {}).items():
        registrar.settings(name).update(settings or {})
    for name in data.get("plugins") or []:
        plugins.load(str(name))(registrar)
    for name, cfg in (data.get("effect_providers") or {}).items():
        where = f"effect_providers.{name}"
        if not isinstance(cfg, dict) or "provider" not in cfg:
            raise ConfigError(f"{where} must be a mapping with a `provider`")
        cfg = dict(cfg)
        kind = cfg.pop("provider")
        policies = cfg.pop("policies", {}) or {}
        if not isinstance(policies, dict):
            raise ConfigError(f"{where}.policies must be a mapping of tool to policy")
        if kind == "python":
            _python_module(registrar, name, _known(cfg, PYTHON_KEYS, where))
        else:
            factory = registrar._r.provider_factories.get(kind)
            if factory is None:
                raise LookupError(
                    f"no provider kind {kind!r} for {name}; enable the plugin that offers it"
                )
            try:
                provider = factory(name=name, **cfg)
            except TypeError as exc:
                raise ConfigError(f"{where}: {exc}") from None
            registrar.providers.register(name, provider)
        for operation, policy in policies.items():
            full = operation if "." in operation else f"{name}.{operation}"
            registrar.tools.policy(full, **policy_fields(policy, f"{where}.policies.{operation}"))
    for package in data.get("packages") or []:
        if isinstance(package, str):
            registrar.agents.load_package(package)
        else:
            _known(package, ("package", "prefix"), "packages[]")
            registrar.agents.load_package(package["package"], package.get("prefix"))
    for name, declared in (data.get("agents") or {}).items():
        registrar.agents.from_manifest(name, declared)


def _python_module(registrar: Registrar, name: str, cfg: dict[str, Any]) -> None:
    """Every `@tool` function of a module, served by a provider of this entry's name with the
    entry's concurrency, process count and execution mode."""
    from ..providers.python_effects import PythonProvider

    where = f"effect_providers.{name}"
    if not isinstance(cfg.get("module"), str):
        raise ConfigError(f"{where}.module must name a module")
    execution = cfg.get("execution")
    if execution is not None and execution not in EXECUTIONS:
        raise ConfigError(f"{where}.execution must be one of {EXECUTIONS}, not {execution!r}")
    concurrency = cfg.get("concurrency")
    provider = PythonProvider(
        concurrency=None if concurrency is None else _count(concurrency, f"{where}.concurrency", 1),
        processes=None
        if "processes" not in cfg
        else _count(cfg["processes"], f"{where}.processes", 1),
    )
    module = importlib.import_module(cfg["module"])
    found = []
    for _name, value in inspect.getmembers(module):
        if callable(value) and hasattr(value, "__statecraft_effect__"):
            definition = value.__statecraft_effect__.but(provider=name)
            if execution is not None:
                if inspect.iscoroutinefunction(value) and execution != "async":
                    raise ConfigError(
                        f"{where}.execution {execution!r}: {definition.name} is a coroutine"
                    )
                if not inspect.iscoroutinefunction(value) and execution == "async":
                    raise ConfigError(f"{where}.execution async: {definition.name} is not async")
                definition = definition.but(execution=execution)
            found.append((definition, value))
    registrar.providers.register(name, provider)
    for definition, fn in found:
        provider.add(definition, fn)
        registrar.tools.register(definition)


__all__ = [
    "ConfigError",
    "configure",
    "duration",
    "policy_fields",
    "read",
    "runtime_from_config",
    "substitute",
]
