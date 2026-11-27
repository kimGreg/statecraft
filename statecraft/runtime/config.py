"""One configuration builds a runtime (architecture §13).

    runtime: {concurrency: 16, executors: 1, drain: 30, lease_ttl: 120, beat: 30}
    store: {provider: sqlite, path: ./statecraft.db}            # or memory
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
are never stored). Order: settings, plugins (they register agents, tools and provider kinds),
providers from `effect_providers`, packages, manifest agents. The runtime's `start()` then
discovers, freezes and validates.
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
from . import plugins
from .registry import Registrar

ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def substitute(value: Any) -> Any:
    if isinstance(value, str):
        return ENV.sub(lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: substitute(v) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v) for v in value]
    return value


def read(config: str | Path | dict[str, Any]) -> tuple[dict[str, Any], Path]:
    if isinstance(config, dict):
        return substitute(config), Path.cwd()
    path = Path(config)
    return substitute(yaml.safe_load(path.read_text(encoding="utf-8")) or {}), path.parent


def _path(base: Path, value: str) -> Path:
    p = Path(value)
    return p if p.is_absolute() else base / p


def runtime_from_config(config: str | Path | dict[str, Any], **overrides: Any) -> Any:
    from ..providers.stores import MemoryStore, SqliteStore
    from ..providers.workspaces import LocalWorkspace, MemoryWorkspace
    from .runtime import Runtime

    data, base = read(config)
    store_cfg = data.get("store") or {"provider": "memory"}
    if store_cfg.get("provider", "sqlite") == "sqlite":
        store: Any = SqliteStore(_path(base, store_cfg.get("path", "statecraft.db")))
    else:
        store = MemoryStore()
    ws_cfg = data.get("workspace") or {"provider": "memory"}
    if ws_cfg.get("provider", "local") == "local":
        workspace: Any = LocalWorkspace(_path(base, ws_cfg.get("root", "workspaces")))
    else:
        workspace = MemoryWorkspace()
    options = {
        k: v
        for k, v in (data.get("runtime") or {}).items()
        if k in ("concurrency", "executors", "lease_ttl", "beat", "scan_every", "drain")
    }
    runtime = Runtime(store=store, workspace=workspace, **{**options, **overrides})
    configure(runtime.registrar, data, base)
    return runtime


def configure(registrar: Registrar, data: dict[str, Any], base: Path | None = None) -> None:
    base = base or Path.cwd()
    for name, settings in (data.get("extensions") or {}).items():
        registrar.settings(name).update(settings or {})
    for name in data.get("plugins") or []:
        plugins.load(str(name))(registrar)
    for name, cfg in (data.get("effect_providers") or {}).items():
        cfg = dict(cfg)
        kind = cfg.pop("provider")
        policies = cfg.pop("policies", {}) or {}
        if kind == "python":
            _python_module(registrar, cfg)
        else:
            factory = registrar._r.provider_factories.get(kind)
            if factory is None:
                raise LookupError(
                    f"no provider kind {kind!r} for {name}; enable the plugin that offers it"
                )
            registrar.providers.register(name, factory(name=name, **cfg))
        for operation, policy in policies.items():
            full = operation if "." in operation else f"{name}.{operation}"
            registrar.tools.policy(full, **EffectPolicy.of(policy).as_dict())
    for package in data.get("packages") or []:
        if isinstance(package, str):
            registrar.agents.load_package(package)
        else:
            registrar.agents.load_package(package["package"], package.get("prefix"))
    for name, declared in (data.get("agents") or {}).items():
        registrar.agents.from_manifest(name, declared)


def _python_module(registrar: Registrar, cfg: dict[str, Any]) -> None:
    """Every `@tool` function of a module."""
    module = importlib.import_module(cfg["module"])
    for _name, value in inspect.getmembers(module):
        if callable(value) and hasattr(value, "__statecraft_effect__"):
            registrar.tools.register(value)


__all__ = ["configure", "read", "runtime_from_config", "substitute"]
