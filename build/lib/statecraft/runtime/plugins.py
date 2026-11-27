"""Plugins: installed packages that register agents, tools and providers.

A plugin is a function `register(registrar)`, declared by its package as an entry point:

    [project.entry-points."statecraft.plugins"]
    research = "research_agents.plugin:register"

Discovery is not activation: an installed plugin is registered only when the configuration lists
it (`plugins: [research]`); `module:function` may also be named directly.
"""

from __future__ import annotations

import importlib
from importlib.metadata import entry_points
from typing import Any

GROUP = "statecraft.plugins"


def installed() -> dict[str, str]:
    """Installed plugins: name -> `module:function` (not activated)."""
    return {ep.name: ep.value for ep in entry_points(group=GROUP)}


def load(name: str) -> Any:
    """An enabled plugin's register function, by entry-point name or `module:function`."""
    found = {ep.name: ep for ep in entry_points(group=GROUP)}
    if name in found:
        return found[name].load()
    module, sep, attr = name.partition(":")
    if not sep:
        raise LookupError(f"plugin {name!r} is not installed; installed: {sorted(found)}")
    return getattr(importlib.import_module(module), attr)


#: Store kinds offered by packages: `store: {provider: <name>, ...}` builds the store with the
#: entry point's `factory(settings, base_folder)`.
STORES = "statecraft.stores"


def store_factory(kind: str) -> Any:
    """The factory of an installed store kind (`statecraft.stores`), or `module:function`."""
    found = {ep.name: ep for ep in entry_points(group=STORES)}
    if kind in found:
        return found[kind].load()
    module, sep, attr = kind.partition(":")
    if not sep:
        raise LookupError(f"no store kind {kind!r}; installed: {sorted(found)}")
    return getattr(importlib.import_module(module), attr)


__all__ = ["GROUP", "STORES", "installed", "load", "store_factory"]
