"""Which agents exist: feature packages, their `agent.yaml` declarations and their classes.

An agent is named `<feature>:<id>` in records (`notes:digest_all`); a parent spawns a
child of its own feature by `agent.<id>`, which it must declare in `tools`. A
feature's `agents/<...>/agent.py` defines one `Agent` subclass; a skill with a contract runs as
the general agentic agent (`statecraft.agentic.SkillAgent`).
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
from dataclasses import dataclass, field
from pathlib import Path

from .agent import Agent, AgentDefinitionError
from .spec import AGENT_PREFIX, SKILL_AGENT_ENTRY, AgentRegistry, AgentSpec

#: Agents the framework itself provides: `core:decider`.
CORE = "core:"


def split_ref(ref: str) -> tuple[str, str]:
    feature, sep, agent_id = ref.partition(":")
    if not sep or not feature or not agent_id:
        raise ValueError(f"an agent is named <feature>:<id>, not {ref!r}")
    return feature, agent_id


def code_hash(spec: AgentSpec) -> str:
    """A hash of the agent's own folder: changes when its code or declaration changes."""
    if spec.folder is None:
        return ""
    digest = hashlib.sha256()
    for path in sorted(Path(spec.folder).glob("*")):
        if path.is_file() and path.suffix in (".py", ".yaml", ".md"):
            digest.update(path.name.encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


@dataclass
class Catalog:
    """Feature name -> its package (`notes` -> `my_agents_notes`)."""

    packages: dict[str, str]
    #: Classes given directly, for tests: `<feature>:<id>` -> (Agent subclass, spec or None).
    extra: dict[str, tuple[type[Agent], AgentSpec | None]] = field(default_factory=dict)
    _registries: dict[str, AgentRegistry] = field(default_factory=dict)
    _classes: dict[str, type[Agent]] = field(default_factory=dict)

    def registry(self, feature: str) -> AgentRegistry:
        if feature not in self._registries:
            if feature not in self.packages:
                raise KeyError(f"no feature {feature!r}; features: {sorted(self.packages)}")
            self._registries[feature] = AgentRegistry(self.packages[feature])
        return self._registries[feature]

    def spec(self, ref: str) -> AgentSpec | None:
        if ref in self.extra:
            return self.extra[ref][1]
        if ref.startswith(CORE):
            return None
        feature, agent_id = split_ref(ref)
        return self.registry(feature).get(agent_id)

    def agent_class(self, ref: str) -> type[Agent]:
        if ref in self.extra:
            return self.extra[ref][0]
        if ref not in self._classes and ref.startswith(CORE):
            from . import agentic

            builtins = {f"{CORE}decider": agentic.Decider}
            if ref not in builtins:
                raise KeyError(f"no built-in agent {ref!r}")
            self._classes[ref] = builtins[ref]
        if ref not in self._classes:
            spec = self.spec(ref)
            assert spec is not None
            if spec.entry == SKILL_AGENT_ENTRY:
                from .agentic import SkillAgent

                cls: type[Agent] = SkillAgent
            else:
                module = importlib.import_module(spec.entry.partition(":")[0])
                found = [
                    v
                    for v in vars(module).values()
                    if inspect.isclass(v)
                    and issubclass(v, Agent)
                    and v.__module__ == module.__name__
                ]
                if len(found) != 1:
                    raise AgentDefinitionError(
                        f"{module.__name__} defines {len(found)} Agent classes; one is expected"
                    )
                cls = found[0]
            cls.check_definition()
            self._classes[ref] = cls
        return self._classes[ref]

    def child_ref(self, parent_ref: str, name: str) -> str:
        """`agent.<id>` from a parent -> `<feature>:<id>`; a full `<feature>:<id>` is kept."""
        if name.startswith(AGENT_PREFIX):
            return f"{split_ref(parent_ref)[0]}:{name.removeprefix(AGENT_PREFIX)}"
        split_ref(name)
        return name

    def skill_folder(self, ref: str, skill_id: str) -> Path:
        feature, _ = split_ref(ref)
        return Path(self.registry(feature).skill(skill_id).folder)

    def code(self, ref: str) -> str:
        spec = self.spec(ref)
        return code_hash(spec) if spec is not None else ""


def declared(spec: AgentSpec | None, name: str) -> bool:
    return spec is None or spec.may_call(name)


__all__ = ["Catalog", "code_hash", "declared", "split_ref"]
