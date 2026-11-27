"""Registries: what can run (agents) and what it can call (tools/effects), resolved by name.

Built when the runtime starts, through a **registrar** that can only register, then frozen
(architecture §10). Three ways in, one result:

    registrar.agents.register("research", Research, version="1", tools=["search.query"])
    registrar.agents.load_package("my_agents", prefix="research")   # agents/<...>/agent.yaml
    manifest: `agents:` in the configuration (an entry and its contract)

Tools come from `registrar.tools.register(fn)` (Python, `@tool`) and from providers'
`discover()`. An instance pins what it resolved (definition code digest, each binding's provider,
operation, version, schema and policy digests) and keeps it for its whole life.
"""

from __future__ import annotations

import fnmatch
import hashlib
import importlib
import inspect
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

from ..kernel.effects import EffectPolicy
from ..ports.effects import CAPABILITIES, CAPACITY_SCOPES
from ..sdk.agent import Agent, AgentDefinitionError, states_of
from ..sdk.effect import EffectDefinition, definition_of

AGENT_FILE = "agent.yaml"
#: Keys of `agent.yaml` the runtime reads. Any other key must be namespaced (`x-...`), and is
#: kept in `meta`; an unknown plain key (a typo such as `tool:`) refuses the start.
AGENT_KEYS = frozenset(
    {
        "id",
        "name",
        "version",
        "description",
        "entry",
        "input",
        "tools",
        "spawns",
        "reads",
        "optional_reads",
        "writes",
        "workspace",
        "attach",
        "extensions",
    }
)
SKIPPED_FOLDERS = frozenset({"tests", "examples", "docs", "__pycache__"})
ORCHESTRATOR_FOLDER = "orchestrator"
#: Legacy spawn prefix: `agent.<id>` names a sibling of the same package.
LEGACY_PREFIX = "agent."


class RegistryError(RuntimeError):
    """The registry cannot start: what is wrong, all at once."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("registry is invalid:\n  " + "\n  ".join(problems))
        self.problems = problems


class Frozen(RuntimeError):
    """The registry is frozen while the runtime runs; register before `start()`."""


@dataclass(frozen=True)
class AgentDefinition:
    """An agent's contract: what runs, what it takes, what it may call, start, read and write."""

    name: str
    cls: type | None = None
    entry: str = ""
    version: str = "0"
    description: str = ""
    input_schema: dict[str, Any] | None = None
    #: tools (effects) it may call: exact names or `prefix.*`
    tools: tuple[str, ...] = ()
    #: agents it may start: names, or `*`
    spawns: tuple[str, ...] = ()
    #: artifact types (None: undeclared, anything)
    reads: tuple[str, ...] | None = None
    optional_reads: tuple[str, ...] = ()
    writes: tuple[str, ...] | None = None
    workspace: str = "shared"
    #: What its children may be attached to: "none" (default), "ro" (another run's workspace,
    #: read-only) or "rw" (read-write: a separate, explicit grant).
    attach: str = "none"
    extensions: dict[str, Any] = field(default_factory=dict, hash=False, compare=False)
    meta: dict[str, Any] = field(default_factory=dict, hash=False, compare=False)
    folder: Path | None = None
    code: str = ""

    def load(self) -> type:
        if self.cls is not None:
            return self.cls
        module, _, attr = self.entry.partition(":")
        loaded = importlib.import_module(module)
        if attr:
            return getattr(loaded, attr)
        found = [
            v
            for v in vars(loaded).values()
            if inspect.isclass(v) and issubclass(v, Agent) and v.__module__ == loaded.__name__
        ]
        if len(found) != 1:
            raise AgentDefinitionError(
                f"{module} defines {len(found)} Agent classes; one is expected (or name it)"
            )
        return found[0]

    def permits_tool(self, name: str) -> bool:
        return any(name == t or fnmatch.fnmatchcase(name, t) for t in self.tools)

    def may_read(self, kind: str) -> bool:
        return (
            self.reads is None
            or "*" in self.optional_reads
            or kind in (*self.reads, *self.optional_reads)
        )

    def may_write(self, kind: str) -> bool:
        return self.writes is None or kind in self.writes

    def extension(self, name: str) -> dict[str, Any]:
        return dict(self.extensions.get(name) or {})

    def pin(self) -> dict[str, Any]:
        return {"name": self.name, "version": self.version, "code": self.code}


def code_digest(definition: AgentDefinition, cls: type | None) -> str:
    """Changes when the agent's code or declaration changes: its folder's files, else the source
    of the module that defines its class."""
    digest = hashlib.sha256()
    if definition.folder is not None:
        for path in sorted(Path(definition.folder).glob("*")):
            if path.is_file() and path.suffix in (".py", ".yaml", ".md"):
                digest.update(path.name.encode())
                digest.update(path.read_bytes())
    elif cls is not None:
        module = sys.modules.get(cls.__module__)
        try:
            source = inspect.getsource(module) if module is not None else ""
        except (OSError, TypeError):
            source = cls.__qualname__
        digest.update(source.encode())
    return "sha256:" + digest.hexdigest()[:32]


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(v) for v in value)


def _optional(value: Any) -> tuple[str, ...] | None:
    return None if value is None else _strings(value)


def agent_id_of(folder: Path) -> str:
    return folder.parent.name if folder.name == ORCHESTRATOR_FOLDER else folder.name


class Registry:
    """Agents, tools, providers and extension settings; frozen while the runtime runs."""

    def __init__(self) -> None:
        self.agents: dict[str, AgentDefinition] = {}
        self.effects: dict[str, EffectDefinition] = {}
        self.providers: dict[str, Any] = {}
        self.provider_factories: dict[str, Callable[..., Any]] = {}
        self.settings: dict[str, dict[str, Any]] = {}
        self.validators: list[Callable[[Registry], Iterable[str]]] = []
        #: objects extensions provide to their agents (`ctx.service(name)`)
        self.services: dict[str, Any] = {}
        self.policy_overrides: dict[str, dict[str, Any]] = {}
        self.problems: list[str] = []
        #: providers that did not start (their tools resolve once they are back)
        self.unreachable: set[str] = set()
        self.frozen = False
        self._python: Any = None  # the Python tool provider, created on first registration

    # ---- registration (through the registrar) ----------------------------------------------
    def _check_open(self) -> None:
        if self.frozen:
            raise Frozen("the registry is frozen while the runtime runs; register before start()")

    def add_agent(self, definition: AgentDefinition) -> AgentDefinition:
        self._check_open()
        if definition.attach not in ("none", "ro", "rw"):
            self.problems.append(
                f"agent {definition.name}: attach is none, ro or rw, not {definition.attach!r}"
            )
        if definition.name in self.agents:
            self.problems.append(f"agent {definition.name} is registered twice")
        self.agents[definition.name] = definition
        return definition

    def add_effect(self, definition: EffectDefinition) -> EffectDefinition:
        self._check_open()
        if definition.name in self.effects:
            self.problems.append(f"tool {definition.name} is registered twice")
        self.effects[definition.name] = definition
        return definition

    def add_provider(self, name: str, provider: Any) -> Any:
        self._check_open()
        if name in self.providers:
            self.problems.append(f"provider {name} is registered twice")
        self.providers[name] = provider
        return provider

    # ---- start: discovery, then freeze ---------------------------------------------------
    async def start_providers(self) -> list[str]:
        """Start every provider and collect the tools it discovers. Returns warnings (a provider
        that cannot be reached; its pinned or configured tools still resolve by name later)."""
        warnings: list[str] = []
        for name, provider in self.providers.items():
            start = getattr(provider, "start", None)
            try:
                if start is not None:
                    await start()
                discover = getattr(provider, "discover", None)
                found = await discover() if discover is not None else []
            except Exception as exc:
                warnings.append(f"provider {name} did not start: {type(exc).__name__}: {exc}")
                self.unreachable.add(name)
                continue
            for definition in found:
                definition = definition.but(provider=definition.provider or name)
                override = self.policy_overrides.get(definition.name)
                if override is not None:
                    definition = definition.but(
                        policy=EffectPolicy.of({**definition.policy.as_dict(), **override})
                    )
                existing = self.effects.get(definition.name)
                if existing is None:
                    self.effects[definition.name] = definition
                elif existing.provider != definition.provider:
                    self.problems.append(
                        f"tool {definition.name} is offered by {existing.provider} and by "
                        f"{definition.provider}: rename one (a provider names its tools "
                        f"<provider>.<tool>)"
                    )
        return warnings

    def freeze(self) -> None:
        self._apply_policy_overrides()
        for name, definition in list(self.agents.items()):
            try:
                cls = definition.load()
            except Exception as exc:
                self.problems.append(f"agent {name}: cannot load {definition.entry}: {exc}")
                continue
            self.agents[name] = replace(definition, cls=cls, code=code_digest(definition, cls))
        self.frozen = True

    def _apply_policy_overrides(self) -> None:
        """The operator's policies count for every tool, registered or discovered; a policy for
        a tool that does not exist is a problem (a typo would otherwise be silent), unless its
        provider did not start (then its tools are unknown, and that is already a warning)."""
        for name, override in self.policy_overrides.items():
            definition = self.effects.get(name)
            if definition is None:
                if name.split(".", 1)[0] not in self.unreachable:
                    self.problems.append(f"a policy is set for {name}, which no provider offers")
                continue
            merged = EffectPolicy.of({**definition.policy.as_dict(), **override})
            if merged != definition.policy:
                self.effects[name] = definition.but(policy=merged)

    # ---- validation -------------------------------------------------------------------------
    def validate(self) -> tuple[list[str], list[str]]:
        """(errors, warnings). Errors refuse the start; warnings are reported."""
        errors = list(self.problems)
        warnings: list[str] = []
        for name, provider in self.providers.items():
            declared = set(getattr(provider, "capabilities", None) or ())
            unknown = sorted(declared - CAPABILITIES)
            if unknown:
                errors.append(
                    f"provider {name} declares unknown capabilities {unknown}; "
                    f"known: {sorted(CAPABILITIES)}"
                )
            if "reconcile" in declared and not callable(getattr(provider, "reconcile", None)):
                errors.append(f"provider {name} declares reconcile but has no reconcile()")
            scope = getattr(provider, "capacity_scope", "process") or "process"
            if scope not in CAPACITY_SCOPES:
                errors.append(
                    f"provider {name}: capacity_scope must be one of {CAPACITY_SCOPES}, "
                    f"not {scope!r}"
                )
        for name, definition in self.agents.items():
            cls = definition.cls
            if cls is None:
                continue
            if isinstance(cls, type) and issubclass(cls, Agent):
                try:
                    cls.check_definition()
                except AgentDefinitionError as exc:
                    errors.append(f"agent {name}: {exc}")
            elif not callable(getattr(cls, "step", None)) or not getattr(cls, "start", None):
                errors.append(f"agent {name}: {cls!r} has no async step() and start state")
            for pattern in definition.tools:
                if any(t == pattern or fnmatch.fnmatchcase(t, pattern) for t in self.effects):
                    continue
                if any(c in pattern for c in "*?["):
                    continue  # a pattern may match nothing yet; exact names must resolve
                if pattern.partition(".")[0] in self.unreachable:
                    warnings.append(f"agent {name} may call {pattern}; its provider is unreachable")
                else:
                    errors.append(f"agent {name} may call {pattern}, which no tool matches")
            for target in definition.spawns:
                if target != "*" and self.resolve_spawn(definition, target) is None:
                    errors.append(f"agent {name} may spawn {target}, which is not registered")
        for validator in self.validators:
            errors.extend(validator(self))
        for name, effect in self.effects.items():
            if effect.provider not in self.providers:
                errors.append(f"tool {name}: provider {effect.provider!r} is not registered")
                continue
            check = getattr(self.providers[effect.provider], "policy_problem", None)
            problem = check(effect) if check is not None else None
            if problem:  # the provider cannot keep this policy (e.g. no deduplication)
                errors.append(problem)
        return sorted(set(errors)), sorted(set(warnings))

    def lint(self) -> list[str]:
        """Optional findings that are not errors: spawn cycles (recursion is legitimate), reads
        nobody writes (artifacts may come from outside), tools nobody may call."""
        found: list[str] = []
        written = {w for d in self.agents.values() for w in (d.writes or ())}
        for d in self.agents.values():
            found += [
                f"{d.name} reads {r}, which no agent writes"
                for r in d.reads or ()
                if r not in written
            ]

        def visit(name: str, chain: tuple[str, ...]) -> None:
            if name in chain:
                found.append("spawn cycle: " + " -> ".join((*chain, name)))
                return
            d = self.agents.get(name)
            for target in d.spawns if d else ():
                resolved = self.resolve_spawn(d, target) if target != "*" else None
                if resolved is not None:
                    visit(resolved.name, (*chain, name))

        for name in self.agents:
            visit(name, ())
        used = {t for t in self.effects if any(d.permits_tool(t) for d in self.agents.values())}
        found += [f"no agent may call tool {t}" for t in self.effects if t not in used]
        return sorted(set(found))

    # ---- resolution -------------------------------------------------------------------------
    def agent(self, name: str) -> AgentDefinition:
        try:
            return self.agents[name]
        except KeyError:
            raise KeyError(f"no agent {name!r}; registered: {sorted(self.agents)}") from None

    def resolve_spawn(self, parent: AgentDefinition, target: str) -> AgentDefinition | None:
        """A spawn target as the parent wrote it: a registered name, a sibling of the same
        package (`<prefix>:<id>`), or the legacy `agent.<id>`."""
        bare = target.removeprefix(LEGACY_PREFIX)
        candidates = [bare]
        if ":" in parent.name and ":" not in bare:
            candidates.append(f"{parent.name.rpartition(':')[0]}:{bare}")
        return next((self.agents[c] for c in candidates if c in self.agents), None)

    def may_spawn(self, parent: AgentDefinition, target: str) -> bool:
        if "*" in parent.spawns:
            return True
        resolved = self.resolve_spawn(parent, target)
        bare = target.removeprefix(LEGACY_PREFIX)
        for allowed in parent.spawns:
            if allowed in (target, bare):
                return True
            if resolved is not None and self.resolve_spawn(parent, allowed) is resolved:
                return True
        return False

    def bindings_for(self, definition: AgentDefinition) -> dict[str, dict[str, Any]]:
        """The bindings an instance of the definition pins: every tool it may call that exists."""
        return {
            name: effect.binding()
            for name, effect in sorted(self.effects.items())
            if definition.permits_tool(name)
        }

    def effect(self, name: str, binding: dict[str, Any]) -> EffectDefinition | None:
        """The tool an instance's binding names, if it is still the same (provider, operation,
        version, schema and policy); None when it changed or is gone."""
        current = self.effects.get(name)
        if current is None:
            return None
        return current if current.binding() == binding else None

    def pin_problems(
        self, record_definition: dict[str, Any], bindings: dict[str, dict[str, Any]]
    ) -> list[str]:
        """What changed since an instance pinned its definition and bindings."""
        found = []
        name = record_definition.get("name")
        if name:
            current = self.agents.get(name)
            if current is None:
                found.append(f"agent {name} is no longer registered")
            elif record_definition.get("code") and current.code != record_definition["code"]:
                found.append(f"agent {name} code changed since this instance started")
        for effect, binding in bindings.items():
            if self.effect(effect, binding) is None:
                found.append(f"tool {effect} changed or is gone since this instance started")
        return found

    # ---- listings (API, console) ----------------------------------------------------------
    def describe_agent(self, name: str) -> dict[str, Any]:
        d = self.agent(name)
        return {
            "name": d.name,
            "version": d.version,
            "description": d.description,
            "entry": d.entry or (f"{d.cls.__module__}:{d.cls.__qualname__}" if d.cls else ""),
            "code": d.code,
            "input_schema": d.input_schema,
            "tools": list(d.tools),
            "spawns": list(d.spawns),
            "reads": None if d.reads is None else list(d.reads),
            "writes": None if d.writes is None else list(d.writes),
            "extensions": d.extensions,
            "meta": d.meta,
            "states": sorted(states_of(d.cls)) if d.cls is not None else [],
            "start": getattr(d.cls, "start", None),
        }

    def describe_effect(self, name: str) -> dict[str, Any]:
        e = self.effects[name]
        return {
            "name": e.name,
            "description": e.description,
            "provider": e.provider,
            "operation": e.operation or e.name,
            "version": e.version,
            "input_schema": e.input_schema,
            "output_schema": e.output_schema,
            "policy": e.policy.as_dict(),
            "execution": e.execution,
            "binding": e.binding(),
        }


# ---- the registrar --------------------------------------------------------------------------


class _Agents:
    def __init__(self, registry: Registry) -> None:
        self._r = registry

    def register(
        self,
        name: str,
        agent: type | str,
        *,
        version: str = "0",
        description: str = "",
        input: dict[str, Any] | None = None,
        tools: Iterable[str] = (),
        spawns: Iterable[str] = (),
        reads: Iterable[str] | None = None,
        optional_reads: Iterable[str] = (),
        writes: Iterable[str] | None = None,
        workspace: str = "shared",
        attach: str = "none",
        extensions: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
        folder: Path | None = None,
    ) -> AgentDefinition:
        return self._r.add_agent(
            AgentDefinition(
                name=name,
                cls=agent if isinstance(agent, type) else None,
                entry=agent if isinstance(agent, str) else "",
                version=str(version),
                description=description
                or (inspect.getdoc(agent) or "" if isinstance(agent, type) else ""),
                input_schema=input,
                tools=_strings(tools),
                spawns=_strings(spawns),
                reads=_optional(reads),
                optional_reads=_strings(optional_reads),
                writes=_optional(writes),
                workspace=workspace,
                attach=attach,
                extensions=dict(extensions or {}),
                meta=dict(meta or {}),
                folder=folder,
            )
        )

    def from_manifest(
        self, name: str, declared: dict[str, Any], *, folder: Path | None = None
    ) -> AgentDefinition:
        """A definition from a manifest entry or an `agent.yaml`."""
        unknown = sorted(k for k in declared if k not in AGENT_KEYS and not str(k).startswith("x-"))
        if unknown:
            self._r.problems.append(
                f"agent {name}: unknown keys {unknown} (namespace your own as x-...); "
                f"known: {sorted(AGENT_KEYS)}"
            )
        return self.register(
            name,
            str(declared["entry"]),
            version=str(declared.get("version", "0")),
            description=str(declared.get("description") or ""),
            input=declared.get("input"),
            tools=_strings(declared.get("tools")),
            spawns=_strings(declared.get("spawns")),
            reads=_optional(declared.get("reads")),
            optional_reads=_strings(declared.get("optional_reads")),
            writes=_optional(declared.get("writes")),
            workspace=str(declared.get("workspace") or "shared"),
            attach=str(declared.get("attach") or "none"),
            extensions=dict(declared.get("extensions") or {}),
            meta={k: v for k, v in declared.items() if k not in AGENT_KEYS},
            folder=folder,
        )

    def load_package(self, package: str, prefix: str | None = None) -> list[AgentDefinition]:
        """Every `agents/<...>/agent.yaml` in a package: name `<prefix>:<id>` (the prefix defaults
        to the package name); the entry is the folder's `agent` module, unless declared."""
        found = importlib.util.find_spec(package)
        if found is None or not found.submodule_search_locations:
            raise ValueError(f"agent package {package!r} is not importable")
        root = Path(next(iter(found.submodule_search_locations)))
        prefix = prefix or package
        made = []
        for marker in sorted(root.rglob(AGENT_FILE)):
            folder = marker.parent
            relative = folder.relative_to(root)
            if any(p in SKIPPED_FOLDERS or p.startswith(".") for p in relative.parts):
                continue
            declared = yaml.safe_load(marker.read_text(encoding="utf-8")) or {}
            agent_id = agent_id_of(folder)
            if str(declared.get("id") or agent_id) != agent_id:
                self._r.problems.append(f"{marker}: id must be {agent_id!r} (its folder name)")
                continue
            module = ".".join((package, *relative.parts, "agent"))
            declared = {"entry": module, **declared}
            made.append(self.from_manifest(f"{prefix}:{agent_id}", declared, folder=folder))
        return made


class _Tools:
    def __init__(self, registry: Registry) -> None:
        self._r = registry

    def register(self, fn_or_definition: Any, *, provider: str = "python") -> EffectDefinition:
        """A `@tool` function (served by the Python provider), or an `EffectDefinition` a
        provider of that name carries out."""
        if isinstance(fn_or_definition, EffectDefinition):
            return self._r.add_effect(
                fn_or_definition.but(provider=fn_or_definition.provider or provider)
            )
        definition = definition_of(fn_or_definition)
        if self._r._python is None:
            from ..providers.python_effects import PythonProvider

            self._r._python = PythonProvider()
            self._r.add_provider("python", self._r._python)
        self._r._python.add(definition, fn_or_definition)
        return self._r.add_effect(definition)

    def policy(self, name: str, **policy: Any) -> None:
        """Set (part of) the policy of a discovered tool: the operator's word counts, not the
        server's hints."""
        self._r._check_open()
        self._r.policy_overrides[name] = {**self._r.policy_overrides.get(name, {}), **policy}


class _Providers:
    def __init__(self, registry: Registry) -> None:
        self._r = registry

    def register(self, name: str, provider: Any) -> Any:
        return self._r.add_provider(name, provider)

    def factory(self, kind: str, factory: Callable[..., Any]) -> None:
        """A provider kind configuration can name (`effect_providers: {x: {provider: kind}}`)."""
        self._r._check_open()
        self._r.provider_factories[kind] = factory


class Registrar:
    """What a plugin gets: it can register, nothing else (no runs, no store)."""

    def __init__(self, registry: Registry) -> None:
        self._r = registry
        self.agents = _Agents(registry)
        self.tools = _Tools(registry)
        self.effects = self.tools
        self.providers = _Providers(registry)

    def settings(self, extension: str) -> dict[str, Any]:
        """The configuration's `extensions.<name>` section (opaque to the runtime)."""
        return self._r.settings.setdefault(extension, {})

    def provide(self, name: str, service: Any) -> Any:
        """An object an extension's agents reach with `ctx.service(name)` (a catalog, a client).
        The runtime never reads it."""
        self._r._check_open()
        self._r.services[name] = service
        return service

    def service(self, name: str) -> Any:
        return self._r.services.get(name)

    def validator(self, check: Callable[[Registry], Iterable[str]]) -> None:
        """Called at validation with the whole registry; returns problems (extensions check
        their own settings and agents' `extensions.<name>` here)."""
        self._r._check_open()
        self._r.validators.append(check)


__all__ = [
    "AgentDefinition",
    "Frozen",
    "Registrar",
    "Registry",
    "RegistryError",
    "code_digest",
]
