"""Agents and skills as data: leaf folders in a feature package's `agents/` and `skills/`.

    notes/                                  a feature package (a folder that is also a package)
      agents/                               its agents, grouped freely
        digest_all/
          orchestrator/  agent.yaml agent.py   an agent that calls others (id: digest_all)
          subagents/
            digest/      agent.yaml agent.py   an agent: a folder with agent.yaml
      skills/                               its skills, grouped freely
        notes/note-style/  SKILL.md references/...   a skill: a folder with SKILL.md
      libs/                                 code its agents share (never imports an agent)
      tests/  examples/  docs/

A folder's name is its id (an `orchestrator/` folder takes its parent's) and must be unique in
the feature. An agent outside `agents/` or a skill outside `skills/` is reported. Two node sets:

    # notes/digest/agent.yaml
    id: digest
    description: Summarizes each note.
    modes: [python, llm]                # ways run(ctx) can go (default [python])
    reads: [documents]                  # artifact types it needs in the session
    optional_reads: []                  # may be missing; ["*"]: any type (a run reader)
    writes: [digest]
    tools: [files.read_file, agent.intake]   # MCP tools and other agents it may call
    skills: [note-style]                # skills installed in its workspace when it starts
    workspace: own                      # shared (the session, default) or own (a sub-space)
    owner: research                     # any other key is the application's (`spec.meta`)

    # notes/note-style/SKILL.md          (the Agent Skills format Google ADK loads)
    ---
    name: note-style
    description: How to write a note summary.
    allowed-tools: files.read_file      # tools the skill needs; its agents must declare them
    ---
    instructions ...

A skill whose `metadata` declares what it writes is also an agent: the general, model-driven agent
(`statecraft.agentic.SkillAgent`) with the skill as its instruction and the skill's tools.
It is called like any agent, with the goal as input: `ctx.call("agent.notes-brief", goal=...)`.

    ---
    name: notes-brief
    description: Brief the notes of the inbox.
    allowed-tools: files.read_file python.run
    metadata:
      reads: [documents]                 # artifact types it needs (optional_reads: may be missing)
      writes: [brief]                    # what it must write: this makes the skill an agent
      workspace: shared                  # or own
    ---

An agent that calls other agents (`agent.<id>` in its tools) is how a workflow is made. Calls go
through `ctx.call(name, **arguments)`, which is async.
"""

from __future__ import annotations

import importlib
import importlib.util
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

AGENT_MODES = ("python", "llm")
DEFAULT_MODE = "python"
AGENT_PREFIX = "agent."
WORKSPACES = ("shared", "own")
#: In `optional_reads`: the agent may read any artifact type (a reader of finished runs).
ANY_TYPE = "*"
AGENT_FILE = "agent.yaml"
SKILL_FILE = "SKILL.md"
#: The Agent Skills name rule Google ADK enforces when it loads a skill.
SKILL_NAME = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
#: The agent a skill with a contract runs as.
SKILL_AGENT_ENTRY = "statecraft.agentic:SkillAgent"
#: Artifact types the framework's own agents write (`core:decider` writes `decision`).
CORE_WRITES = frozenset({"decision"})
#: The keys of `agent.yaml` the engine reads; the others are kept in `AgentSpec.meta`.
AGENT_KEYS = frozenset(
    {
        "id",
        "description",
        "modes",
        "reads",
        "optional_reads",
        "writes",
        "tools",
        "skills",
        "workspace",
    }
)
#: Folders of a feature that never hold agents or skills.
SKIPPED_FOLDERS = frozenset({"tests", "examples", "docs", "__pycache__"})
#: Where a feature keeps its agents and its skills (its shared code is in `libs/`).
AGENTS_FOLDER, SKILLS_FOLDER_NAME = "agents", "skills"
#: An orchestrator's own folder, beside its `subagents/`; it takes its parent folder's name.
ORCHESTRATOR_FOLDER = "orchestrator"


@dataclass(frozen=True)
class SkillSpec:
    """One skill: a folder with SKILL.md (and references/, assets/, scripts/)."""

    id: str
    folder: Path
    description: str = ""
    allowed_tools: tuple[str, ...] = ()
    reads: tuple[str, ...] = ()
    optional_reads: tuple[str, ...] = ()
    writes: tuple[str, ...] = ()
    workspace: str = "shared"

    @property
    def is_agent(self) -> bool:
        """A skill that declares what it writes is a task the general agent can be spawned for."""
        return bool(self.writes)

    def as_agent(self) -> AgentSpec:
        """The general agent spawned with this skill: model-driven, the skill's tools and
        contract, the skill installed in its workspace."""
        return AgentSpec(
            id=self.id,
            entry=SKILL_AGENT_ENTRY,
            modes=("llm",),
            reads=self.reads,
            optional_reads=self.optional_reads,
            writes=self.writes,
            tools=self.allowed_tools,
            skills=(self.id,),
            workspace=self.workspace,
            description=self.description,
            folder=self.folder,
        )

    def files(self) -> list[Path]:
        """Every file of the skill, relative to its folder."""
        return sorted(
            p.relative_to(self.folder)
            for p in self.folder.rglob("*")
            if p.is_file() and "__pycache__" not in p.parts
        )


@dataclass(frozen=True)
class AgentSpec:
    """One agent: where it is and what it may read, write and call.

    `reads`, `writes` and `tools` are None when undeclared (an agent built in a test); declared,
    the runtime refuses anything outside them."""

    id: str
    entry: str
    modes: tuple[str, ...] = (DEFAULT_MODE,)
    reads: tuple[str, ...] | None = None
    optional_reads: tuple[str, ...] = ()
    writes: tuple[str, ...] | None = None
    tools: tuple[str, ...] | None = None
    skills: tuple[str, ...] = ()
    workspace: str = "shared"
    #: The declaration's other keys, for the application (the engine reads none of them).
    meta: dict[str, Any] = field(default_factory=dict, hash=False, compare=False)
    description: str = ""
    folder: Path | None = None

    def mode_for(self, requested: str) -> str:
        """The mode this agent runs in when a run asks for `requested`: that one if declared,
        else its first declared mode."""
        return requested if requested in self.modes else self.modes[0]

    def may_call(self, name: str) -> bool:
        return self.tools is None or name in self.tools

    def may_read(self, artifact_type: str) -> bool:
        return (
            self.reads is None
            or ANY_TYPE in self.optional_reads
            or artifact_type in (*self.reads, *self.optional_reads)
        )

    def may_write(self, artifact_type: str) -> bool:
        return self.writes is None or artifact_type in self.writes

    def agents_called(self) -> list[str]:
        return [
            t.removeprefix(AGENT_PREFIX) for t in self.tools or () if t.startswith(AGENT_PREFIX)
        ]


def _strings(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in value or [])


def _workspace(owner: str, declared: Any) -> str:
    workspace = str(declared or "shared")
    if workspace not in WORKSPACES:
        raise ValueError(f"{owner}: workspace must be one of {list(WORKSPACES)}")
    return workspace


def read_frontmatter(path: Path) -> dict[str, Any]:
    """The YAML block between the first two `---` lines of a markdown file."""
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return {}
    _, block, _rest = text.split("---", 2)
    return dict(yaml.safe_load(block) or {})


def load_skill(folder: Path) -> SkillSpec:
    front = read_frontmatter(folder / SKILL_FILE)
    name = str(front.get("name") or "")
    if name != folder.name:
        raise ValueError(f"{folder}/{SKILL_FILE}: name {name!r} must be the folder name")
    if not SKILL_NAME.fullmatch(name):
        raise ValueError(f"{folder}: a skill name is lowercase kebab-case (a-z, 0-9, hyphens)")
    allowed = front.get("allowed-tools", front.get("allowed_tools")) or ""
    contract = dict(front.get("metadata") or {})
    return SkillSpec(
        id=name,
        folder=folder,
        description=str(front.get("description") or ""),
        allowed_tools=tuple(str(allowed).split()),
        reads=_strings(contract.get("reads")),
        optional_reads=_strings(contract.get("optional_reads")),
        writes=_strings(contract.get("writes")),
        workspace=_workspace(f"skill {name}", contract.get("workspace")),
    )


def agent_id_of(folder: Path) -> str:
    """An agent's (or skill's) id: its folder name; an `orchestrator/` folder takes its parent's."""
    return folder.parent.name if folder.name == ORCHESTRATOR_FOLDER else folder.name


def load_agent(module: str, folder: Path) -> AgentSpec:
    declared = yaml.safe_load((folder / AGENT_FILE).read_text(encoding="utf-8")) or {}
    agent_id = agent_id_of(folder)
    if str(declared.get("id") or "") != agent_id:
        raise ValueError(f"{folder}/{AGENT_FILE} must declare id {agent_id!r} (its folder name)")
    modes = _strings(declared.get("modes")) or (DEFAULT_MODE,)
    unknown = [m for m in modes if m not in AGENT_MODES]
    if unknown:
        raise ValueError(f"agent {agent_id}: unknown modes {unknown}; known: {list(AGENT_MODES)}")
    workspace = _workspace(f"agent {agent_id}", declared.get("workspace"))
    return AgentSpec(
        id=agent_id,
        entry=f"{module}.agent:run",
        modes=modes,
        reads=_strings(declared.get("reads")),
        optional_reads=_strings(declared.get("optional_reads")),
        writes=_strings(declared.get("writes")),
        tools=_strings(declared.get("tools")),
        skills=_strings(declared.get("skills")),
        workspace=workspace,
        meta={k: v for k, v in declared.items() if k not in AGENT_KEYS},
        description=str(declared.get("description") or ""),
        folder=folder,
    )


@dataclass
class AgentRegistry:
    """The agents and skills of one feature package, found by their leaf folders."""

    package: str
    _agents: dict[str, AgentSpec] | None = None
    _skills: dict[str, SkillSpec] | None = None
    _duplicates: list[str] = field(default_factory=list)

    @property
    def root(self) -> Path:
        found = importlib.util.find_spec(self.package)
        if found is None or not found.submodule_search_locations:
            raise ValueError(f"agent package {self.package!r} is not importable")
        return Path(next(iter(found.submodule_search_locations)))

    def _scan(self) -> None:
        if self._agents is not None:
            return
        agents: dict[str, AgentSpec] = {}
        skills: dict[str, SkillSpec] = {}
        root = self.root
        for marker in sorted([*root.rglob(AGENT_FILE), *root.rglob(SKILL_FILE)]):
            folder = marker.parent
            relative = folder.relative_to(root)
            if any(part in SKIPPED_FOLDERS or part.startswith(".") for part in relative.parts):
                continue
            home = AGENTS_FOLDER if marker.name == AGENT_FILE else SKILLS_FOLDER_NAME
            if relative.parts[0] != home:
                self._duplicates.append(
                    f"{relative} is outside {home}/ (agents in agents/, skills in skills/)"
                )
                continue
            node_id = agent_id_of(folder)
            if node_id in agents or node_id in skills:
                self._duplicates.append(f"{node_id} is defined twice ({relative})")
                continue
            if marker.name == AGENT_FILE:
                module = ".".join((self.package, *relative.parts))
                agents[node_id] = load_agent(module, folder)
            else:
                skills[node_id] = load_skill(folder)
        # a skill with a contract is also an agent: the general agent spawned with it
        agents.update({s.id: s.as_agent() for s in skills.values() if s.is_agent})
        self._agents, self._skills = agents, skills

    def ids(self) -> list[str]:
        self._scan()
        return sorted(self._agents or {})

    def skill_ids(self) -> list[str]:
        self._scan()
        return sorted(self._skills or {})

    def get(self, agent_id: str) -> AgentSpec:
        self._scan()
        if agent_id not in (self._agents or {}):
            raise KeyError(f"{self.package} has no agent {agent_id!r}; agents: {self.ids()}")
        return self._agents[agent_id]  # type: ignore[index]

    def skill(self, skill_id: str) -> SkillSpec:
        self._scan()
        if skill_id not in (self._skills or {}):
            raise KeyError(f"{self.package} has no skill {skill_id!r}; skills: {self.skill_ids()}")
        return self._skills[skill_id]  # type: ignore[index]

    def problems(self) -> list[str]:
        """What is wrong with the declarations: a duplicate id, an unknown agent or skill, a call
        cycle, a read no agent writes (optional reads may be missing), a skill whose tools its
        agent does not declare."""
        self._scan()
        found: list[str] = list(self._duplicates)
        specs = dict(self._agents or {})
        skills = dict(self._skills or {})
        written = {t for spec in specs.values() for t in spec.writes or ()} | CORE_WRITES
        for spec in specs.values():
            found += [
                f"{spec.id} calls unknown agent {c}" for c in spec.agents_called() if c not in specs
            ]
            found += [
                f"{spec.id} reads {r}, which no agent writes"
                for r in spec.reads or ()
                if r not in written
            ]
            for skill_id in spec.skills:
                if skill_id not in skills:
                    found.append(f"{spec.id} uses unknown skill {skill_id}")
                    continue
                missing = [t for t in skills[skill_id].allowed_tools if not spec.may_call(t)]
                found += [f"{spec.id} uses skill {skill_id}, which needs {t}" for t in missing]

        def visit(agent_id: str, chain: tuple[str, ...]) -> None:
            if agent_id in chain:
                found.append("call cycle: " + " -> ".join((*chain, agent_id)))
                return
            for child in specs[agent_id].agents_called() if agent_id in specs else []:
                visit(child, (*chain, agent_id))

        for agent_id in specs:
            visit(agent_id, ())
        return sorted(set(found))


__all__ = [
    "AGENT_MODES",
    "AGENT_PREFIX",
    "ANY_TYPE",
    "DEFAULT_MODE",
    "SKILL_AGENT_ENTRY",
    "WORKSPACES",
    "AgentRegistry",
    "AgentSpec",
    "SkillSpec",
    "load_agent",
    "load_skill",
    "read_frontmatter",
]
