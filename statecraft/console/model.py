"""What the console shows, read from an engine: plain data, no web framework.

    registry(engine)            features -> agents (declarations, kind, state machine), skills
    graph(engine)               the static graph: who spawns whom, which artifact flows where
    runs(engine)                every root with its status counts
    run(engine, root)           a root's tree: each agent's view, state, step, waits, outputs
    agent(engine, agent_id)     one record, its history and its observed path of states
    files(engine, root, prefix) the root's workspace, read only

An agent's state machine is read from its class: its states, its start, and the transitions its
code names (`return "<state>"`, `then=`, `on_fault=`, read from the source; a state that
computes its next state's name shows no edge for it).
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from typing import Any

from ..agent import Agent
from ..agentic import AgenticAgent
from ..catalog import CORE
from ..engine import Engine
from ..process import Record
from ..spec import AGENT_PREFIX, SKILL_AGENT_ENTRY, AgentSpec
from ..transitions import view

#: Built-in agents every engine has.
BUILTINS = (f"{CORE}decider",)


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def machine(cls: type[Agent]) -> dict[str, Any]:
    """States, the start, and the transitions the code names (best effort, from the source)."""
    states = list(cls.agent_states())
    edges: list[dict[str, str]] = []
    for name, method in cls.agent_states().items():
        try:
            tree = ast.parse(textwrap.dedent(inspect.getsource(method)))
        except (OSError, TypeError, SyntaxError):
            continue
        found: set[tuple[str, str]] = set()
        for node in ast.walk(tree):
            value = getattr(node, "value", None)
            if isinstance(node, ast.Return) and isinstance(value, ast.Constant):
                if value.value in states:
                    found.add((str(value.value), "next"))
            elif isinstance(node, ast.Return) and isinstance(value, ast.Name):
                if value.id == "Done":
                    found.add(("(done)", "done"))
            elif (
                isinstance(node, ast.keyword)
                and node.arg in ("then", "on_fault")
                and isinstance(value, ast.Constant)
                and value.value in states
            ):
                found.add((str(value.value), "fault" if node.arg == "on_fault" else "wake"))
        edges += [{"from": name, "to": to, "kind": kind} for to, kind in sorted(found)]
    return {
        "start": getattr(cls, "start", None),
        "states": states,
        "on_term": getattr(cls, "on_term", None),
        "transitions": edges,
    }


def _kind(spec: AgentSpec | None, cls: type[Agent] | None) -> str:
    if spec is not None and spec.entry == SKILL_AGENT_ENTRY:
        return "skill"
    if cls is not None and issubclass(cls, AgenticAgent):
        return "agentic"
    return "state machine"


def describe_agent(engine: Engine, ref: str) -> dict[str, Any]:
    catalog = engine.services.catalog
    spec = catalog.spec(ref)
    entry: dict[str, Any] = {"ref": ref, "id": ref.partition(":")[2]}
    cls = None
    try:
        cls = catalog.agent_class(ref)
        entry["machine"] = machine(cls)
        entry["class"] = f"{cls.__module__}.{cls.__qualname__}"
    except Exception as exc:  # an agent that does not load is shown with why
        entry["load_error"] = f"{type(exc).__name__}: {exc}"
    entry["kind"] = _kind(spec, cls)
    if spec is not None:
        tools = list(spec.tools or ())
        entry.update(
            description=spec.description,
            modes=list(spec.modes),
            reads=list(spec.reads or ()),
            optional_reads=list(spec.optional_reads),
            writes=list(spec.writes or ()),
            calls=[t.removeprefix(AGENT_PREFIX) for t in tools if t.startswith(AGENT_PREFIX)],
            tools=[t for t in tools if not t.startswith(AGENT_PREFIX)],
            skills=list(spec.skills),
            workspace=spec.workspace,
            meta=_jsonable(spec.meta),
            folder=str(spec.folder or ""),
        )
    return entry


def registry(engine: Engine) -> dict[str, Any]:
    catalog = engine.services.catalog
    features = []
    for name, package in sorted(catalog.packages.items()):
        reg = catalog.registry(name)
        try:
            ids = reg.ids()
            problems = reg.problems()
        except Exception as exc:
            ids, problems = [], [f"{type(exc).__name__}: {exc}"]
        skills = []
        for skill_id in reg.skill_ids():
            skill = reg.skill(skill_id)
            skills.append(
                {
                    "id": skill.id,
                    "description": skill.description,
                    "tools": list(skill.allowed_tools),
                    "is_agent": skill.is_agent,
                }
            )
        features.append(
            {
                "name": name,
                "package": package,
                "agents": [describe_agent(engine, f"{name}:{i}") for i in ids],
                "skills": skills,
                "problems": problems,
            }
        )
    extra = [describe_agent(engine, ref) for ref in sorted(catalog.extra)]
    builtins = [describe_agent(engine, ref) for ref in BUILTINS]
    return {"features": features, "extra": extra, "builtins": builtins}


def graph(engine: Engine) -> dict[str, Any]:
    """Nodes: agents, tool servers and artifact types. Edges: spawns (`agent.<id>` declared),
    tool use (by server), writes and reads (by artifact type)."""
    found = registry(engine)
    agents = [a for f in found["features"] for a in f["agents"]] + found["extra"]
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, str]] = []
    for a in agents:
        nodes[a["ref"]] = {"id": a["ref"], "type": "agent", "kind": a["kind"], "label": a["id"]}
    for a in agents:
        feature = a["ref"].partition(":")[0]
        for called in a.get("calls", []):
            target = called if ":" in called else f"{feature}:{called}"
            nodes.setdefault(target, {"id": target, "type": "agent", "kind": "?", "label": called})
            edges.append({"from": a["ref"], "to": target, "kind": "spawns"})
        for server in sorted({t.partition(".")[0] for t in a.get("tools", [])}):
            sid = f"server:{server}"
            nodes.setdefault(sid, {"id": sid, "type": "server", "label": server})
            edges.append({"from": a["ref"], "to": sid, "kind": "uses"})
        for kind in a.get("writes", []):
            tid = f"type:{kind}"
            nodes.setdefault(tid, {"id": tid, "type": "artifact", "label": kind})
            edges.append({"from": a["ref"], "to": tid, "kind": "writes"})
        for kind in [*a.get("reads", []), *a.get("optional_reads", [])]:
            if kind == "*":
                continue
            tid = f"type:{kind}"
            nodes.setdefault(tid, {"id": tid, "type": "artifact", "label": kind})
            edges.append({"from": tid, "to": a["ref"], "kind": "reads"})
    return {"nodes": list(nodes.values()), "edges": edges}


def declared_servers(engine: Engine) -> list[dict[str, Any]]:
    """Tool servers as the declarations name them: each server's tools and who uses them."""
    found = registry(engine)
    servers: dict[str, dict[str, Any]] = {}
    for a in [x for f in found["features"] for x in f["agents"]] + found["extra"]:
        for tool in a.get("tools", []):
            server, _, name = tool.partition(".")
            entry = servers.setdefault(server, {"name": server, "tools": {}, "status": "declared"})
            entry["tools"].setdefault(name, {"name": name, "used_by": []})["used_by"].append(
                a["ref"]
            )
    return [
        {**s, "tools": sorted(s["tools"].values(), key=lambda t: t["name"])}
        for s in sorted(servers.values(), key=lambda s: s["name"])
    ]


def _row(record: Record, lease: Any, now: float) -> dict[str, Any]:
    return {
        "id": record.id,
        "parent": record.parent,
        "name": record.name,
        "agent": record.agent,
        "state": record.state,
        "step": record.step,
        "revision": record.revision,
        "status": record.status,
        "view": view(record, lease, now),
        "reasons": _jsonable(record.reasons),
        "error": record.error,
        "published": sorted(record.published),
        "children": dict(record.children),
        "workspace": record.workspace,
        "inherits": _jsonable(record.inherits),
    }


async def runs(engine: Engine) -> list[dict[str, Any]]:
    now = engine.services.clock()
    found = []
    for root in await engine.store.roots():
        tree = await engine.store.tree(root.id)
        counts: dict[str, int] = {}
        for record in tree:
            v = view(record, await engine.store.lease(record.id), now)
            counts[v] = counts.get(v, 0) + 1
        found.append(
            {
                **_row(root, await engine.store.lease(root.id), now),
                "agents": len(tree),
                "views": counts,
                "questions": sum(1 for r in tree for x in r.reasons if x.get("kind") == "answer"),
            }
        )
    return found


async def run(engine: Engine, root: str) -> dict[str, Any]:
    now = engine.services.clock()
    tree = await engine.store.tree(root)
    if not tree:
        raise KeyError(root)
    rows = [_row(r, await engine.store.lease(r.id), now) for r in tree]
    return {"root": root, "agents": rows, "questions": _jsonable(await engine.questions(root))}


async def agent(engine: Engine, agent_id: str) -> dict[str, Any]:
    record = await engine.store.load(agent_id)
    history = await engine.store.history(agent_id)
    path = [
        {
            "revision": h["revision"],
            "kind": h["kind"],
            "state": h["record"]["state"],
            "step": h["record"]["step"],
            "error": h["record"].get("error"),
        }
        for h in history
    ]
    found = record.to_dict() if hasattr(record, "to_dict") else dict(vars(record))
    found.pop("data", None)
    data = record.data
    return {
        "record": _jsonable(found),
        "data": _jsonable({k: v for k, v in data.items() if k != "messages"}),
        "messages": _jsonable(data.get("messages") or []),
        "path": path,
        "lease": _jsonable(vars(lease)) if (lease := await engine.store.lease(agent_id)) else None,
        "definition": describe_agent(engine, record.agent),
    }


async def files(engine: Engine, root: str, prefix: str = "") -> list[str]:
    await engine.services.prepare(root)
    return sorted(await engine.workspace(root).list(prefix))


async def read_file(engine: Engine, root: str, path: str, limit: int = 200_000) -> dict[str, Any]:
    await engine.services.prepare(root)
    body = await engine.workspace(root).get(path)
    try:
        text = body[:limit].decode("utf-8")
    except UnicodeDecodeError:
        return {"path": path, "bytes": len(body), "binary": True}
    return {"path": path, "bytes": len(body), "text": text, "truncated": len(body) > limit}


__all__ = [
    "agent",
    "declared_servers",
    "describe_agent",
    "files",
    "graph",
    "machine",
    "read_file",
    "registry",
    "run",
    "runs",
]
