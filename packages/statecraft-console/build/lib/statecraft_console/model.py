"""What the console shows, read from a `Runtime` through its public API: plain data.

    registry(runtime)            agents grouped by their name's prefix: contract, state machine
    graph(runtime)               who may spawn whom, which provider's tools, which artifacts flow
    servers(runtime)             providers with health and their tools, who may call each
    runs / run / agent / files   every root, a root's tree, one agent's record, data, path, files

An agent's state machine is read from its class: its states, its start, and the transitions its
code names (`return "<state>"`, `then=`, `on_fault=`; a state that computes its next state's name
shows no edge for it). The console knows no extension: what an agent is beyond its contract shows
as its class's bases and its `extensions` settings.
"""

from __future__ import annotations

import ast
import inspect
import json
import textwrap
from typing import Any

from statecraft import Agent


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def machine(cls: type) -> dict[str, Any]:
    if not (isinstance(cls, type) and issubclass(cls, Agent)):
        return {
            "start": getattr(cls, "start", None),
            "states": list(getattr(cls, "states", ())),
            "on_term": getattr(cls, "on_term", None),
            "transitions": [],
        }
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


def _kind(cls: type | None, definition: dict[str, Any]) -> str:
    if cls is None:
        return "unloaded"
    bases = [k.__name__ for k in cls.__mro__[1:] if k not in (object, Agent)]
    if bases:
        return bases[-1].removesuffix("Agent").lower() or "state machine"
    return "state machine"


def describe(runtime: Any, name: str) -> dict[str, Any]:
    d = runtime.registry.describe_agent(name)
    cls = runtime.registry.agents[name].cls
    return {
        "ref": name,
        "id": name.rpartition(":")[2],
        "kind": _kind(cls, d),
        "description": d["description"],
        "version": d["version"],
        "code": d["code"],
        "reads": d["reads"] or [],
        "writes": d["writes"] or [],
        "calls": d["spawns"],
        "tools": d["tools"],
        "skills": list((d["extensions"].get("agentic") or {}).get("skills") or []),
        "modes": [],
        "meta": _jsonable(
            {**d["meta"], **({"extensions": d["extensions"]} if d["extensions"] else {})}
        ),
        "class": d["entry"],
        "machine": machine(cls) if cls is not None else None,
        "input_schema": d["input_schema"],
    }


def registry(runtime: Any) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for name in sorted(runtime.registry.agents):
        prefix = name.rpartition(":")[0] or "(agents)"
        groups.setdefault(prefix, []).append(describe(runtime, name))
    return {
        "features": [
            {"name": prefix, "package": "", "agents": agents, "skills": [], "problems": []}
            for prefix, agents in groups.items()
        ],
        "extra": [],
        "builtins": [],
        "warnings": list(runtime.warnings),
        "lint": runtime.registry.lint(),
    }


def graph(runtime: Any) -> dict[str, Any]:
    reg = runtime.registry
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, str]] = []
    for name, d in reg.agents.items():
        nodes[name] = {"id": name, "type": "agent", "kind": _kind(d.cls, {}), "label": name}
    for name, d in reg.agents.items():
        for target in d.spawns:
            resolved = reg.resolve_spawn(d, target) if target != "*" else None
            if resolved is not None:
                edges.append({"from": name, "to": resolved.name, "kind": "spawns"})
        providers = sorted({e.provider for t, e in reg.effects.items() if d.permits_tool(t)})
        for provider in providers:
            sid = f"server:{provider}"
            nodes.setdefault(sid, {"id": sid, "type": "server", "label": provider})
            edges.append({"from": name, "to": sid, "kind": "uses"})
        for kind in d.writes or ():
            tid = f"type:{kind}"
            nodes.setdefault(tid, {"id": tid, "type": "artifact", "label": kind})
            edges.append({"from": name, "to": tid, "kind": "writes"})
        for kind in [*(d.reads or ()), *d.optional_reads]:
            if kind != "*":
                tid = f"type:{kind}"
                nodes.setdefault(tid, {"id": tid, "type": "artifact", "label": kind})
                edges.append({"from": tid, "to": name, "kind": "reads"})
    return {"nodes": list(nodes.values()), "edges": edges}


def servers(runtime: Any) -> list[dict[str, Any]]:
    reg = runtime.registry
    found = []
    for provider in runtime.providers():
        tools = []
        for tool in provider["tools"]:
            e = reg.effects[tool]
            tools.append(
                {
                    "name": tool,
                    "description": e.description,
                    "policy": e.policy.kind,
                    "used_by": sorted(n for n, d in reg.agents.items() if d.permits_tool(tool)),
                }
            )
        found.append(
            {
                "name": provider["name"],
                "url": provider["kind"],
                "status": provider["status"],
                "tools": tools,
                "concurrency": provider["concurrency"],
            }
        )
    return found


async def run(runtime: Any, root: str) -> dict[str, Any]:
    rows = await runtime.tree(root)
    if not rows:
        raise KeyError(root)
    return {
        "root": root,
        "agents": _jsonable(rows),
        "questions": _jsonable(await runtime.questions(root)),
    }


async def agent(runtime: Any, agent_id: str) -> dict[str, Any]:
    got = await runtime.get(agent_id)
    history = await runtime.history(agent_id)
    path = [
        {
            "revision": h["revision"],
            "kind": h["kind"],
            "state": h["record"]["state"],
            "step": h["record"]["step"],
            "error": h["record"].get("error"),
            "crossings": h.get("crossings", []),
        }
        for h in history
    ]
    data = dict(got.pop("data") or {})
    messages = data.pop("messages", [])
    definition = None
    name = (got.get("definition") or {}).get("name") or got["agent"]
    if name in runtime.registry.agents:
        definition = describe(runtime, name)
    record = await runtime.store.load(agent_id)
    return {
        "record": _jsonable(got),
        "data": _jsonable(data),
        "messages": _jsonable(messages),
        "path": _jsonable(path),
        "lease": got.get("lease"),
        "definition": definition,
        # lifecycle and diagnostics (D3): what is pending, what never arrived, what is unknown
        "intents": _jsonable([i for i in record.intents if not i.get("done")]),
        "undelivered": _jsonable(record.undelivered),
        "joins": _jsonable([r for r in record.reasons if r.get("kind") == "join"]),
        "calls": _jsonable(await runtime.calls(agent_id)),
        "attempts": _jsonable(await runtime.attempts(agent_id)),
    }


async def files(runtime: Any, root: str, prefix: str = "") -> list[str]:
    return await runtime.files(root, prefix)


async def read_file(runtime: Any, root: str, path: str, limit: int = 200_000) -> dict[str, Any]:
    body = await runtime.read_file(root, path)
    try:
        text = body[:limit].decode("utf-8")
    except UnicodeDecodeError:
        return {"path": path, "bytes": len(body), "binary": True}
    return {"path": path, "bytes": len(body), "text": text, "truncated": len(body) > limit}


__all__ = [
    "agent",
    "describe",
    "files",
    "graph",
    "machine",
    "read_file",
    "registry",
    "run",
    "servers",
]
