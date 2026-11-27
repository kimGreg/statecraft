"""The HTTP API as an OpenAPI 3.1 document (roadmap R1), served at `GET /openapi.json`.

What must hold: every route the server answers is in this document with the same methods, and
nothing else is (`tests/test_public_api.py` compares them). Bodies are described as far as the
routes check them; records, events and artifacts are the runtime's JSON (`additionalProperties`).
"""

from __future__ import annotations

from typing import Any

VERSION = "1"

ANY: dict[str, Any] = {"type": "object", "additionalProperties": True}
LIST: dict[str, Any] = {"type": "array", "items": ANY}
ERROR = {
    "description": "refused: 400 bad input, 401 no token, 403 not allowed, 404 unknown, "
    "409 ended or closed, 415 not JSON",
    "content": {
        "application/json": {
            "schema": {"type": "object", "properties": {"error": {"type": "string"}}}
        }
    },
}
SCOPE = "x-statecraft-scope"


def _json(schema: dict[str, Any], description: str = "ok") -> dict[str, Any]:
    return {"description": description, "content": {"application/json": {"schema": schema}}}


def _op(
    summary: str,
    scope: str,
    *,
    body: dict[str, Any] | None = None,
    ok: dict[str, Any] | None = None,
    params: list[dict[str, Any]] | None = None,
    status: str = "200",
) -> dict[str, Any]:
    op: dict[str, Any] = {
        "summary": summary,
        SCOPE: scope,
        "responses": {status: ok or _json(ANY), "4XX": ERROR},
    }
    if body is not None:
        op["requestBody"] = {"required": True, "content": {"application/json": {"schema": body}}}
    if params:
        op["parameters"] = params
    return op


def _path(name: str) -> dict[str, Any]:
    return {"name": name, "in": "path", "required": True, "schema": {"type": "string"}}


def _query(name: str, kind: str = "string") -> dict[str, Any]:
    return {"name": name, "in": "query", "required": False, "schema": {"type": kind}}


AGENT = [_path("id")]
SIGNAL = {
    "type": "object",
    "required": ["signal"],
    "properties": {
        "signal": {"enum": ["STOP", "CONT", "TERM", "KILL", "USR"]},
        "message": {},
    },
}
SPAWN = {
    "type": "object",
    "required": ["agent"],
    "properties": {
        "agent": {"type": "string"},
        "input": {"type": "object"},
        "root": {"type": "string"},
        "attach": {"type": "string"},
        "attach_mode": {"enum": ["ro", "rw"]},
        "inherit": {"type": "object"},
        "budget": {"type": "object", "additionalProperties": {"type": "number"}},
    },
}

PATHS: dict[str, dict[str, Any]] = {
    "/agents": {
        "get": _op(
            "Every run (root) the caller may see, with status counts", "read", ok=_json(LIST)
        ),
        "post": _op(
            "Spawn a run",
            "control",
            body=SPAWN,
            status="201",
            ok=_json({"type": "object", "properties": {"root": {"type": "string"}}}, "spawned"),
        ),
    },
    "/agents/{id}": {
        "get": _op(
            "An agent's record (?tree=1: its root's tree)", "read", params=[*AGENT, _query("tree")]
        )
    },
    "/agents/{id}/history": {
        "get": _op("Committed changes with their calls", "read", params=AGENT, ok=_json(LIST))
    },
    "/agents/{id}/events": {
        "get": {
            **_op(
                "Server-sent events from the run's durable log",
                "read",
                params=[*AGENT, _query("since", "integer"), _query("follow")],
            ),
            "responses": {"200": {"description": "text/event-stream"}, "4XX": ERROR},
        }
    },
    "/agents/{id}/signals": {
        "post": _op(
            "Send a signal",
            "control",
            body=SIGNAL,
            params=AGENT,
            status="204",
            ok={"description": "sent"},
        )
    },
    "/agents/{id}/repin": {
        "post": _op("Accept the current definition and tools", "control", params=AGENT)
    },
    "/agents/{id}/artifacts/{kind}": {
        "get": _op("A published artifact", "read", params=[*AGENT, _path("kind"), _query("by")])
    },
    "/agents/{id}/files": {
        "get": _op(
            "Workspace files",
            "read",
            params=[*AGENT, _query("prefix")],
            ok=_json({"type": "array", "items": {"type": "string"}}),
        )
    },
    "/agents/{id}/file": {
        "get": {
            **_op("One workspace file", "read", params=[*AGENT, _query("path")]),
            "responses": {"200": {"description": "application/octet-stream"}, "4XX": ERROR},
        }
    },
    "/agents/{id}/calls": {
        "get": _op("The agent's journaled calls", "read", params=AGENT, ok=_json(LIST))
    },
    "/agents/{id}/attempts": {
        "get": _op(
            "Attempts that did not commit, with their calls", "read", params=AGENT, ok=_json(LIST)
        )
    },
    "/agents/{id}/budget": {
        "get": _op("The tree's budget: limit, spent, held", "read", params=AGENT),
        "post": _op(
            "Set the tree's budget limits",
            "admin",
            body={"type": "object", "properties": {"limit": {"type": ["object", "null"]}}},
            params=AGENT,
        ),
    },
    "/questions": {"get": _op("Open questions", "read", params=[_query("root")], ok=_json(LIST))},
    "/questions/{id}/answer": {
        "post": _op(
            "Answer a question",
            "control",
            body={"type": "object", "properties": {"answer": {}, "by": {"type": "string"}}},
            params=[_path("id")],
            status="204",
            ok={"description": "answered"},
        )
    },
    "/calls/{key}/resolve": {
        "post": _op(
            "Settle a call whose outcome is unknown",
            "admin",
            body={
                "type": "object",
                "required": ["outcome"],
                "properties": {
                    "outcome": {"enum": ["completed", "failed", "replay"]},
                    "result": {},
                },
            },
            params=[_path("key")],
        )
    },
    "/faults": {
        "get": _op("Agents suspended by a fault, with their calls", "read", ok=_json(LIST))
    },
    "/metrics": {
        "get": {
            **_op(
                "Counts (Prometheus text; ?format=json: JSON)", "read", params=[_query("format")]
            ),
            "responses": {"200": {"description": "text/plain or JSON"}, "4XX": ERROR},
        }
    },
    "/maintenance": {
        "get": _op("Whether maintenance mode is on", "read"),
        "post": _op(
            "Turn maintenance mode on or off",
            "admin",
            body={"type": "object", "required": ["on"], "properties": {"on": {"type": "boolean"}}},
        ),
    },
    "/health": {"get": _op("Liveness (served without a token)", "none")},
    "/registry/agents": {"get": _op("Registered agents", "read", ok=_json(LIST))},
    "/registry/tools": {"get": _op("Registered tools", "read", ok=_json(LIST))},
    "/registry/providers": {"get": _op("Providers and their status", "read", ok=_json(LIST))},
    "/openapi.json": {"get": _op("This document", "read")},
}


def document() -> dict[str, Any]:
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "statecraft",
            "version": VERSION,
            "description": "The statecraft runtime API. Ids contain `/`. Authentication: "
            "`Authorization: Bearer <token>`; each operation's `x-statecraft-scope` is the "
            "scope it needs (read < control < admin).",
        },
        "components": {
            "securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}},
        },
        "security": [{"bearer": []}],
        "paths": PATHS,
    }


__all__ = ["PATHS", "VERSION", "document"]
