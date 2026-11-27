"""The console's HTTP app over a `Runtime` (the runtime API only).

app = console_app(runtime, title="statecraft")

GET  /                                  the console page
GET  /api/overview                      title, counts, providers' names
GET  /api/registry                      agents (contract, state machine), warnings, lint
GET  /api/graph                         spawns, providers, artifact flow
GET  /api/servers                       providers with health and tools, who may call each
GET  /api/runs                          every root with its status counts
GET  /api/runs/{root}                   its tree and open questions
GET  /api/agents/{id}                   one record: data, conversation, path, crossings
GET  /api/runs/{root}/files?prefix=     the root's workspace files
GET  /api/runs/{root}/file?path=        one file (text, capped)
GET  /api/runs/{root}/events?since=     server-sent events from the durable log, resumable
GET  /api/events?since=                 every root's events
POST /api/runs                          {"agent", "input"?, "root"?} spawn a root
POST /api/answers                       {"question_id", "answer"}
POST /api/signals                       {"agent", "signal", "message"?}
POST /api/agents/{id}/repin             accept the current definition and tools
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import FileResponse, Response, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from statecraft.kernel.store import Ended
from statecraft.kernel.transitions import InvalidAnswer
from statecraft.providers.workspaces import Escape
from statecraft.server.app import BadRequest, optional_field, read_body, refused, since_of
from statecraft.server.guard import Guard

from . import model

STATIC = Path(__file__).parent / "static"
POLL_SECONDS = 1.0


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json(value: Any, status: int = 200) -> Response:
    return Response(_dumps(value), status_code=status, media_type="application/json")


def _sse(event: dict[str, Any]) -> str:
    """One event as a plain message (its kind is in the data), so a page gets every kind."""
    return f"id: {event['seq']}\ndata: {_dumps(event)}\n\n"


def console_app(
    runtime: Any,
    *,
    title: str = "statecraft",
    token: str | None = None,
    hosts: list[str] | tuple[str, ...] = (),
) -> Starlette:
    """Every request passes the runtime's HTTP guard (`statecraft.server.guard`); with a token,
    open `/?token=<token>` once and the page keeps it as a same-site cookie."""

    async def page(request: Request) -> Response:
        return FileResponse(STATIC / "index.html")

    async def overview(request: Request) -> Response:
        roots = await runtime.runs()
        return _json(
            {
                "title": title,
                "features": len({n.rpartition(":")[0] for n in runtime.registry.agents}),
                "agents": len(runtime.registry.agents),
                "runs": len(roots),
                "active": sum(1 for r in roots if r["status"] == "active"),
                "questions": sum(r["questions"] for r in roots),
                "servers": [p["name"] for p in runtime.providers()],
            }
        )

    async def registry(request: Request) -> Response:
        return _json(model.registry(runtime))

    async def graph(request: Request) -> Response:
        return _json(model.graph(runtime))

    async def server_list(request: Request) -> Response:
        return _json(model.servers(runtime))

    async def run_list(request: Request) -> Response:
        return _json(await runtime.runs())

    async def run(request: Request) -> Response:
        try:
            return _json(await model.run(runtime, request.path_params["root"]))
        except KeyError:
            return _json({"error": "no such run"}, 404)

    async def agent(request: Request) -> Response:
        try:
            return _json(await model.agent(runtime, request.path_params["agent"]))
        except KeyError:
            return _json({"error": "no such agent"}, 404)

    async def files(request: Request) -> Response:
        prefix = request.query_params.get("prefix", "")
        return _json(await model.files(runtime, request.path_params["root"], prefix))

    async def file(request: Request) -> Response:
        try:
            found = await model.read_file(
                runtime, request.path_params["root"], request.query_params["path"]
            )
        except Escape as exc:
            return _json({"error": str(exc)}, 403)
        except (KeyError, FileNotFoundError, IsADirectoryError):
            return _json({"error": "no such file"}, 404)
        except (ValueError, OSError) as exc:
            return _json({"error": str(exc)}, 400)
        return _json(found)

    def stream(roots: Callable[[], Awaitable[list[str]]], since: int) -> StreamingResponse:
        async def events() -> AsyncIterator[str]:
            seen: dict[str, int] = {}
            first = True
            while True:
                sent = False
                for root in await roots():
                    after = seen.get(root, since if first else 0)
                    for event in await runtime.store.read_events(root, after):
                        seen[root] = event["seq"]
                        yield _sse(event)
                        sent = True
                first = False
                if not sent:  # the durable log is the source; polling it is enough
                    yield ": keep-alive\n\n"
                    await asyncio.sleep(POLL_SECONDS)

        return StreamingResponse(
            events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        )

    async def run_events(request: Request) -> Response:
        root = request.path_params["root"]
        since = since_of(request)

        async def one() -> list[str]:
            return [root]

        return stream(one, since)

    async def all_events(request: Request) -> Response:
        async def every() -> list[str]:
            return [r.id for r in await runtime.store.roots()]

        return stream(every, since_of(request))

    async def spawn(request: Request) -> Response:
        body = await read_body(request)
        agent = body.get("agent")
        if not isinstance(agent, str) or not agent:
            raise BadRequest("'agent' must name an agent")
        try:
            root = await runtime.spawn(
                agent,
                optional_field(body, "input", dict) or {},
                root_id=optional_field(body, "root", str),
            )
        except (KeyError, ValueError, TypeError) as exc:
            return _json({"error": str(exc)}, 400)
        return _json({"root": root}, 201)

    async def answer(request: Request) -> Response:
        body = await read_body(request)
        question = body.get("question_id")
        if not isinstance(question, str):
            raise BadRequest("'question_id' must name a question")
        try:
            await runtime.answer(question, body.get("answer"))
        except (InvalidAnswer, KeyError, Ended) as exc:
            return _json({"error": str(exc)}, 409)
        except ValueError as exc:  # an answer that cannot be stored (not JSON)
            return _json({"error": str(exc)}, 400)
        return Response(status_code=204)

    async def signal(request: Request) -> Response:
        body = await read_body(request)
        agent = body.get("agent")
        if not isinstance(agent, str) or "signal" not in body:
            raise BadRequest('expected {"agent", "signal", "message"?}')
        sent: Any = body["signal"]
        if sent == "USR":
            sent = ("USR", body.get("message"))
        try:
            await runtime.signal(agent, sent)
        except ValueError as exc:
            return _json({"error": str(exc)}, 400)
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        except Ended as exc:
            return _json({"error": str(exc)}, 409)
        return Response(status_code=204)

    async def repin(request: Request) -> Response:
        try:
            return _json(await runtime.repin(request.path_params["agent"]))
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        except Ended as exc:
            return _json({"error": str(exc)}, 409)

    return Starlette(
        routes=[
            Route("/", page),
            Route("/api/overview", overview),
            Route("/api/registry", registry),
            Route("/api/graph", graph),
            Route("/api/servers", server_list),
            Route("/api/runs", run_list),
            Route("/api/runs", spawn, methods=["POST"]),
            Route("/api/runs/{root}", run),
            Route("/api/runs/{root}/files", files),
            Route("/api/runs/{root}/file", file),
            Route("/api/runs/{root}/events", run_events),
            Route("/api/events", all_events),
            Route("/api/agents/{agent:path}/repin", repin, methods=["POST"]),
            Route("/api/agents/{agent:path}", agent),
            Route("/api/answers", answer, methods=["POST"]),
            Route("/api/signals", signal, methods=["POST"]),
            Mount("/static", StaticFiles(directory=STATIC), name="static"),
        ],
        middleware=[Middleware(Guard, token=token, hosts=tuple(hosts), cookie=True)],
        exception_handlers={BadRequest: refused},
    )


__all__ = ["console_app"]
