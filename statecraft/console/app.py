"""The console's HTTP app: the engine's state as JSON, its events as server-sent events, and the
page that shows them. Needs the `console` extra (`pip install "statecraft[console]"`).

    app = console_app(engine, servers=None, title="statecraft", executors=0)

    GET  /                                  the console page
    GET  /api/overview                      title, counts, servers' names
    GET  /api/registry                      features, agents (declarations, state machines), skills
    GET  /api/graph                         spawns, tool servers, artifact flow
    GET  /api/servers                       tool servers: the application's live view, else the
                                            declarations' (`servers`: async () -> list of
                                            {name, url?, status, tools: [{name, description?}]})
    GET  /api/runs                          every root with its status counts
    GET  /api/runs/{root}                   its tree, each agent's view, its open questions
    GET  /api/agents/{id}                   one record: data, conversation, path of states
    GET  /api/runs/{root}/files?prefix=     the root's workspace files
    GET  /api/runs/{root}/file?path=        one file (text, capped)
    GET  /api/runs/{root}/events?since=     server-sent events, resumable by `Last-Event-ID`
    GET  /api/events?since=                 every root's events (the overview's live feed)
    POST /api/runs                          {"agent", "input"?, "root"?} spawn a root
    POST /api/answers                       {"question_id", "answer"}
    POST /api/signals                       {"agent", "signal", "message"?}

The console records requests and reads the store; executors run elsewhere, or beside it when
`executors` > 0.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, Response, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from ..engine import Engine
from ..transitions import InvalidAnswer
from . import model

STATIC = Path(__file__).parent / "static"
POLL_SECONDS = 1.0
Servers = Callable[[], Awaitable[list[dict[str, Any]]]]


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json(value: Any, status: int = 200) -> Response:
    return Response(_dumps(value), status_code=status, media_type="application/json")


def _sse(event: dict[str, Any]) -> str:
    """One event as a plain message (its kind is in the data), so a page gets every kind."""
    return f"id: {event['seq']}\ndata: {_dumps(event)}\n\n"


def console_app(
    engine: Engine,
    *,
    servers: Servers | None = None,
    title: str = "statecraft",
    executors: int = 0,
) -> Starlette:
    async def page(request: Request) -> Response:
        return FileResponse(STATIC / "index.html")

    async def overview(request: Request) -> Response:
        reg = model.registry(engine)
        roots = await model.runs(engine)
        known = await listed_servers()
        return _json(
            {
                "title": title,
                "features": len(reg["features"]),
                "agents": sum(len(f["agents"]) for f in reg["features"]) + len(reg["extra"]),
                "runs": len(roots),
                "active": sum(1 for r in roots if r["status"] == "active"),
                "questions": sum(r["questions"] for r in roots),
                "servers": [s["name"] for s in known],
            }
        )

    async def listed_servers() -> list[dict[str, Any]]:
        declared = {s["name"]: s for s in model.declared_servers(engine)}
        if servers is None:
            return list(declared.values())
        try:
            live = await servers()
        except Exception as exc:
            return [{**s, "status": f"unreachable: {exc}"} for s in declared.values()]
        merged = []
        for entry in live:
            uses = {
                t["name"]: t["used_by"] for t in declared.get(entry["name"], {}).get("tools", [])
            }
            tools = [{**t, "used_by": uses.get(t["name"], [])} for t in entry.get("tools", [])]
            merged.append({**entry, "tools": tools})
        merged += [
            {**s, "status": "declared, not connected"}
            for name, s in declared.items()
            if name not in {e["name"] for e in live}
        ]
        return sorted(merged, key=lambda s: s["name"])

    async def registry(request: Request) -> Response:
        return _json(model.registry(engine))

    async def graph(request: Request) -> Response:
        return _json(model.graph(engine))

    async def server_list(request: Request) -> Response:
        return _json(await listed_servers())

    async def run_list(request: Request) -> Response:
        return _json(await model.runs(engine))

    async def run(request: Request) -> Response:
        try:
            return _json(await model.run(engine, request.path_params["root"]))
        except KeyError:
            return _json({"error": "no such run"}, 404)

    async def agent(request: Request) -> Response:
        try:
            return _json(await model.agent(engine, request.path_params["agent"]))
        except KeyError:
            return _json({"error": "no such agent"}, 404)

    async def files(request: Request) -> Response:
        prefix = request.query_params.get("prefix", "")
        return _json(await model.files(engine, request.path_params["root"], prefix))

    async def file(request: Request) -> Response:
        try:
            found = await model.read_file(
                engine, request.path_params["root"], request.query_params["path"]
            )
        except (KeyError, FileNotFoundError):
            return _json({"error": "no such file"}, 404)
        return _json(found)

    def stream(roots: Callable[[], Awaitable[list[str]]], since: int) -> StreamingResponse:
        async def events() -> AsyncIterator[str]:
            seen: dict[str, int] = {}
            first = True
            while True:
                sent = False
                for root in await roots():
                    after = seen.get(root, since if first else 0)
                    for event in await engine.store.read_events(root, after):
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
        since = int(request.query_params.get("since") or request.headers.get("last-event-id") or 0)

        async def one() -> list[str]:
            return [root]

        return stream(one, since)

    async def all_events(request: Request) -> Response:
        async def every() -> list[str]:
            return [r.id for r in await engine.store.roots()]

        return stream(every, int(request.query_params.get("since") or 0))

    async def spawn(request: Request) -> Response:
        body = await request.json()
        try:
            root = await engine.spawn(
                str(body["agent"]), dict(body.get("input") or {}), root_id=body.get("root")
            )
        except (KeyError, ValueError) as exc:
            return _json({"error": str(exc)}, 400)
        return _json({"root": root}, 201)

    async def answer(request: Request) -> Response:
        body = await request.json()
        try:
            await engine.answer(str(body["question_id"]), body.get("answer"))
        except (InvalidAnswer, KeyError) as exc:
            return _json({"error": str(exc)}, 409)
        return Response(status_code=204)

    async def signal(request: Request) -> Response:
        body = await request.json()
        sent: Any = body["signal"]
        if sent == "USR":
            sent = ("USR", body.get("message"))
        try:
            await engine.signal(str(body["agent"]), sent)
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        return Response(status_code=204)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        stop = asyncio.Event()
        tasks = [asyncio.create_task(engine.executor().run(stop)) for _ in range(executors)]
        try:
            yield
        finally:
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)

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
            Route("/api/agents/{agent:path}", agent),
            Route("/api/answers", answer, methods=["POST"]),
            Route("/api/signals", signal, methods=["POST"]),
            Mount("/static", StaticFiles(directory=STATIC), name="static"),
        ],
        lifespan=lifespan,
    )


__all__ = ["Servers", "console_app"]
