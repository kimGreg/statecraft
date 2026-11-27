"""The runtime's HTTP API (architecture §11.1). Agent and question ids contain `/`.

GET  /agents                                every root with its status counts
POST /agents                                {"agent", "input"?, "root"?, "attach"?,
                                             "attach_mode"?, "inherit"?} -> {"root"}
GET  /agents/{id}                           the record (?tree=1: the root's tree)
GET  /agents/{id}/history                   committed changes, with each step's tool calls
GET  /agents/{root}/events?since=           server-sent events from the durable log
POST /agents/{id}/signals                   {"signal": STOP|CONT|TERM|KILL|USR, "message"?}
POST /agents/{id}/repin                     accept the current definition and tools
GET  /agents/{root}/artifacts/{kind}?by=    a published artifact
GET  /agents/{root}/files?prefix=           workspace files; /file?path= one of them
GET  /questions                             open questions
POST /questions/{id}/answer                 {"answer", "by"?}
GET  /registry/agents | /registry/tools | /registry/providers
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from ..kernel.transitions import InvalidAnswer
from ..runtime.runtime import Runtime

KEEPALIVE_SECONDS = 15


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json(value: Any, status: int = 200) -> Response:
    return Response(_dumps(value), status_code=status, media_type="application/json")


def server_app(runtime: Runtime, *, manage: bool = False) -> Starlette:
    """`manage`: start the runtime with the app and close it on shutdown."""

    async def runs(request: Request) -> Response:
        return _json(await runtime.runs())

    async def spawn(request: Request) -> Response:
        body = await request.json()
        try:
            root = await runtime.spawn(
                str(body["agent"]),
                dict(body.get("input") or {}),
                root_id=body.get("root"),
                attach=body.get("attach"),
                attach_mode=body.get("attach_mode", "ro"),
                inherit=body.get("inherit"),
            )
        except (KeyError, ValueError) as exc:
            return _json({"error": str(exc)}, 400)
        return _json({"root": root}, 201)

    async def get(request: Request) -> Response:
        agent_id = request.path_params["id"]
        try:
            if request.query_params.get("tree"):
                rows = await runtime.tree(agent_id)
                return _json(rows) if rows else _json({"error": "no such root"}, 404)
            return _json(await runtime.get(agent_id))
        except KeyError:
            return _json({"error": "no such agent"}, 404)

    async def history(request: Request) -> Response:
        return _json(await runtime.history(request.path_params["id"]))

    async def events(request: Request) -> Response:
        root = request.path_params["id"]
        since = int(request.query_params.get("since") or request.headers.get("last-event-id") or 0)
        follow = request.query_params.get("follow", "1") != "0"

        async def stream() -> AsyncIterator[str]:
            source = runtime.events(root, since=since, follow=follow, poll=0.5)
            pending: asyncio.Task[Any] | None = None
            try:
                while True:
                    pending = pending or asyncio.ensure_future(anext(source))
                    done, _ = await asyncio.wait({pending}, timeout=KEEPALIVE_SECONDS)
                    if not done:
                        yield ": keep-alive\n\n"
                        continue
                    try:
                        event = pending.result()
                    except StopAsyncIteration:
                        return
                    pending = None
                    yield f"id: {event['seq']}\ndata: {_dumps(event)}\n\n"
            finally:
                if pending is not None:
                    pending.cancel()
                await source.aclose()

        return StreamingResponse(
            stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        )

    async def signal(request: Request) -> Response:
        body = await request.json()
        sent: Any = body["signal"]
        if sent == "USR":
            sent = ("USR", body.get("message"))
        try:
            await runtime.signal(request.path_params["id"], sent)
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        return Response(status_code=204)

    async def repin(request: Request) -> Response:
        try:
            return _json(await runtime.repin(request.path_params["id"]))
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)

    async def artifact(request: Request) -> Response:
        try:
            return _json(
                await runtime.artifact(
                    request.path_params["id"],
                    request.path_params["kind"],
                    request.query_params.get("by"),
                )
            )
        except LookupError as exc:
            return _json({"error": str(exc)}, 404)

    async def files(request: Request) -> Response:
        return _json(
            await runtime.files(request.path_params["id"], request.query_params.get("prefix", ""))
        )

    async def file(request: Request) -> Response:
        try:
            body = await runtime.read_file(request.path_params["id"], request.query_params["path"])
        except (KeyError, FileNotFoundError, ValueError):
            return _json({"error": "no such file"}, 404)
        return Response(body, media_type="application/octet-stream")

    async def questions(request: Request) -> Response:
        return _json(await runtime.questions(request.query_params.get("root")))

    async def answer(request: Request) -> Response:
        body = await request.json()
        try:
            await runtime.answer(
                request.path_params["id"], body.get("answer"), by=str(body.get("by") or "person")
            )
        except (InvalidAnswer, KeyError) as exc:
            return _json({"error": str(exc)}, 409)
        return Response(status_code=204)

    async def agents(request: Request) -> Response:
        return _json(runtime.agents())

    async def tools(request: Request) -> Response:
        return _json(runtime.tools())

    async def providers(request: Request) -> Response:
        return _json(runtime.providers())

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        if manage:
            await runtime.start()
        try:
            yield
        finally:
            if manage:
                await runtime.close()

    return Starlette(
        routes=[
            Route("/agents", runs),
            Route("/agents", spawn, methods=["POST"]),
            Route("/agents/{id:path}/history", history),
            Route("/agents/{id:path}/events", events),
            Route("/agents/{id:path}/signals", signal, methods=["POST"]),
            Route("/agents/{id:path}/repin", repin, methods=["POST"]),
            Route("/agents/{id:path}/artifacts/{kind}", artifact),
            Route("/agents/{id:path}/files", files),
            Route("/agents/{id:path}/file", file),
            Route("/agents/{id:path}", get),
            Route("/questions", questions),
            Route("/questions/{id:path}/answer", answer, methods=["POST"]),
            Route("/registry/agents", agents),
            Route("/registry/tools", tools),
            Route("/registry/providers", providers),
        ],
        lifespan=lifespan,
    )


__all__ = ["server_app"]
