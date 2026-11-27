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

Every request passes the guard (`guard.py`): same-origin, JSON bodies, loopback names unless a
token is given. Bad input is a 4xx with `{"error"}`: 400 for a body or parameter that is not what
the route takes, 403 for a path that leaves the workspace, 404 for an unknown agent or file, 409
for a change to an agent that has ended or a question that closed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from ..kernel.store import Ended
from ..kernel.transitions import InvalidAnswer
from ..providers.workspaces import Escape
from ..runtime.runtime import Runtime
from .guard import Guard

KEEPALIVE_SECONDS = 15


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json(value: Any, status: int = 200) -> Response:
    return Response(_dumps(value), status_code=status, media_type="application/json")


class BadRequest(ValueError):
    """A body or parameter that is not what the route takes."""


async def read_body(request: Request) -> dict[str, Any]:
    try:
        body = json.loads(await request.body(), parse_constant=_no_constant)
    except ValueError:
        raise BadRequest("the body is not JSON (NaN and Infinity are not JSON)") from None
    if not isinstance(body, dict):
        raise BadRequest("the body must be a JSON object")
    return body


def _no_constant(name: str) -> Any:
    raise ValueError(f"{name} is not JSON")


def optional_field(body: dict[str, Any], key: str, kind: type, default: Any = None) -> Any:
    value = body.get(key, default)
    if value is not None and not isinstance(value, kind):
        raise BadRequest(f"{key!r} must be {kind.__name__}, not {type(value).__name__}")
    return value


def since_of(request: Request) -> int:
    raw = request.query_params.get("since") or request.headers.get("last-event-id") or "0"
    try:
        since = int(raw)
    except ValueError:
        raise BadRequest(f"since must be a whole number, not {raw!r}") from None
    if since < 0:
        raise BadRequest("since must not be negative")
    return since


async def refused(request: Request, exc: Exception) -> Response:
    return _json({"error": str(exc)}, 400)


def server_app(
    runtime: Runtime,
    *,
    manage: bool = False,
    token: str | None = None,
    hosts: list[str] | tuple[str, ...] = (),
) -> Starlette:
    """`manage`: start the runtime with the app and close it on shutdown. `token`: every request
    must carry it; `hosts`: names served besides loopback when there is no token."""

    async def runs(request: Request) -> Response:
        return _json(await runtime.runs())

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
                attach=optional_field(body, "attach", str),
                attach_mode=optional_field(body, "attach_mode", str, "ro"),
                inherit=optional_field(body, "inherit", dict),
            )
        except (KeyError, ValueError, TypeError) as exc:
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
        agent_id = request.path_params["id"]
        try:
            await runtime.store.load(agent_id)
        except KeyError:
            return _json({"error": "no such agent"}, 404)
        return _json(await runtime.history(agent_id))

    async def events(request: Request) -> Response:
        root = request.path_params["id"]
        since = since_of(request)
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
        body = await read_body(request)
        if "signal" not in body:
            return _json({"error": 'expected {"signal": ..., "message"?: ...}'}, 400)
        sent: Any = body["signal"]
        if sent == "USR":
            sent = ("USR", body.get("message"))
        try:
            await runtime.signal(request.path_params["id"], sent)
        except ValueError as exc:
            return _json({"error": str(exc)}, 400)
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        except Ended as exc:
            return _json({"error": str(exc)}, 409)
        return Response(status_code=204)

    async def repin(request: Request) -> Response:
        try:
            return _json(await runtime.repin(request.path_params["id"]))
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        except Ended as exc:
            return _json({"error": str(exc)}, 409)

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
        prefix = request.query_params.get("prefix", "")
        try:
            return _json(await runtime.files(request.path_params["id"], prefix))
        except Escape as exc:
            return _json({"error": str(exc)}, 403)
        except KeyError:
            return _json({"error": "no such root"}, 404)
        except (ValueError, OSError) as exc:
            return _json({"error": str(exc)}, 400)

    async def file(request: Request) -> Response:
        path = request.query_params.get("path")
        if not path:
            raise BadRequest("give ?path=")
        try:
            body = await runtime.read_file(request.path_params["id"], path)
        except Escape as exc:
            return _json({"error": str(exc)}, 403)
        except (KeyError, FileNotFoundError, IsADirectoryError):
            return _json({"error": "no such file"}, 404)
        except (ValueError, OSError) as exc:
            return _json({"error": str(exc)}, 400)
        return Response(body, media_type="application/octet-stream")

    async def questions(request: Request) -> Response:
        return _json(await runtime.questions(request.query_params.get("root")))

    async def answer(request: Request) -> Response:
        body = await read_body(request)
        try:
            await runtime.answer(
                request.path_params["id"],
                body.get("answer"),
                by=optional_field(body, "by", str) or "person",
            )
        except (InvalidAnswer, KeyError, Ended) as exc:
            return _json({"error": str(exc)}, 409)
        except ValueError as exc:  # an answer that cannot be stored (not JSON)
            return _json({"error": str(exc)}, 400)
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
        middleware=[Middleware(Guard, token=token, hosts=tuple(hosts))],
        exception_handlers={BadRequest: refused},
    )


__all__ = ["BadRequest", "optional_field", "read_body", "refused", "server_app", "since_of"]
