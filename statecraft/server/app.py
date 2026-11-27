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
GET  /agents/{id}/calls                     the agent's journaled calls
POST /calls/{key}/resolve                   {"outcome": completed|failed|replay, "result"?}
GET  /faults                                agents suspended by a fault, with their calls
GET  /metrics                               Prometheus text (?format=json: the same as JSON)
GET  /maintenance | POST /maintenance       {"on": bool}: no new steps start (admin to change)
GET  /health                                {"ok", "started"}: served without a token
GET  /registry/agents | /registry/tools | /registry/providers

Every request passes the guard (`guard.py`): same-origin, JSON bodies, loopback names unless a
token is given. With named tokens (`tokens.py`) each request acts as a principal: `read` looks,
`control` spawns, signals, answers and repins, `admin` also settles calls of unknown outcome; a
token restricted to some runs sees only those runs and is refused (403) any other. Every change
made over HTTP (spawn, signal, repin, answer, resolve) is recorded in the run's durable event log
as an `audit` event naming the principal. Bad input is a 4xx with `{"error"}`: 400 for a body or
parameter that is not what the route takes, 403 for a path that leaves the workspace or a run the
principal may not touch, 404 for an unknown agent or file, 409 for a change to an agent that has
ended or a question that closed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
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
from ..runtime import operate
from ..runtime.runtime import Runtime, agent_of_question
from .guard import Guard
from .tokens import OPERATOR, Principal, TokenBook

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


class Forbidden(PermissionError):
    """The principal may not touch this run, or may not do this."""


async def forbidden(request: Request, exc: Exception) -> Response:
    return _json({"error": str(exc)}, 403)


def principal_of(request: Request) -> Principal:
    found = request.scope.get("state", {}).get("principal")
    return found if isinstance(found, Principal) else OPERATOR


def root_of(agent_id: str) -> str:
    return agent_id.split("/", 1)[0]


def must_own(request: Request, agent_id: str) -> Principal:
    principal = principal_of(request)
    if not principal.owns(root_of(agent_id)):
        raise Forbidden(f"{principal.name} may not touch {root_of(agent_id)}")
    return principal


def must_allow(request: Request, scope: str) -> Principal:
    principal = principal_of(request)
    if not principal.allows(scope):
        raise Forbidden(f"{principal.name} may not {scope}")
    return principal


def server_app(
    runtime: Runtime,
    *,
    manage: bool = False,
    token: str | None = None,
    hosts: list[str] | tuple[str, ...] = (),
    tokens: TokenBook | None = None,
) -> Starlette:
    """`manage`: start the runtime with the app and close it on shutdown. `token`: every request
    must carry it; `tokens`: named tokens with scopes and runs; `hosts`: names served besides
    loopback when there is no token."""

    async def audit(request: Request, agent_id: str, op: str, **detail: Any) -> None:
        entry = {"op": op, "agent": agent_id, "by": principal_of(request).name, **detail}
        with contextlib.suppress(KeyError):
            await runtime.store.audit(root_of(agent_id), entry)

    async def runs(request: Request) -> Response:
        principal = principal_of(request)
        return _json([r for r in await runtime.runs() if principal.owns(r["id"])])

    async def spawn(request: Request) -> Response:
        body = await read_body(request)
        agent = body.get("agent")
        if not isinstance(agent, str) or not agent:
            raise BadRequest("'agent' must name an agent")
        principal = principal_of(request)
        root_id = optional_field(body, "root", str)
        attach = optional_field(body, "attach", str)
        inherit = optional_field(body, "inherit", dict)
        if principal.roots is not None and root_id is None:
            raise Forbidden(f"{principal.name} must name the root (one of {principal.roots}*)")
        for touched in (root_id, attach, (inherit or {}).get("root")):
            if touched is not None:
                must_own(request, str(touched))
        try:
            root = await runtime.spawn(
                agent,
                optional_field(body, "input", dict) or {},
                root_id=root_id,
                attach=attach,
                attach_mode=optional_field(body, "attach_mode", str, "ro"),
                inherit=inherit,
            )
        except (KeyError, ValueError, TypeError) as exc:
            return _json({"error": str(exc)}, 400)
        await audit(request, root, "spawn", definition=agent, attach=attach)
        return _json({"root": root}, 201)

    async def get(request: Request) -> Response:
        agent_id = request.path_params["id"]
        must_own(request, agent_id)
        try:
            if request.query_params.get("tree"):
                rows = await runtime.tree(agent_id)
                return _json(rows) if rows else _json({"error": "no such root"}, 404)
            return _json(await runtime.get(agent_id))
        except KeyError:
            return _json({"error": "no such agent"}, 404)

    async def history(request: Request) -> Response:
        agent_id = request.path_params["id"]
        must_own(request, agent_id)
        try:
            await runtime.store.load(agent_id)
        except KeyError:
            return _json({"error": "no such agent"}, 404)
        return _json(await runtime.history(agent_id))

    async def events(request: Request) -> Response:
        root = request.path_params["id"]
        must_own(request, root)
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
        agent_id = request.path_params["id"]
        must_own(request, agent_id)
        sent: Any = body["signal"]
        if sent == "USR":
            sent = ("USR", body.get("message"))
        try:
            await runtime.signal(agent_id, sent)
        except ValueError as exc:
            return _json({"error": str(exc)}, 400)
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        except Ended as exc:
            return _json({"error": str(exc)}, 409)
        await audit(request, agent_id, "signal", signal=body["signal"])
        return Response(status_code=204)

    async def repin(request: Request) -> Response:
        agent_id = request.path_params["id"]
        must_own(request, agent_id)
        try:
            changed = await runtime.repin(agent_id)
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        except Ended as exc:
            return _json({"error": str(exc)}, 409)
        await audit(request, agent_id, "repin")
        return _json(changed)

    async def artifact(request: Request) -> Response:
        must_own(request, request.path_params["id"])
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
        must_own(request, request.path_params["id"])
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
        must_own(request, request.path_params["id"])
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
        principal = principal_of(request)
        root = request.query_params.get("root")
        if root:
            must_own(request, root)
        found = await runtime.questions(root)
        return _json([q for q in found if principal.owns(root_of(q["agent"]))])

    async def answer(request: Request) -> Response:
        body = await read_body(request)
        question_id = request.path_params["id"]
        try:
            agent_id = agent_of_question(question_id)
        except InvalidAnswer as exc:
            return _json({"error": str(exc)}, 409)
        principal = must_own(request, agent_id)
        # with named tokens the answerer is the principal; the body may not claim another name
        by = optional_field(body, "by", str) or "person"
        if principal is not OPERATOR:
            by = principal.name
        try:
            await runtime.answer(question_id, body.get("answer"), by=by)
        except (InvalidAnswer, KeyError, Ended) as exc:
            return _json({"error": str(exc)}, 409)
        except ValueError as exc:  # an answer that cannot be stored (not JSON)
            return _json({"error": str(exc)}, 400)
        await audit(request, agent_id, "answer", question=question_id)
        return Response(status_code=204)

    async def calls(request: Request) -> Response:
        agent_id = request.path_params["id"]
        must_own(request, agent_id)
        return _json(await runtime.calls(agent_id))

    async def resolve(request: Request) -> Response:
        principal = must_allow(request, "admin")
        body = await read_body(request)
        key = request.path_params["key"]
        entry = await runtime.store.call(key)
        if entry is None:
            return _json({"error": f"no call {key}"}, 404)
        must_own(request, entry["agent"])
        outcome = body.get("outcome")
        if not isinstance(outcome, str):
            raise BadRequest("'outcome' must be completed, failed or replay")
        try:
            settled = await runtime.resolve_call(
                key, outcome, result=body.get("result"), by=principal.name
            )
        except KeyError as exc:
            return _json({"error": str(exc)}, 404)
        except ValueError as exc:
            return _json({"error": str(exc)}, 409)
        return _json(settled)

    async def faults(request: Request) -> Response:
        principal = principal_of(request)
        return _json([f for f in await runtime.faults() if principal.owns(f["root"])])

    async def metrics(request: Request) -> Response:
        found = await runtime.metrics()
        if request.query_params.get("format") == "json":
            return _json(found)
        return Response(operate.prometheus(found), media_type="text/plain; version=0.0.4")

    async def maintenance(request: Request) -> Response:
        if request.method == "GET":
            return _json({"on": await runtime.maintenance()})
        principal = must_allow(request, "admin")
        on = (await read_body(request)).get("on")
        if not isinstance(on, bool):
            raise BadRequest("'on' must be true or false")
        changed = await runtime.maintenance(on)
        logging.getLogger("statecraft.server").warning(
            "maintenance %s by %s", "on" if on else "off", principal.name
        )
        return _json({"on": changed})

    async def health(request: Request) -> Response:
        return _json({"ok": True, "started": runtime.started})

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
            Route("/agents/{id:path}/calls", calls),
            Route("/agents/{id:path}", get),
            Route("/questions", questions),
            Route("/questions/{id:path}/answer", answer, methods=["POST"]),
            Route("/calls/{key:path}/resolve", resolve, methods=["POST"]),
            Route("/faults", faults),
            Route("/metrics", metrics),
            Route("/maintenance", maintenance, methods=["GET", "POST"]),
            Route("/health", health),
            Route("/registry/agents", agents),
            Route("/registry/tools", tools),
            Route("/registry/providers", providers),
        ],
        lifespan=lifespan,
        middleware=[Middleware(Guard, token=token, hosts=tuple(hosts), tokens=tokens)],
        exception_handlers={BadRequest: refused, Forbidden: forbidden},
    )


__all__ = [
    "BadRequest",
    "Forbidden",
    "forbidden",
    "must_allow",
    "must_own",
    "optional_field",
    "principal_of",
    "read_body",
    "refused",
    "root_of",
    "server_app",
    "since_of",
]
