"""Who may talk to a runtime over HTTP (the API and the console).

What must hold
  * a request a browser sends on behalf of another site is refused: an `Origin` that is not the
    server's own, or `Sec-Fetch-Site: cross-site`;
  * a request that changes something (POST, PUT, PATCH, DELETE) carries JSON
    (`Content-Type: application/json`) or no body at all — a cross-site form or a `text/plain`
    "simple request" never reaches a route;
  * with no token, only loopback names are served (`localhost`, `127.0.0.1`, `[::1]`, or the
    names given in `hosts`): a page whose name was rebound to this machine is refused by its Host;
  * with a token, every request carries it (`Authorization: Bearer <token>`, or the console's
    cookie set by opening `/?token=<token>` once); the Host is then free, since a rebound page
    does not know the token;
  * with named tokens (`tokens.py`), the token names a principal: GET and HEAD need `read`,
    anything else `control`; the principal is left in `scope["state"]["principal"]` for the
    routes, which check the runs it may touch. Without any token, the local caller is the
    machine's operator;
  * a server bound to a non-loopback address must have a token (`require_token`);
  * `GET /health` is served to anyone (a liveness probe); it says nothing about runs.
"""

from __future__ import annotations

import asyncio
import hmac
import ipaddress
import json
import os
import re
from collections.abc import Awaitable, Callable, Iterable
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .tokens import OPERATOR, Principal, TokenBook

TOKEN_ENV = "STATECRAFT_TOKEN"
COOKIE = "statecraft_token"
LOOPBACK_NAMES = ("localhost", "127.0.0.1", "::1")
UNSAFE = ("POST", "PUT", "PATCH", "DELETE")
#: what a token (and so the console's cookie) may contain
COOKIE_VALUE = re.compile(r"[A-Za-z0-9_.\-]{1,200}")
#: paths served to anyone (GET only), saying nothing about runs: health probes
OPEN = ("/health",)

ASGI = Callable[..., Awaitable[None]]


def is_loopback(host: str) -> bool:
    name = host.strip("[]").lower()
    if name in LOOPBACK_NAMES or name.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def require_token(host: str, token: str | None, tokens: Any = None) -> str | None:
    """The token to serve with, from the argument or `STATECRAFT_TOKEN`; refuses to serve a
    non-loopback address without one or a token book (`tokens`)."""
    token = token or os.environ.get(TOKEN_ENV) or None
    if token is None and tokens is None and not is_loopback(host):
        raise SystemExit(
            f"serving on {host} needs a token: pass --token, --tokens FILE or set {TOKEN_ENV} "
            "(clients send it as `Authorization: Bearer <token>`)"
        )
    return token


def _name(host_header: str) -> str:
    """The host name of a Host header or an origin's netloc, without its port."""
    if host_header.startswith("["):
        return host_header[1 : host_header.find("]")]
    return host_header.rsplit(":", 1)[0] if host_header.count(":") == 1 else host_header


class Guard:
    """ASGI middleware in front of an app (see the module docstring)."""

    def __init__(
        self,
        app: ASGI,
        *,
        token: str | None = None,
        hosts: Iterable[str] = (),
        cookie: bool = False,
        tokens: TokenBook | None = None,
    ) -> None:
        self.app = app
        self.token = token or None
        #: named tokens with scopes and runs; with them (or `token`), every request is signed in
        self.tokens = tokens
        self.hosts = {h.lower() for h in hosts}
        self.cookie = cookie

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}
        presented = self._presented(scope, headers)
        signed: Principal | None = None
        if self._signs_in and presented is not None:  # the slow hash runs off the event loop
            signed = await asyncio.to_thread(self._principal, presented)
        problem = self.problem(scope, headers, signed=signed)
        if problem is not None:
            status, message = problem
            return await _refuse(send, status, message)
        given = _query_token(scope)
        if self.cookie and self._signs_in and given is not None:
            # only the query token itself becomes the cookie, once it proved to be a token
            if given == presented and signed is not None and COOKIE_VALUE.fullmatch(given):
                return await _set_cookie(send, given)
            return await _refuse(send, 401, "that token is not valid")
        return await self.app(scope, receive, send)

    @property
    def _signs_in(self) -> bool:
        return self.token is not None or self.tokens is not None

    def _presented(self, scope: dict[str, Any], headers: dict[str, str]) -> str | None:
        given = headers.get("authorization", "")
        if given.lower().startswith("bearer "):
            return given[7:]
        if self.cookie and _query_token(scope) is not None:  # signing in: the address's token
            return _query_token(scope)
        if self.cookie:
            for part in headers.get("cookie", "").split(";"):
                name, _, value = part.strip().partition("=")
                if name == COOKIE and value:
                    return value
            return _query_token(scope)
        return None

    def _principal(self, presented: str | None) -> Principal | None:
        if presented is None:
            return None
        if self.token is not None and hmac.compare_digest(presented, self.token):
            return OPERATOR
        return self.tokens.verify(presented) if self.tokens is not None else None

    def problem(
        self, scope: dict[str, Any], headers: dict[str, str], signed: Principal | None = None
    ) -> tuple[int, str] | None:
        host = headers.get("host", "")
        principal: Principal | None = OPERATOR  # local, without tokens: the machine's operator
        if scope.get("path") in OPEN and scope.get("method") in ("GET", "HEAD"):
            scope.setdefault("state", {})["principal"] = None
            return None  # a liveness probe: it says only that the server answers
        if self._signs_in:
            principal = signed
            if principal is None:
                return 401, "a token is required (Authorization: Bearer <token>)"
            needed = "read" if scope.get("method") in ("GET", "HEAD") else "control"
            if not principal.allows(needed):
                return 403, f"{principal.name} may not {needed}"
        elif _name(host).lower() not in self.hosts and not is_loopback(_name(host)):
            return 403, f"host {host!r} is not served without a token"
        scope.setdefault("state", {})["principal"] = principal
        origin = headers.get("origin")
        if origin is not None and origin != "null":
            parts = urlsplit(origin)
            if parts.netloc.lower() != host.lower():
                return 403, f"cross-origin request from {origin!r} refused"
        elif origin == "null":
            return 403, "a request from an opaque origin is refused"
        if headers.get("sec-fetch-site") == "cross-site":
            return 403, "cross-site request refused"
        if scope.get("method") in UNSAFE:
            kind = headers.get("content-type", "").split(";", 1)[0].strip().lower()
            length = headers.get("content-length")
            empty = kind == "" and length in (None, "0") and "transfer-encoding" not in headers
            if kind != "application/json" and not empty:
                return 415, "a request that changes something must send application/json"
        return None


def _query_token(scope: dict[str, Any]) -> str | None:
    """`?token=` on the console page, the one place a token may come in the address."""
    if scope.get("path") != "/":
        return None
    found = parse_qs(scope.get("query_string", b"").decode("latin-1")).get("token")
    return found[0] if found else None


async def _refuse(send: Any, status: int, message: str) -> None:
    body = json.dumps({"error": message}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _set_cookie(send: Any, token: str) -> None:
    cookie = f"{COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/"
    await send(
        {
            "type": "http.response.start",
            "status": 303,
            "headers": [(b"location", b"/"), (b"set-cookie", cookie.encode("latin-1"))],
        }
    )
    await send({"type": "http.response.body", "body": b""})


__all__ = ["COOKIE", "TOKEN_ENV", "Guard", "is_loopback", "require_token"]
