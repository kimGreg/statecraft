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
  * a server bound to a non-loopback address must have a token (`require_token`).
"""

from __future__ import annotations

import hmac
import ipaddress
import json
import os
from collections.abc import Awaitable, Callable, Iterable
from typing import Any
from urllib.parse import parse_qs, urlsplit

TOKEN_ENV = "STATECRAFT_TOKEN"
COOKIE = "statecraft_token"
LOOPBACK_NAMES = ("localhost", "127.0.0.1", "::1")
UNSAFE = ("POST", "PUT", "PATCH", "DELETE")

ASGI = Callable[..., Awaitable[None]]


def is_loopback(host: str) -> bool:
    name = host.strip("[]").lower()
    if name in LOOPBACK_NAMES or name.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def require_token(host: str, token: str | None) -> str | None:
    """The token to serve with, from the argument or `STATECRAFT_TOKEN`; refuses to serve a
    non-loopback address without one."""
    token = token or os.environ.get(TOKEN_ENV) or None
    if token is None and not is_loopback(host):
        raise SystemExit(
            f"serving on {host} needs a token: pass --token or set {TOKEN_ENV} "
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
    ) -> None:
        self.app = app
        self.token = token or None
        self.hosts = {h.lower() for h in hosts}
        self.cookie = cookie

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}
        problem = self.problem(scope, headers)
        if problem is not None:
            status, message = problem
            return await _refuse(send, status, message)
        if self.cookie and self.token is not None and _query_token(scope) is not None:
            return await _set_cookie(send, self.token)  # the token was checked above
        return await self.app(scope, receive, send)

    def problem(self, scope: dict[str, Any], headers: dict[str, str]) -> tuple[int, str] | None:
        host = headers.get("host", "")
        if self.token is not None:
            if not self._has_token(scope, headers):
                return 401, "a token is required (Authorization: Bearer <token>)"
        elif _name(host).lower() not in self.hosts and not is_loopback(_name(host)):
            return 403, f"host {host!r} is not served without a token"
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

    def _has_token(self, scope: dict[str, Any], headers: dict[str, str]) -> bool:
        assert self.token is not None
        given = headers.get("authorization", "")
        if given.lower().startswith("bearer ") and hmac.compare_digest(given[7:], self.token):
            return True
        if self.cookie:
            for part in headers.get("cookie", "").split(";"):
                name, _, value = part.strip().partition("=")
                if name == COOKIE and hmac.compare_digest(value, self.token):
                    return True
            given = _query_token(scope)
            if given is not None and hmac.compare_digest(given, self.token):
                return True
        return False


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
