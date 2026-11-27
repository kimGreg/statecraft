"""The HTTP API answers only its own pages and says 4xx for bad input.

What must hold
  * a cross-site browser request never reaches a route: a `text/plain` "simple" POST, a foreign
    `Origin`, `Sec-Fetch-Site: cross-site` or an opaque origin are refused, and nothing is spawned
    or signalled;
  * without a token only loopback names are served, so a page whose name was rebound to this
    machine is refused by its Host; names given in `hosts` are served too;
  * with a token every request must carry it, from any Host; the client sends it;
  * a server bound to a non-loopback address will not start without a token;
  * bad input is 4xx, never 500: a body that is not JSON or not an object, fields of the wrong
    type, a non-numeric `since`, a path outside the workspace (403), an unknown agent's history
    (404), a repin of an ended agent (409);
  * `statecraft resume` of an unknown root says so instead of crashing.
"""

from __future__ import annotations

import httpx
import pytest

from statecraft import Agent, Done, Wait, state
from statecraft.cli.main import main as cli
from statecraft.client import StatecraftClient
from statecraft.server import server_app
from statecraft.server.guard import require_token
from statecraft.testing import memory_runtime


class Holder(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "ok", "question": "?"}, then="done")

    @state
    async def done(self, ctx, data):
        return Done


def runtime():
    rt = memory_runtime()
    rt.registrar.agents.register("holder", Holder)
    return rt


def client(app, host: str = "localhost", **headers) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=f"http://{host}", headers=headers
    )


async def test_cross_site_requests_never_reach_a_route() -> None:
    async with runtime() as rt:
        app = server_app(rt)
        async with client(app) as c:
            spawn = '{"agent": "holder", "root": "csrf"}'
            refused = [
                await c.post("/agents", content=spawn, headers={"content-type": "text/plain"}),
                await c.post(
                    "/agents",
                    content=spawn,
                    headers={"content-type": "application/x-www-form-urlencoded"},
                ),
                await c.post(
                    "/agents", json={"agent": "holder"}, headers={"origin": "http://evil.example"}
                ),
                await c.post("/agents", json={"agent": "holder"}, headers={"origin": "null"}),
                await c.post(
                    "/agents", json={"agent": "holder"}, headers={"sec-fetch-site": "cross-site"}
                ),
                await c.get("/agents", headers={"origin": "http://evil.example"}),
            ]
            assert [r.status_code for r in refused] == [415, 415, 403, 403, 403, 403]
            same = await c.post(
                "/agents",
                json={"agent": "holder", "root": "mine"},
                headers={"origin": "http://localhost", "sec-fetch-site": "same-origin"},
            )
            assert same.status_code == 201
            root = same.json()["root"]
            empty = await c.post(f"/agents/{root}/repin")  # no body is not a form
            assert empty.status_code == 200
        assert [r.id for r in await rt.store.roots()] == [root]


async def test_a_rebound_name_is_refused_without_a_token() -> None:
    async with runtime() as rt:
        for host, status in (("evil.example", 403), ("127.0.0.1:8080", 200), ("[::1]:80", 200)):
            async with client(server_app(rt), host) as c:
                assert (await c.get("/agents")).status_code == status, host
        async with client(server_app(rt, hosts=["box.lan"]), "box.lan:8080") as c:
            assert (await c.get("/agents")).status_code == 200


async def test_a_token_is_required_from_any_host() -> None:
    async with runtime() as rt:
        app = server_app(rt, token="s3cret")
        async with client(app) as c:
            assert (await c.get("/agents")).status_code == 401
            bad = await c.get("/agents", headers={"authorization": "Bearer nope"})
            assert bad.status_code == 401
        async with client(app, "box.lan", authorization="Bearer s3cret") as c:
            assert (await c.get("/agents")).status_code == 200
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://box.lan")
        api = StatecraftClient("http://box.lan", http=http, token="s3cret")
        async with api:
            assert await api.runs() == []


def test_a_public_bind_needs_a_token(monkeypatch) -> None:
    monkeypatch.delenv("STATECRAFT_TOKEN", raising=False)
    with pytest.raises(SystemExit, match="needs a token"):
        require_token("0.0.0.0", None)
    assert require_token("127.0.0.1", None) is None
    assert require_token("0.0.0.0", "t") == "t"
    monkeypatch.setenv("STATECRAFT_TOKEN", "env")
    assert require_token("192.168.1.4", None) == "env"


async def test_bad_input_is_a_client_error() -> None:
    async with runtime() as rt, client(server_app(rt)) as c:
        root = (await c.post("/agents", json={"agent": "holder", "root": "h"})).json()["root"]
        cases = {
            "not json": await c.post(
                "/agents", content="{", headers={"content-type": "application/json"}
            ),
            "not an object": await c.post("/agents", json=["holder"]),
            "no agent": await c.post("/agents", json={"input": {}}),
            "input list": await c.post("/agents", json={"agent": "holder", "input": [1]}),
            "inherit str": await c.post("/agents", json={"agent": "holder", "inherit": "x"}),
            "signal list": await c.post(f"/agents/{root}/signals", json=["KILL"]),
            "answer list": await c.post("/questions/x/answer", json=[1]),
            "since": await c.get(f"/agents/{root}/events?since=abc"),
            "no path": await c.get(f"/agents/{root}/file"),
        }
        assert {k: r.status_code for k, r in cases.items()} == dict.fromkeys(cases, 400)
        for r in cases.values():
            assert "error" in r.json()
        outside = await c.get(f"/agents/{root}/files?prefix=../other")
        assert outside.status_code in (400, 403)
        assert (await c.get("/agents/nobody/history")).status_code == 404
        await c.post(f"/agents/{root}/signals", json={"signal": "KILL"})
        assert (await c.post(f"/agents/{root}/repin")).status_code == 409


def test_resume_of_an_unknown_root_says_so(tmp_path, capsys) -> None:
    config = tmp_path / "statecraft.yaml"
    config.write_text(
        f"store: {{provider: sqlite, path: '{tmp_path}/db'}}\n"
        f"workspace: {{provider: local, root: '{tmp_path}/ws'}}\n"
    )
    assert cli(["resume", "nobody", "--config", str(config)]) == 1
    assert "no root 'nobody'" in capsys.readouterr().err
