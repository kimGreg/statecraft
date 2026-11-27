"""Named tokens over HTTP: who may do what, to which runs, and the record of it (roadmap D1).

What must hold
  * the token book never holds a token, only a salted hash; a token is shown once; a revoked
    token is refused;
  * scopes: a `read` token looks but cannot change anything; a `control` token spawns, signals,
    answers and repins but cannot settle a call of unknown outcome; `admin` can;
  * a token restricted to some runs lists only those runs and questions, is refused (403) every
    other run, must name its new root, and cannot attach to or inherit from a run it does not own;
  * every change over HTTP is an `audit` event in the run's durable log naming the principal; an
    answer is recorded as given by the principal, whatever the body claims;
  * `statecraft token add|list|revoke` manages the book; `serve` on a non-loopback address starts
    with a token book alone;
  * the console takes the same tokens (its cookie) and shows a restricted principal its runs only.
"""

from __future__ import annotations

import json

import httpx
import pytest
from starlette.testclient import TestClient

from statecraft import Agent, Done, Wait, state
from statecraft.cli.main import main as cli
from statecraft.server import server_app
from statecraft.server.guard import require_token
from statecraft.server.tokens import TokenBook
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


@pytest.fixture
def book(tmp_path):
    made = TokenBook(tmp_path / "tokens.json")
    secrets = {
        "reader": made.add("reader", scopes=["read"]),
        "ci": made.add("ci", scopes=["control"], roots=["ci-"]),
        "ops": made.add("ops", scopes=["admin"]),
    }
    return made, secrets


def client(app, secret: str | None) -> httpx.AsyncClient:
    headers = {"authorization": f"Bearer {secret}"} if secret else {}
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api.example", headers=headers
    )


def test_the_book_keeps_hashes_only_and_revokes(book) -> None:
    made, secrets = book
    text = made.path.read_text()
    assert all(secret not in text for secret in secrets.values())
    assert oct(made.path.stat().st_mode & 0o777) == "0o600"
    assert made.verify(secrets["ci"]).name == "ci" and made.verify("sc_wrong") is None
    assert made.revoke("ci") and not made.revoke("ci")
    assert TokenBook(made.path).verify(secrets["ci"]) is None
    with pytest.raises(ValueError):
        made.add("x", scopes=["root"])


async def test_scopes_decide_what_a_token_may_do(book) -> None:
    made, secrets = book
    async with runtime() as rt:
        app = server_app(rt, tokens=made)
        async with client(app, None) as anyone:
            assert (await anyone.get("/agents")).status_code == 401
        async with client(app, "sc_not_a_token") as wrong:
            assert (await wrong.get("/agents")).status_code == 401
        async with client(app, secrets["reader"]) as reader:
            assert (await reader.get("/agents")).status_code == 200
            refused = await reader.post("/agents", json={"agent": "holder", "root": "r1"})
            assert refused.status_code == 403
        async with client(app, secrets["ci"]) as ci:
            made_run = await ci.post("/agents", json={"agent": "holder", "root": "ci-1"})
            assert made_run.status_code == 201
        assert await rt.store.find("r1") is None


async def test_a_restricted_token_sees_and_touches_its_runs_only(book) -> None:
    made, secrets = book
    async with runtime() as rt:
        await rt.spawn("holder", root_id="other")
        await rt.spawn("holder", root_id="ci-a")
        await rt.run("other", timeout=5)
        await rt.run("ci-a", timeout=5)
        app = server_app(rt, tokens=made)
        async with client(app, secrets["ci"]) as ci:
            assert [r["id"] for r in (await ci.get("/agents")).json()] == ["ci-a"]
            assert {q["agent"] for q in (await ci.get("/questions")).json()} == {"ci-a"}
            for path in ("/agents/other", "/agents/other/history", "/agents/other/calls"):
                assert (await ci.get(path)).status_code == 403, path
            assert (await ci.get("/questions?root=other")).status_code == 403
            assert (
                await ci.post("/agents/other/signals", json={"signal": "TERM"})
            ).status_code == 403
            assert (await ci.post("/agents/other/repin", json={})).status_code == 403
            [question] = await rt.questions("other")
            answered = await ci.post(
                f"/questions/{question['question_id']}/answer", json={"answer": 1}
            )
            assert answered.status_code == 403
            # a new run must be named, inside its prefixes, and may not borrow another run
            for body in (
                {"agent": "holder"},
                {"agent": "holder", "root": "mine"},
                {"agent": "holder", "root": "ci-b", "attach": "other"},
                {"agent": "holder", "root": "ci-c", "inherit": {"root": "other"}},
            ):
                assert (await ci.post("/agents", json=body)).status_code == 403, body
        async with client(app, secrets["ops"]) as ops:
            assert {r["id"] for r in (await ops.get("/agents")).json()} == {"other", "ci-a"}
        assert (await rt.get("other"))["view"] != "ended"


async def test_changes_are_audited_by_the_principal(book) -> None:
    made, secrets = book
    async with runtime() as rt:
        app = server_app(rt, tokens=made)
        async with client(app, secrets["ci"]) as ci:
            await ci.post("/agents", json={"agent": "holder", "root": "ci-x"})
            await rt.run("ci-x", timeout=5)
            [question] = (await ci.get("/questions")).json()
            answered = await ci.post(
                f"/questions/{question['question_id']}/answer",
                json={"answer": 7, "by": "someone-else"},
            )
            assert answered.status_code == 204
            await ci.post("/agents/ci-x/signals", json={"signal": "STOP"})
        audits = [e for e in await rt.store.read_events("ci-x", 0) if e.get("kind") == "audit"]
        assert [(e["op"], e["by"]) for e in audits] == [
            ("spawn", "ci"),
            ("answer", "ci"),
            ("signal", "ci"),
        ]
        await rt.run("ci-x", timeout=5)
        history = json.dumps(await rt.history("ci-x"))
        assert "someone-else" not in history


async def test_only_admin_settles_a_call_of_unknown_outcome(book) -> None:
    made, secrets = book
    async with runtime() as rt:
        await rt.spawn("holder", root_id="ci-q")
        await rt.run("ci-q", timeout=5)
        key = "ci-q/0/go/call/x"
        await rt.store.begin_call("ci-q", key, "shell.exec", "d", 1.0)
        await rt.store.end_call(key, "indeterminate", clear_by="operator")
        app = server_app(rt, tokens=made)
        async with client(app, secrets["ci"]) as ci:
            [listed] = (await ci.get("/agents/ci-q/calls")).json()
            assert listed["status"] == "indeterminate"
            refused = await ci.post(f"/calls/{key}/resolve", json={"outcome": "failed"})
            assert refused.status_code == 403
        async with client(app, secrets["ops"]) as ops:
            assert (
                await ops.post("/calls/ci-q/none/resolve", json={"outcome": "failed"})
            ).status_code == 404
            assert (await ops.post(f"/calls/{key}/resolve", json={})).status_code == 400
            settled = await ops.post(f"/calls/{key}/resolve", json={"outcome": "failed"})
            assert settled.status_code == 200, settled.text
            again = await ops.post(f"/calls/{key}/resolve", json={"outcome": "failed"})
            assert again.status_code == 409
        assert (await rt.store.call(key))["status"] == "failed"
        audits = [e for e in await rt.store.read_events("ci-q", 0) if e.get("kind") == "audit"]
        assert audits[-1]["op"] == "resolve_call" and audits[-1]["by"] == "ops"


def test_the_cli_manages_the_book_and_serve_accepts_it(tmp_path, capsys) -> None:
    path = str(tmp_path / "book.json")
    assert cli(["token", "add", "ci", "--scope", "control", "--root", "ci-", "--tokens", path]) == 0
    secret = capsys.readouterr().out.strip()
    assert secret.startswith("sc_") and secret not in (tmp_path / "book.json").read_text()
    assert TokenBook(path).verify(secret).roots == ("ci-",)
    assert cli(["token", "list", "--tokens", path]) == 0
    assert '"ci"' in capsys.readouterr().out
    assert cli(["token", "revoke", "ci", "--tokens", path]) == 0
    assert TokenBook(path).verify(secret) is None
    with pytest.raises(SystemExit):
        require_token("0.0.0.0", None)
    assert require_token("0.0.0.0", None, TokenBook(path)) is None


async def test_the_console_signs_in_a_principal(book) -> None:
    from statecraft_console.app import console_app

    made, secrets = book
    async with runtime() as rt:
        await rt.spawn("holder", root_id="other")
        await rt.spawn("holder", root_id="ci-a")
        app = console_app(rt, tokens=made)
        with TestClient(app, base_url="http://console.example") as page:
            assert page.get("/api/runs").status_code == 401
            signed = page.get(f"/?token={secrets['ci']}", follow_redirects=False)
            assert signed.status_code == 303
            assert [r["id"] for r in page.get("/api/runs").json()] == ["ci-a"]
            assert page.get("/api/runs/other").status_code == 403
            refused = page.post("/api/signals", json={"agent": "other", "signal": "TERM"})
            assert refused.status_code == 403


async def test_review_regressions_for_tokens_and_runs(tmp_path) -> None:
    """A token revoked by another process is refused by a running server; a restricted admin
    cannot change store-wide maintenance or read store-wide metrics; an attachment through an
    owned run cannot reach (or write) a run the token does not own; a spawn's audit names its
    attachment, inheritance and budget; an unknown token costs no slow hash."""
    import time

    book = TokenBook(tmp_path / "t.json")
    ops = book.add("ops", scopes=["admin"])
    ci = book.add("ci", scopes=["admin"], roots=["ci-"])
    async with runtime() as rt:
        await rt.spawn("holder", root_id="other")
        await rt.spawn("holder", root_id="ci-shared", attach="other")  # read-only
        app = server_app(rt, tokens=book)
        async with client(app, ci) as c:
            assert (await c.post("/maintenance", json={"on": True})).status_code == 403
            assert (await c.get("/metrics")).status_code == 403
            chained = await c.post(
                "/agents",
                json={
                    "agent": "holder",
                    "root": "ci-x",
                    "attach": "ci-shared",
                    "attach_mode": "rw",
                },
            )
            assert chained.status_code == 403  # it works in `other`
            made = await c.post(
                "/agents", json={"agent": "holder", "root": "ci-b", "budget": {"usd": 1}}
            )
            assert made.status_code == 201
        [spawned] = [e for e in await rt.store.read_events("ci-b") if e["kind"] == "audit"]
        assert spawned["budget"] == {"usd": 1} and "attach_mode" in spawned
        with pytest.raises(ValueError, match="read-only"):
            await rt.spawn("holder", root_id="w2", attach="ci-shared", attach_mode="rw")
        TokenBook(book.path).revoke("ci")  # another process
        async with client(app, ci) as c:
            assert (await c.get("/agents")).status_code == 401
        async with client(app, ops) as c:
            assert (await c.get("/metrics")).status_code == 200
    started = time.perf_counter()
    for n in range(20):
        assert book.verify(f"sc_unknown{n}.x") is None
    assert time.perf_counter() - started < 0.5


def test_the_console_cookie_is_only_a_proven_token(tmp_path) -> None:
    from statecraft_console.app import console_app

    book = TokenBook(tmp_path / "t.json")
    ops = book.add("ops", scopes=["admin"])
    app = console_app(memory_runtime(), tokens=book)
    with TestClient(app, base_url="http://console.example") as page:
        injected = page.get(
            "/?token=garbage%3B%20Domain%3Dexample.org",
            headers={"authorization": f"Bearer {ops}"},
            follow_redirects=False,
        )
        assert injected.status_code == 401 and "set-cookie" not in injected.headers
        assert page.get("/?token=sc_nope.x", follow_redirects=False).status_code == 401
        signed = page.get(f"/?token={ops}", follow_redirects=False)
        assert signed.status_code == 303 and ops in signed.headers["set-cookie"]
