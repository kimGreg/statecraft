"""What an operator sees and does (roadmap D3, D4).

What must hold
  * maintenance mode is store-wide and durable: no new step starts while it is on, KILL still
    ends an agent, and turning it off lets the run go on where it stopped;
  * `faults()` lists a faulted agent with its error, failing state and the calls of the failing
    attempt; a restricted token sees its runs' faults only;
  * `metrics()` counts agents by view, unknown outcomes and undelivered messages, and this
    process's effect calls; `/metrics` serves them as Prometheus text;
  * a span per step and per effect call goes to the tracer (the call's span inside its step's),
    with the outcome, and without arguments or results;
  * the executor's log records carry the agent and root ids;
  * `/health` answers without a token and says nothing about runs; changing maintenance mode
    over HTTP needs `admin`;
  * the CLI lists faults and metrics, repins, lists calls, resolves one, and turns maintenance
    mode on and off.
"""

from __future__ import annotations

import contextlib
import json
import logging

import httpx

from statecraft import Agent, Done, Wait, state
from statecraft.cli.main import main as cli
from statecraft.server import server_app
from statecraft.server.tokens import TokenBook
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Counter(Agent):
    start = "one"

    @state
    async def one(self, ctx, data):
        data["n"] = await ctx.call("math.add", a=1, b=1)
        return "two"

    @state
    async def two(self, ctx, data):
        return Done


class Breaks(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        await ctx.call("math.add", a=2, b=3)
        raise RuntimeError("the disk is full")


class Holder(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        return Wait(answer={"name": "ok", "question": "?"}, then="go")


def runtime(**options):
    rt = memory_runtime(**options)
    rt.registrar.tools.register(add)
    rt.registrar.agents.register("counter", Counter, tools=["math.add"])
    rt.registrar.agents.register("breaks", Breaks, tools=["math.add"])
    rt.registrar.agents.register("holder", Holder)
    return rt


async def test_maintenance_mode_holds_new_steps() -> None:
    async with runtime() as rt:
        assert await rt.maintenance() is False
        await rt.maintenance(True)
        await rt.spawn("counter", root_id="c")
        await rt.spawn("holder", root_id="h")
        await rt.run(timeout=5)
        assert (await rt.store.load("c")).step == 0  # nothing ran
        await rt.signal("h", "KILL")  # a kill still ends an agent
        assert (await rt.get("h"))["view"] == "ended" or (await rt.store.load("h")).ended
        await rt.maintenance(False)
        await rt.run("c", timeout=5)
        assert (await rt.get("c"))["view"] == "succeeded"


async def test_faults_and_metrics_say_what_is_wrong() -> None:
    async with runtime() as rt:
        await rt.spawn("breaks", root_id="b")
        await rt.spawn("counter", root_id="c")
        await rt.run(timeout=5)
        [fault] = await rt.faults()
        assert fault["agent"] == "b" and fault["state"] == "go"
        assert "the disk is full" in fault["error"]
        assert [c["effect"] for c in fault["crossings"]] == ["math.add"]
        key = "b/0/go/call/x"
        await rt.store.begin_call("b", key, "math.add", "d", 1.0)
        await rt.store.end_call(key, "indeterminate", clear_by="operator")
        found = await rt.metrics()
        assert found["runs"] == 2 and found["agents"]["succeeded"] == 1
        assert found["agents"]["suspended"] == 1
        assert found["indeterminate_calls"] == 1
        assert found["process"]["effects"]["calls"] >= 2
        assert found["maintenance"] is False


class Span:
    def __init__(self, name, attributes, parent):
        self.name, self.attributes, self.parent, self.status = name, dict(attributes), parent, None

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def set_status(self, status):
        self.status = status

    def end(self, end_time=None):
        pass


class Tracer:
    def __init__(self):
        self.spans: list[Span] = []
        self.current: Span | None = None

    @contextlib.contextmanager
    def start_as_current_span(self, name, attributes=None):
        span = Span(name, attributes or {}, self.current)
        self.spans.append(span)
        before, self.current = self.current, span
        try:
            yield span
        finally:
            self.current = before

    def start_span(self, name, attributes=None, start_time=None):
        span = Span(name, attributes or {}, self.current)
        self.spans.append(span)
        return span


async def test_steps_and_calls_are_traced() -> None:
    tracer = Tracer()
    async with runtime(tracer=tracer) as rt:
        await rt.spawn("breaks", root_id="t")
        await rt.run("t", timeout=5)
    step = next(s for s in tracer.spans if s.name == "statecraft.step")
    call = next(s for s in tracer.spans if s.name == "statecraft.effect")
    assert call.parent is step
    assert step.attributes["statecraft.agent"] == "t"
    assert step.attributes["statecraft.outcome"] == "fault" and step.status is not None
    assert call.attributes["statecraft.effect"] == "math.add"
    assert call.attributes["statecraft.ok"] is True
    assert not any(str(v) == "5" for v in call.attributes.values())  # no result


async def test_log_records_carry_the_agent_and_root(caplog) -> None:
    caplog.set_level(logging.INFO, logger="statecraft.executor")
    async with runtime() as rt:
        await rt.spawn("holder", root_id="lg")
        await rt.run("lg", timeout=5)
        await rt.store.append_inbox("lg", "odd", {"kind": "not-a-message"})  # undeliverable
        await rt.run("lg", timeout=5)
    tagged = [r for r in caplog.records if getattr(r, "root", None) == "lg"]
    assert tagged and all(r.agent.startswith("lg") for r in tagged)


async def test_operator_routes_over_http(tmp_path) -> None:
    book = TokenBook(tmp_path / "tokens.json")
    reader = book.add("reader", scopes=["read"], roots=["mine-"])
    ops = book.add("ops", scopes=["admin"])
    async with runtime() as rt:
        await rt.spawn("breaks", root_id="mine-1")
        await rt.spawn("breaks", root_id="other")
        await rt.run(timeout=5)
        app = server_app(rt, tokens=book)

        def client(secret=None):
            headers = {"authorization": f"Bearer {secret}"} if secret else {}
            return httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://ops.example",
                headers=headers,
            )

        async with client() as anyone:
            health = await anyone.get("/health")
            assert health.status_code == 200 and health.json() == {"ok": True, "started": True}
            assert (await anyone.get("/metrics")).status_code == 401
        async with client(reader) as c:
            assert [f["agent"] for f in (await c.get("/faults")).json()] == ["mine-1"]
            assert (await c.get("/metrics")).status_code == 403  # store-wide: unrestricted only
            assert (await c.get("/maintenance")).json() == {"on": False}
            assert (await c.post("/maintenance", json={"on": True})).status_code == 403
        async with client(ops) as c:
            text = (await c.get("/metrics")).text
            assert 'statecraft_agents{view="suspended"} 2' in text
            assert (await c.post("/maintenance", json={"on": True})).json() == {"on": True}
            assert (await c.post("/maintenance", json={"on": "yes"})).status_code == 400
        assert await rt.maintenance() is True


def test_the_cli_operates_a_store(tmp_path, capsys) -> None:
    config = tmp_path / "statecraft.yaml"
    config.write_text(
        f"store: {{provider: sqlite, path: '{tmp_path}/state.db'}}\n"
        f"workspace: {{provider: local, root: '{tmp_path}/ws'}}\n"
    )
    base = ["--config", str(config)]

    def run(*args) -> object:
        capsys.readouterr()
        assert cli([*args, *base]) == 0, args
        return json.loads(capsys.readouterr().out)

    assert run("maintenance", "on") == {"maintenance": True}
    assert run("maintenance", "status") == {"maintenance": True}
    assert run("maintenance", "off") == {"maintenance": False}
    assert run("faults") == []
    assert run("metrics")["runs"] == 0
    from statecraft.kernel.process import Record
    from statecraft.providers.stores import SqliteStore

    async def journal() -> None:
        store = SqliteStore(tmp_path / "state.db")
        await store.create(Record(id="a", agent="t:a", state="go"))
        await store.begin_call("a", "a/0/go/call/x", "e", "d", 1.0)
        await store.end_call("a/0/go/call/x", "indeterminate", clear_by="operator")
        store.close()

    import asyncio

    asyncio.run(journal())
    assert [c["status"] for c in run("calls", "a")] == ["indeterminate"]
    assert run("resolve", "a/0/go/call/x", "completed", "--result", '{"n": 1}')["status"] == (
        "completed"
    )
    assert run("calls", "a")[0]["result"] == {"n": 1}
