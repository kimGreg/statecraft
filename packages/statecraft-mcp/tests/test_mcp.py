"""statecraft-mcp: an MCP server's tools as statecraft tools.

What must hold
  * discovery names tools `<provider>.<tool>` with their schemas; policies are proposed from
    annotations only for a trusted provider; unannotated or untrusted tools are non_repeatable
    (refused) until the operator sets a policy;
  * calls return the tool's value; an error result is `failed`;
  * the same works over stdio and streamable HTTP; over HTTP with a session header each root
    gets its own session;
  * an unreachable server is a startup warning and its provider is listed as unreachable.
"""

from __future__ import annotations

import asyncio
import socket
import sys
from pathlib import Path

import uvicorn
from statecraft_mcp import MCPProvider

from statecraft import Agent, Done, EffectError, state
from statecraft.testing import memory_runtime

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from calc_server import server  # noqa: E402


class Caller(Agent):
    start = "go"

    @state
    async def go(self, ctx, data):
        out = {}
        for tool, args in ctx.input["calls"]:
            try:
                out[tool] = await ctx.call(tool, **args)
            except EffectError as exc:
                out[tool] = {"failure": exc.cls}
        await ctx.write("out", out)
        return Done


async def run_calls(provider: MCPProvider, calls: list, *, policies=None) -> tuple[dict, list]:
    runtime = memory_runtime()
    runtime.registrar.providers.register(provider.name, provider)
    for tool, kind in (policies or {}).items():
        runtime.registrar.tools.policy(tool, kind=kind)
    runtime.registrar.agents.register("caller", Caller, tools=[f"{provider.name}.*"])
    async with runtime:
        root = await runtime.spawn("caller", {"calls": calls})
        await runtime.run(root, timeout=20)
        return (await runtime.artifact(root, "out"))["payload"], runtime.tools()


async def test_in_memory_discovery_policies_and_calls() -> None:
    out, tools = await run_calls(
        MCPProvider("calc", server=server, trusted=True),
        [["calc.add", {"a": 2, "b": 3}], ["calc.send", {"message": "x"}], ["calc.boom", {}]],
    )
    assert out == {
        "calc.add": 5,
        "calc.send": {"failure": "refused"},
        "calc.boom": {"failure": "failed"},
    }
    policies = {t["name"]: t["policy"]["kind"] for t in tools}
    assert policies == {
        "calc.add": "read_only",
        "calc.boom": "read_only",
        "calc.die": "non_repeatable",
        "calc.send": "non_repeatable",
        "calc.slow": "read_only",
        "calc.store": "idempotent",
    }
    add = next(t for t in tools if t["name"] == "calc.add")
    assert (
        add["input_schema"]["required"] == ["a", "b"] and add["output_schema"]["type"] == "integer"
    )


async def test_untrusted_servers_propose_nothing_until_the_operator_decides() -> None:
    out, _tools = await run_calls(
        MCPProvider("calc", server=server),
        [["calc.add", {"a": 1, "b": 1}], ["calc.send", {"message": "x"}]],
        policies={"calc.send": "idempotent"},
    )
    assert out == {"calc.add": {"failure": "refused"}, "calc.send": "sent"}


async def test_over_stdio() -> None:
    provider = MCPProvider(
        "calc", command=sys.executable, args=[str(HERE / "calc_server.py"), "stdio"], trusted=True
    )
    out, _ = await run_calls(provider, [["calc.add", {"a": 20, "b": 22}]])
    assert out == {"calc.add": 42}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def test_over_http_with_a_session_per_root() -> None:
    seen: list[str] = []
    app = server.streamable_http_app()

    async def recording(scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers") or [])
            if b"x-session-id" in headers:
                seen.append(headers[b"x-session-id"].decode())
        await app(scope, receive, send)

    port = free_port()
    config = uvicorn.Config(
        recording, host="127.0.0.1", port=port, log_level="warning", lifespan="on"
    )
    http = uvicorn.Server(config)
    task = asyncio.create_task(http.serve())
    while not http.started:
        await asyncio.sleep(0.02)
    try:
        provider = MCPProvider(
            "calc", url=f"http://127.0.0.1:{port}/mcp", session_header="X-Session-Id", trusted=True
        )
        runtime = memory_runtime()
        runtime.registrar.providers.register("calc", provider)
        runtime.registrar.agents.register("caller", Caller, tools=["calc.*"])
        async with runtime:
            for root in ("r1", "r2"):
                await runtime.spawn(
                    "caller", {"calls": [["calc.add", {"a": 1, "b": 2}]]}, root_id=root
                )
                await runtime.run(root, timeout=20)
                assert (await runtime.artifact(root, "out"))["payload"] == {"calc.add": 3}
        assert {"r1", "r2"} <= set(seen)
    finally:
        http.should_exit = True
        await task


async def test_an_unreachable_server_is_a_startup_warning() -> None:
    runtime = memory_runtime()
    runtime.registrar.providers.register(
        "far", MCPProvider("far", url="http://127.0.0.1:9/mcp", timeout=2)
    )
    async with runtime:
        assert any("far did not start" in w for w in runtime.warnings)
        assert next(p for p in runtime.providers() if p["name"] == "far")["status"] == "unreachable"
