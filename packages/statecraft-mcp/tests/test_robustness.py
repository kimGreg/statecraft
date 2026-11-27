"""The MCP provider keeps working when connections misbehave (roadmap B2).

What must hold
  * a stdio server that died is started again on the next call (the broken session is dropped);
  * a session is never closed while a call on it is in flight, even past `max_sessions`;
  * a slow connect on one session key holds up no call on another;
  * a connect that is cancelled leaves no task running;
  * failures say what happened: a JSON-RPC error about the request is `invalid`, another
    answered error `failed`, a write to a closed connection `unavailable` and not sent, a
    connection closed with a request out `transient`.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import anyio
import pytest
from mcp.shared.exceptions import MCPError
from statecraft_mcp import MCPProvider

from statecraft.ports.effects import EffectContext

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from calc_server import server  # noqa: E402


def ctx(session: str = "") -> EffectContext:
    return EffectContext(
        root="r", agent="r", step=0, state="s", workspace=session, key=None, deadline=None
    )


async def test_a_dead_stdio_server_is_started_again() -> None:
    provider = MCPProvider("calc", command="python", args=[str(HERE / "calc_server.py"), "stdio"])
    await provider.start()
    assert await provider.invoke("add", {"a": 1, "b": 2}, ctx()) == 3
    with pytest.raises(Exception):  # noqa: B017 - the server dies with the call
        await provider.invoke("die", {}, ctx())
    assert await provider.invoke("add", {"a": 2, "b": 2}, ctx()) == 4
    await provider.close()


async def test_a_session_in_use_is_not_closed_past_the_limit() -> None:
    provider = MCPProvider("calc", server=server, session_header="X-S", max_sessions=1)
    slow = asyncio.ensure_future(provider.invoke("slow", {"seconds": 0.5}, ctx("one")))
    await asyncio.sleep(0.1)
    assert await provider.invoke("add", {"a": 1, "b": 1}, ctx("two")) == 2
    assert await slow == "done"  # its session outlived the limit while it was in use
    await provider.close()


async def test_a_slow_connect_holds_up_no_other_session() -> None:
    provider = MCPProvider("calc", server=server, session_header="X-S")
    await provider.invoke("add", {"a": 1, "b": 1}, ctx("fast"))
    hold = provider._hold
    gate = asyncio.Event()

    async def slow_hold(key, ready, closing):
        if key == "slow":
            await gate.wait()
        await hold(key, ready, closing)

    provider._hold = slow_hold
    connecting = asyncio.ensure_future(provider.invoke("add", {"a": 1, "b": 2}, ctx("slow")))
    await asyncio.sleep(0.05)
    started = asyncio.get_running_loop().time()
    assert await provider.invoke("add", {"a": 2, "b": 2}, ctx("fast")) == 4
    assert asyncio.get_running_loop().time() - started < 0.5
    gate.set()
    assert await connecting == 3
    await provider.close()


async def test_a_cancelled_connect_leaves_nothing_running() -> None:
    provider = MCPProvider("calc", server=server, session_header="X-S")
    started: list[asyncio.Task] = []

    async def never(key, ready, closing):
        started.append(asyncio.current_task())
        await asyncio.Event().wait()

    provider._hold = never
    call = asyncio.ensure_future(provider.invoke("add", {"a": 1, "b": 1}, ctx("x")))
    await asyncio.sleep(0.05)
    call.cancel()
    await asyncio.gather(call, return_exceptions=True)
    await asyncio.sleep(0.05)
    assert started and all(t.done() for t in started)
    assert provider._connecting == {} and provider._sessions == {}


@pytest.mark.parametrize(
    ("exc", "cls", "sent"),
    [
        (MCPError(-32602, "invalid params"), "invalid", True),
        (MCPError(-32601, "no such method"), "invalid", True),
        (MCPError(-32603, "internal error"), "failed", True),
        (MCPError(-32000, "Connection closed"), "transient", True),
        (anyio.ClosedResourceError(), "unavailable", False),
        (ConnectionResetError("reset"), "transient", True),
    ],
)
def test_failures_say_what_happened(exc, cls, sent) -> None:
    failure = MCPProvider._failure(exc, "calc.x")
    assert (failure.cls, failure.sent) == (cls, sent)


async def test_a_new_session_is_not_evicted_while_the_others_are_busy() -> None:
    """With every older session in use, the one just opened is used, not closed and opened again
    in a loop (the limit is passed for a while)."""
    from calc_server import server
    from statecraft_mcp import MCPProvider

    from statecraft.ports.effects import EffectContext

    def on(session):
        return EffectContext(
            root="r", agent="r", step=0, state="s", workspace=session, key=None, deadline=None
        )

    provider = MCPProvider("calc", server=server, session_header="X-S", max_sessions=1)
    connects = {"two": 0}
    hold = provider._hold

    async def counting(key, ready, closing):
        if key == "two":
            connects["two"] += 1
        await hold(key, ready, closing)

    provider._hold = counting
    slow = asyncio.ensure_future(provider.invoke("slow", {"seconds": 0.5}, on("one")))
    await asyncio.sleep(0.1)
    assert await provider.invoke("add", {"a": 1, "b": 1}, on("two")) == 2
    await slow
    await provider.close()
    assert connects["two"] == 1
