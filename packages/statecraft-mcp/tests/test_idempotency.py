"""Deduplicated MCP tools: the key travels with every attempt, and the server must apply it once.

What must hold
  * every attempt of a deduplicated call carries the call's idempotency key in the request's
    `_meta` (`io.statecraft/idempotencyKey`): the same key on a retry and on a rerun of the state
    after a fault; a server that honours it applies the effect once;
  * a tool may be deduplicated only when its server declares it (`io.statecraft/deduplicates` in
    the tool's `_meta`) or the operator vouches for it; otherwise the runtime refuses to start;
  * a trusted server's declaration proposes the `deduplicated` policy.
"""

from __future__ import annotations

import pytest
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from statecraft_mcp import MCPProvider
from statecraft_mcp.provider import DEDUP_META, KEY_META

from statecraft import Agent, Done, state
from statecraft.runtime.registry import RegistryError
from statecraft.testing import memory_runtime


def payment_server(declares: bool) -> tuple[MCPServer, list[str], dict[str, str]]:
    server = MCPServer("pay")
    seen: list[str] = []
    applied: dict[str, str] = {}

    @server.tool(meta={DEDUP_META: True} if declares else None)
    def charge(amount: int, ctx: Context) -> str:
        """Charge an amount once per idempotency key."""
        key = str((ctx.request_context.meta or {}).get(KEY_META) or "")
        seen.append(key)
        if key not in applied:
            applied[key] = f"charged {amount}"
        return applied[key]

    return server, seen, applied


class Payer(Agent):
    start = "go"
    faults: int = 1

    @state
    async def go(self, ctx, data):
        data["result"] = await ctx.call(
            "pay.charge", idempotency_key=ctx.effect_key("charge"), amount=10
        )
        if Payer.faults:
            Payer.faults -= 1
            raise RuntimeError("crash after the call, before the commit")
        return Done


def runtime_with(provider: MCPProvider, policy: str | None = "deduplicated"):
    rt = memory_runtime()
    rt.registrar.providers.register("pay", provider)
    if policy:
        rt.registrar.tools.policy("pay.charge", kind=policy, max_attempts=3)
    rt.registrar.agents.register("payer", Payer, tools=["pay.charge"])
    return rt


async def test_a_declared_deduplicated_tool_gets_one_key_and_one_effect() -> None:
    server, seen, applied = payment_server(declares=True)
    Payer.faults = 2
    async with runtime_with(MCPProvider("pay", server=server, timeout=5)) as rt:
        root = await rt.spawn("payer", {}, root_id="p")
        await rt.run(root, timeout=10)  # the call, then the state faults before its commit
        for _ in range(2):  # each CONT runs the state again: the same key
            await rt.signal(root, "CONT")
            await rt.run(root, timeout=10)
        record = await rt.get(root)
    assert record["view"] == "succeeded"
    assert len(seen) == 3 and len(set(seen)) == 1 and seen[0] == "p/0/go/charge"
    assert applied == {"p/0/go/charge": "charged 10"}


async def test_an_undeclared_tool_cannot_be_deduplicated() -> None:
    server, _seen, _applied = payment_server(declares=False)
    with pytest.raises(RegistryError, match="keys alone do not deduplicate"):
        async with runtime_with(MCPProvider("pay", server=server)):
            pass
    vouched = MCPProvider("pay", server=server, deduplicating=["charge"])
    async with runtime_with(vouched):  # the operator vouches for it: starts
        pass


async def test_a_trusted_servers_declaration_proposes_the_policy() -> None:
    server, _seen, _applied = payment_server(declares=True)
    async with runtime_with(MCPProvider("pay", server=server, trusted=True), policy=None) as rt:
        assert rt.registry.effects["pay.charge"].policy.kind == "deduplicated"
