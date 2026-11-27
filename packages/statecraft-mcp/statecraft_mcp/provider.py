"""MCP as an effect provider (architecture §12.3): a server's tools become statecraft tools.

    MCPProvider("search", url="http://localhost:8001/mcp")            # streamable HTTP
    MCPProvider("files", command="python", args=["-m", "my_server"])   # stdio
    MCPProvider("calc", server=my_mcp_server)                          # in memory (same process)

Tools are named `<provider>.<tool>`. Discovery lists the server's tools with their schemas; the
policy is **proposed** from the tool's annotations only when the provider is `trusted`
(read-only hint -> read_only, idempotent hint -> idempotent); otherwise, and for unannotated
tools, it is `non_repeatable` (refused) until the operator sets one (`policies` in the
configuration). Annotations are hints, not guarantees.

Sessions: with `session_header`, each root gets its own connection carrying the root's workspace
id in that header (the server works in that session); otherwise one connection is shared.
Failures: a connection that cannot be opened is `unavailable` (not sent); a tool's error result
is `failed`; anything else during a call is `transient` (it may have been executed).
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from collections import OrderedDict
from typing import Any

from statecraft import EffectDefinition, EffectError, EffectPolicy

UNANNOTATED = EffectPolicy(kind="non_repeatable")


class MCPProvider:
    def __init__(
        self,
        name: str,
        *,
        url: str | None = None,
        command: str | None = None,
        args: list[str] | None = None,
        env: dict[str, str] | None = None,
        server: Any = None,
        session_header: str | None = None,
        trusted: bool = False,
        concurrency: int | None = None,
        timeout: float = 30,
        max_sessions: int = 64,
    ) -> None:
        if sum(x is not None for x in (url, command, server)) != 1:
            raise ValueError("an MCP provider has exactly one of url, command, server")
        self.name = name
        self.url, self.command, self.args, self.env = url, command, list(args or []), env
        self.server = server
        self.session_header = session_header
        self.trusted = trusted
        self.concurrency = concurrency
        self.timeout = timeout
        self.max_sessions = max_sessions
        self.server_version = ""
        self._sessions: OrderedDict[str, tuple[asyncio.Task[None], Any, asyncio.Event]] = (
            OrderedDict()
        )
        self._lock = asyncio.Lock()

    # ---- connections ------------------------------------------------------------------------
    def _target(self, session: str | None) -> Any:
        from mcp import StdioServerParameters

        if self.server is not None:
            return self.server
        if self.command is not None:
            # `python` means the interpreter running statecraft (its environment has the server)
            command = sys.executable if self.command in ("python", "python3") else self.command
            return StdioServerParameters(command=command, args=self.args, env=self.env)
        if self.session_header and session:
            import httpx2
            from mcp.client.streamable_http import streamable_http_client

            http = httpx2.AsyncClient(headers={self.session_header: session}, timeout=self.timeout)
            return streamable_http_client(self.url, http_client=http)
        return self.url

    async def _hold(self, key: str, ready: asyncio.Future[Any], closing: asyncio.Event) -> None:
        """Open the connection, hand it out, keep it until closing: a connection is closed by
        the task that opened it."""
        from mcp import Client

        try:
            async with Client(self._target(key or None)) as client:
                if not ready.done():
                    ready.set_result(client)
                await closing.wait()
        except Exception as exc:
            if not ready.done():
                ready.set_exception(exc)

    async def _client(self, session: str | None) -> Any:
        key = session if self.session_header else ""
        async with self._lock:
            held = self._sessions.get(key)
            if held is not None and not held[0].done():
                self._sessions.move_to_end(key)
                return held[1]
            ready: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            closing = asyncio.Event()
            task = asyncio.create_task(self._hold(key, ready, closing))
            try:
                client = await asyncio.wait_for(ready, self.timeout)
            except Exception as exc:
                closing.set()
                raise EffectError(
                    "unavailable",
                    f"cannot connect: {type(exc).__name__}: {exc}",
                    effect=self.name,
                    sent=False,
                ) from exc
            self._sessions[key] = (task, client, closing)
            while len(self._sessions) > self.max_sessions:
                _old, (_task, _c, old_closing) = self._sessions.popitem(last=False)
                old_closing.set()
            return client

    async def start(self) -> None:
        await self._client(None)

    async def close(self) -> None:
        for _task, _client, closing in self._sessions.values():
            closing.set()
        for task, _client, _closing in self._sessions.values():
            with contextlib.suppress(Exception):
                await asyncio.wait_for(task, 5)
        self._sessions.clear()

    # ---- the provider interface -------------------------------------------------------------
    def _policy(self, annotations: Any) -> EffectPolicy:
        if not self.trusted or annotations is None:
            return UNANNOTATED
        if getattr(annotations, "read_only_hint", None):
            return EffectPolicy(kind="read_only", timeout=self.timeout)
        if getattr(annotations, "idempotent_hint", None):
            return EffectPolicy(kind="idempotent", timeout=self.timeout)
        return UNANNOTATED

    async def discover(self) -> list[EffectDefinition]:
        client = await self._client(None)
        listed = await client.list_tools()
        info = getattr(client, "server_info", None)
        self.server_version = str(getattr(info, "version", "") or "") if info else ""
        found = []
        for tool in listed.tools:
            output = tool.output_schema
            if _wrapped(output):
                output = output["properties"]["result"]
            found.append(
                EffectDefinition(
                    name=f"{self.name}.{tool.name}",
                    description=tool.description or "",
                    input_schema=tool.input_schema or {"type": "object"},
                    output_schema=output,
                    policy=self._policy(tool.annotations),
                    provider=self.name,
                    operation=tool.name,
                    version=self.server_version,
                )
            )
        return found

    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any:
        client = await self._client(getattr(context, "workspace", None))
        try:
            result = await client.call_tool(operation, arguments)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise EffectError(
                "transient", f"{type(exc).__name__}: {exc}", effect=f"{self.name}.{operation}"
            ) from exc
        if result.is_error:
            text = " ".join(getattr(c, "text", "") for c in result.content)
            raise EffectError(
                "failed",
                text[:500] or "the tool reported an error",
                effect=f"{self.name}.{operation}",
            )
        structured = result.structured_content
        if isinstance(structured, dict) and set(structured) == {"result"}:
            return structured["result"]
        if structured is not None:
            return structured
        texts = [getattr(c, "text", None) for c in result.content]
        return texts[0] if len(texts) == 1 else texts


def _wrapped(schema: Any) -> bool:
    return (
        isinstance(schema, dict)
        and set(schema.get("properties") or {}) == {"result"}
        and schema.get("required") == ["result"]
    )


def register(registrar: Any) -> None:
    """The plugin (`plugins: [mcp]`): configuration can name `provider: mcp`."""
    registrar.providers.factory("mcp", lambda name, **cfg: MCPProvider(name, **cfg))


__all__ = ["MCPProvider", "register"]
