"""The runtime's API, remotely: the same methods as `Runtime`, over HTTP.

async with StatecraftClient("http://localhost:8080") as client:
    root = await client.spawn("research", input={"query": "..."})
    async for event in client.events(root): ...
"""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote

import httpx


class StatecraftError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"{status}: {message}")
        self.status = status


def _path(agent_id: str) -> str:
    return quote(agent_id, safe="/")


class StatecraftClient:
    def __init__(
        self, base_url: str, *, timeout: float = 60, http: Any = None, token: str | None = None
    ) -> None:
        """`token`: sent as `Authorization: Bearer`; defaults to `$STATECRAFT_TOKEN`."""
        token = token or os.environ.get("STATECRAFT_TOKEN") or None
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.http = http or httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout)
        self.http.headers.update(headers)

    async def __aenter__(self) -> StatecraftClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self.http.aclose()

    async def _json(self, method: str, url: str, **kwargs: Any) -> Any:
        response = await self.http.request(method, url, **kwargs)
        if response.status_code >= 400:
            try:
                message = response.json().get("error", response.text)
            except ValueError:
                message = response.text
            raise StatecraftError(response.status_code, str(message))
        return response.json() if response.content else None

    async def spawn(
        self,
        agent: str,
        input: dict[str, Any] | None = None,
        *,
        root_id: str | None = None,
        attach: str | None = None,
        attach_mode: str = "ro",
        inherit: dict[str, Any] | None = None,
    ) -> str:
        body = {
            "agent": agent,
            "input": input or {},
            "root": root_id,
            "attach": attach,
            "attach_mode": attach_mode,
            "inherit": inherit,
        }
        return (await self._json("POST", "/agents", json=body))["root"]

    async def runs(self) -> list[dict[str, Any]]:
        return await self._json("GET", "/agents")

    async def get(self, agent_id: str) -> dict[str, Any]:
        return await self._json("GET", f"/agents/{_path(agent_id)}")

    async def tree(self, root: str) -> list[dict[str, Any]]:
        return await self._json("GET", f"/agents/{_path(root)}", params={"tree": 1})

    async def history(self, agent_id: str) -> list[dict[str, Any]]:
        return await self._json("GET", f"/agents/{_path(agent_id)}/history")

    async def signal(self, agent_id: str, signal: Any) -> None:
        body: dict[str, Any] = {"signal": signal}
        if isinstance(signal, (list, tuple)):
            body = {"signal": signal[0], "message": signal[1]}
        await self._json("POST", f"/agents/{_path(agent_id)}/signals", json=body)

    async def repin(self, agent_id: str) -> dict[str, Any]:
        return await self._json("POST", f"/agents/{_path(agent_id)}/repin")

    async def answer(self, question_id: str, answer: Any, *, by: str = "person") -> None:
        await self._json(
            "POST", f"/questions/{_path(question_id)}/answer", json={"answer": answer, "by": by}
        )

    async def questions(self, root: str | None = None) -> list[dict[str, Any]]:
        return await self._json("GET", "/questions", params={"root": root} if root else None)

    async def artifact(self, root: str, kind: str, by: str | None = None) -> dict[str, Any]:
        return await self._json(
            "GET", f"/agents/{_path(root)}/artifacts/{kind}", params={"by": by} if by else None
        )

    async def files(self, root: str, prefix: str = "") -> list[str]:
        return await self._json("GET", f"/agents/{_path(root)}/files", params={"prefix": prefix})

    async def agents(self) -> list[dict[str, Any]]:
        return await self._json("GET", "/registry/agents")

    async def tools(self) -> list[dict[str, Any]]:
        return await self._json("GET", "/registry/tools")

    async def providers(self) -> list[dict[str, Any]]:
        return await self._json("GET", "/registry/providers")

    async def events(
        self, root: str, since: int = 0, *, follow: bool = True
    ) -> AsyncIterator[dict[str, Any]]:
        params = {"since": since, "follow": "1" if follow else "0"}
        async with self.http.stream(
            "GET", f"/agents/{_path(root)}/events", params=params, timeout=None
        ) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    yield json.loads(line[6:])


__all__ = ["StatecraftClient", "StatecraftError"]
