"""What the provider needs from a sandbox service, and the OpenSandbox HTTP client.

The provider talks to a `Backend`; `OpenSandboxHTTP` implements it over the OpenSandbox lifecycle
API and the `execd` daemon inside each sandbox; `FakeBackend` (fake.py) implements it on this
machine for deterministic tests. Facts about OpenSandbox the provider relies on (A2 evidence):
execution ids are chosen by `execd` and arrive in the first event of the submit stream; output
is kept for background commands; command records live in `execd`'s memory (gone, 404, when it
restarts); cancelling ends the command's process group, not processes that left it; there is
no server-side deduplication.
"""

from __future__ import annotations

import json
from typing import Any, Protocol

import httpx


class Backend(Protocol):
    async def create(self, labels: dict[str, str], mounts: list[dict[str, Any]]) -> str: ...
    async def find(self, labels: dict[str, str]) -> list[dict[str, Any]]: ...  # [{id, state}]
    async def state(self, sandbox: str) -> str | None: ...  # Running, Pending, ...; None: gone
    async def delete(self, sandbox: str) -> None: ...
    async def submit(self, sandbox: str, command: str) -> str: ...  # the execution id
    async def status(self, sandbox: str, execution: str) -> dict[str, Any] | None: ...  # None: lost
    async def output(self, sandbox: str, execution: str) -> str: ...
    async def interrupt(self, sandbox: str, execution: str) -> None: ...


class OpenSandboxHTTP:
    """OpenSandbox's lifecycle server (`/v1/sandboxes`) and each sandbox's `execd`."""

    def __init__(
        self,
        endpoint: str,
        *,
        api_key: str = "",
        image: str = "alpine:latest",
        entrypoint: list[str] | None = None,
        execd_port: int = 44772,
        timeout: int | None = None,
        resources: dict[str, str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.headers = {"OPEN-SANDBOX-API-KEY": api_key} if api_key else {}
        self.image = image
        self.entrypoint = entrypoint or ["tail", "-f", "/dev/null"]
        self.execd_port = execd_port
        self.timeout = timeout
        self.resources = resources or {"cpu": "500m", "memory": "512Mi"}
        self.extra = extra or {}
        self._execd: dict[str, str] = {}

    async def _lc(self, method: str, path: str, **kw: Any) -> httpx.Response:
        async with httpx.AsyncClient(timeout=kw.pop("timeout", 120)) as client:
            return await client.request(
                method, f"{self.endpoint}/v1{path}", headers=self.headers, **kw
            )

    async def create(self, labels: dict[str, str], mounts: list[dict[str, Any]]) -> str:
        body: dict[str, Any] = {
            "image": {"uri": self.image},
            "entrypoint": self.entrypoint,
            "resourceLimits": self.resources,
            "metadata": labels,
            "volumes": [
                {
                    "name": f"m{n}",
                    "host": {"path": m["host"]},
                    "mountPath": m["target"],
                    "readOnly": m["mode"] == "ro",
                }
                for n, m in enumerate(mounts)
            ],
            **self.extra,
        }
        if self.timeout is not None:
            body["timeout"] = self.timeout
        response = await self._lc("POST", "/sandboxes", json=body)
        response.raise_for_status()
        return str(response.json()["id"])

    async def find(self, labels: dict[str, str]) -> list[dict[str, Any]]:
        query = "&".join(f"{k}={v}" for k, v in labels.items())
        response = await self._lc("GET", "/sandboxes", params={"metadata": query, "pageSize": 100})
        response.raise_for_status()
        return [
            {"id": item["id"], "state": (item.get("status") or {}).get("state")}
            for item in response.json().get("items", [])
        ]

    async def state(self, sandbox: str) -> str | None:
        response = await self._lc("GET", f"/sandboxes/{sandbox}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return (response.json().get("status") or {}).get("state")

    async def delete(self, sandbox: str) -> None:
        response = await self._lc("DELETE", f"/sandboxes/{sandbox}")
        if response.status_code not in (200, 202, 204, 404):
            response.raise_for_status()
        self._execd.pop(sandbox, None)

    async def _base(self, sandbox: str) -> str:
        if sandbox not in self._execd:
            response = await self._lc("GET", f"/sandboxes/{sandbox}/endpoints/{self.execd_port}")
            response.raise_for_status()
            self._execd[sandbox] = "http://" + response.json()["endpoint"]
        return self._execd[sandbox]

    async def submit(self, sandbox: str, command: str) -> str:
        base = await self._base(sandbox)
        body = {"command": command, "background": True}
        async with (
            httpx.AsyncClient(timeout=httpx.Timeout(30, read=60)) as client,
            client.stream("POST", f"{base}/command", json=body) as response,
        ):
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.strip():
                    continue
                event = json.loads(line.removeprefix("data:").strip())
                if event.get("type") == "init":
                    return str(event["text"])
                if event.get("type") == "error":
                    raise RuntimeError(f"submit failed: {event.get('error')}")
        raise ConnectionError("the submit stream ended before the execution id")

    async def status(self, sandbox: str, execution: str) -> dict[str, Any] | None:
        base = await self._base(sandbox)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f"{base}/command/status/{execution}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        found = response.json()
        return {"running": bool(found.get("running")), "exit": found.get("exit_code")}

    async def output(self, sandbox: str, execution: str) -> str:
        base = await self._base(sandbox)
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f"{base}/command/{execution}/logs")
        response.raise_for_status()
        return response.text

    async def interrupt(self, sandbox: str, execution: str) -> None:
        base = await self._base(sandbox)
        async with httpx.AsyncClient(timeout=60) as client:
            await client.delete(f"{base}/command", params={"id": execution})


__all__ = ["Backend", "OpenSandboxHTTP"]
