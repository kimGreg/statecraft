"""Models as effects: the extension registers one tool per configured model, `model.<name>`,
carried out by `ModelProvider` (architecture §12.2). The kernel never sees a model.

    model.default(messages=[...], tools=[...]) ->
        {"content": str | None, "tool_calls": [{"id", "name", "arguments"}], "usage": {...}}

Policy: `read_only` (no side effects), `deterministic: false` (a repeat may answer differently),
`cost: metered` (a repeat costs again). Committed replies are reused on recovery; only an
unfinished turn asks again. The model's private reasoning is never kept.

Clients: `ChatModel` (any OpenAI-compatible endpoint; `openrouter(...)` asks for zero retention),
`ScriptedModel` (tests: replayed steps or a function).
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

from statecraft import EffectDefinition, EffectError, EffectPolicy

PROVIDER = "model"
MODEL_POLICY = EffectPolicy(
    kind="read_only", deterministic=False, cost="metered", timeout=180, max_attempts=3
)
INPUT_SCHEMA = {
    "type": "object",
    "properties": {"messages": {"type": "array"}, "tools": {"type": "array"}},
    "required": ["messages"],
}


class Client(Protocol):
    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]: ...


class ModelError(RuntimeError):
    """The model endpoint answered something unusable."""


OPENROUTER_BASE = "https://openrouter.ai/api/v1"
OPENROUTER_KEY_ENVS = ("OPENROUTER_API_KEY", "OPENROUTER_KEY")
#: Every OpenRouter request asks for providers that keep no prompts and collect nothing.
OPENROUTER_PRIVACY = {"provider": {"zdr": True, "data_collection": "deny"}}


@dataclass
class ChatModel:
    """An OpenAI-compatible chat completions endpoint. The key is read from the environment at
    each call and never logged or stored. Failures are classified for the effect manager."""

    name: str
    base_url: str
    key_envs: tuple[str, ...] = ("OPENAI_API_KEY",)
    extra_body: dict[str, Any] = field(default_factory=dict)
    timeout: float = 180

    def _key(self) -> str | None:
        return next((os.environ[e] for e in self.key_envs if os.environ.get(e)), None)

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"model": self.name, "messages": messages, **self.extra_body}
        if tools:
            body["tools"] = tools
        headers = {"Content-Type": "application/json"}
        key = self._key()
        if key:
            headers["Authorization"] = f"Bearer {key}"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions", json=body, headers=headers
                )
        except httpx.ConnectError as exc:
            raise EffectError(
                "unavailable", f"cannot connect: {type(exc).__name__}", sent=False
            ) from exc
        except httpx.TransportError as exc:
            raise EffectError("transient", f"transport: {type(exc).__name__}") from exc
        if response.status_code >= 500 or response.status_code == 429:
            raise EffectError("transient", f"endpoint answered {response.status_code}")
        if response.status_code >= 400:
            raise EffectError(
                "failed", f"endpoint answered {response.status_code}: {response.text[:300]}"
            )
        try:
            return reply_of(response.json())
        except (json.JSONDecodeError, ModelError) as exc:
            raise EffectError("failed", str(exc)[:300]) from exc


def openrouter(model: str) -> ChatModel:
    """A model through OpenRouter: zero retention and no collection on every request."""
    return ChatModel(
        name=model.removeprefix("openrouter/"),
        base_url=OPENROUTER_BASE,
        key_envs=OPENROUTER_KEY_ENVS,
        extra_body=OPENROUTER_PRIVACY,
    )


#: Chat-format tokens some models leak into a tool's name (`read_file<|channel|>commentary`).
FORMAT_TOKEN = re.compile(r"<\|.*$", re.S)


def tool_name_of(raw: str) -> str:
    return FORMAT_TOKEN.sub("", str(raw)).strip()


def reply_of(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("error"):
        raise ModelError(f"model error: {str(raw['error'])[:300]}")
    choices = raw.get("choices") or []
    if not choices:
        raise ModelError("model answered no choices")
    message = choices[0].get("message") or {}
    calls = []
    for call_ in message.get("tool_calls") or []:
        function = call_.get("function") or {}
        try:
            arguments = json.loads(function.get("arguments") or "{}")
        except json.JSONDecodeError:
            arguments = {"_unparsed": function.get("arguments")}
        calls.append(
            {
                "id": call_.get("id") or f"call_{len(calls)}",
                "name": tool_name_of(function.get("name", "")),
                "arguments": arguments,
            }
        )
    return {"content": message.get("content"), "tool_calls": calls, "usage": raw.get("usage") or {}}


@dataclass
class ScriptedModel:
    """Replays steps (`say(...)`, `call(...)`) by the number of assistant turns so far, or asks
    `step_fn(messages, tools)` each turn. For tests and examples."""

    steps: list[dict[str, Any]] = field(default_factory=list)
    step_fn: Callable[[list[dict[str, Any]], list[dict[str, Any]]], dict[str, Any]] | None = None
    seen: list[list[dict[str, Any]]] = field(default_factory=list)
    #: the tools offered on each turn (their function names)
    offered: list[list[str]] = field(default_factory=list)

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]:
        self.seen.append(messages)
        self.offered.append([t["function"]["name"] for t in tools])
        if self.step_fn is not None:
            step = self.step_fn(messages, tools)
        else:
            turn = sum(1 for m in messages if m["role"] == "assistant")
            step = self.steps[turn] if turn < len(self.steps) else say("done")
        calls = [
            {"id": f"call_{len(self.seen)}_{i}", "name": n, "arguments": a}
            for i, (n, a) in enumerate(step.get("calls", []))
        ]
        return {
            "content": step.get("text"),
            "tool_calls": calls,
            "usage": {"prompt_tokens": 10, "completion_tokens": 5},
        }


def say(text: str) -> dict[str, Any]:
    return {"text": text}


def call(tool: str, **arguments: Any) -> dict[str, Any]:
    return {"calls": [(tool, arguments)]}


def client_from(settings: dict[str, Any]) -> Client:
    """A client from configuration: {provider: openai-compatible | openrouter | scripted, ...}."""
    kind = settings.get("provider", "openai-compatible")
    if kind == "openrouter":
        return openrouter(str(settings["model"]))
    if kind == "scripted":
        steps = [
            call(s["call"], **s.get("arguments", {})) if "call" in s else say(s["say"])
            for s in settings.get("steps", [])
        ]
        return ScriptedModel(steps)
    if kind == "openai-compatible":
        key_env = settings.get("api_key_env", "OPENAI_API_KEY")
        return ChatModel(
            name=str(settings["model"]),
            base_url=str(settings["endpoint"]),
            key_envs=(key_env,),
            extra_body=dict(settings.get("body") or {}),
            timeout=float(settings.get("timeout", 180)),
        )
    raise ValueError(f"unknown model provider {kind!r}")


class ModelProvider:
    """Carries out `model.<name>` effects with the configured clients."""

    def __init__(self, *, concurrency: int | None = None) -> None:
        self.models: dict[str, Client] = {}
        self.concurrency = concurrency

    def add(self, name: str, client: Client) -> None:
        self.models[name] = client

    async def discover(self) -> list[EffectDefinition]:
        return [
            EffectDefinition(
                name=f"model.{name}",
                description=f"Ask the model {name!r}: messages (and tools) in, a reply out.",
                input_schema=INPUT_SCHEMA,
                policy=MODEL_POLICY,
                provider=PROVIDER,
                operation=name,
                version=type(client).__name__,
            )
            for name, client in sorted(self.models.items())
        ]

    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any:
        client = self.models.get(operation)
        if client is None:
            raise EffectError("invalid", f"no model {operation!r}", sent=False)
        return await client.complete(
            list(arguments["messages"]), list(arguments.get("tools") or [])
        )

    def usage_of(self, result: Any) -> dict[str, Any]:
        return dict((result or {}).get("usage") or {}) if isinstance(result, dict) else {}


__all__ = [
    "MODEL_POLICY",
    "ChatModel",
    "ModelError",
    "ModelProvider",
    "ScriptedModel",
    "call",
    "client_from",
    "openrouter",
    "reply_of",
    "say",
]
