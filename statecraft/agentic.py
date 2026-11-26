"""Model-driven agents as ordinary state machines (no Google ADK).

    class Brief(AgenticAgent):
        instruction = "Brief the notes of the inbox."       # or the agent's first skill

An `AgenticAgent` is an `Agent` whose states run a model loop, one tool call per transition:

    prepare -> think -> act_one -> act_one -> ... -> think -> ... -> finish -> Done
                          |
                          +- a boundary tool (spawn_agent, wait_for, ask_person, sleep) returns
                             Spawn/Wait; wait_result adds its result as the tool's response

So the engine checkpoints between model turns and between tool calls: a crash reruns only the
unfinished turn or call. The conversation is in `data`; the model's private reasoning is never
kept. Tool failures go back to the model as the tool's response; limits and model failures are
faults (the parent decides). The final answer is the agent's output artifact.

The model is a port (`Model`): `ChatModel` for any OpenAI-compatible endpoint (OpenRouter with
zero retention), `ScriptedModel` for tests.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar, Protocol

import httpx

from .agent import Agent, Done, Spawn, Wait, state
from .spec import AGENT_PREFIX

# ---- the model port -------------------------------------------------------------------------


class Model(Protocol):
    name: str

    async def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        model: str | None = None,
    ) -> dict[str, Any]:
        """-> {"content": str | None, "tool_calls": [{"id", "name", "arguments": dict}],
        "usage": {...}}; never the model's reasoning."""
        ...


class ModelError(RuntimeError):
    """The model endpoint failed or answered something unusable."""


OPENROUTER_BASE = "https://openrouter.ai/api/v1"
OPENROUTER_KEY_ENVS = ("OPENROUTER_API_KEY", "OPENROUTER_KEY")
#: Every OpenRouter request asks for providers that keep no prompts and collect nothing.
OPENROUTER_PRIVACY = {"provider": {"zdr": True, "data_collection": "deny"}}


@dataclass
class ChatModel:
    """An OpenAI-compatible chat completions endpoint. The key is read from the environment at
    each call and never logged or stored."""

    name: str
    base_url: str
    key_envs: tuple[str, ...] = ("OPENAI_API_KEY",)
    extra_body: dict[str, Any] = field(default_factory=dict)
    timeout: float = 180
    retries: int = 2

    def _key(self) -> str | None:
        return next((os.environ[e] for e in self.key_envs if os.environ.get(e)), None)

    async def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        model: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": model or self.name,
            "messages": messages,
            **self.extra_body,
        }
        if tools:
            body["tools"] = tools
        headers = {"Content-Type": "application/json"}
        key = self._key()
        if key:
            headers["Authorization"] = f"Bearer {key}"
        last: Exception | None = None
        for _ in range(self.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(
                        f"{self.base_url}/chat/completions", json=body, headers=headers
                    )
                if response.status_code >= 500 or response.status_code == 429:
                    last = ModelError(f"model endpoint answered {response.status_code}")
                    continue
                if response.status_code >= 400:
                    raise ModelError(
                        f"model endpoint answered {response.status_code}: {response.text[:300]}"
                    )
                return _reply(response.json())
            except (httpx.TransportError, json.JSONDecodeError) as exc:
                last = ModelError(f"model endpoint unreachable: {type(exc).__name__}")
        raise last or ModelError("model endpoint failed")


#: Chat-format tokens some models leak into a tool's name (`read_file<|channel|>commentary`).
FORMAT_TOKEN = re.compile(r"<\|.*$", re.S)


def tool_name_of(raw: str) -> str:
    """The tool name a model meant: format tokens it leaked cut off, spaces trimmed."""
    return FORMAT_TOKEN.sub("", str(raw)).strip()


def _reply(raw: dict[str, Any]) -> dict[str, Any]:
    if raw.get("error"):
        raise ModelError(f"model error: {str(raw['error'])[:300]}")
    choices = raw.get("choices") or []
    if not choices:
        raise ModelError("model answered no choices")
    message = choices[0].get("message") or {}
    calls = []
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        try:
            arguments = json.loads(function.get("arguments") or "{}")
        except json.JSONDecodeError:
            arguments = {"_unparsed": function.get("arguments")}
        calls.append(
            {
                "id": call.get("id") or f"call_{len(calls)}",
                "name": tool_name_of(function.get("name", "")),
                "arguments": arguments,
            }
        )
    return {"content": message.get("content"), "tool_calls": calls, "usage": raw.get("usage") or {}}


def openrouter(model: str) -> ChatModel:
    """A model through OpenRouter, zero retention and no collection on every request."""
    name = model.removeprefix("openrouter/")
    if not any(os.environ.get(e) for e in OPENROUTER_KEY_ENVS):
        raise ModelError(f"set {' or '.join(OPENROUTER_KEY_ENVS)} to use OpenRouter")
    return ChatModel(
        name=name,
        base_url=OPENROUTER_BASE,
        key_envs=OPENROUTER_KEY_ENVS,
        extra_body=OPENROUTER_PRIVACY,
    )


@dataclass
class ScriptedModel:
    """Replays steps (`say(...)`, `call(...)`), or asks `step_fn(messages, tools)` each turn."""

    steps: list[dict[str, Any]] = field(default_factory=list)
    step_fn: Callable[[list[dict[str, Any]], list[dict[str, Any]]], dict[str, Any]] | None = None
    name: str = "scripted"
    seen: list[list[dict[str, Any]]] = field(default_factory=list)

    async def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        *,
        model: str | None = None,
    ) -> dict[str, Any]:
        self.seen.append(messages)
        if self.step_fn is not None:
            step = self.step_fn(messages, tools)
        else:
            turn = sum(1 for m in messages if m["role"] == "assistant")
            step = self.steps[turn] if turn < len(self.steps) else say("done")
        calls = [
            {"id": f"call_{i}", "name": n, "arguments": a}
            for i, (n, a) in enumerate(step.get("calls", []))
        ]
        return {"content": step.get("text"), "tool_calls": calls, "usage": {}}


def say(text: str) -> dict[str, Any]:
    return {"text": text}


def call(tool: str, **arguments: Any) -> dict[str, Any]:
    return {"calls": [(tool, arguments)]}


# ---- the agent ------------------------------------------------------------------------------

GENERAL = """You are one agent in a larger procedure. Work only with your tools.
- Computed values (coordinates, distances, areas, counts) come from tools; never invent them.
- `spawn_agent` starts another agent; `wait_for` waits until it ends; its output is then read
  with `read_artifact`. `ask_person` asks a person and waits for the answer.
- When you are done, answer with your final result only{output}."""

BOUNDARY = {"spawn_agent", "wait_for", "ask_person", "sleep"}
RESULT_CHARS = 12_000


def _function(name: str, description: str, schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {"name": name, "description": description[:1000], "parameters": schema},
    }


def model_name_of(declared: str) -> str:
    return declared.replace(".", "__")


def _clip(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= RESULT_CHARS else text[:RESULT_CHARS] + " ...[cut]"


_JSON_BLOCK = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)


def parse_json(text: str) -> Any:
    block = _JSON_BLOCK.search(text or "")
    return json.loads(block.group(1) if block else text)


class AgenticAgent(Agent):
    start = "prepare"

    #: The task instruction; None: the text of the agent's first declared skill.
    instruction: ClassVar[str | None] = None
    #: Declared tools to offer (None: every declared MCP tool).
    offered: ClassVar[tuple[str, ...] | None] = None
    #: The artifact type the final answer is written as (None: the first declared write).
    output_type: ClassVar[str | None] = None
    #: The final answer is JSON (an object) when True, text (`{"text": ...}`) otherwise.
    json_output: ClassVar[bool] = True
    model_override: ClassVar[str | None] = None
    max_model_turns: ClassVar[int] = 24
    max_tool_calls: ClassVar[int] = 48
    max_children: ClassVar[int] = 8
    max_output_retries: ClassVar[int] = 2

    # ---- hooks a subclass may override ------------------------------------------------------
    async def task(self, ctx) -> str:
        """The first user message: the spawn input (goal, question, facts)."""
        return json.dumps(ctx.input, ensure_ascii=False, indent=1)

    async def task_instruction(self, ctx) -> str:
        if self.instruction is not None:
            return self.instruction
        if ctx.spec is not None and ctx.spec.skills:
            return await ctx.skill(ctx.spec.skills[0])
        return ""

    def check_output(self, ctx, data: dict[str, Any], payload: Any) -> str | None:
        """A problem with the final answer (asked again), or None."""
        return None

    def shape_output(self, ctx, data: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        """The artifact payload made from a checked answer (measured values joined in here)."""
        return payload

    # ---- states -----------------------------------------------------------------------------
    @state
    async def prepare(self, ctx, data):
        output = self._output_type(ctx)
        shape = " as one JSON object" if self.json_output else ""
        system = "\n\n".join(
            p
            for p in (
                await self.task_instruction(ctx),
                GENERAL.format(output=shape),
                self._skill_list(ctx),
            )
            if p
        )
        data.update(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": await self.task(ctx)},
            ],
            pending=[],
            cursor=0,
            turns=0,
            calls=0,
            children=0,
            retries=0,
            output=output,
            usage={"prompt": 0, "completion": 0},
        )
        return "think"

    @state
    async def think(self, ctx, data):
        if data["turns"] >= self.max_model_turns:
            raise RuntimeError(f"more than {self.max_model_turns} model turns")
        if ctx.model is None:
            raise RuntimeError("an agentic agent needs a model (none configured)")
        reply = await ctx.model.complete(
            [dict(m) for m in data["messages"]], self._tools(ctx), model=self.model_override
        )
        data["turns"] += 1
        for key, value in (reply.get("usage") or {}).items():
            short = {"prompt_tokens": "prompt", "completion_tokens": "completion"}.get(key)
            if short and isinstance(value, int):
                data["usage"][short] += value
        content = reply.get("content") or ""
        calls = list(reply.get("tool_calls") or [])
        message: dict[str, Any] = {"role": "assistant", "content": content or None}
        if calls:
            message["tool_calls"] = [
                {
                    "id": c["id"],
                    "type": "function",
                    "function": {
                        "name": c["name"],
                        "arguments": json.dumps(c["arguments"], ensure_ascii=False),
                    },
                }
                for c in calls
            ]
        data["messages"].append(message)
        if calls:
            data.update(pending=calls, cursor=0)
            return "act_one"
        data["final"] = content
        return "finish"

    @state
    async def act_one(self, ctx, data):
        call_ = data["pending"][data["cursor"]]
        data["calls"] += 1
        if data["calls"] > self.max_tool_calls:
            raise RuntimeError(f"more than {self.max_tool_calls} tool calls")
        name, arguments = call_["name"], call_["arguments"]
        if name in BOUNDARY:
            boundary = self._boundary(ctx, data, name, arguments)
            if isinstance(boundary, (Spawn, Wait)):
                data["boundary"] = {"name": name, "arguments": arguments}
                return boundary
            return self._answer(data, boundary)
        return self._answer(data, await self._call_tool(ctx, name, arguments))

    @state
    async def wait_result(self, ctx, data):
        boundary = data.pop("boundary")
        woke = ctx.woke
        if boundary["name"] == "spawn_agent":
            result: Any = {"spawned": boundary["arguments"].get("name")}
        elif boundary["name"] == "wait_for":
            result = {
                "children": woke.get("children", {}),
                "faulted": [
                    {"child": f["child"], "error": f["error"]} for f in woke.get("faulted", [])
                ],
            }
        elif boundary["name"] == "ask_person":
            result = {"answer": woke.get("answer"), "by": woke.get("by")}
        else:
            result = {"slept": True}
        return self._answer(data, result)

    @state
    async def finish(self, ctx, data):
        text = data.get("final") or ""
        try:
            payload = parse_json(text) if self.json_output else {"text": text}
            if not isinstance(payload, dict):
                raise ValueError("the answer is not a JSON object")
            problem = self.check_output(ctx, data, payload)
        except (ValueError, json.JSONDecodeError) as exc:
            problem = f"{type(exc).__name__}: {exc}"
            payload = None
        if problem:
            data["retries"] += 1
            if data["retries"] > self.max_output_retries:
                raise RuntimeError(f"no usable final answer: {problem}")
            data["messages"].append(
                {
                    "role": "user",
                    "content": (
                        f"Your final answer cannot be used ({problem}). Answer again"
                        + (" with one JSON object." if self.json_output else ".")
                    ),
                }
            )
            return "think"
        if data["output"]:
            await ctx.write(
                data["output"],
                self.shape_output(ctx, data, payload),
                summary={"turns": data["turns"], "tool_calls": data["calls"]},
            )
        return Done

    # ---- helpers ----------------------------------------------------------------------------
    def _answer(self, data: dict[str, Any], result: Any) -> str:
        call_ = data["pending"][data["cursor"]]
        data["messages"].append(
            {"role": "tool", "tool_call_id": call_["id"], "content": _clip(result)}
        )
        data["cursor"] += 1
        if data["cursor"] < len(data["pending"]):
            return "act_one"
        data.update(pending=[], cursor=0)
        return "think"

    def _output_type(self, ctx) -> str | None:
        if self.output_type is not None:
            return self.output_type
        writes = ctx.spec.writes if ctx.spec is not None else None
        return writes[0] if writes else None

    def model_tools(self, ctx) -> list[str]:
        """The declared tools this agent's model may use (all of them, or `offered`)."""
        declared = list(ctx.spec.tools or ()) if ctx.spec is not None else []
        if self.offered is not None:
            declared = [t for t in declared if t in self.offered] or list(self.offered)
        return declared

    def _skill_list(self, ctx) -> str:
        skills = list(ctx.spec.skills[1:]) if ctx.spec is not None else []
        return ("Skills you can load with `load_skill`: " + ", ".join(skills)) if skills else ""

    def _tools(self, ctx) -> list[dict[str, Any]]:
        declared = self.model_tools(ctx)
        describe = getattr(ctx._services.tools_of(ctx.id), "describe", None)
        out = []
        for name in declared:
            if name.startswith(AGENT_PREFIX):
                continue
            server, _, tool = name.partition(".")
            info = describe(server, tool) if describe else None
            out.append(
                _function(
                    model_name_of(name),
                    (info or {}).get("description", name),
                    (info or {}).get("input_schema") or {"type": "object"},
                )
            )
        agents = [t.removeprefix(AGENT_PREFIX) for t in declared if t.startswith(AGENT_PREFIX)]
        out.append(
            _function(
                "read_artifact",
                "Read a published artifact of this run by type.",
                {
                    "type": "object",
                    "properties": {"type": {"type": "string"}, "by": {"type": "string"}},
                    "required": ["type"],
                },
            )
        )
        out.append(
            _function(
                "read_file",
                "Read a workspace file (shared/..., skills).",
                {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                },
            )
        )
        out.append(
            _function(
                "list_files",
                "List workspace files under a prefix.",
                {
                    "type": "object",
                    "properties": {"prefix": {"type": "string"}},
                    "required": ["prefix"],
                },
            )
        )
        if ctx.spec is not None and len(ctx.spec.skills) > 1:
            out.append(
                _function(
                    "load_skill",
                    "Load one of your skills' instructions.",
                    {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                        "required": ["name"],
                    },
                )
            )
        if agents:
            out.append(
                _function(
                    "spawn_agent",
                    "Start an agent; wait_for it, then read its artifact.",
                    {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "agent": {"type": "string", "enum": agents},
                            "input": {"type": "object"},
                        },
                        "required": ["name", "agent"],
                    },
                )
            )
            out.append(
                _function(
                    "wait_for",
                    "Wait until the named agents end.",
                    {
                        "type": "object",
                        "properties": {"names": {"type": "array", "items": {"type": "string"}}},
                        "required": ["names"],
                    },
                )
            )
        out.append(
            _function(
                "ask_person",
                "Ask a person and wait for the answer.",
                {
                    "type": "object",
                    "properties": {
                        "question": {"type": "string"},
                        "options": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["question"],
                },
            )
        )
        return out

    def _boundary(self, ctx, data, name: str, arguments: dict[str, Any]) -> Any:
        if name == "spawn_agent":
            child = str(arguments.get("name") or "")
            agent = str(arguments.get("agent") or "")
            if f"{AGENT_PREFIX}{agent}" not in self.model_tools(ctx):
                return {"error": f"you cannot spawn {agent!r}"}
            if not re.fullmatch(r"[a-z0-9_-]{1,40}", child) or child in ctx.record.children:
                return {"error": f"choose a new child name (a-z, 0-9, _ or -), not {child!r}"}
            data["children"] += 1
            if data["children"] > self.max_children:
                raise RuntimeError(f"more than {self.max_children} children")
            return Spawn(
                {child: (f"{AGENT_PREFIX}{agent}", dict(arguments.get("input") or {}))},
                then="wait_result",
            )
        if name == "wait_for":
            names = [n for n in arguments.get("names") or [] if n in ctx.record.children]
            if not names:
                return {"error": "name children you spawned"}
            return Wait(join=names, then="wait_result", on_fault="wait_result")
        if name == "ask_person":
            options = [str(o) for o in arguments.get("options") or []]
            return Wait(
                answer={
                    "name": f"q{data['turns']}-{data['cursor']}",
                    "question": str(arguments.get("question") or ""),
                    "schema": {"enum": options} if options else {},
                },
                then="wait_result",
            )
        return Wait(sleep=float(arguments.get("seconds") or 1), then="wait_result")

    async def _call_tool(self, ctx, name: str, arguments: dict[str, Any]) -> Any:
        try:
            if name == "read_artifact":
                artifact = await ctx.read(str(arguments.get("type")), by=arguments.get("by"))
                return artifact.model_dump(exclude_defaults=True)
            if name == "read_file":
                return await ctx.files.read_text(str(arguments.get("path")))
            if name == "list_files":
                return await ctx.files.list(str(arguments.get("prefix") or ""))
            if name == "load_skill":
                return await ctx.skill(str(arguments.get("name")))
            declared = {
                model_name_of(t): t for t in self.model_tools(ctx) if not t.startswith(AGENT_PREFIX)
            }
            if name not in declared:
                return {"error": f"unknown tool {name!r}"}
            return await ctx.call(declared[name], **dict(arguments or {}))
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}


class SkillAgent(AgenticAgent):
    """The general agent spawned for a skill with a contract: the skill is its instruction."""


class Decider(AgenticAgent):
    """Answers one question: input {question, facts, schema, default, skill?}; output `decision`
    {answer, reason}. Its parent joins it and reads the decision, or uses its default on a fault."""

    output_type = "decision"
    offered = ()

    async def task_instruction(self, ctx) -> str:
        return str(ctx.input.get("instruction") or "Decide the question from the facts given.")

    async def task(self, ctx) -> str:
        spec = {k: ctx.input.get(k) for k in ("question", "facts", "schema")}
        return (
            json.dumps(spec, ensure_ascii=False, indent=1)
            + '\n\nAnswer as {"answer": <matching the schema>, "reason": "<one sentence>"}.'
        )

    def check_output(self, ctx, data: dict[str, Any], payload: Any) -> str | None:
        if "answer" not in payload:
            return "no answer field"
        schema = ctx.input.get("schema") or {}
        options = schema.get("enum")
        if options and payload["answer"] not in options:
            return f"the answer must be one of {options}"
        try:
            import jsonschema  # optional: without it the enum check above stands
        except ImportError:
            return None
        try:
            jsonschema.validate(payload["answer"], schema)
        except jsonschema.ValidationError as exc:
            return f"the answer does not match its schema: {exc.message[:300]}"
        return None


__all__ = [
    "AgenticAgent",
    "ChatModel",
    "Decider",
    "Model",
    "ModelError",
    "ScriptedModel",
    "SkillAgent",
    "call",
    "openrouter",
    "parse_json",
    "say",
]
