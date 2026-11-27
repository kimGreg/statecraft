"""Model-driven agents as ordinary state machines (architecture §12.2).

    prepare -> think -> act_one -> act_one -> ... -> think -> ... -> finish -> Done
                 |        |                                            |
                 |        +- a boundary tool (spawn_agent, wait_for,     +- a follow-up waits:
                 |           ask_person, sleep) returns Spawn/Wait;        think again
                 |           wait_result adds its result
                 +- the context is over budget: compact -> think

`think` calls a model tool (`model.<name>`) through `ctx.call`; `act_one` calls one tool the model
chose. So the runtime checkpoints between model turns and between tool calls: a crash reruns only
the unfinished turn or call, and every call is checked, retried by policy and recorded like any
other. The conversation is in `data`; the model's private reasoning is never kept.

The conversation is the agent's durable history; a model turn is sent a projection of it
(`context.project`): the system prompt and the task, then a summary of compacted messages, then
the rest. When the projection is over the context budget, `compact` (its own state, so its model
call is checkpointed like a turn) summarizes the oldest messages that can be cut without
separating a tool call from its result. Nothing is deleted from the history.

People steer a running agent with USR signals (durable, like every signal):
    ("USR", {"type": "steer", "text": "..."})       read before the next model turn (after the
                                                    tool calls of the current turn)
    ("USR", {"type": "follow_up", "text": "..."})   kept until the agent would finish; then it
                                                    goes on with it instead (the last answer is
                                                    the output)
Any other USR message goes to `on_signal`.

Settings come from the definition's `extensions.agentic`:

    model: default                  # which model tool to use (`model.default`)
    instruction: ...                # else the class's `instruction`, else the first skill
    tools: [search.query]           # tools offered to the model (within the agent's grant)
    skills: [literature-review]     # pinned at start (see `skills`)
    output: report                  # the artifact type of the final answer (else the first write)
    context:
      max_tokens: 60000             # the context budget (none: never compacted)
      keep_tokens: 15000            # recent messages kept as they are (default: a quarter)
      files: [shared/AGENTS.md]     # workspace files given as project context (when present)
"""

from __future__ import annotations

import json
import re
from typing import Any, ClassVar

from statecraft import Agent, Done, Spawn, Wait, state

from .context import cut_for, estimate, project, summary_request

GENERAL = """You are one agent in a larger procedure. Work only with your tools.
- Computed values (coordinates, distances, areas, counts) come from tools; never invent them.
- `spawn_agent` starts another agent; `wait_for` waits until it ends; its output is then read
  with `read_artifact`. `ask_person` asks a person and waits for the answer.
- When you are done, answer with your final result only{output}."""

BOUNDARY = {"spawn_agent", "wait_for", "ask_person", "sleep"}
BUILTIN = {"read_artifact", "read_file", "list_files", "load_skill"}
RESULT_CHARS = 12_000
_JSON_BLOCK = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)
SKILLS_SERVICE = "agentic.skills"


def _function(name: str, description: str, schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {"name": name, "description": description[:1000], "parameters": schema},
    }


def model_name_of(effect: str) -> str:
    """Tool names a model sees: `search.query` -> `search__query`."""
    return effect.replace(".", "__")


def _clip(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= RESULT_CHARS else text[:RESULT_CHARS] + " ...[cut]"


def parse_json(text: str) -> Any:
    block = _JSON_BLOCK.search(text or "")
    return json.loads(block.group(1) if block else text)


class AgenticAgent(Agent):
    start = "prepare"

    #: The task instruction; None: `extensions.agentic.instruction`, else the first skill's text.
    instruction: ClassVar[str | None] = None
    #: The model tool's name (`model.<model>`) unless the definition names one.
    model: ClassVar[str] = "default"
    #: The artifact type of the final answer (None: settings `output`, else the first write).
    output_type: ClassVar[str | None] = None
    json_output: ClassVar[bool] = True
    max_model_turns: ClassVar[int] = 24
    max_tool_calls: ClassVar[int] = 48
    max_children: ClassVar[int] = 8
    max_output_retries: ClassVar[int] = 2
    #: The context budget in tokens (None: never compacted); `context.max_tokens` overrides it.
    context_tokens: ClassVar[int | None] = None
    #: Workspace files given as project context when present; `context.files` overrides it.
    context_files: ClassVar[tuple[str, ...]] = ()

    # ---- hooks a subclass may override ------------------------------------------------------
    async def task(self, ctx) -> str:
        """The first user message: the spawn input."""
        return json.dumps(ctx.input, ensure_ascii=False, indent=1)

    async def task_instruction(self, ctx, data) -> str:
        if self.instruction is not None:
            return self.instruction
        settings = ctx.extension("agentic")
        if settings.get("instruction"):
            return str(settings["instruction"])
        skills = list(data.get("available_skills") or {})
        if skills:
            return await self._skill_text(ctx, data, skills[0])
        return ""

    def check_output(self, ctx, data: dict[str, Any], payload: Any) -> str | None:
        """A problem with the final answer (asked again), or None."""
        return None

    def shape_output(self, ctx, data: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        return payload

    def skills_of(self, ctx) -> list[str]:
        return list(ctx.extension("agentic").get("skills") or [])

    def on_signal(self, ctx, data: dict[str, Any], message: Any) -> None:
        """A USR message that is neither a steer nor a follow-up (ignored by default)."""
        return None

    # ---- states -----------------------------------------------------------------------------
    @state
    async def prepare(self, ctx, data):
        catalog = ctx.service(SKILLS_SERVICE)
        names = self.skills_of(ctx)
        if names and catalog is None:
            raise RuntimeError("skills are configured but the agentic extension has no catalog")
        data["available_skills"] = catalog.pin(names) if names else {}
        for name in names:
            await catalog.keep(ctx, name)  # the immutable copy recovery reloads
        output = self._output_type(ctx)
        shape = " as one JSON object" if self.json_output else ""
        others = list(data["available_skills"])[1:]
        system = "\n\n".join(
            p
            for p in (
                await self.task_instruction(ctx, data),
                GENERAL.format(output=shape),
                ("Skills you can load with `load_skill`: " + ", ".join(others)) if others else "",
                await self._project_context(ctx),
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
            compaction=None,
            follow_ups=[],
            steered=0,
        )
        return "think"

    @state
    async def think(self, ctx, data):
        if data["turns"] >= self.max_model_turns:
            raise RuntimeError(f"more than {self.max_model_turns} model turns")
        self._take_signals(ctx, data)
        budget, keep = self._budget(ctx)
        over = budget is not None and self._context_tokens(data) > budget
        if over and cut_for(data["messages"], data.get("compaction"), keep) is not None:
            return "compact"
        sent = project(data["messages"], data.get("compaction"))
        reply = await ctx.call(
            f"model.{self._model(ctx)}", messages=sent, tools=self._tools(ctx, data)
        )
        data["turns"] += 1
        self._count(data, reply)
        prompt = (reply.get("usage") or {}).get("prompt_tokens")
        data["last_prompt"] = {
            "tokens": prompt if isinstance(prompt, int) else estimate(sent),
            "upto": len(data["messages"]),
            "compactions": (data.get("compaction") or {}).get("count", 0),
        }
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
    async def compact(self, ctx, data):
        """Summarize the oldest messages that can be cut; the history keeps them."""
        _budget, keep = self._budget(ctx)
        earlier = data.get("compaction")
        cut = cut_for(data["messages"], earlier, keep)
        if cut is None:
            return "think"
        reply = await ctx.call(
            f"model.{self._model(ctx)}",
            messages=summary_request(data["messages"], earlier, cut),
            tools=[],
        )
        self._count(data, reply)
        summary = str(reply.get("content") or "").strip()
        if not summary:
            raise RuntimeError("the model gave no summary to compact the context with")
        data["compaction"] = {
            "covered_until": cut,
            "summary": summary,
            "count": (earlier or {}).get("count", 0) + 1,
        }
        data.pop("last_prompt", None)
        return "think"

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
        return self._answer(data, await self._call_tool(ctx, data, name, arguments))

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
        self._take_signals(ctx, data)
        if not problem and data["follow_ups"]:
            text = data["follow_ups"].pop(0)
            data["messages"].append({"role": "user", "content": text})
            data["final"] = None
            return "think"
        if problem:
            data["retries"] += 1
            if data["retries"] > self.max_output_retries:
                raise RuntimeError(f"no usable final answer: {problem}")
            data["messages"].append(
                {
                    "role": "user",
                    "content": f"Your final answer cannot be used ({problem}). Answer again"
                    + (" with one JSON object." if self.json_output else "."),
                }
            )
            return "think"
        if data["output"]:
            await ctx.write(
                data["output"],
                self.shape_output(ctx, data, payload),
                summary={
                    "turns": data["turns"],
                    "tool_calls": data["calls"],
                    "usage": data["usage"],
                },
            )
        return Done

    # ---- helpers ----------------------------------------------------------------------------
    def _count(self, data: dict[str, Any], reply: dict[str, Any]) -> None:
        for key, value in (reply.get("usage") or {}).items():
            short = {"prompt_tokens": "prompt", "completion_tokens": "completion"}.get(key)
            if short and isinstance(value, int):
                data["usage"][short] += value

    def _take_signals(self, ctx, data: dict[str, Any]) -> None:
        """Steers become user messages now; follow-ups wait until the agent would finish."""
        for message in ctx.signals:
            kind = message.get("type") if isinstance(message, dict) else None
            text = str(message.get("text") or "") if isinstance(message, dict) else ""
            if kind == "steer" and text:
                data["messages"].append({"role": "user", "content": text})
                data["steered"] = data.get("steered", 0) + 1
            elif kind == "follow_up" and text:
                data.setdefault("follow_ups", []).append(text)
            else:
                self.on_signal(ctx, data, message)

    def _budget(self, ctx) -> tuple[int | None, int]:
        settings = ctx.extension("agentic").get("context") or {}
        budget = settings.get("max_tokens", self.context_tokens)
        if budget is None:
            return None, 0
        return int(budget), int(settings.get("keep_tokens") or int(budget) // 4)

    def _context_tokens(self, data: dict[str, Any]) -> int:
        """The projection's size: the tokens the model reported for the previous turn plus an
        estimate of what was added since; an estimate of the whole after a compaction."""
        last = data.get("last_prompt")
        count = (data.get("compaction") or {}).get("count", 0)
        if last and last.get("compactions") == count and last["upto"] <= len(data["messages"]):
            return int(last["tokens"]) + estimate(data["messages"][last["upto"] :])
        return estimate(project(data["messages"], data.get("compaction")))

    async def _project_context(self, ctx) -> str:
        settings = ctx.extension("agentic").get("context") or {}
        found = []
        for path in settings.get("files", self.context_files) or ():
            try:
                text = await ctx.files.read_text(str(path))
            except (FileNotFoundError, KeyError, PermissionError):
                continue
            found.append(f"Project context ({path}):\n{text.strip()}")
        return "\n\n".join(found)

    def _model(self, ctx) -> str:
        return str(ctx.extension("agentic").get("model") or self.model)

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
        settings = ctx.extension("agentic")
        if settings.get("output"):
            return str(settings["output"])
        writes = ctx.definition.writes
        return writes[0] if writes else None

    def offered(self, ctx) -> list[str]:
        """Tools offered to the model: the settings' `tools`, within the agent's grant; model
        tools are never offered."""
        granted = [t for t in ctx.tools() if not t.startswith("model.")]
        wanted = ctx.extension("agentic").get("tools")
        return granted if wanted is None else [t for t in wanted if t in granted]

    def spawnable(self, ctx) -> list[str]:
        return [s for s in ctx.definition.spawns if s != "*"]

    def _tools(self, ctx, data) -> list[dict[str, Any]]:
        out = []
        for name in self.offered(ctx):
            spec = ctx.tool_spec(name) or {}
            out.append(
                _function(
                    model_name_of(name),
                    spec.get("description") or name,
                    spec.get("input_schema") or {"type": "object"},
                )
            )
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
                "Read a workspace file (shared/...).",
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
        if len(data.get("available_skills") or {}) > 1:
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
        spawnable = self.spawnable(ctx)
        if spawnable:
            out.append(
                _function(
                    "spawn_agent",
                    "Start an agent; wait_for it, then read its artifact.",
                    {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "agent": {"type": "string", "enum": spawnable},
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
            if agent not in self.spawnable(ctx):
                return {"error": f"you cannot spawn {agent!r}"}
            if not re.fullmatch(r"[a-z0-9_-]{1,40}", child) or child in ctx.children:
                return {"error": f"choose a new child name (a-z, 0-9, _ or -), not {child!r}"}
            data["children"] += 1
            if data["children"] > self.max_children:
                raise RuntimeError(f"more than {self.max_children} children")
            return Spawn({child: (agent, dict(arguments.get("input") or {}))}, then="wait_result")
        if name == "wait_for":
            names = [n for n in arguments.get("names") or [] if n in ctx.children]
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

    async def _skill_text(self, ctx, data, name: str) -> str:
        pinned = (data.get("available_skills") or {}).get(name)
        if pinned is None:
            raise KeyError(f"skill {name!r} is not available to this agent")
        return await ctx.service(SKILLS_SERVICE).read(ctx, pinned)

    async def _call_tool(self, ctx, data, name: str, arguments: dict[str, Any]) -> Any:
        try:
            if name == "read_artifact":
                artifact = await ctx.read(str(arguments.get("type")), by=arguments.get("by"))
                return artifact.model_dump(exclude_defaults=True)
            if name == "read_file":
                return await ctx.files.read_text(str(arguments.get("path")))
            if name == "list_files":
                return await ctx.files.list(str(arguments.get("prefix") or ""))
            if name == "load_skill":
                return await self._skill_text(ctx, data, str(arguments.get("name")))
            offered = {model_name_of(t): t for t in self.offered(ctx)}
            if name not in offered:
                return {"error": f"unknown tool {name!r}"}
            return await ctx.call(offered[name], **dict(arguments or {}))
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}


__all__ = ["BOUNDARY", "AgenticAgent", "model_name_of", "parse_json"]
