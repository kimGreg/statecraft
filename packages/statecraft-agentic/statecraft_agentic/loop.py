"""Model-driven agents as ordinary state machines (architecture §12.2).

    prepare -> think -> act_one -> act_one -> ... -> think -> ... -> finish -> Done
                 |        |                                            |
                 |        +- a boundary tool (spawn_agent, join_agents,  +- a follow-up waits:
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

Children are processes the model manages (when the agent may spawn): `spawn_agent` starts one and
returns at once (the parent goes on working: spawning is asynchronous); `list_agents` and
`inspect_agent` show their phase and what they published; `join_agents` waits for the ones whose
results it needs (a waiting parent holds no execution slot); `signal_agent` stops, continues,
ends or steers one. A spawn the runtime would refuse (not permitted, over the tree's limits) is
told to the model instead. An agent may not finish while a child is live (the runtime faults it):
it is told once before it tries, to join the children or end them.

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

import contextlib
import json
import re
from typing import Any, ClassVar

from statecraft import Agent, Done, EffectError, Spawn, Wait, state

from .context import boundaries, covered, cut_for, estimate, project, summary_request

#: how many times a compaction halves its chunk when the summary request itself overflows
COMPACTION_SHRINKS = 4


def chunk_end(
    messages: list[dict[str, Any]], compaction: dict[str, Any] | None, cut: int, limit: int | None
) -> int:
    """How far one summary request reaches: the furthest message boundary up to `cut` whose
    request fits `limit` tokens (the nearest boundary when none fits; `cut` without a limit)."""
    if limit is None:
        return cut
    start = covered(compaction)
    candidates = [i for i in boundaries(messages, start) if start < i <= cut] or [cut]
    fitting = [i for i in candidates if estimate(summary_request(messages, compaction, i)) <= limit]
    return max(fitting) if fitting else min(candidates)


GENERAL = """You are one agent in a larger procedure. Work only with the tools you are given.
- Computed values (coordinates, distances, areas, counts) come from tools; never invent them.
- When you are done, answer with your final result only{output}."""

COLLABORATE = """Working with other agents:
- `spawn_agent` delegates an independent task and returns at once: the child runs on its own while
  you go on with your own work. Give it a precise task and the shared file paths it needs; it does
  not see your conversation.
- All agents share the workspace (`shared/...`); each has its own conversation. Agree on who
  writes which file: two agents must not write the same file.
- `list_agents` / `inspect_agent` show how your children are doing; `read_artifact` with `by` reads
  what one published, also before it ended.
- `join_agents` only when you need results you do not have yet; never wait without a reason.
- `signal_agent` steers a child (`steer`, `follow_up`) or controls it (STOP, CONT, TERM, KILL).
- You cannot finish while a child is live: join the children you still need and end the
  others (TERM) first."""

BOUNDARY = {"spawn_agent", "join_agents", "wait_for", "ask_person", "sleep"}
#: Tools that manage children (offered when the agent may spawn); answered within the step.
CHILD_TOOLS = {"list_agents", "inspect_agent", "signal_agent"}
SIGNALS = {"STOP", "CONT", "TERM", "KILL"}
BUILTIN = {"read_artifact", "read_file", "list_files", "load_skill"}
RESULT_CHARS = 12_000
ENDED = ("succeeded", "terminated")
#: Model calls over its context window in a row that compaction may answer before a fault.
MAX_OVERFLOWS = 2
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

    def loadable_skills(self, ctx, data: dict[str, Any]) -> list[str]:
        """The pinned skills the model loads with `load_skill`: all of them, except the first
        when it is the task instruction (no class or configured instruction)."""
        skills = list(data.get("available_skills") or {})
        if self.instruction is None and not ctx.extension("agentic").get("instruction"):
            return skills[1:]
        return skills

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
        others = self.loadable_skills(ctx, data)
        system = "\n\n".join(
            p
            for p in (
                await self.task_instruction(ctx, data),
                GENERAL.format(output=shape),
                ("Skills you can load with `load_skill`: " + ", ".join(others)) if others else "",
                COLLABORATE if self.spawnable(ctx) else "",
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
        self._take_signals(ctx, data)  # an `extend` sent with a CONT counts before the check
        if data["turns"] >= self.max_model_turns + data.get("extra_turns", 0):
            raise RuntimeError(
                f"more than {self.max_model_turns + data.get('extra_turns', 0)} model turns "
                '(raise it: USR {"type": "extend", "turns": n}, then CONT)'
            )
        budget, keep = self._budget(ctx)
        over = budget is not None and self._context_tokens(data) > budget
        if over and cut_for(data["messages"], data.get("compaction"), keep) is not None:
            return "compact"
        sent = project(data["messages"], data.get("compaction"))
        try:
            reply = await ctx.call(
                f"model.{self._model(ctx)}", messages=sent, tools=self._tools(ctx, data)
            )
        except EffectError as exc:
            if exc.code != "context_overflow" or data.get("overflows", 0) >= MAX_OVERFLOWS:
                raise
            data["overflows"] = data.get("overflows", 0) + 1
            data["force_compact"] = True  # the model's window is smaller than the budget
            return "compact"
        data["overflows"] = 0
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
            limit = self.max_tool_calls + data.get("extra_calls", 0)
            if data["calls"] + len(calls) > limit:  # checked here, after an `extend` was read
                raise RuntimeError(
                    f"more than {limit} tool calls "
                    '(raise it: USR {"type": "extend", "tool_calls": n}, then CONT)'
                )
            data.update(pending=calls, cursor=0)
            return "act_one"
        data["final"] = content
        return "finish"

    @state
    async def compact(self, ctx, data):
        """Summarize the oldest messages that can be cut; the history keeps them."""
        _budget, keep = self._budget(ctx)
        forced = data.pop("force_compact", False)
        earlier = data.get("compaction")
        cut = cut_for(data["messages"], earlier, 0 if forced else keep)
        if cut is None:
            if forced:
                raise RuntimeError("over the model's context window, and nothing to compact")
            return "think"
        budget, _keep = self._budget(ctx)
        limit = budget or None  # a summary request fits the context window the budget stands for
        end = chunk_end(data["messages"], earlier, cut, limit)
        for _ in range(COMPACTION_SHRINKS + 1):
            try:
                reply = await ctx.call(
                    f"model.{self._model(ctx)}",
                    messages=summary_request(data["messages"], earlier, end),
                    tools=[],
                )
                break
            except EffectError as exc:  # the summary request itself is too big: a smaller chunk
                smaller = chunk_end(
                    data["messages"],
                    earlier,
                    end,
                    estimate(summary_request(data["messages"], earlier, end)) // 2,
                )
                if exc.code != "context_overflow" or smaller >= end:
                    raise
                end = smaller
        else:  # pragma: no cover - the loop always breaks or raises
            raise RuntimeError("compaction could not fit the model's context window")
        self._count(data, reply)
        summary = str(reply.get("content") or "").strip()
        if not summary:
            raise RuntimeError("the model gave no summary to compact the context with")
        data["compaction"] = {
            "covered_until": end,
            "summary": summary,
            "count": (earlier or {}).get("count", 0) + 1,
        }
        data.pop("last_prompt", None)
        if end < cut:  # summarized in chunks: the next chunk, with the summary so far
            data["force_compact"] = forced
            return "compact"
        return "think"

    @state
    async def act_one(self, ctx, data):
        call_ = data["pending"][data["cursor"]]
        data["calls"] += 1

        name, arguments = call_["name"], call_["arguments"]
        offered = {t["function"]["name"] for t in self._tools(ctx, data)}
        if name == "wait_for" and "join_agents" in offered:
            name = "join_agents"  # the earlier name of join_agents
        if name not in offered:  # what the model was not offered is never run
            return self._answer(data, {"error": f"unknown tool {name!r}: not offered to you"})
        if not isinstance(arguments, dict):  # the model's mistake: told, not a fault
            return self._answer(data, {"error": f"{name}: arguments must be a JSON object"})
        if name in BOUNDARY:
            try:
                boundary = await self._boundary(ctx, data, name, arguments)
            except (TypeError, ValueError, AttributeError, KeyError) as exc:
                return self._answer(
                    data, {"error": f"{name}: bad arguments: {type(exc).__name__}: {exc}"[:500]}
                )
            if isinstance(boundary, (Spawn, Wait)):
                data["boundary"] = {"name": name, "arguments": arguments}
                return boundary
            return self._answer(data, boundary)
        # the call's identity comes from the committed model reply: the same call when this
        # state runs again, a different one for every other tool call
        identity = f"tool/{call_.get('id') or data['cursor']}"
        return self._answer(data, await self._call_tool(ctx, data, name, arguments, identity))

    @state
    async def wait_result(self, ctx, data):
        boundary = data.pop("boundary")
        woke = ctx.woke
        if boundary["name"] == "spawn_agent":
            result: Any = {"spawned": boundary["arguments"].get("name")}
        elif boundary["name"] in ("join_agents", "wait_for"):
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
            if not self.json_output and not text.strip():
                raise ValueError("the answer is empty: say what you did and found")
            problem = self.check_output(ctx, data, payload)
        except (ValueError, json.JSONDecodeError) as exc:
            problem = f"{type(exc).__name__}: {exc}"
            payload = None
        steered = data.get("steered", 0)
        self._take_signals(ctx, data)
        if data.get("steered", 0) > steered:  # a steer came as it finished: the model sees it
            data["final"] = None
            return "think"
        if not problem:
            live = [c for c in await ctx.children_view() if c.get("phase") not in ENDED]
            told = set(data.get("children_told") or [])  # names it was told about (once each)
            new = [c for c in live if c["name"] not in told]
            if new:
                data["children_told"] = sorted(told | {c["name"] for c in live})
                names = ", ".join(f"{c['name']} ({c['phase']})" for c in live)
                data["messages"].append(
                    {
                        "role": "user",
                        "content": f"Before you finish: children still live: {names}. Join the "
                        "ones whose results you need and end the others (signal_agent TERM); "
                        "finishing with a live child is a fault.",
                    }
                )
                return "think"
        if not problem and data["follow_ups"]:
            text = data["follow_ups"].pop(0)
            data["messages"].append({"role": "user", "content": text})
            data["final"] = None
            return "think"
        if problem:
            data["retries"] += 1
            if data["retries"] > self.max_output_retries + data.get("extra_retries", 0):
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
        return await self.answered(ctx, data, payload)

    async def answered(self, ctx, data: dict[str, Any], payload: dict[str, Any]) -> Any:
        """What follows a usable final answer (published): the agent ends (`Done`). A subclass
        may wait for more instead (a session)."""
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
            elif kind == "extend":
                self._extend(data, message)
            else:
                self.on_signal(ctx, data, message)

    @staticmethod
    def _extend(data: dict[str, Any], message: dict[str, Any]) -> None:
        """An operator raises the loop's limits (then CONTs a fault the limit made)."""
        for field, extra in (
            ("turns", "extra_turns"),
            ("tool_calls", "extra_calls"),
            ("retries", "extra_retries"),
        ):
            with contextlib.suppress(TypeError, ValueError):
                data[extra] = data.get(extra, 0) + max(0, int(message.get(field) or 0))

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

    def _spawn_menu(self, ctx, spawnable: list[str]) -> str:
        """One line per agent it may spawn: its description and its input schema."""
        lines = []
        for target in spawnable:
            info = ctx.spawn_target(target) or {}
            line = f"- {target}: {(info.get('description') or '').strip()[:300]}"
            if info.get("input_schema"):
                line += f" Input schema: {json.dumps(info['input_schema'], ensure_ascii=False)}"
            lines.append(line)
        return "\n".join(lines)

    async def _child_tool(self, ctx, name: str, arguments: dict[str, Any]) -> Any:
        children = await ctx.children_view()
        if name == "list_agents":
            keys = ("name", "agent", "phase", "published", "error")
            return [{k: c.get(k) for k in keys if c.get(k) is not None} for c in children]
        child = next((c for c in children if c["name"] == arguments.get("name")), None)
        if child is None:
            return {"error": f"no child {arguments.get('name')!r}; yours: {sorted(ctx.children)}"}
        if name == "inspect_agent":
            return {k: v for k, v in child.items() if v not in (None, [], "")}
        if child.get("phase") in ENDED:
            return {"error": f"{child['name']} has ended ({child['phase']}): it takes no signals"}
        signal = str(arguments.get("signal") or "")
        if signal in SIGNALS:
            ctx.signal(child["name"], signal)
        elif signal in ("steer", "follow_up") and arguments.get("text"):
            ctx.signal(child["name"], ("USR", {"type": signal, "text": str(arguments["text"])}))
        else:
            return {"error": "signal is STOP, CONT, TERM, KILL, or steer/follow_up with text"}
        return {"sent": signal, "to": child["name"], "after": "this step commits"}

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
        if self.loadable_skills(ctx, data):
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
            names = {"type": "array", "items": {"type": "string"}}
            out += [
                _function(
                    "spawn_agent",
                    "Start a child agent on a task. Returns at once; the child runs while you "
                    "go on. Its input is all it knows of the task. Agents:\n"
                    + self._spawn_menu(ctx, spawnable),
                    {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "description": "a new name: a-z 0-9 _ -"},
                            "agent": {"type": "string", "enum": spawnable},
                            "input": {
                                "type": "object",
                                "description": "everything the child gets: its task and the "
                                "facts and file paths it needs",
                            },
                        },
                        "required": ["name", "agent", "input"],
                    },
                ),
                _function(
                    "list_agents",
                    "Your children: each one's phase (running, waiting, faulted, stopped, "
                    "succeeded, terminated) and what it published.",
                    {"type": "object", "properties": {}},
                ),
                _function(
                    "inspect_agent",
                    "One child in detail: phase, state, step, published artifacts, its error, an "
                    "open question, its own children.",
                    {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                        "required": ["name"],
                    },
                ),
                _function(
                    "join_agents",
                    "Wait until the named children end (you pause meanwhile). Only for results "
                    "you need now.",
                    {"type": "object", "properties": {"names": names}, "required": ["names"]},
                ),
                _function(
                    "signal_agent",
                    "Send a child a signal: STOP, CONT, TERM, KILL; or `steer` (a new instruction "
                    "for its next turn) or `follow_up` (more work after it would finish), with "
                    "`text`.",
                    {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "signal": {
                                "type": "string",
                                "enum": [*sorted(SIGNALS), "steer", "follow_up"],
                            },
                            "text": {"type": "string"},
                        },
                        "required": ["name", "signal"],
                    },
                ),
            ]
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

    async def _boundary(self, ctx, data, name: str, arguments: dict[str, Any]) -> Any:
        if name == "spawn_agent":
            child = str(arguments.get("name") or "")
            agent = str(arguments.get("agent") or "")
            if agent not in self.spawnable(ctx):
                return {"error": f"you cannot spawn {agent!r}"}
            if not re.fullmatch(r"[a-z0-9_-]{1,40}", child) or child in ctx.children:
                return {"error": f"choose a new child name (a-z, 0-9, _ or -), not {child!r}"}
            if data["children"] >= self.max_children:
                return {"error": f"you may start at most {self.max_children} children"}
            given = arguments.get("input")
            if not isinstance(given, dict):
                return {"error": "input must be an object: the child's task and what it needs"}
            refused = await ctx.spawn_problem(agent, input=given)
            if refused:
                return {"error": f"refused: {refused}"}
            data["children"] += 1
            return Spawn({child: (agent, dict(given))}, then="wait_result")
        if name in ("join_agents", "wait_for"):
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

    async def _call_tool(
        self, ctx, data, name: str, arguments: dict[str, Any], identity: str | None = None
    ) -> Any:
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
            if name in CHILD_TOOLS:
                return await self._child_tool(ctx, name, arguments)
            offered = {model_name_of(t): t for t in self.offered(ctx)}
            if name not in offered:
                return {"error": f"unknown tool {name!r}"}
            return await ctx.invoke(offered[name], dict(arguments or {}), call_name=identity)
        except EffectError as exc:  # classified by the runtime: the model decides what next
            return {
                "error": {
                    "class": exc.cls,
                    "code": exc.code or None,
                    "message": exc.message,
                    "may_work_later": exc.cls in ("transient", "timeout", "unavailable"),
                }
            }
        except Exception as exc:
            return {
                "error": {
                    "class": "failed",
                    "message": f"{type(exc).__name__}: {exc}"[:500],
                    "may_work_later": False,
                }
            }


__all__ = ["BOUNDARY", "AgenticAgent", "model_name_of", "parse_json"]
