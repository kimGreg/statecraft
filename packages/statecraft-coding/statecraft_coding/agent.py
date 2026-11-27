"""A coding agent on the agentic harness (roadmap F1).

    agents:
      coder:
        entry: statecraft_coding:CodingAgent
        tools: [model.default, sh.exec]      # sh.exec: the shell effect (local, OpenSandbox, ...)
        writes: [answer]

Input: {"task": "...", "session"?: true} (other keys are shown to the model too). In a session
the agent waits for the next instruction after each answer (a question; an empty answer ends
it), keeping one conversation. The model works in the
project (the workspace's `shared/` folder, the sandbox's `/workspace`) with project-relative
paths, through these tools:

    list_dir(path)                  the entries of one folder
    read_file(path, offset?, limit?) lines with their numbers
    write_file(path, content)       create or replace a file
    edit_file(path, old, new)       replace one exact, unique piece of text
    sh(command)                     a shell command in the project folder (when sh.exec is
                                    granted): exit code and shaped output

What must hold
  * every file change is safe to repeat (a state runs again after a crash): writing the same
    content again changes nothing; an edit keeps a note of the file's digests before and after
    (in the agent's private folder, under the model's tool call) before it changes the file, so
    a repeat reports `already` instead of applying it twice; an edit made against text someone
    else changed meanwhile is refused;
  * a command goes through the granted shell effect, keyed by the model's tool call, so a state
    that runs again never runs it twice (the effect's own contract);
  * long outputs and files are cut to their head and tail with what was cut said;
  * the project's `AGENTS.md` is in the system prompt; nested ones are listed;
  * the shell process is never managed here (that is the shell provider's).
"""

from __future__ import annotations

import hashlib
import posixpath
import json
from typing import Any, ClassVar

from statecraft_agentic import AgenticAgent

from statecraft import Done, EffectError, Wait, state
from statecraft.runtime.workspace import FileChanged, content_digest

from .discovery import ROOT, discover
from .shaping import FILE_CHARS, clip, numbered, shell_result

PROMPT = """You are a careful software engineer working in a project folder through tools.

How you work:
- Look before you change: list folders and read the files you will touch.
- Change files with `edit_file` (one exact, unique piece of old text and its replacement) or
  `write_file` (whole files, for new files). Keep changes small and in the project's style.
- Run the project's tests or checks with `sh` after changing code, and fix what fails.
- Paths are relative to the project root. Never touch files outside it.
- When the task is done (or cannot be done), answer with a short summary of what you changed,
  what you ran and its result, and anything left open."""

SHELL = "sh.exec"
#: the agentic loop's own file tools, replaced by project-relative ones
REPLACED = {"read_file", "list_files"}
CODING_TOOLS = {"list_dir", "read_file", "write_file", "edit_file", "sh"}


def _fn(name: str, description: str, properties: dict[str, Any], required: list[str]) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required},
        },
    }


def project_path(path: Any) -> str:
    text = str(path or "").strip().lstrip("/")
    if text.startswith("workspace/"):  # a sandbox path
        text = text.removeprefix("workspace/")
    if text in ("", "."):
        return ROOT.rstrip("/")
    found = posixpath.normpath(ROOT + text)
    if found != ROOT.rstrip("/") and not found.startswith(ROOT):
        raise PermissionError(f"{path} is outside the project")
    return found


class CodingAgent(AgenticAgent):
    instruction: ClassVar[str | None] = PROMPT
    json_output: ClassVar[bool] = False
    max_model_turns: ClassVar[int] = 60

    async def answered(self, ctx, data: dict[str, Any], payload: dict[str, Any]) -> Any:
        """In a session (`input.session`), the agent waits for the next instruction after each
        answer, keeping its conversation; an empty instruction ends it."""
        if not ctx.input.get("session"):
            return Done
        data["rounds"] = data.get("rounds", 0) + 1
        return Wait(
            answer={
                "name": f"next-{data['rounds']}",
                "question": "Next instruction (empty to end the session)",
                "schema": {"type": "string"},
            },
            then="next_instruction",
        )

    @state
    async def next_instruction(self, ctx, data):
        text = str(ctx.woke.get("answer") or "").strip()
        if not text:
            return Done
        data["messages"].append({"role": "user", "content": text})
        data["final"] = None
        data["retries"] = 0
        # each instruction has the model-turn allowance of a task
        data["extra_turns"] = data.get("extra_turns", 0) + data.get("turns", 0)
        return "think"

    async def task(self, ctx) -> str:
        given = dict(ctx.input)
        task = given.pop("task", None)
        if task is None:
            return await super().task(ctx)
        extra = "".join(f"\n{k}: {v}" for k, v in given.items())
        return f"{task}{extra}"

    async def _project_context(self, ctx) -> str:
        found = [await discover(ctx.files)]
        extra = await super()._project_context(ctx)
        return "\n\n".join(p for p in (*found, extra) if p)

    def offered(self, ctx) -> list[str]:
        """Effects the model sees as tools of their own: not the shell (it is `sh`)."""
        return [t for t in super().offered(ctx) if t != SHELL]

    def _shell(self, ctx) -> bool:
        return SHELL in ctx.tools()

    def _tools(self, ctx, data) -> list[dict[str, Any]]:
        out = [t for t in super()._tools(ctx, data) if t["function"]["name"] not in REPLACED]
        path = {"type": "string", "description": "a path relative to the project root"}
        out += [
            _fn(
                "list_dir",
                "List one folder of the project (`.` for the root).",
                {"path": path},
                ["path"],
            ),
            _fn(
                "read_file",
                "Read a project file with line numbers. `offset` (1-based line) and `limit` "
                "(lines) read part of a long file.",
                {"path": path, "offset": {"type": "integer"}, "limit": {"type": "integer"}},
                ["path"],
            ),
            _fn(
                "write_file",
                "Create a file, or replace a whole file, with `content`.",
                {"path": path, "content": {"type": "string"}},
                ["path", "content"],
            ),
            _fn(
                "edit_file",
                "Replace `old` with `new` in a file. `old` must appear exactly once: include "
                "enough surrounding lines to make it unique. Prefer this to rewriting a file.",
                {"path": path, "old": {"type": "string"}, "new": {"type": "string"}},
                ["path", "old", "new"],
            ),
        ]
        if self._shell(ctx):
            out.append(
                _fn(
                    "sh",
                    "Run a shell command in the project root (tests, builds, git, grep). "
                    "Returns its exit code and output (long output is cut in the middle). "
                    "Commands do not share state: `cd` does not last; no interactive input.",
                    {"command": {"type": "string"}},
                    ["command"],
                )
            )
        return out

    async def _call_tool(
        self, ctx, data, name: str, arguments: dict[str, Any], identity: str | None = None
    ) -> Any:
        if name not in CODING_TOOLS:
            return await super()._call_tool(ctx, data, name, arguments, identity)
        try:
            return await self._coding_tool(ctx, name, arguments, identity)
        except EffectError as exc:
            return {"error": {"class": exc.cls, "message": exc.message}}
        except FileNotFoundError:
            return {"error": f"no such file: {arguments.get('path')}"}
        except (PermissionError, ValueError, TypeError, UnicodeDecodeError) as exc:
            return {"error": f"{type(exc).__name__}: {exc}"[:500]}

    async def _coding_tool(
        self, ctx, name: str, arguments: dict[str, Any], identity: str | None
    ) -> Any:
        files = ctx.files
        if name == "sh":
            if not self._shell(ctx):
                return {"error": "no shell is granted to this agent"}
            command = str(arguments.get("command") or "")
            result = await ctx.invoke(SHELL, {"command": command}, call_name=identity)
            return shell_result(result)
        path = project_path(arguments.get("path"))
        if name == "list_dir":
            prefix = path.rstrip("/") + "/"
            entries = set()
            for found in await files.list(prefix):
                rest = found.removeprefix(prefix)
                head, sep, _ = rest.partition("/")
                entries.add(head + "/" if sep else head)
            return sorted(entries) or "an empty or missing folder"
        if name == "read_file":
            text = await files.read_text(path)
            lines = text.split("\n")
            offset = max(int(arguments.get("offset") or 1), 1)
            limit = arguments.get("limit")
            chosen = lines[offset - 1 : offset - 1 + int(limit)] if limit else lines[offset - 1 :]
            shown = numbered("\n".join(chosen), offset)
            if len(shown) > FILE_CHARS:
                shown = clip(shown, FILE_CHARS) + "\n(read the rest with offset and limit)"
            return shown
        if name == "write_file":
            content = str(arguments.get("content") or "")
            before = await files.digest(path)
            if before == content_digest(content.encode("utf-8")):
                return f"already: {arguments.get('path')} has this content"
            await files.write(path, content)
            return f"wrote {arguments.get('path')} ({len(content.encode('utf-8'))} bytes)"
        if name == "edit_file":
            return await self._edit(ctx, path, arguments, identity)
        return {"error": f"unknown tool {name}"}

    async def _edit(self, ctx, path: str, arguments: dict[str, Any], identity: str | None) -> Any:
        """Replace one unique `old` with `new`, safe to repeat: before the file changes, the
        edit's file digests before and after are kept in the agent's private folder under the
        model's tool call (stable when the state runs again). A repeat finds that note: the
        file already at the after digest is `already`; at the before digest it is applied
        again; anything else changed meanwhile. (Text alone cannot tell: `new` may contain
        `old`, as when inserting after an anchor.)"""
        files, shown = ctx.files, arguments.get("path")
        old, new = str(arguments.get("old") or ""), str(arguments.get("new") or "")
        if not old:
            return {"error": "`old` must not be empty (use write_file for a new file)"}
        note = (
            f"home/{ctx.id}/private/.edits/"
            + hashlib.sha256(f"{ctx.step}/{identity}".encode()).hexdigest()[:32]
            + ".json"
        )
        text = await files.read_text(path)
        current = content_digest(text.encode("utf-8"))
        try:
            earlier = json.loads(await files.read_text(note))
        except FileNotFoundError:
            earlier = None
        if earlier is not None and earlier.get("path") == path:
            if current == earlier["after"]:
                return f"already: {shown}"
            if current != earlier["before"]:
                return {"error": "the file changed since this edit was made; read it again"}
        count = text.count(old)
        if count == 0:
            if new and new in text:  # nothing to apply; most likely done by an earlier call
                return f"already: {shown} has the new text"
            return {"error": "`old` was not found; read the file and copy the text exactly"}
        if count > 1:
            return {"error": f"`old` appears {count} times; add lines around it"}
        changed = text.replace(old, new, 1)
        after = content_digest(changed.encode("utf-8"))
        await files.write(note, json.dumps({"path": path, "before": current, "after": after}))
        try:
            outcome = await files.edit(path, changed, expected=current)
        except FileChanged:
            return {"error": "the file changed while editing; read it again"}
        return f"{outcome}: {shown}"


__all__ = ["PROMPT", "CodingAgent", "project_path"]
