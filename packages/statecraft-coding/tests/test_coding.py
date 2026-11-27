"""The coding agent (roadmap F1).

What must hold
  * the system prompt carries the project's files, its tooling and its `AGENTS.md`;
  * the model lists, reads (numbered), edits and writes project files with project-relative paths,
    and runs commands through the granted shell effect, with shaped output;
  * file changes are safe to repeat: the same write or an applied edit reports `already`; an
    edit whose old text is missing or not unique is told to the model, not applied;
  * in a session the agent answers, waits for the next instruction, keeps its conversation, and
    ends on an empty one; the terminal session drives that loop;
  * long outputs are cut to head and tail with what was cut said.
"""

from __future__ import annotations

from pathlib import Path

from statecraft_agentic import ScriptedModel, add_model, call, say
from statecraft_agentic import register as agentic
from statecraft_coding import CodingAgent, clip
from statecraft_coding.__main__ import session

from statecraft import EffectDefinition, EffectPolicy, Runtime
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.testing.local_shell import LocalShell

BUGGY = "def double(n):\n    return n + n + 1\n"


def project(tmp_path: Path, root: str) -> Path:
    shared = tmp_path / "ws" / root / "shared"
    (shared / "pkg").mkdir(parents=True)
    (shared / "calc.py").write_text(BUGGY)
    (shared / "pyproject.toml").write_text("[project]\nname = 'calc'\n")
    (shared / "AGENTS.md").write_text("Run the checks before you answer. ALWAYS-SAY-HELLO\n")
    (shared / "pkg" / "AGENTS.md").write_text("pkg rules\n")
    return shared


def runtime(tmp_path: Path, model: ScriptedModel) -> Runtime:
    rt = Runtime(
        store=SqliteStore(tmp_path / "state.db"),
        workspace=LocalWorkspace(tmp_path / "ws"),
        executors=0,
        scan_every=0.01,
    )
    agentic(rt.registrar)
    add_model(rt.registrar, "default", model)
    rt.registrar.providers.register(
        "sh", LocalShell(receipts=tmp_path / "receipts", workspaces=tmp_path / "ws")
    )
    rt.registrar.tools.register(
        EffectDefinition(
            name="sh.exec",
            provider="sh",
            operation="exec",
            policy=EffectPolicy(kind="deduplicated", timeout=30, max_attempts=1),
        )
    )
    rt.registrar.agents.register(
        "coder", CodingAgent, tools=["model.default", "sh.exec"], writes=["answer"]
    )
    return rt


def tool_results(model: ScriptedModel) -> list[str]:
    return [str(m["content"]) for m in model.seen[-1] if m["role"] == "tool"]


async def test_the_agent_fixes_a_bug_with_safe_tools(tmp_path) -> None:
    shared = project(tmp_path, "fix")
    fixed = "    return n + n\n"
    model = ScriptedModel(
        [
            call("list_dir", path="."),
            call("read_file", path="calc.py"),
            call("edit_file", path="calc.py", old="    return n + n + 1\n", new=fixed),
            call("edit_file", path="calc.py", old="    return n + n + 1\n", new=fixed),
            call("edit_file", path="calc.py", old="nothing like this", new="x"),
            call("write_file", path="notes/TODO.md", content="done\n"),
            call("write_file", path="notes/TODO.md", content="done\n"),
            call("sh", command="python3 -c 'from calc import double; assert double(2) == 4'"),
            call("sh", command="python3 -c 'print(\"x\" * 20000)'"),
            say("Fixed double; the check passes."),
        ]
    )
    async with runtime(tmp_path, model) as rt:
        await rt.spawn("coder", {"task": "Fix double in calc.py."}, root_id="fix")
        await rt.run("fix", timeout=30)
        record = await rt.get("fix")
        assert record["view"] == "succeeded", record["error"]
        answer = (await rt.artifact("fix", "answer"))["payload"]
    system = model.seen[0][0]["content"]
    assert "ALWAYS-SAY-HELLO" in system and "Python project" in system
    assert "pkg/AGENTS.md" in system and "calc.py" in system
    offered = set(model.offered[0])
    assert {"list_dir", "read_file", "write_file", "edit_file", "sh"} <= offered
    assert "sh__exec" not in offered and "list_files" not in offered
    results = tool_results(model)
    assert "calc.py" in results[0] and "pkg/" in results[0]
    assert "    2|     return n + n + 1" in results[1]
    assert results[2].startswith("applied") and results[3].startswith("already")
    assert "not found" in results[4]
    assert results[5].startswith("wrote") and results[6].startswith("already")
    assert '"exit": 0' in results[7]
    assert "characters cut" in results[8]
    assert (shared / "calc.py").read_text() == "def double(n):\n" + fixed
    assert (shared / "notes" / "TODO.md").read_text() == "done\n"
    assert answer == {"text": "Fixed double; the check passes."}


async def test_a_session_keeps_its_conversation(tmp_path) -> None:
    project(tmp_path, "s1")
    model = ScriptedModel([say("first done"), say("second done")])
    instructions = iter(["now the second thing", ""])
    config = tmp_path / "statecraft.yaml"

    class Args:
        pass

    args = Args()
    rt = runtime(tmp_path, model)
    printed: list[str] = []

    import statecraft_coding.__main__ as cli

    original = Runtime.from_config
    Runtime.from_config = staticmethod(lambda *a, **k: rt)  # the session uses this runtime
    cli._say = printed.append
    try:
        args.config, args.agent, args.root, args.output = str(config), "coder", "s1", "answer"
        args.task, args.timeout = "do the first thing", 30
        code = await session(args, read=lambda prompt: next(instructions))
    finally:
        Runtime.from_config = original
    assert code == 0
    assert any("first done" in p for p in printed) and any("second done" in p for p in printed)
    assert printed[-1] == "session s1 ended (succeeded)"
    last = model.seen[-1]
    assert [m["content"] for m in last if m["role"] == "user"][-1] == "now the second thing"
    assert any(m.get("content") == "first done" for m in last if m["role"] == "assistant")


def test_long_output_keeps_head_and_tail() -> None:
    text = "\n".join(f"line {n}" for n in range(5000))
    shaped = clip(text, 1000)
    assert shaped.startswith("line 0") and shaped.endswith("line 4999")
    assert "lines" in shaped and "characters cut" in shaped and len(shaped) < 1200


async def test_an_insert_after_an_anchor_is_not_applied_twice() -> None:
    """The same tool call again (its state ran again after a crash): `new` contains `old`, so
    the text alone cannot tell; the edit's note can."""
    from statecraft.runtime.workspace import FileChanged, content_digest

    class Files:
        def __init__(self):
            self.data = {"shared/a.py": "import os\n\ndef main():\n    pass\n"}

        async def read_text(self, path):
            if path not in self.data:
                raise FileNotFoundError(path)
            return self.data[path]

        async def write(self, path, text):
            self.data[path] = text

        async def edit(self, path, text, *, expected):
            if content_digest(self.data[path].encode()) != expected:
                raise FileChanged(path)
            self.data[path] = text
            return "applied"

    class Ctx:
        id, step, files = "c", 3, Files()

    agent = CodingAgent.__new__(CodingAgent)
    args = {"path": "a.py", "old": "import os\n", "new": "import os\nimport sys\n"}
    first = await agent._coding_tool(Ctx, "edit_file", args, "tool/x")
    again = await agent._coding_tool(Ctx, "edit_file", args, "tool/x")
    assert first.startswith("applied") and again.startswith("already")
    assert Ctx.files.data["shared/a.py"].count("import sys") == 1
    other = await agent._coding_tool(Ctx, "edit_file", args, "tool/y")  # a new call: applies
    assert other.startswith("applied")


def test_paths_stay_in_the_project() -> None:
    import pytest
    from statecraft_coding import project_path

    assert project_path("src/a.py") == "shared/src/a.py" and project_path(".") == "shared"
    for outside in ("../home/c/private/x", "src/../../run/views.json"):
        with pytest.raises(PermissionError):
            project_path(outside)
