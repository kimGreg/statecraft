"""A real model coding through the sandbox shell (roadmap A5.5), after the deterministic tests.

Runs only with `STATECRAFT_REAL_MODEL=1` and an OpenRouter key (`STATECRAFT_MODEL` picks the
model, default openai/gpt-oss-20b). What must hold: the agent writes and runs a small program in
its sandbox through `sh.exec` only, succeeds, answers with the program's output; every shell
call it made has its own identity and was submitted to the sandbox exactly once.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from statecraft_agentic import AgenticAgent, add_model, openrouter
from statecraft_agentic import register as agentic
from statecraft_agentic.models import OPENROUTER_KEY_ENVS
from statecraft_opensandbox import FakeBackend, OpenSandboxProvider

from statecraft import Runtime
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace

pytestmark = pytest.mark.skipif(
    os.environ.get("STATECRAFT_REAL_MODEL") != "1"
    or not any(os.environ.get(k) for k in OPENROUTER_KEY_ENVS),
    reason="a real-model run: set STATECRAFT_REAL_MODEL=1 and an OpenRouter key",
)
MODEL = os.environ.get("STATECRAFT_MODEL", "openai/gpt-oss-20b")
TASK = (
    "Create a file hello.py that prints the sum of 2 and 3. Run it with python3. "
    "Answer with exactly what it printed."
)


class Coder(AgenticAgent):
    instruction = (
        "You work in a shared workspace through the sh__exec tool: a shell whose working folder "
        "is /workspace. Do the task with shell commands, run what you wrote to check it, and "
        "answer with the program's output only."
    )
    max_model_turns = 20


async def test_a_real_model_codes_through_the_sandbox_shell(tmp_path: Path) -> None:
    rt = Runtime(
        store=SqliteStore(tmp_path / "state.db"),
        workspace=LocalWorkspace(tmp_path / "ws"),
        scan_every=0.05,
    )
    r = rt.registrar
    agentic(r)
    add_model(r, "default", openrouter(MODEL))
    backend = FakeBackend()
    r.providers.register(
        "sh", OpenSandboxProvider(backend, receipts=tmp_path / "receipts.db", poll=0.05)
    )
    r.agents.register("coder", Coder, tools=["model.default", "sh.exec"], writes=["answer"])
    async with rt:
        root = await rt.spawn("coder", {"task": TASK}, root_id="code")
        await rt.run(root, timeout=300)
        record = await rt.get(root)
        assert record["view"] == "succeeded", record["error"]
        answer = (await rt.artifact(root, "answer"))["payload"]
        calls = await rt.calls(root)
    assert (tmp_path / "ws" / "code" / "shared" / "hello.py").exists()
    assert "5" in str(answer)
    assert calls and all(c["status"] == "completed" for c in calls)
    assert len({c["key"] for c in calls}) == len(calls) == backend.submits
