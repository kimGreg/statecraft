"""The shared coding sample: a lead and two workers in one workspace, each in its own sandbox.

What must hold
  * the lead spawns the workers and goes on (its own command runs before it joins them); it
    verifies, in its sandbox, the modules the workers wrote natively (the check imports them:
    the sandbox sees the native files); one sandbox per agent, three in all;
  * every file is where its owner wrote it;
  * a worker whose command's outcome is unknown (its submit answer lost) faults and stays for a
    person; the lead goes on with the other worker and then waits for it; once an operator
    settles the call and CONTs it, the worker finishes without its command being submitted
    again, and the lead verifies everything.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from statecraft import Runtime

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def at_the_repository_root(monkeypatch):
    monkeypatch.chdir(ROOT)
    monkeypatch.syspath_prepend(str(ROOT))


def config(tmp_path: Path) -> dict:
    data = yaml.safe_load((ROOT / "examples" / "shared_coding" / "statecraft.yaml").read_text())
    data["store"] = {"provider": "sqlite", "path": str(tmp_path / "state.db")}
    data["workspace"] = {"provider": "local", "root": str(tmp_path / "ws")}
    data["runtime"] = {"concurrency": 4, "scan_every": 0.01}
    data["effect_providers"]["sh"]["receipts"] = str(tmp_path / "receipts.db")
    data["effect_providers"]["sh"]["poll"] = 0.02
    return data


async def test_a_team_codes_in_one_workspace(tmp_path: Path) -> None:
    async with Runtime.from_config(config(tmp_path)) as rt:
        root = await rt.spawn("shared_coding:lead", root_id="team")
        await rt.run(root, timeout=30)
        result = (await rt.artifact(root, "result"))["payload"]
        backend = rt.registry.providers["sh"].backend
    assert result == {"verified": ["mathx", "textx"], "recovered": [], "exit": 0}
    shared = tmp_path / "ws" / "team" / "shared"
    assert (shared / "mathx" / "core.py").exists() and (shared / "textx" / "core.py").exists()
    assert (shared / "README.md").read_text() == "# shared coding\n"
    agents = sorted(box["labels"]["statecraft-agent"] for box in backend.sandboxes.values())
    assert agents == ["team", "team-mathx", "team-textx"]


async def test_an_unknown_outcome_is_settled_by_an_operator(tmp_path: Path) -> None:
    rt = Runtime.from_config(config(tmp_path))
    async with rt:
        backend = rt.registry.providers["sh"].backend
        original = backend.submit

        lost = []

        async def lose_for_textx(sandbox, command):  # the textx worker's test, once
            execution = await original(sandbox, command)
            if "textx" in command and not lost:  # submitted; its answer is lost
                lost.append(command)
                raise ConnectionError("the submit answer was lost")
            return execution

        backend.submit = lose_for_textx
        root = await rt.spawn("shared_coding:lead", root_id="team")
        await rt.run(root, timeout=30)
        assert (await rt.get("team/mathx"))["view"] == "succeeded"
        worker = await rt.get("team/textx")
        assert worker["view"] == "suspended" and "submit answer was lost" in worker["error"]
        assert (await rt.store.load(root)).reason("join") is not None  # the lead waits for it
        [entry] = [c for c in await rt.calls("team/textx") if c["status"] == "indeterminate"]
        submits = backend.submits
        # the person checks the sandbox, sees the test ran and passed, and says so
        await rt.resolve_call(entry["key"], "completed", result={"exit": 0, "output": ""})
        await rt.signal("team/textx", "CONT")
        await rt.run(root, timeout=30)
        worker = await rt.get("team/textx")
        result = (await rt.artifact(root, "result"))["payload"]
    assert worker["view"] == "succeeded" and backend.submits == submits + 1  # the lead's verify
    assert result == {"verified": ["mathx", "textx"], "recovered": ["textx"], "exit": 0}
