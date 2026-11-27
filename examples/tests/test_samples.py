"""The samples, run end to end (the product acceptance checklist, architecture §17).

What must hold
  * orders: tools with policies; a short stock asks a person; a payment whose response is lost
    is retried with its idempotency key and charged once; a child's fault is decided by a person;
  * research: children run in parallel, two searches run at once inside a state, the CPU-heavy
    ranking runs in a process;
  * assistant: an agentic agent prices a quote with MCP tools under its pinned skill; a decider
    approves it; every model and tool call is recorded;
  * recovery: a process killed mid-state resumes from the store; the committed state is not run
    again, the unfinished one reruns with the same key;
  * notes plugin: a package unrelated to statecraft, installed and enabled by name, registers an
    agent, a Python tool and a skill, uses an MCP tool, and recovers from a killed process.
"""

from __future__ import annotations

import importlib
import json
import os
import signal
import subprocess
import sys
import time
from importlib.metadata import entry_points
from pathlib import Path

import pytest
import yaml

from statecraft import Runtime

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"


def config_for(sample: str, tmp_path: Path, **runtime: object) -> dict:
    """The sample's configuration with its store and workspace in `tmp_path`."""
    data = yaml.safe_load((EXAMPLES / sample / "statecraft.yaml").read_text())
    data["store"] = {"provider": "sqlite", "path": str(tmp_path / "state.db")}
    data["workspace"] = {"provider": "local", "root": str(tmp_path / "ws")}
    data["runtime"] = {**(data.get("runtime") or {}), "executors": 0, "scan_every": 0.01, **runtime}
    return data


@pytest.fixture(autouse=True)
def at_the_repository_root(monkeypatch):
    monkeypatch.chdir(ROOT)
    monkeypatch.syspath_prepend(str(ROOT))


def crossings(history: list[dict]) -> list[dict]:
    return [c for entry in history for c in entry.get("crossings", [])]


async def test_orders(tmp_path) -> None:
    tools = importlib.import_module("examples.orders.tools")
    tools.LEDGER.clear()
    tools.RECEIVED["charges"] = 0
    tools.LABELS["failures_left"] = 1
    order = {"order_id": "o1", "item": "lamp", "qty": 2, "amount": 30, "address": "1 Main St"}
    async with Runtime.from_config(config_for("orders", tmp_path)) as runtime:
        root = await runtime.spawn("order", order, root_id="o1")
        await runtime.run(root, timeout=20)
        [question] = await runtime.questions(root)
        assert question["name"] == "backorder"
        await runtime.answer(question["question_id"], "accept")
        await runtime.run(root, timeout=20)
        [question] = await runtime.questions(root)
        assert question["name"] == "shipping" and "jammed" in question["question"]
        await runtime.answer(question["question_id"], "retry")
        rows = await runtime.run(root, timeout=20)
        assert {r["view"] for r in rows} == {"succeeded"}
        receipt = (await runtime.artifact(root, "receipt"))["payload"]
        assert receipt == {
            "order": "o1",
            "status": "shipped",
            "charge": "ch-1",
            "label": "LBL-o1",
            "backorder": True,
        }
        charge = next(
            c for c in crossings(await runtime.history(root)) if c["effect"] == "payments.charge"
        )
        assert charge["attempts"] == 2  # the lost response was retried with the same key
        assert len(tools.LEDGER) == 1 and tools.RECEIVED["charges"] == 2  # charged once


async def test_research(tmp_path) -> None:
    topics = ["robots", "batteries", "sensors"]
    async with Runtime.from_config(config_for("research", tmp_path, concurrency=4)) as runtime:
        root = await runtime.spawn("research", {"topics": topics})
        rows = await runtime.run(root, timeout=120)
        assert {r["view"] for r in rows} == {"succeeded"} and len(rows) == 4
        report = (await runtime.artifact(root, "report"))["payload"]
        assert sorted(report["sections"]) == sorted(topics)
        for n in range(3):
            found = (await runtime.artifact(root, "findings", by=f"t{n}"))["payload"]
            assert found["search_seconds"] < 0.35  # the two 0.2 s searches ran at once
            ranked = [
                c
                for c in crossings(await runtime.history(f"{root}/t{n}"))
                if c["effect"] == "text.rank"
            ]
            assert ranked and ranked[0]["ok"]


async def test_assistant(tmp_path) -> None:
    items = [{"qty": 2, "price": 3.5}, {"qty": 1, "price": 10}]
    async with Runtime.from_config(config_for("assistant", tmp_path)) as runtime:
        assert {"calc.add", "calc.multiply", "model.default"} <= {
            t["name"] for t in runtime.tools()
        }
        root = await runtime.spawn("quote", {"items": items})
        rows = await runtime.run(root, timeout=60)
        assert {r["view"] for r in rows} == {"succeeded"}, rows
        quote = (await runtime.artifact(root, "quote"))["payload"]
        assert quote == {"total": 17.0, "lines": 2, "decision": "approve"}
        assistant = await runtime.get(f"{root}/pricer")
        assert list(assistant["data"]["available_skills"]) == ["arithmetic-review"]
        effects = [c["effect"] for c in crossings(await runtime.history(f"{root}/pricer"))]
        assert effects.count("calc.multiply") == 2 and "calc.add" in effects
        assert effects.count("model.default") >= 3


def write_config(sample: str, tmp_path: Path) -> Path:
    data = config_for(sample, tmp_path)
    data["runtime"] = {"lease_ttl": 1, "beat": 0.2, "scan_every": 0.05}
    path = tmp_path / "statecraft.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def statecraft(*args: str) -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), *sys.path])}
    return subprocess.Popen(
        [sys.executable, "-m", "statecraft", *args],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def kill_when(path_glob: str, folder: Path, process: subprocess.Popen) -> None:
    deadline = time.time() + 60
    while time.time() < deadline and not list(folder.glob(path_glob)):
        assert process.poll() is None, process.communicate()
        time.sleep(0.05)
    assert list(folder.glob(path_glob)), "the slow state never started"
    process.send_signal(signal.SIGKILL)
    assert process.wait(10) == -signal.SIGKILL


def test_recovery_after_a_killed_process(tmp_path) -> None:
    config = write_config("recovery", tmp_path)
    first = statecraft(
        "run",
        "pipeline",
        "--config",
        str(config),
        "--root",
        "p1",
        "--input",
        json.dumps({"slow": True, "rows": 4}),
    )
    kill_when("ws/p1/home/p1/private/started", tmp_path, first)
    second = statecraft("resume", "p1", "--config", str(config), "--timeout", "60")
    out, err = second.communicate(timeout=90)
    assert second.returncode == 0, err
    assert json.loads(out)[0]["view"] == "succeeded"
    logs = sorted(p.name for p in (tmp_path / "ws" / "p1" / "shared" / "log").iterdir())
    stages = [name.split("_")[3].rsplit("-", 1)[0] for name in logs]
    assert stages.count("fetch") == 1 and stages.count("publish") == 1
    transforms = [n for n in logs if "_transform_" in n]
    assert len(transforms) == 2 and len({n.rsplit("-", 1)[0] for n in transforms}) == 1


def test_an_unrelated_plugin_package_registers_runs_and_recovers(tmp_path) -> None:
    if "notes" not in {ep.name for ep in entry_points(group="statecraft.plugins")}:
        pytest.skip("install the sample plugin first: pip install -e examples/notes_plugin")
    config = write_config("notes_plugin", tmp_path)
    notes = {"notes": ["  buy milk", "call Ann about the trip", ""], "slow": True}
    first = statecraft(
        "run", "notes:digest", "--config", str(config), "--root", "d1", "--input", json.dumps(notes)
    )
    kill_when("ws/d1/home/d1/private/counted", tmp_path, first)
    second = statecraft("resume", "d1", "--config", str(config), "--timeout", "60")
    out, err = second.communicate(timeout=120)
    assert second.returncode == 0, err
    rows = json.loads(out)
    assert {r["view"] for r in rows} == {"succeeded"} and len(rows) == 2
    shown = statecraft("inspect", "d1", "--config", str(config))
    record = json.loads(shown.communicate(timeout=60)[0])
    assert set(record["bindings"]) == {"notes.normalize", "words.count"}
    assert record["definition"]["name"] == "notes:digest"
    objects = list((tmp_path / "ws" / "d1" / "home" / "d1" / "objects").glob("*.json"))
    digest = next(
        json.loads(p.read_text())["payload"]
        for p in objects
        if json.loads(p.read_text())["artifact_type"] == "digest"
    )
    assert digest == {"notes": 2, "words": 7, "summary": "2 notes, starting with: buy milk"}


@pytest.mark.skipif(
    not os.environ.get("STATECRAFT_REAL_MODEL")
    or not (os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OPENROUTER_KEY")),
    reason="set STATECRAFT_REAL_MODEL (e.g. openai/gpt-oss-20b) and an OpenRouter key",
)
async def test_assistant_on_a_real_model(tmp_path) -> None:
    """The same agents on a real model (zero retention): prices with the MCP tools, approves."""
    items = [{"qty": 2, "price": 3.5}, {"qty": 1, "price": 10}]
    async with Runtime.from_config(config_for("assistant", tmp_path, scan_every=0.05)) as runtime:
        root = await runtime.spawn("quote", {"items": items})
        rows = await runtime.run(root, timeout=300)
        assert {r["view"] for r in rows} == {"succeeded"}, rows
        quote = (await runtime.artifact(root, "quote"))["payload"]
        assert quote["total"] == 17.0
        effects = [c["effect"] for c in crossings(await runtime.history(f"{root}/pricer"))]
        assert "calc.multiply" in effects and "model.default" in effects
