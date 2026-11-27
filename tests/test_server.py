"""The runtime served over HTTP, and the client (architecture §11.1; checklist 19, 22).

What must hold
  * the same scenario gives the same result embedded (`Runtime`) and served (`StatecraftClient`
    against `server_app`): spawn, events, questions, answer, signal, tree, history, artifacts,
    registry listings;
  * errors come back as statuses (an unknown agent 404, a second answer 409);
  * the CLI runs an agent from a configuration and lists the registry.
"""

from __future__ import annotations

import asyncio
import json
import socket
from typing import Any

import pytest
import uvicorn

from statecraft import Agent, Done, Wait, state
from statecraft.cli.main import main as cli
from statecraft.client import StatecraftClient, StatecraftError
from statecraft.server import server_app
from statecraft.testing import memory_runtime
from statecraft.testing.tools import add


class Asker(Agent):
    start = "add"

    @state
    async def add(self, ctx, data):
        data["sum"] = await ctx.call("math.add", a=ctx.input["a"], b=ctx.input["b"])
        return Wait(
            answer={"name": "scale", "question": "by?", "schema": {"type": "integer"}}, then="done"
        )

    @state
    async def done(self, ctx, data):
        await ctx.write("result", {"value": data["sum"] * ctx.woke["answer"]})
        return Done


def make(**options):
    runtime = memory_runtime(**{"executors": 1, "scan_every": 0.01, **options})
    runtime.registrar.tools.register(add)
    runtime.registrar.agents.register("asker", Asker, tools=["math.add"], writes=["result"])
    return runtime


async def scenario(api: Any) -> dict[str, Any]:
    """Spawn, wait for the question, answer it, wait for the end; read everything back."""
    root = await api.spawn("asker", {"a": 2, "b": 3}, root_id="s1")
    for _ in range(500):
        if await api.questions(root):
            break
        await asyncio.sleep(0.01)
    [question] = await api.questions(root)
    await api.answer(question["question_id"], 10)
    for _ in range(500):
        if (await api.get(root))["status"] == "succeeded":
            break
        await asyncio.sleep(0.01)
    events = []
    async for event in api.events(root, follow=False):
        if event["kind"] != "audit":  # changes over HTTP are audited as well; the rest is equal
            events.append((event["kind"], event["state"]))
    return {
        "result": (await api.artifact(root, "result"))["payload"],
        "tree": [(r["id"], r["view"]) for r in await api.tree(root)],
        "history": [e["kind"] for e in await api.history(root)],
        "events": events,
        "agents": [a["name"] for a in (await maybe(api.agents()))],
        "tools": [t["name"] for t in (await maybe(api.tools()))],
    }


async def maybe(value: Any) -> Any:
    return await value if asyncio.iscoroutine(value) else value


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def test_the_same_api_embedded_and_served() -> None:
    async with make() as runtime:
        embedded = await scenario(runtime)

    served_runtime = make()
    port = free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            server_app(served_runtime, manage=True),
            host="127.0.0.1",
            port=port,
            log_level="warning",
        )
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)
    try:
        async with StatecraftClient(f"http://127.0.0.1:{port}") as client:
            served = await scenario(client)
            with pytest.raises(StatecraftError) as unknown:
                await client.get("nobody")
            assert unknown.value.status == 404
            assert await client.questions() == []  # the question was answered
            # an ended agent takes no more signals: refused, not silently dropped
            with pytest.raises(StatecraftError) as ended:
                await client.signal("s1", "STOP")
            assert ended.value.status == 409
    finally:
        server.should_exit = True
        await task
    assert served == embedded
    assert embedded["result"] == {"value": 50}
    assert ("step", "done") in embedded["events"]


def test_the_cli_runs_an_agent_from_a_configuration(tmp_path, capsys, monkeypatch) -> None:
    (tmp_path / "cli_summer.py").write_text(
        "from statecraft import Agent, Done, state\n"
        "class Summer(Agent):\n"
        "    start = 'go'\n"
        "    @state\n"
        "    async def go(self, ctx, data):\n"
        "        await ctx.write('sum', {'v': await ctx.call('math.add', a=1, b=2)})\n"
        "        return Done\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    config = tmp_path / "statecraft.yaml"
    config.write_text(
        f"store: {{provider: sqlite, path: '{tmp_path}/state.db'}}\n"
        f"workspace: {{provider: local, root: '{tmp_path}/ws'}}\n"
        "runtime: {scan_every: 0.01}\n"
        "effect_providers:\n"
        "  math: {provider: python, module: statecraft.testing.tools}\n"
        "agents:\n"
        "  summer: {entry: cli_summer:Summer, tools: [math.add]}\n"
    )
    assert cli(["run", "summer", "--config", str(config), "--root", "c1"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert rows == [{"id": "c1", "state": "go", "view": "succeeded", "error": None}]
    assert cli(["agents", "--config", str(config)]) == 0
    assert json.loads(capsys.readouterr().out)[0]["name"] == "summer"
    assert cli(["inspect", "c1", "--config", str(config)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "succeeded"
