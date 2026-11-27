"""The `statecraft` command: the same API embedded (`--config`) or against a server (`--url`).

    statecraft serve --config statecraft.yaml [--host 127.0.0.1] [--port 8080]
    statecraft run NAME --config statecraft.yaml [--input '{"k": 1}' | --input file.json]
    statecraft resume ROOT --config statecraft.yaml      run an existing root until quiet
    statecraft agents|tools|providers (--config FILE | --url URL)
    statecraft ps | questions (--config FILE | --url URL)
    statecraft inspect ID | tree ROOT | history ID (--config FILE | --url URL)
    statecraft signal ID STOP|CONT|TERM|KILL|USR [MESSAGE] (--config FILE | --url URL)
    statecraft answer QUESTION_ID VALUE (--config FILE | --url URL)
    statecraft events ROOT [--since N] [--follow] (--config FILE | --url URL)
    statecraft plugins                      installed plugins (not enabled ones)

`run` exits 0 when the run succeeded, 3 when it waits on a person, a stop or a fault, 1 otherwise.
Embedded commands other than `run` and `serve` start no executors: they record and read.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

WAITING = 3


def _value(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _input(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    path = Path(text)
    return json.loads(path.read_text()) if path.is_file() else json.loads(text)


def show(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=1, default=str))


class _Embedded:
    """The runtime from a configuration, started with no executors (records and reads)."""

    def __init__(self, config: str, executors: int = 0) -> None:
        from ..runtime.runtime import Runtime

        self.runtime = Runtime.from_config(config, executors=executors)

    async def __aenter__(self) -> Any:
        return await self.runtime.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.runtime.close()


def _api(args: argparse.Namespace) -> Any:
    if args.url:
        from ..client import StatecraftClient

        return StatecraftClient(args.url)
    if args.config:
        return _Embedded(args.config)
    raise SystemExit("give --config FILE (embedded) or --url URL (a server)")


async def _call(args: argparse.Namespace) -> int:
    command = args.command
    if command == "plugins":
        from ..runtime import plugins

        show(plugins.installed())
        return 0
    if command == "run":
        async with _Embedded(args.config) as runtime:
            root = await runtime.spawn(args.name, _input(args.input), root_id=args.root)
            rows = await runtime.run(root, timeout=args.timeout)
            show([{k: r[k] for k in ("id", "state", "view", "error")} for r in rows])
            head = rows[0]["view"]
            return 0 if head == "succeeded" else WAITING if head == "suspended" else 1
    if command == "resume":
        async with _Embedded(args.config) as runtime:
            rows = await runtime.run(args.root, timeout=args.timeout)
            show([{k: r[k] for k in ("id", "state", "view", "error")} for r in rows])
            head = rows[0]["view"]
            return 0 if head == "succeeded" else WAITING if head == "suspended" else 1
    async with _api(args) as api:
        if command in ("agents", "tools", "providers"):
            result = getattr(api, command)()
            show(await result if asyncio.iscoroutine(result) else result)
        elif command == "ps":
            show(
                [
                    {k: r[k] for k in ("id", "agent", "status", "view", "agents", "questions")}
                    for r in await api.runs()
                ]
            )
        elif command == "questions":
            show(await api.questions())
        elif command == "inspect":
            show(await api.get(args.id))
        elif command == "tree":
            show(await api.tree(args.id))
        elif command == "history":
            show(await api.history(args.id))
        elif command == "signal":
            signal = (args.signal, _value(args.message)) if args.signal == "USR" else args.signal
            await api.signal(args.id, signal)
        elif command == "answer":
            await api.answer(args.id, _value(args.value))
        elif command == "events":
            async for event in api.events(args.id, since=args.since, follow=args.follow):
                print(json.dumps(event, ensure_ascii=False, default=str), flush=True)
    return 0


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    from ..runtime.runtime import Runtime
    from ..server import server_app

    overrides = {} if args.executors is None else {"executors": args.executors}
    runtime = Runtime.from_config(args.config, **overrides)
    uvicorn.run(
        server_app(runtime, manage=True), host=args.host, port=args.port, log_level="warning"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="statecraft")
    sub = parser.add_subparsers(dest="command", required=True)

    def target(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
        p.add_argument("--config")
        p.add_argument("--url")
        return p

    serve = sub.add_parser("serve")
    serve.add_argument("--config", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--executors", type=int)
    run = sub.add_parser("run")
    run.add_argument("name")
    run.add_argument("--config", required=True)
    run.add_argument("--input")
    run.add_argument("--root")
    run.add_argument("--timeout", type=float)
    resume = sub.add_parser("resume")
    resume.add_argument("root")
    resume.add_argument("--config", required=True)
    resume.add_argument("--timeout", type=float)
    for name in ("agents", "tools", "providers", "ps", "questions"):
        target(sub.add_parser(name))
    for name in ("inspect", "tree", "history"):
        target(sub.add_parser(name)).add_argument("id")
    signal = target(sub.add_parser("signal"))
    signal.add_argument("id")
    signal.add_argument("signal", choices=["STOP", "CONT", "TERM", "KILL", "USR"])
    signal.add_argument("message", nargs="?")
    answer = target(sub.add_parser("answer"))
    answer.add_argument("id")
    answer.add_argument("value")
    events = target(sub.add_parser("events"))
    events.add_argument("id")
    events.add_argument("--since", type=int, default=0)
    events.add_argument("--follow", action="store_true")
    sub.add_parser("plugins")
    args = parser.parse_args(argv)
    if args.command == "serve":
        return _serve(args)
    return asyncio.run(_call(args))


if __name__ == "__main__":
    sys.exit(main())
