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
    statecraft faults | metrics (--config FILE | --url URL)   faulted agents; counts
    statecraft repin ID | calls ID | attempts ID (--config FILE | --url URL)
    statecraft resolve KEY completed|failed|replay [--result JSON] (--config | --url)
    statecraft maintenance on|off|status (--config FILE | --url URL)
    statecraft budget ROOT [--set JSON] (--config FILE | --url URL)
    statecraft plugins                      installed plugins (not enabled ones)
    statecraft store check|migrate --config FILE           the store's data format
    statecraft store collect [--grace S] [--apply] --config FILE   staging and unnamed objects
    statecraft store compact ROOT [--apply] --config FILE  smaller history, events and inbox
    statecraft store forget ROOT [--files] [--apply] --config FILE   delete an ended run
    statecraft token add NAME --scope S [--root PREFIX] --tokens FILE | list | revoke NAME

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

        return StatecraftClient(args.url, token=args.token)
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
            if args.root not in {r.id for r in await runtime.store.roots()}:
                print(f"no root {args.root!r}", file=sys.stderr)
                return 1
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
        elif command == "faults":
            show(await api.faults())
        elif command == "metrics":
            show(await api.metrics())
        elif command == "repin":
            show(await api.repin(args.id))
        elif command == "calls":
            show(await api.calls(args.id))
        elif command == "attempts":
            show(await api.attempts(args.id))
        elif command == "budget":
            if args.set is not None:
                show(await api.set_budget(args.id, _value(args.set)))
            else:
                show(await api.budget(args.id))
        elif command == "resolve":
            result = _value(args.result) if args.result is not None else None
            show(await api.resolve_call(args.key, args.outcome, result=result))
        elif command == "maintenance":
            state = {"on": True, "off": False, "status": None}[args.state]
            show({"maintenance": await api.maintenance(state)})
        elif command == "events":
            async for event in api.events(args.id, since=args.since, follow=args.follow):
                print(json.dumps(event, ensure_ascii=False, default=str), flush=True)
    return 0


async def _upkeep(args: argparse.Namespace) -> int:
    """`store collect|compact|forget`: storage upkeep through the runtime (C2). Dry unless
    `--apply`."""
    from ..kernel.store import StoreError

    dry = not args.apply
    async with _Embedded(args.config) as runtime:
        if args.action == "collect":
            show(await runtime.collect(dry_run=dry, grace=args.grace))
            return 0
        if not args.root:
            raise SystemExit(f"store {args.action} needs ROOT")
        try:
            if args.action == "compact":
                show(await runtime.compact(args.root, dry_run=dry))
            else:
                show(await runtime.forget(args.root, files=args.files, dry_run=dry))
        except KeyError:
            print(f"no root {args.root!r}", file=sys.stderr)
            return 1
        except (ValueError, StoreError) as exc:  # refused: still at work, or read by a run
            print(str(exc), file=sys.stderr)
            return 1
    return 0


def _store(args: argparse.Namespace) -> int:
    """`store check`: the data format and whether this statecraft reads it (exit 1 when not);
    `store migrate`: back the store up and upgrade it."""
    if args.action in ("collect", "compact", "forget"):
        return asyncio.run(_upkeep(args))
    from ..providers.stores import sqlite
    from ..runtime.config import read

    data, base = read(args.config)
    cfg = data.get("store") or {}
    if cfg.get("provider") != "sqlite":
        print("only a sqlite store has a data format to check or migrate", file=sys.stderr)
        return 1
    path = Path(cfg.get("path", "statecraft.db"))
    path = path if path.is_absolute() else base / path
    if args.action == "check":
        found = sqlite.check(path)
        show(found)
        return 0 if found["ok"] else 1
    before, after = sqlite.migrate(path)
    show({"path": str(path), "from": before, "to": after})
    return 0


def _tokens(args: argparse.Namespace) -> int:
    from ..server.tokens import TokenBook

    book = TokenBook(args.tokens)
    if args.action == "add":
        if not args.name or not args.scope:
            raise SystemExit("token add needs NAME and at least one --scope")
        try:
            secret = book.add(args.name, scopes=args.scope, roots=args.root)
        except ValueError as exc:
            raise SystemExit(str(exc)) from None
        print(secret)  # shown once: the file keeps only its salted hash
        return 0
    if args.action == "revoke":
        if not args.name:
            raise SystemExit("token revoke needs NAME")
        return 0 if book.revoke(args.name) else 1
    show(book.names())
    return 0


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    from ..runtime.runtime import Runtime
    from ..server import server_app
    from ..server.guard import require_token
    from ..server.tokens import TokenBook

    tokens = TokenBook(args.tokens) if args.tokens else None
    token = require_token(args.host, args.token, tokens)
    overrides = {} if args.executors is None else {"executors": args.executors}
    runtime = Runtime.from_config(args.config, **overrides)
    app = server_app(runtime, manage=True, token=token, hosts=args.allow_host or (), tokens=tokens)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="statecraft")
    sub = parser.add_subparsers(dest="command", required=True)

    def target(p: argparse.ArgumentParser) -> argparse.ArgumentParser:
        p.add_argument("--config")
        p.add_argument("--url")
        p.add_argument("--token", help="bearer token for --url (default: $STATECRAFT_TOKEN)")
        return p

    store = sub.add_parser(
        "store", help="check or migrate the store's format; collect, compact or forget"
    )
    store.add_argument("action", choices=["check", "migrate", "collect", "compact", "forget"])
    store.add_argument("root", nargs="?")
    store.add_argument("--config", required=True)
    store.add_argument("--apply", action="store_true", help="do it (upkeep is dry by default)")
    store.add_argument("--grace", type=float, default=3600.0, help="collect: minimum object age")
    store.add_argument("--files", action="store_true", help="forget: the run's files too")
    serve = sub.add_parser("serve")
    serve.add_argument("--config", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--executors", type=int)
    serve.add_argument("--token", help="require this bearer token (default: $STATECRAFT_TOKEN)")
    serve.add_argument("--tokens", help="a token book (`statecraft token add`): named tokens")
    serve.add_argument(
        "--allow-host", action="append", help="a Host name served without a token (repeatable)"
    )
    token = sub.add_parser("token", help="make, list or revoke named API tokens")
    token.add_argument("action", choices=["add", "list", "revoke"])
    token.add_argument("name", nargs="?")
    token.add_argument("--tokens", required=True, help="the token book file")
    token.add_argument("--scope", action="append", choices=["read", "control", "admin"])
    token.add_argument("--root", action="append", help="a run id prefix it may touch (repeatable)")
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
    for name in ("agents", "tools", "providers", "ps", "questions", "faults", "metrics"):
        target(sub.add_parser(name))
    for name in ("inspect", "tree", "history", "repin", "calls", "attempts"):
        target(sub.add_parser(name)).add_argument("id")
    resolve = target(sub.add_parser("resolve", help="settle a call whose outcome is unknown"))
    resolve.add_argument("key")
    resolve.add_argument("outcome", choices=["completed", "failed", "replay"])
    resolve.add_argument("--result", help="completed: what the state receives (JSON)")
    budget = target(sub.add_parser("budget", help="a tree's budget: limit, spent, held"))
    budget.add_argument("id")
    budget.add_argument("--set", help="new limits, e.g. '{\"usd\": 2}' (null: none)")
    hold = target(sub.add_parser("maintenance", help="no new steps start while on"))
    hold.add_argument("state", choices=["on", "off", "status"])
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
    if args.command == "store":
        return _store(args)
    if args.command == "token":
        return _tokens(args)
    return asyncio.run(_call(args))


if __name__ == "__main__":
    sys.exit(main())
