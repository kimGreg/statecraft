"""A coding session in the terminal, durable: `python -m statecraft_coding`.

    python -m statecraft_coding --config statecraft.yaml [--agent coder] [--root ID] "task"
    python -m statecraft_coding --config statecraft.yaml --root ID          # resume a session

The agent works until it answers; the answer is printed with the tool calls it made; then the
next instruction is read from the terminal (an empty line ends the session). The session is a
statecraft run: stop the program at any point and resume it with `--root`.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any

from statecraft import Runtime


def _say(text: str) -> None:
    print(text, flush=True)


async def _calls_since(runtime: Runtime, root: str, seen: int) -> tuple[list[str], int]:
    lines = []
    history = await runtime.history(root)
    for entry in history[seen:]:
        for c in entry.get("crossings") or []:
            mark = "" if c.get("ok") else f"  ({c.get('failure')})"
            lines.append(f"  · {c.get('effect')}{mark}")
    return lines, len(history)


async def _question(runtime: Runtime, root: str) -> dict[str, Any] | None:
    found = [q for q in await runtime.questions(root) if q["agent"] == root]
    return found[0] if found else None


async def session(args: argparse.Namespace, read=input) -> int:
    runtime = Runtime.from_config(args.config, executors=0)
    async with runtime:
        root = args.root
        if root is None or await runtime.store.find(root) is None:
            task = args.task or read("task> ").strip()
            if not task:
                return 1
            root = await runtime.spawn(args.agent, {"task": task, "session": True}, root_id=root)
            _say(f"session {root}")
        seen = 0
        while True:
            await runtime.run(root, timeout=args.timeout)
            calls, seen = await _calls_since(runtime, root, seen)
            record = await runtime.get(root)
            if calls:
                _say("\n".join(calls))
            if record["view"] in ("succeeded", "terminated"):
                _say(f"session {root} ended ({record['view']})")
                return 0
            question = await _question(runtime, root)
            if question is None:
                _say(f"{root} is {record['view']}: {record.get('error') or 'waiting'}")
                return 3
            try:
                answer = await runtime.artifact(root, args.output)
                _say("\n" + str(answer["payload"].get("text", answer["payload"])) + "\n")
            except LookupError:
                pass
            try:
                text = read("> ")
            except EOFError:
                text = ""
            await runtime.answer(question["question_id"], text.strip())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m statecraft_coding")
    parser.add_argument("task", nargs="?")
    parser.add_argument("--config", required=True)
    parser.add_argument("--agent", default="coder")
    parser.add_argument("--root", help="the session's run id (resume it when it exists)")
    parser.add_argument("--output", default="answer", help="the artifact type of answers")
    parser.add_argument("--timeout", type=float, default=None)
    return asyncio.run(session(parser.parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
