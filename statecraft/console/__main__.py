"""python -m statecraft.console --engine module:function [--host H] [--port P] [--executors N]"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import sys


def main(argv: list[str] | None = None) -> int:
    import uvicorn

    from .app import console_app

    parser = argparse.ArgumentParser(prog="python -m statecraft.console")
    parser.add_argument("--engine", required=True, help="module:function returning an Engine")
    parser.add_argument("--title", default="statecraft")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--executors", type=int, default=0)
    args = parser.parse_args(argv)
    module, _, name = args.engine.partition(":")
    made = getattr(importlib.import_module(module), name)()
    if inspect.isawaitable(made):
        made = asyncio.run(made)
    engine, servers = made if isinstance(made, tuple) else (made, None)
    app = console_app(engine, servers=servers, title=args.title, executors=args.executors)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    sys.exit(main())
