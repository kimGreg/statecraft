"""The console as a command:

    python -m statecraft_console --config statecraft.yaml [--executors N] [--port 8790]

It builds the runtime from the configuration, starts it with the server (executors run beside
the console when `--executors` > 0, or elsewhere on the same store) and closes it on shutdown.
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from collections.abc import AsyncIterator


def main(argv: list[str] | None = None) -> int:
    import uvicorn
    from starlette.applications import Starlette

    from statecraft import Runtime
    from statecraft.server.guard import require_token

    from .app import console_app

    parser = argparse.ArgumentParser(prog="python -m statecraft_console")
    parser.add_argument("--config", required=True)
    parser.add_argument("--title", default="statecraft")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--executors", type=int, default=None)
    parser.add_argument("--token", help="require this token (default: $STATECRAFT_TOKEN)")
    parser.add_argument("--allow-host", action="append", help="a Host served without a token")
    args = parser.parse_args(argv)
    token = require_token(args.host, args.token)
    overrides = {} if args.executors is None else {"executors": args.executors}
    runtime = Runtime.from_config(args.config, **overrides)
    inner = console_app(runtime, title=args.title, token=token, hosts=args.allow_host or ())

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        await runtime.start()
        try:
            yield
        finally:
            await runtime.close()

    app = Starlette(
        routes=inner.routes,
        lifespan=lifespan,
        middleware=inner.user_middleware,
        exception_handlers=inner.exception_handlers,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    sys.exit(main())
