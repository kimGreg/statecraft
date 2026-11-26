"""The statecraft console: a web page over an engine's registry, graphs, runs and workspaces.

    from statecraft.console import console_app
    app = console_app(engine)                     # an ASGI app (Starlette)

    python -m statecraft.console --engine my_app.engines:make --port 8790

`--engine` names a function that returns an `Engine`, or `(engine, servers)` where `servers` is
an async function listing the application's live tool servers. Needs the `console` extra.
"""

from .model import agent, describe_agent, files, graph, machine, registry, run, runs

__all__ = [
    "agent",
    "console_app",
    "describe_agent",
    "files",
    "graph",
    "machine",
    "registry",
    "run",
    "runs",
]


def __getattr__(name: str):  # the app needs the console extra; the model does not
    if name == "console_app":
        from .app import console_app

        return console_app
    raise AttributeError(name)
