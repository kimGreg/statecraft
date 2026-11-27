"""statecraft-console: the visualizer, over the runtime API only.

python -m statecraft_console --config statecraft.yaml
from statecraft_console import console_app; app = console_app(runtime)
"""

from .app import console_app
from .model import graph, machine, registry, servers

__all__ = ["console_app", "graph", "machine", "registry", "servers"]
