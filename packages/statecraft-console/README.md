# statecraft-console

The visualizer, over the runtime API only: registry (agents with their state machines), graph
(spawns, providers, artifact flow), providers and their tools, and live runs (tree, walked path,
tool calls with attempts and durations, open questions answered in place, signals, workspace
files, events).

```bash
python -m statecraft_console --config statecraft.yaml [--executors 1] [--port 8790]
```

In code: `from statecraft_console import console_app; app = console_app(runtime)`.
