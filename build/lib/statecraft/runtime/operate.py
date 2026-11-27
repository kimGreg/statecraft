"""What an operator sees and does (roadmap D3, D4): health, metrics, faults, maintenance mode.

What must hold
  * maintenance mode is store-wide (every executor on the store reads it each pass): running
    steps finish, no new step starts; signals, answers and intents are still applied, so TERM and
    KILL work; turning it off lets everything go on where it stopped;
  * `faults()` lists every agent suspended by a fault, with its error, the state and step that
    failed, and the effect calls of the failing attempt (its crossings);
  * `metrics()` counts agents by view across the store, unknown outcomes (indeterminate calls)
    and messages accepted but never delivered, and this process's running steps, executor
    failures, effect calls, failures, retries, capacity waits and capacity held;
  * nothing here carries arguments, results or credentials.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..kernel.transitions import view

if TYPE_CHECKING:
    from .runtime import Runtime

MAINTENANCE = "maintenance"


async def maintenance(runtime: Runtime, on: bool | None = None) -> bool:
    """Turn maintenance mode on or off (None: only read it); returns whether it is on."""
    if on is not None:
        await runtime.store.set_setting(MAINTENANCE, bool(on))
        runtime.notify()
    return bool(await runtime.store.setting(MAINTENANCE))


async def faults(runtime: Runtime) -> list[dict[str, Any]]:
    found = []
    for root in await runtime.store.roots():
        for record in await runtime.store.tree(root.id):
            reason = record.reason("fault")
            if reason is None:
                continue
            history = await runtime.store.history(record.id)
            failing = next((h for h in reversed(history) if h["kind"] == "fault"), None)
            found.append(
                {
                    "agent": record.id,
                    "root": root.id,
                    "state": reason.get("state", record.state),
                    "step": record.step,
                    "error": reason.get("error") or record.error,
                    "crossings": (failing or {}).get("crossings", []),
                    "revision": record.revision,
                }
            )
    return found


async def metrics(runtime: Runtime) -> dict[str, Any]:
    views: dict[str, int] = {}
    indeterminate = undelivered = runs = 0
    now = runtime.clock()
    for root in await runtime.store.roots():
        runs += 1
        for record in await runtime.store.tree(root.id):
            v = view(record, await runtime.store.lease(record.id), now)
            views[v] = views.get(v, 0) + 1
            undelivered += len(record.undelivered)
            indeterminate += sum(
                1 for c in await runtime.store.calls(record.id) if c["status"] == "indeterminate"
            )
    return {
        "runs": runs,
        "agents": views,
        "indeterminate_calls": indeterminate,
        "undelivered_messages": undelivered,
        "maintenance": await maintenance(runtime),
        "process": {
            "running_steps": sum(len(e.running) for e in runtime.executors),
            "executor_failures": sum(len(e.failures) for e in runtime.executors),
            "effects": dict(runtime.effects.counters),
            "capacity": runtime.effects.capacity(),
        },
    }


def prometheus(found: dict[str, Any]) -> str:
    """The metrics in Prometheus' text format."""
    lines = [
        "# TYPE statecraft_runs gauge",
        f"statecraft_runs {found['runs']}",
        "# TYPE statecraft_agents gauge",
        *(f'statecraft_agents{{view="{v}"}} {n}' for v, n in sorted(found["agents"].items())),
        "# TYPE statecraft_indeterminate_calls gauge",
        f"statecraft_indeterminate_calls {found['indeterminate_calls']}",
        "# TYPE statecraft_undelivered_messages gauge",
        f"statecraft_undelivered_messages {found['undelivered_messages']}",
        "# TYPE statecraft_maintenance gauge",
        f"statecraft_maintenance {int(found['maintenance'])}",
        "# TYPE statecraft_running_steps gauge",
        f"statecraft_running_steps {found['process']['running_steps']}",
        "# TYPE statecraft_executor_failures counter",
        f"statecraft_executor_failures {found['process']['executor_failures']}",
    ]
    for name, n in sorted(found["process"]["effects"].items()):
        lines += [f"# TYPE statecraft_effect_{name} counter", f"statecraft_effect_{name} {n}"]
    for provider, slots in sorted(found["process"]["capacity"].items()):
        for key in ("limit", "held"):
            lines.append(f'statecraft_capacity_{key}{{provider="{provider}"}} {slots[key]}')
    return "\n".join(lines) + "\n"


__all__ = ["MAINTENANCE", "faults", "maintenance", "metrics", "prometheus"]
