"""Tracing (roadmap D3): a span per step and per effect call, through the OpenTelemetry API.

Statecraft uses only the API (`opentelemetry-api`, optional): without an SDK configured by the
application its spans cost nothing, and with one they go wherever the application exports them.
A runtime may also be given any tracer with the API's shape (`start_as_current_span`,
`start_span`), which is how the tests watch it.

    step span    statecraft.step    agent, root, state, step, outcome (transition|fault), error
    effect span  statecraft.effect  effect, provider, operation, attempts, waited, ok, failure,
                                    a child of its step's span, timed from the call's crossing

Never recorded: arguments, results, credentials. Only what crossings already record.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator
from typing import Any

#: crossing fields copied to an effect span
EFFECT_FIELDS = ("effect", "provider", "operation", "attempts", "waited", "ok", "failure", "key")


def default_tracer() -> Any:
    """The OpenTelemetry API's tracer for statecraft, or None when the API is not installed."""
    try:
        from opentelemetry import trace
    except ImportError:
        return None
    return trace.get_tracer("statecraft")


def _clean(attributes: dict[str, Any]) -> dict[str, Any]:
    return {
        k: v for k, v in attributes.items() if isinstance(v, str | bool | int | float) and v != ""
    }


@contextlib.contextmanager
def step_span(tracer: Any, record: Any) -> Iterator[Any]:
    if tracer is None:
        yield None
        return
    attributes = {
        "statecraft.agent": record.id,
        "statecraft.root": record.id.split("/")[0],
        "statecraft.state": record.state,
        "statecraft.step": record.step,
        "statecraft.definition": record.agent,
    }
    with tracer.start_as_current_span("statecraft.step", attributes=_clean(attributes)) as span:
        yield span


def step_outcome(span: Any, outcome: Any) -> None:
    if span is None:
        return
    error = getattr(outcome, "error", None)
    span.set_attribute("statecraft.outcome", "fault" if error is not None else "transition")
    if error is not None:
        span.set_attribute("statecraft.error", str(error)[:300])
        _error_status(span, str(error)[:300])


def effect_span(tracer: Any, crossing: dict[str, Any]) -> None:
    """A finished call as a span, timed from its crossing (a child of the current step span)."""
    if tracer is None:
        return
    end = time.time_ns()
    start = end - int(float(crossing.get("duration") or 0) * 1e9)
    attributes = _clean({f"statecraft.{k}": crossing.get(k) for k in EFFECT_FIELDS})
    span = tracer.start_span("statecraft.effect", attributes=attributes, start_time=start)
    if crossing.get("failure"):
        _error_status(span, f"{crossing['failure']}: {crossing.get('error', '')}"[:300])
    span.end(end_time=end)


def _error_status(span: Any, message: str) -> None:
    try:
        from opentelemetry.trace import Status, StatusCode
    except ImportError:
        span.set_attribute("statecraft.status", "error")
        return
    span.set_status(Status(StatusCode.ERROR, message))


__all__ = ["default_tracer", "effect_span", "step_outcome", "step_span"]
