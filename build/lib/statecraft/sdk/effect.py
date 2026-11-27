"""Tools: external calls a state makes through the runtime (`ctx.call`). The execution model calls
them effects; `@tool` and `@effect` are the same decorator.

    @tool(name="math.add", policy="read_only")
    async def add(a: float, b: float) -> float: ...

    @tool(name="report.render", policy="idempotent", execution="process")
    def render(document: dict) -> bytes: ...       # blocking: run in a process, off the loop

Every tool declares a policy (what a repeat means, `kernel.effects`); there is no default, because
a wrong one breaks recovery. The input schema comes from the signature and annotations, the
output schema from the return annotation.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any

from ..kernel.effects import EffectPolicy, digest_of

EXECUTIONS = ("async", "thread", "process")
#: A Python tool that declares this parameter receives the call's idempotency key (deduplicated
#: tools); it is not part of the tool's input schema.
KEY_PARAMETER = "idempotency_key"


@dataclass(frozen=True)
class EffectDefinition:
    """One effect: its contract (schemas, policy) and its binding (provider, operation)."""

    name: str
    description: str = ""
    input_schema: dict[str, Any] = field(default_factory=lambda: {"type": "object"})
    output_schema: dict[str, Any] | None = None
    policy: EffectPolicy = field(default_factory=EffectPolicy)
    provider: str = ""
    operation: str = ""
    version: str = ""
    #: How a Python implementation runs: on the event loop, in a thread, or in a process.
    execution: str = "async"

    def __post_init__(self) -> None:
        if not isinstance(self.policy, EffectPolicy):
            object.__setattr__(self, "policy", EffectPolicy.of(self.policy))

    def schema_digest(self) -> str:
        return digest_of({"input": self.input_schema, "output": self.output_schema})

    def binding(self) -> dict[str, Any]:
        """What an instance pins for this effect."""
        return {
            "provider": self.provider,
            "operation": self.operation or self.name,
            "version": self.version,
            "schema_digest": self.schema_digest(),
            "policy_digest": self.policy.digest(),
        }

    def but(self, **changes: Any) -> EffectDefinition:
        return replace(self, **changes)


def schemas_of(fn: Callable[..., Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """(input schema, output schema) from a function's signature and annotations."""
    from pydantic import TypeAdapter, create_model

    hints = inspect.get_annotations(fn, eval_str=True)
    fields: dict[str, Any] = {}
    for name, param in inspect.signature(fn).parameters.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD) or name == KEY_PARAMETER:
            continue
        annotation = hints.get(name, Any)
        default = ... if param.default is param.empty else param.default
        fields[name] = (annotation, default)
    model = create_model(f"{fn.__name__}_input", **fields)
    input_schema = model.model_json_schema()
    input_schema.pop("title", None)
    output_schema = None
    if "return" in hints and hints["return"] not in (None, type(None)):
        try:
            output_schema = TypeAdapter(hints["return"]).json_schema()
        except Exception:
            output_schema = None
    return input_schema, output_schema


def effect(
    name: str | None = None,
    *,
    policy: EffectPolicy | str | dict[str, Any] | None = None,
    description: str | None = None,
    execution: str | None = None,
    version: str = "",
    timeout: float | None = None,
    max_attempts: int | None = None,
    deterministic: bool | None = None,
    cost: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Declare a Python function as a tool; `runtime.tools.register(fn)` makes it callable."""
    if policy is None:
        raise TypeError(
            "a tool declares its policy: read_only, idempotent, deduplicated or "
            "non_repeatable (what a repeat of the call means)"
        )
    base = EffectPolicy.of(policy)
    overrides = {
        k: v
        for k, v in {
            "timeout": timeout,
            "max_attempts": max_attempts,
            "deterministic": deterministic,
            "cost": cost,
        }.items()
        if v is not None
    }
    resolved = replace(base, **overrides) if overrides else base

    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        mode = execution or ("async" if inspect.iscoroutinefunction(fn) else "thread")
        if mode not in EXECUTIONS:
            raise ValueError(f"execution must be one of {EXECUTIONS}, not {mode!r}")
        if mode == "process" and inspect.iscoroutinefunction(fn):
            raise ValueError("a process tool is a plain (blocking) function, not async")
        input_schema, output_schema = schemas_of(fn)
        effect_name = name or fn.__name__
        fn.__statecraft_effect__ = EffectDefinition(  # type: ignore[attr-defined]
            name=effect_name,
            description=(description or inspect.getdoc(fn) or "").strip(),
            input_schema=input_schema,
            output_schema=output_schema,
            policy=resolved,
            provider="python",
            operation=effect_name,
            version=version,
            execution=mode,
        )
        return fn

    return decorate


tool = effect


def definition_of(fn: Any) -> EffectDefinition:
    found = getattr(fn, "__statecraft_effect__", None)
    if found is None:
        raise TypeError(f"{fn!r} is not a tool: decorate it with @tool")
    return found


__all__ = ["EXECUTIONS", "EffectDefinition", "definition_of", "effect", "schemas_of", "tool"]
