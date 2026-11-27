"""Effects as the kernel sees them: a policy saying what a repeat means, and classified failures.

A state can run again (design §2, invariant 3), so an effect called from it can be sent again.
The kernel never reads what an effect does; it only needs to know whether repeating it is safe:

    read_only        no side effects on the outside world                  repeat freely
    idempotent       the same request repeated has the same effect          repeat freely
    deduplicated     the provider applies an idempotency key once           repeat with the key
    non_repeatable   none of the above                                      never repeated; refused

`deterministic` (the same request gives the same result) and `cost` (a repeat costs again) are
separate from safety: a model call is safe to repeat, not deterministic, and metered.

Failures have one type with a class; whether a failed call is tried again follows the class and
the policy (`may_retry`). `unavailable` means the request provably was not sent.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

KINDS = ("read_only", "idempotent", "deduplicated", "non_repeatable")
COSTS = ("none", "metered")
FAILURES = ("invalid", "refused", "transient", "timeout", "unavailable", "failed")


@dataclass(frozen=True)
class EffectPolicy:
    kind: str = "non_repeatable"
    deterministic: bool = True
    cost: str = "none"
    timeout: float | None = 30.0
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"effect policy kind must be one of {KINDS}, not {self.kind!r}")
        if self.cost not in COSTS:
            raise ValueError(f"effect cost must be one of {COSTS}, not {self.cost!r}")
        if self.max_attempts < 1:
            raise ValueError("max_attempts is at least 1")

    @classmethod
    def of(cls, value: EffectPolicy | str | dict[str, Any] | None) -> EffectPolicy:
        if value is None:
            return cls()
        if isinstance(value, EffectPolicy):
            return value
        if isinstance(value, str):
            return cls(kind=value)
        return cls(**{k: v for k, v in value.items() if k in cls.__dataclass_fields__})

    def repeatable(self, keyed: bool) -> bool:
        """May a call be sent again (a rerun of its state, or a retry)?"""
        if self.kind in ("read_only", "idempotent"):
            return True
        return self.kind == "deduplicated" and keyed

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def digest(self) -> str:
        return digest_of(self.as_dict())


class EffectError(Exception):
    """A classified effect failure. `cls` is one of FAILURES."""

    def __init__(
        self, cls: str, message: str, *, effect: str = "", code: str = "", sent: bool = True
    ) -> None:
        if cls not in FAILURES:
            raise ValueError(f"failure class must be one of {FAILURES}, not {cls!r}")
        super().__init__(f"{effect + ': ' if effect else ''}{cls}: {message}")
        self.cls, self.message, self.effect, self.code = cls, message, effect, code
        #: False only when the provider can show the request never left: then a retry is safe.
        self.sent = sent


def may_retry(policy: EffectPolicy, failure: EffectError, *, keyed: bool, attempt: int) -> bool:
    """Try a failed call again? Never past `max_attempts`; `unavailable` (provably not sent)
    always may; `transient` and `timeout` only when repeating is safe; the rest never."""
    if attempt >= policy.max_attempts:
        return False
    if failure.cls == "unavailable" and not failure.sent:
        return True
    if failure.cls in ("transient", "timeout", "unavailable"):
        return policy.repeatable(keyed)
    return False


#: Phrases model endpoints use when a request is over the model's context window.
CONTEXT_OVERFLOW = (
    "context length",
    "context_length",
    "maximum context",
    "context window",
    "too many tokens",
    "prompt is too long",
    "reduce the length",
)


def classify_status(status: int, body: str = "", *, effect: str = "") -> EffectError:
    """An HTTP error answer as a failure: 429 and 5xx `transient`, 408 `timeout`, 401/403
    `refused` (code `auth`), a 400 about the context window `invalid` (code
    `context_overflow`), other 4xx `invalid`."""
    text = body[:300]
    if status == 429 or status >= 500:
        return EffectError("transient", f"answered {status}", effect=effect, code=str(status))
    if status == 408:
        return EffectError("timeout", f"answered {status}", effect=effect, code="408")
    if status in (401, 403):
        return EffectError("refused", f"answered {status}: {text}", effect=effect, code="auth")
    if any(p in body.lower() for p in CONTEXT_OVERFLOW):
        return EffectError(
            "invalid", f"over the context window: {text}", effect=effect, code="context_overflow"
        )
    return EffectError("invalid", f"answered {status}: {text}", effect=effect, code=str(status))


def classify(exc: BaseException, *, effect: str = "") -> EffectError:
    """An exception as a classified failure, for tools that call networks:

        except Exception as exc:
            raise classify(exc) from exc

    `EffectError` as it is; refused connections `unavailable` (never sent); timeouts `timeout`;
    other connection and transport errors `transient`; an HTTP error answer by its status
    (`classify_status`); anything else `failed` (not retried). Exceptions are recognized by
    type name too, so HTTP libraries need not be imported here."""
    if isinstance(exc, EffectError):
        return exc
    names = {k.__name__ for k in type(exc).__mro__}
    message = f"{type(exc).__name__}: {exc}"[:300]
    if names & {"ConnectionRefusedError", "ConnectError", "ConnectTimeout"}:
        return EffectError("unavailable", message, effect=effect, sent=False)
    if names & {"TimeoutError", "TimeoutException", "ReadTimeout", "WriteTimeout", "PoolTimeout"}:
        return EffectError("timeout", message, effect=effect)
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if isinstance(status, int):
        body = str(getattr(exc.response, "text", "") or "")  # type: ignore[attr-defined]
        return classify_status(status, body, effect=effect)
    if names & {"ConnectionError", "TransportError", "RemoteProtocolError", "BrokenPipeError"}:
        return EffectError("transient", message, effect=effect)
    return EffectError("failed", message, effect=effect)


def digest_of(value: Any) -> str:
    body = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()
    return "sha256:" + hashlib.sha256(body).hexdigest()[:32]


__all__ = [
    "CONTEXT_OVERFLOW",
    "COSTS",
    "FAILURES",
    "KINDS",
    "EffectError",
    "EffectPolicy",
    "classify",
    "classify_status",
    "digest_of",
    "may_retry",
]
