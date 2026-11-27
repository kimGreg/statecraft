"""Effects as the kernel sees them: a policy saying what a repeat means, and classified failures.

A state can run again (design §2, invariant 3), so an effect called from it can be sent again.
The kernel never reads what an effect does; it only needs to know whether repeating it is safe:

    read_only        no side effects on the outside world                  repeat freely
    idempotent       the same request repeated has the same effect          repeat freely
    deduplicated     the provider applies an idempotency key once           repeat with the key
    non_repeatable   none of the above                                      never repeated; refused

`deterministic` (the same request gives the same result) and `cost` (a repeat costs again) are
separate from safety: a call may be safe to repeat, not deterministic, and metered.

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
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int):
            raise ValueError(f"max_attempts must be a whole number, not {self.max_attempts!r}")
        if self.max_attempts < 1:
            raise ValueError("max_attempts is at least 1")
        timeout = self.timeout
        if timeout is not None and (
            isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0
        ):
            raise ValueError(f"timeout must be positive seconds or None, not {timeout!r}")
        if not isinstance(self.deterministic, bool):
            raise ValueError(f"deterministic must be true or false, not {self.deterministic!r}")

    @classmethod
    def of(cls, value: EffectPolicy | str | dict[str, Any] | None) -> EffectPolicy:
        if value is None:
            return cls()
        if isinstance(value, EffectPolicy):
            return value
        if isinstance(value, str):
            return cls(kind=value)
        unknown = sorted(set(value) - set(cls.__dataclass_fields__))
        if unknown:
            raise ValueError(
                f"unknown policy fields {unknown}; known: {sorted(cls.__dataclass_fields__)}"
            )
        return cls(**value)

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


def digest_of(value: Any) -> str:
    body = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()
    return "sha256:" + hashlib.sha256(body).hexdigest()[:32]


__all__ = [
    "COSTS",
    "FAILURES",
    "KINDS",
    "EffectError",
    "EffectPolicy",
    "digest_of",
    "may_retry",
]
