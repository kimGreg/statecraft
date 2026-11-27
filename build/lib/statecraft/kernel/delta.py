"""Structural deltas between two JSON values, for history entries (roadmap C2).

    d = diff(old, new)        # None when equal
    patch(old, d) == new

A delta is one of
    {"=": value}                         replace
    {"{": {key: delta, ...}, "-": [keys]}  a dict: changed or added keys, removed keys
    {"+": [items]}                       a list that grew at its end (a conversation, a log)

What must hold
  * `patch(old, diff(old, new)) == new` for any JSON values (property-tested);
  * an append to a long list costs the appended items, not the list: a history of a growing
    conversation is linear in its length, not quadratic.
"""

from __future__ import annotations

import json
from typing import Any


def _same(a: Any, b: Any) -> bool:
    """Equal as JSON (`True == 1` in Python, not in JSON)."""
    return a == b and json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def diff(old: Any, new: Any) -> dict[str, Any] | None:
    if _same(old, new):
        return None
    if isinstance(old, dict) and isinstance(new, dict):
        changed = {}
        for key, value in new.items():
            if key not in old:
                changed[key] = {"=": value}
                continue
            inner = diff(old[key], value)
            if inner is not None:
                changed[key] = inner
        removed = [key for key in old if key not in new]
        found: dict[str, Any] = {"{": changed}
        if removed:
            found["-"] = removed
        return found
    grew = isinstance(old, list) and isinstance(new, list) and len(new) > len(old) and old
    if grew and _same(new[: len(old)], old):
        return {"+": new[len(old) :]}
    return {"=": new}


def patch(old: Any, delta: dict[str, Any] | None) -> Any:
    if delta is None:
        return old
    if "=" in delta:
        return delta["="]
    if "+" in delta:
        return [*old, *delta["+"]]
    result = {k: v for k, v in old.items() if k not in set(delta.get("-", ()))}
    for key, inner in delta["{"].items():
        result[key] = patch(old.get(key), inner) if key in old else inner["="]
    return result


__all__ = ["diff", "patch"]
