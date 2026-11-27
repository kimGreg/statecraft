"""Views: files derived from the committed records, for readers that cannot ask the store.

A view is rebuilt from the records alone, so losing one costs nothing. The executor refreshes a
root's views before a step of that root runs, whenever the tree changed, so a state's tools see
every publication committed before it. A view declares the workspace paths it writes
(`paths`); every agent of the tree may read them.

    class Index:
        paths = ("index/",)

        async def refresh(self, files, tree, *, read, spec_of):
            for _seq, record, kind, ref in published_in_order(tree):
                await files.put(f"index/{kind}.json", await read(ref["path"]))

`StateView` is a ready one: the default state and an index of the latest publications.

`agents_state` is the tree's default cumulative state (`ctx.state()`); an application may give
the engine its own (`Engine(state=...)`).
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from ..kernel.process import Record


class View(Protocol):
    paths: tuple[str, ...]

    async def refresh(self, files: Any, tree: list[Record], *, read: Any, spec_of: Any) -> None: ...


def short_id(record: Record) -> str:
    return record.agent.partition(":")[2] or record.agent


def published_in_order(tree: list[Record]) -> list[tuple[int, Record, str, dict[str, Any]]]:
    """(sequence, record, artifact type, reference) of every publication, in the order the store
    stamped them."""
    found = [
        (int(ref.get("seq") or 0), record, kind, ref)
        for record in tree
        for kind, ref in record.published.items()
    ]
    return sorted(found, key=lambda x: (x[0], x[1].id, int(x[3].get("n") or 0), x[2]))


async def agents_state(tree: list[Record], read: Any, spec_of: Any) -> dict[str, Any]:
    """Each agent's entry by its short id, in id order: its notes, the artifact types it
    published (in order) and the summary of its last artifact."""
    agents: dict[str, dict[str, Any]] = {}
    for _seq, record, kind, ref in published_in_order(tree):
        entry = agents.setdefault(short_id(record), {**record.notes, "published": []})
        if kind not in entry["published"]:
            entry["published"].append(kind)
        entry["summary"] = json.loads(await read(ref["path"])).get("summary", {})
    for record in tree:
        if record.notes and short_id(record) not in agents:
            agents[short_id(record)] = dict(record.notes)
    return {"agents": dict(sorted(agents.items()))}


def dumps(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True, default=str).encode()


class StateView:
    """Writes, for tools that read files:

    view/state.json                  the tree's default state (`agents_state`)
    view/index.json                  artifact type -> its latest publication's object path
    """

    paths = ("view/",)

    async def refresh(self, files: Any, tree: list[Record], *, read: Any, spec_of: Any) -> None:
        index = {kind: ref["path"] for _seq, _record, kind, ref in published_in_order(tree)}
        await files.put("view/index.json", dumps(index))
        await files.put("view/state.json", dumps(await agents_state(tree, read, spec_of)))


__all__ = ["StateView", "View", "agents_state", "dumps", "published_in_order", "short_id"]
