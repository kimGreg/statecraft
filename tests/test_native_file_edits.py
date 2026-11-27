"""Native file changes are safe to repeat (roadmap A1.13).

What must hold
  * writing a whole file is idempotent; creating is create-only;
  * an edit is made against the digest the file had, read in an earlier step (committed data,
    so a rerun of the editing state uses the same expectation): when that state runs again after
    the edit
    was applied, the edit reports "already" and changes nothing; when someone else changed the
    file in between, it is refused and nothing is overwritten.
"""

from __future__ import annotations

from typing import ClassVar

from statecraft import Agent, Done, state
from statecraft.runtime.workspace import FileChanged
from statecraft.testing import memory_runtime


class Edits(Agent):
    """Reads the file in one step (committed), edits it against that in the next."""

    start = "read"
    crash_once: ClassVar[bool] = False
    outcomes: ClassVar[list[str]] = []

    @state
    async def read(self, ctx, data):
        path = "shared/notes.txt"
        data["before"] = await ctx.files.digest(path)
        data["text"] = (await ctx.files.read_text(path)) if data["before"] else ""
        return "edit"

    @state
    async def edit(self, ctx, data):
        try:
            outcome = await ctx.files.edit(
                "shared/notes.txt", data["text"] + "line\n", expected=data["before"]
            )
        except FileChanged:
            outcome = "refused"
        Edits.outcomes.append(outcome)
        if Edits.crash_once:
            Edits.crash_once = False
            raise RuntimeError("crashed after the edit")
        return Done


async def test_an_edit_applied_before_a_crash_is_not_applied_twice() -> None:
    Edits.outcomes, Edits.crash_once = [], True
    rt = memory_runtime()
    rt.registrar.agents.register("edits", Edits)
    async with rt:
        root = await rt.spawn("edits", root_id="e")
        await rt.run(root, timeout=5)
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        text = (await rt.read_file(root, "shared/notes.txt")).decode()
    assert Edits.outcomes == ["applied", "already"] and text == "line\n"


async def test_an_edit_against_a_changed_file_is_refused() -> None:
    Edits.outcomes, Edits.crash_once = [], True
    rt = memory_runtime()
    rt.registrar.agents.register("edits", Edits)
    async with rt:
        root = await rt.spawn("edits", root_id="e")
        await rt.run(root, timeout=5)  # applied, then the state failed
        files = await rt.workspaces.files_of(root)
        await files.put("shared/notes.txt", b"someone else\n")
        await rt.signal(root, "CONT")
        await rt.run(root, timeout=5)
        text = (await rt.read_file(root, "shared/notes.txt")).decode()
    assert Edits.outcomes == ["applied", "refused"] and text == "someone else\n"
