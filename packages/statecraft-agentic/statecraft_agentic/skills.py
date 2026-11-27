"""Skills: knowledge and procedures an agentic agent consults (Agent Skills format: a folder with
`SKILL.md` and its resources). Owned by this extension; the runtime has no notion of a skill.

    catalog = SkillCatalog(); catalog.add_folder("./skills")

A skill's files are read once, when the catalog loads it; its digest is computed from those bytes.
When an agentic agent starts, the skills its definition allows are **pinned** (`{name: digest}`)
and a copy of those bytes is written create-only to the agent's own `home/<agent>/skills/
<digest>/` (no agent may write there, so no other agent can change it). Loading a skill later
reads that copy and checks its digest, so a crash, a changed skill on disk, or a changed copy
change nothing unnoticed. A skill grants no permission: the tools it mentions run only if the
agent's definition permits them; its scripts run only as registered tools.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SKILL_FILE = "SKILL.md"
NAME = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")


@dataclass(frozen=True)
class Skill:
    name: str
    description: str
    folder: Path
    allowed_tools: tuple[str, ...]
    files: tuple[str, ...]
    digest: str
    #: the files' bytes as read when the catalog loaded the skill (what the digest covers)
    contents: dict[str, bytes] = field(default_factory=dict, repr=False, compare=False)


def front_matter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return {}
    _, block, _rest = text.split("---", 2)
    return dict(yaml.safe_load(block) or {})


def load_skill(folder: Path) -> Skill:
    front = front_matter(folder / SKILL_FILE)
    name = str(front.get("name") or "")
    if name != folder.name or not NAME.fullmatch(name):
        raise ValueError(f"{folder}: a skill's name is its folder's name, lowercase kebab-case")
    allowed = front.get("allowed-tools", front.get("allowed_tools")) or ""
    files = tuple(
        str(p.relative_to(folder))
        for p in sorted(folder.rglob("*"))
        if p.is_file() and "__pycache__" not in p.parts
    )
    contents = {relative: (folder / relative).read_bytes() for relative in files}
    return Skill(
        name=name,
        description=str(front.get("description") or ""),
        folder=folder,
        allowed_tools=tuple(allowed.split() if isinstance(allowed, str) else allowed),
        files=files,
        digest=digest_of(contents),
        contents=contents,
    )


def digest_of(contents: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for relative in sorted(contents):
        digest.update(relative.encode())
        digest.update(contents[relative])
    return "sha256-" + digest.hexdigest()[:24]


class SkillCatalog:
    def __init__(self) -> None:
        self.skills: dict[str, Skill] = {}

    def add_folder(self, folder: str | Path) -> list[Skill]:
        found = []
        for marker in sorted(Path(folder).rglob(SKILL_FILE)):
            skill = load_skill(marker.parent)
            self.skills[skill.name] = skill
            found.append(skill)
        return found

    def get(self, name: str) -> Skill:
        if name not in self.skills:
            raise KeyError(f"no skill {name!r}; known: {sorted(self.skills)}")
        return self.skills[name]

    def pin(self, names: list[str]) -> dict[str, str]:
        return {name: self.get(name).digest for name in names}

    async def keep(self, ctx: Any, name: str) -> str:
        """Write the pinned copy of the skill (create-only); returns where it is."""
        skill = self.get(name)
        base = self.copy_root(ctx, skill.digest)
        for relative, data in skill.contents.items():
            await ctx.files.create(f"{base}/{relative}", data)
        return base

    @staticmethod
    def copy_root(ctx: Any, digest: str) -> str:
        return f"home/{ctx.id}/skills/{digest}"

    async def read(self, ctx: Any, digest: str, relative: str = SKILL_FILE) -> str:
        """A file of the pinned copy, after checking the copy still has its digest."""
        base = self.copy_root(ctx, digest)
        paths = await ctx.files.list(base)
        contents = {p.removeprefix(base + "/"): await ctx.files.read(p) for p in paths}
        if digest_of(contents) != digest:
            raise RuntimeError(f"the pinned copy of a skill changed ({digest}): not used")
        return contents[relative].decode("utf-8")


__all__ = ["Skill", "SkillCatalog", "front_matter", "load_skill"]
