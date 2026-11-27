"""Skills: knowledge and procedures an agentic agent consults (Agent Skills format: a folder with
`SKILL.md` and its resources). Owned by this extension; the runtime has no notion of a skill.

    catalog = SkillCatalog(); catalog.add_folder("./skills")

When an agentic agent starts, the skills its definition allows are **pinned** (`{name: digest}`)
and an immutable copy of each is written to the workspace (`shared/.skills/<digest>/...`, or the
agent's private folder when it may not write shared/). Loading a skill later reads that copy, so a
crash and a changed skill between start and first use change nothing. A skill grants no
permission: the tools it mentions run only if the agent's definition permits them; its scripts
run only as registered tools.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
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
    digest = hashlib.sha256()
    for relative in files:
        digest.update(relative.encode())
        digest.update((folder / relative).read_bytes())
    return Skill(
        name=name,
        description=str(front.get("description") or ""),
        folder=folder,
        allowed_tools=tuple(allowed.split() if isinstance(allowed, str) else allowed),
        files=files,
        digest="sha256-" + digest.hexdigest()[:24],
    )


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
        """Write an immutable copy of the skill's current version; returns where it is."""
        skill = self.get(name)
        base = self.copy_root(ctx, skill.digest)
        for relative in skill.files:
            target = f"{base}/{relative}"
            try:
                if await ctx.files.exists(target):
                    continue
            except PermissionError:
                pass
            await ctx.files.write(target, (skill.folder / relative).read_bytes())
        return base

    @staticmethod
    def copy_root(ctx: Any, digest: str) -> str:
        if getattr(ctx, "_mode", "own") == "ro":
            return f"home/{ctx.id}/private/skills/{digest}"
        return f"shared/.skills/{digest}"

    async def read(self, ctx: Any, digest: str, relative: str = SKILL_FILE) -> str:
        return await ctx.files.read_text(f"{self.copy_root(ctx, digest)}/{relative}")


__all__ = ["Skill", "SkillCatalog", "front_matter", "load_skill"]
