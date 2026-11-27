"""What a coding agent learns of a project before its first turn (from the workspace only).

    summary = await discover(files)      # files: an agent's `ctx.files`

The project is the workspace's `shared/` folder (a sandbox mounts it at `/workspace`). The
summary has: the files near the top (folders collapsed), the tools the markers suggest
(`pyproject.toml`: Python and pytest, `package.json`: npm, ...), the root `AGENTS.md` in full,
and where nested `AGENTS.md` files are (each applies to its folder: read it before working there).
"""

from __future__ import annotations

from typing import Any

ROOT = "shared/"
#: marker file -> what it suggests
MARKERS = {
    "pyproject.toml": "Python project (tests: `python -m pytest -q`)",
    "setup.py": "Python project (tests: `python -m pytest -q`)",
    "requirements.txt": "Python dependencies in requirements.txt",
    "package.json": "Node project (tests: `npm test`)",
    "Cargo.toml": "Rust project (tests: `cargo test`)",
    "go.mod": "Go module (tests: `go test ./...`)",
    "Makefile": "a Makefile (see its targets)",
}
SKIP = (".git/", "node_modules/", "__pycache__/", ".venv/", "dist/", "build/")
TREE_ENTRIES = 80
AGENTS_CHARS = 12_000


def _tree(paths: list[str]) -> list[str]:
    """Files at the top two levels; deeper ones counted under their folder."""
    shown: list[str] = []
    deeper: dict[str, int] = {}
    for path in paths:
        parts = path.split("/")
        if len(parts) <= 2:
            shown.append(path)
        else:
            folder = "/".join(parts[:2]) + "/"
            deeper[folder] = deeper.get(folder, 0) + 1
    shown += [f"{folder} ({n} more files)" for folder, n in sorted(deeper.items())]
    if len(shown) > TREE_ENTRIES:
        shown = [*shown[:TREE_ENTRIES], f"... {len(shown) - TREE_ENTRIES} more"]
    return shown


async def discover(files: Any) -> str:
    try:
        found = await files.list(ROOT)
    except (FileNotFoundError, PermissionError, KeyError):
        found = []
    paths = sorted(p.removeprefix(ROOT) for p in found if not any(s in p for s in SKIP))
    if not paths:
        return "The project folder is empty."
    parts = ["Project files (paths are relative to the project root):", *_tree(paths)]
    hints = [hint for marker, hint in MARKERS.items() if marker in paths]
    if hints:
        parts += ["", "Project tooling:", *(f"- {h}" for h in hints)]
    if "AGENTS.md" in paths:
        text = (await files.read_text(ROOT + "AGENTS.md")).strip()
        parts += [
            "",
            "AGENTS.md (the project's instructions for agents; follow them):",
            text[:AGENTS_CHARS],
        ]
    nested = [p for p in paths if p.endswith("/AGENTS.md")]
    if nested:
        parts += [
            "",
            "Nested AGENTS.md files (each applies to its folder; read it before working there):",
            *(f"- {p}" for p in nested),
        ]
    return "\n".join(parts)


__all__ = ["MARKERS", "ROOT", "discover"]
