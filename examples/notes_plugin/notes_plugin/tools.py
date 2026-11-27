"""The plugin's Python tool."""

from __future__ import annotations

from statecraft import tool


@tool(name="notes.normalize", policy="idempotent")
async def normalize(notes: list[str]) -> list[str]:
    """Trim notes and drop empty ones."""
    return [n.strip() for n in notes if n.strip()]
