"""Tools of the research sample: two slow searches (I/O) and a CPU-heavy ranking."""

from __future__ import annotations

import asyncio
import hashlib

from statecraft import tool


@tool(name="search.web", policy="read_only", timeout=10)
async def web(query: str) -> list[str]:
    """Search the web (slow I/O)."""
    await asyncio.sleep(0.2)
    return [f"web: {query} {n}" for n in range(3)]


@tool(name="search.papers", policy="read_only", timeout=10)
async def papers(query: str) -> list[str]:
    """Search papers (slow I/O)."""
    await asyncio.sleep(0.2)
    return [f"paper: {query} {n}" for n in range(2)]


@tool(name="text.rank", policy="read_only", execution="process", timeout=60)
def rank(documents: list[str], rounds: int = 20000) -> list[str]:
    """Rank documents by a deliberately CPU-heavy score (runs in a process)."""

    def score(text: str) -> str:
        digest = text.encode()
        for _ in range(rounds):
            digest = hashlib.sha256(digest).digest()
        return digest.hex()

    return sorted(documents, key=score)


__all__ = ["papers", "rank", "web"]
