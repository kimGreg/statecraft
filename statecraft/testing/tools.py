"""Sample tools for tests and examples, at module level so a process pool can import them."""

from __future__ import annotations

import asyncio
import hashlib
import time

from ..sdk.effect import tool


@tool(name="math.add", policy="read_only")
async def add(a: float, b: float) -> float:
    """Add two numbers."""
    return a + b


@tool(name="math.square", policy="read_only")
async def square(n: float) -> float:
    """Square a number."""
    return n * n


@tool(name="work.sleep", policy="idempotent", timeout=60)
async def sleep(seconds: float) -> float:
    """Wait on the event loop (an I/O-bound call)."""
    await asyncio.sleep(seconds)
    return seconds


@tool(name="work.burn", policy="read_only", execution="process", timeout=120)
def burn(seconds: float) -> str:
    """Hold a CPU for `seconds` (in a process: the event loop keeps moving)."""
    deadline = time.monotonic() + seconds
    digest = b""
    while time.monotonic() < deadline:
        digest = hashlib.sha256(digest).digest()
    return digest.hex()[:8]


@tool(name="work.block", policy="read_only", execution="thread", timeout=60)
def block(seconds: float) -> float:
    """Block a thread for `seconds` (blocking I/O)."""
    time.sleep(seconds)
    return seconds


__all__ = ["add", "block", "burn", "sleep", "square"]
