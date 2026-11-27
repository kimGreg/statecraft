"""The Python tool provider: `@tool` functions, run where their execution mode says.

    async     on the event loop (the function is `async def`)
    thread    in a worker thread (blocking I/O, short CPU work)
    process   in a process pool (CPU-heavy work); the function must be importable by name

An exception the function raises is the tool reporting an error (`failed`); an `EffectError` it
raises keeps its class. Threads and processes cannot be interrupted: a cancelled call stops
waiting for them, and their result is dropped.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from ..kernel.effects import EffectError
from ..sdk.effect import KEY_PARAMETER, EffectDefinition


class PythonProvider:
    def __init__(self, *, concurrency: int | None = None, processes: int | None = None) -> None:
        self.concurrency = concurrency
        self.processes = processes or max(1, min(4, os.cpu_count() or 1))
        self._fns: dict[str, tuple[EffectDefinition, Any]] = {}
        self._pool: ProcessPoolExecutor | None = None

    def add(self, definition: EffectDefinition, fn: Any) -> None:
        self._fns[definition.operation or definition.name] = (definition, fn)

    async def start(self) -> None:
        return None

    async def discover(self) -> list[EffectDefinition]:
        return []  # registered one by one

    def _process_pool(self) -> ProcessPoolExecutor:
        if self._pool is None:
            self._pool = ProcessPoolExecutor(
                max_workers=self.processes, mp_context=multiprocessing.get_context("spawn")
            )
        return self._pool

    async def invoke(self, operation: str, arguments: dict[str, Any], context: Any) -> Any:
        if operation not in self._fns:
            raise EffectError("invalid", f"no Python tool {operation!r}", sent=False)
        definition, fn = self._fns[operation]
        if KEY_PARAMETER in inspect.signature(fn).parameters:
            arguments = {**arguments, KEY_PARAMETER: getattr(context, "key", None)}
        try:
            if definition.execution == "async":
                return await fn(**arguments)
            loop = asyncio.get_running_loop()
            pool = None if definition.execution == "thread" else self._process_pool()
            if pool is None:
                ctx = contextvars.copy_context()
                call = functools.partial(ctx.run, functools.partial(fn, **arguments))
            else:
                call = functools.partial(fn, **arguments)
            work = loop.run_in_executor(pool, call)
            # a thread or a process cannot be stopped: cancelling the call leaves the work
            # running, and the effect manager holds its capacity until it ends
            if context is not None and hasattr(context, "work"):
                context.work.append(work)
            return await asyncio.shield(work)
        except EffectError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise EffectError("failed", f"{type(exc).__name__}: {exc}") from exc

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None


__all__ = ["PythonProvider"]
