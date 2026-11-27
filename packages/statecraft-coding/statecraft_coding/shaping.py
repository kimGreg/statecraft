"""What a coding model sees of long outputs: the head and the tail, and how much was cut."""

from __future__ import annotations

from typing import Any

#: Characters of a command's output or a file a model is shown at once.
OUTPUT_CHARS = 8_000
FILE_CHARS = 40_000


def clip(text: str, limit: int = OUTPUT_CHARS) -> str:
    """The first and last parts of `text` with the number of lines cut between them."""
    if len(text) <= limit:
        return text
    head, tail = text[: limit * 2 // 5], text[-limit * 3 // 5 :]
    cut = text[len(head) : len(text) - len(tail)]
    return f"{head}\n[... {cut.count(chr(10)) + 1} lines, {len(cut)} characters cut ...]\n{tail}"


def numbered(text: str, start: int = 1) -> str:
    """Lines with their numbers (`   12| ...`), as a model reads code before an edit."""
    return "\n".join(f"{n:>5}| {line}" for n, line in enumerate(text.split("\n"), start))


def shell_result(result: Any) -> dict[str, Any]:
    """A shell effect's result for the model: exit code first, output shaped."""
    if not isinstance(result, dict):
        return {"output": clip(str(result))}
    output = result.get("output")
    if output is None:
        output = (result.get("stdout") or "") + (result.get("stderr") or "")
    return {"exit": result.get("exit"), "output": clip(str(output))}


__all__ = ["FILE_CHARS", "OUTPUT_CHARS", "clip", "numbered", "shell_result"]
