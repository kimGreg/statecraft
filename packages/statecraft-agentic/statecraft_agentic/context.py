"""The model's context as a projection of the agent's durable history.

The conversation an agentic agent keeps (`data["messages"]`) is its durable history: it only
grows. What a model turn is sent is a projection of it:

    history     [system, task, m2, m3, ..., m17, m18]
    compaction  {"covered_until": 13, "summary": "...", "count": 1}
    projection  [system, task, <summary of m2..m12>, m13, ..., m18]

The system prompt and the task (the first `PINNED` messages) are always sent as they are. A
compaction replaces the messages before `covered_until` with one summary message; it never deletes
history. A cut is made only at a boundary: never inside an assistant message's tool calls and
their results (a tool result without its call, or a call without its result, is refused by
model APIs).

Tokens are estimated (`estimate`: characters / 4, the usual rule of thumb); the loop prefers the
prompt tokens the model reported for its previous turn, plus an estimate of what was added since.
"""

from __future__ import annotations

import json
from typing import Any

#: The system prompt and the task: never compacted.
PINNED = 2
CHARS_PER_TOKEN = 4
#: A message's text in a summary request is cut to this many characters.
TRANSCRIPT_CHARS = 4_000
SUMMARY_HEAD = (
    "[Summary of the earlier part of this session. Continue from where it ends: what it lists"
    " as done is done; do not do it again.]\n"
)

SUMMARIZE = """You compact the history of an agent's working session so it can go on with less
context. Write a summary the agent can continue from:
- the goal and any change of direction the user gave;
- every step done, in order, with the tool calls and their exact results (values, file paths,
  identifiers, numbers): list each one, do not merge or round them;
- the current state: the latest result the next step builds on;
- what is still to do, starting with the exact next step.
Keep facts, drop chatter. Do not invent anything that is not in the transcript. Answer with the
summary only."""


def text_of(message: dict[str, Any]) -> str:
    parts = [str(message.get("content") or "")]
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        parts.append(f"{function.get('name')}({function.get('arguments')})")
    return "\n".join(p for p in parts if p)


def estimate(messages: list[dict[str, Any]]) -> int:
    """Estimated tokens of messages (their JSON length / 4)."""
    size = len(json.dumps(messages, ensure_ascii=False))
    return (size + CHARS_PER_TOKEN - 1) // CHARS_PER_TOKEN


def boundaries(messages: list[dict[str, Any]], start: int = PINNED) -> list[int]:
    """Indexes `i >= start` where the history may be cut: every tool call before `i` has its
    result before `i`, and `messages[i]` is not a tool result."""
    found = []
    open_calls: set[str] = set()
    for i, message in enumerate(messages):
        if i >= start and not open_calls and message.get("role") != "tool":
            found.append(i)
        if message.get("role") == "assistant":
            open_calls |= {c["id"] for c in message.get("tool_calls") or [] if c.get("id")}
        elif message.get("role") == "tool":
            open_calls.discard(message.get("tool_call_id"))
    if not open_calls and len(messages) >= start:
        found.append(len(messages))
    return found


def covered(compaction: dict[str, Any] | None) -> int:
    return int((compaction or {}).get("covered_until") or PINNED)


def summary_message(compaction: dict[str, Any]) -> dict[str, Any]:
    return {"role": "user", "content": SUMMARY_HEAD + str(compaction["summary"])}


def project(
    messages: list[dict[str, Any]], compaction: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """What the model is sent: the pinned messages, the summary, the messages after it."""
    if not compaction:
        return [dict(m) for m in messages]
    head = [dict(m) for m in messages[:PINNED]]
    return [*head, summary_message(compaction), *(dict(m) for m in messages[covered(compaction) :])]


def cut_for(
    messages: list[dict[str, Any]], compaction: dict[str, Any] | None, keep_tokens: int
) -> int | None:
    """Where a new compaction ends: the latest boundary after the current one that still leaves
    at least `keep_tokens` of recent messages as they are (and at least one message); None when
    no such boundary exists (nothing worth compacting)."""
    start = covered(compaction)
    found = [
        i
        for i in boundaries(messages, start)
        if start < i < len(messages) and estimate(messages[i:]) >= keep_tokens
    ]
    return found[-1] if found else None


def transcript(messages: list[dict[str, Any]], compaction: dict[str, Any] | None, cut: int) -> str:
    """The part to summarize (after the previous summary, up to `cut`), as plain text."""
    lines = []
    if compaction:
        lines.append("Summary so far:\n" + str(compaction["summary"]))
    for message in messages[covered(compaction) : cut]:
        role = message.get("role")
        text = text_of(message)
        if len(text) > TRANSCRIPT_CHARS:
            text = text[:TRANSCRIPT_CHARS] + " ...[cut]"
        lines.append(f"[{role}] {text}")
    return "\n\n".join(lines)


def summary_request(
    messages: list[dict[str, Any]], compaction: dict[str, Any] | None, cut: int
) -> list[dict[str, Any]]:
    task = str(messages[1].get("content") or "") if len(messages) > 1 else ""
    return [
        {"role": "system", "content": SUMMARIZE},
        {
            "role": "user",
            "content": f"The agent's task:\n{task}\n\nThe session to summarize:\n\n"
            + transcript(messages, compaction, cut),
        },
    ]


__all__ = [
    "PINNED",
    "SUMMARIZE",
    "boundaries",
    "covered",
    "cut_for",
    "estimate",
    "project",
    "summary_request",
    "transcript",
]
