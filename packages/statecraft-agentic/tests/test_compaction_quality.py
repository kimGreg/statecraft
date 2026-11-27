"""Compaction keeps what the agent needs (roadmap B3, compaction quality).

What must hold
  * deterministic: every fact planted in a long session reaches a summary request, whether the
    session is summarized at once or in chunks, and each chunk carries the summary so far;
  * with a real model (STATECRAFT_REAL_MODEL=1 and an OpenRouter key): after compacting a session
    with planted facts (decisions, file names, open tasks, tool results), the model answers
    questions about them from the summary alone; at least RECALL of them right. The threshold is
    the measure: change it only with a reason.
"""

from __future__ import annotations

import json
import os

import pytest
from statecraft_agentic.context import estimate, summary_request
from statecraft_agentic.loop import chunk_end
from statecraft_agentic.models import OPENROUTER_KEY_ENVS

RECALL = 0.8
FACTS = {
    "Which file holds the parser?": "src/parse_tree.py",
    "Which database was chosen?": "PostgreSQL",
    "What port does the server listen on?": "8443",
    "What did the tests command print as the number of failures?": "3",
    "Which module still needs a fix?": "auth",
    "Who asked for the change?": "Mira",
    "What is the release tag?": "v2.7.1",
    "Which library was rejected?": "leftpad",
    "What timeout was agreed for requests?": "45 seconds",
    "What color is the deploy dashboard banner?": "teal",
}
STATEMENTS = [
    "The parser lives in src/parse_tree.py.",
    "We decided to use PostgreSQL as the database.",
    "The server listens on port 8443.",
    "The tests printed: 3 failures.",
    "The auth module still needs a fix.",
    "Mira asked for the change.",
    "The release will be tagged v2.7.1.",
    "We rejected the leftpad library.",
    "Requests time out after 45 seconds.",
    "The deploy dashboard banner is teal.",
]


def session() -> list[dict]:
    messages = [
        {"role": "system", "content": "You are an agent."},
        {"role": "user", "content": "Ship it."},
    ]
    for n, statement in enumerate(STATEMENTS):
        call_id = f"c{n}"
        messages += [
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {"name": "note", "arguments": "{}"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": call_id, "content": "filler " * 120 + statement},
        ]
    messages.append({"role": "assistant", "content": "Working on it."})
    return messages


def chunks(messages: list[dict], limit: int | None) -> list[str]:
    """The summary requests compaction makes, chunk by chunk (each summary a placeholder that
    carries the facts it saw, as a perfect summarizer would)."""
    cut, compaction, requests = len(messages) - 1, None, []
    while compaction is None or compaction["covered_until"] < cut:
        end = chunk_end(messages, compaction, cut, limit)
        request = summary_request(messages, compaction, end)
        text = request[1]["content"]
        requests.append(text)
        seen = [s for s in STATEMENTS if s in text]
        compaction = {"covered_until": end, "summary": " ".join(seen), "count": 1}
    return requests


@pytest.mark.parametrize("limit", [None, 1200, 500])
def test_every_fact_reaches_a_summary_request(limit) -> None:
    requests = chunks(session(), limit)
    assert all(any(s in r for r in requests) for s in STATEMENTS)
    if limit is not None and len(requests) > 1:
        assert all(r.startswith("The agent's task") for r in requests)
        assert all("Summary so far" in r for r in requests[1:])  # the chunks build on each other
    assert estimate([{"role": "user", "content": requests[0]}]) > 0


@pytest.mark.skipif(
    os.environ.get("STATECRAFT_REAL_MODEL") != "1"
    or not any(os.environ.get(k) for k in OPENROUTER_KEY_ENVS),
    reason="a real-model run: set STATECRAFT_REAL_MODEL=1 and an OpenRouter key",
)
async def test_a_real_model_keeps_the_planted_facts() -> None:
    from statecraft_agentic import openrouter

    model = openrouter(os.environ.get("STATECRAFT_MODEL", "openai/gpt-oss-20b"))
    messages = session()
    cut, compaction = len(messages) - 1, None
    while compaction is None or compaction["covered_until"] < cut:
        end = chunk_end(messages, compaction, cut, 1500)
        reply = await model.complete(summary_request(messages, compaction, end), [])
        compaction = {"covered_until": end, "summary": reply["content"] or "", "count": 1}
    questions = list(FACTS)
    ask = [
        {
            "role": "system",
            "content": "Answer from the notes only. Reply with one JSON object "
            "mapping each question number to a short answer.",
        },
        {
            "role": "user",
            "content": "Notes:\n"
            + compaction["summary"]
            + "\n\nQuestions:\n"
            + "\n".join(f"{n}. {q}" for n, q in enumerate(questions, 1)),
        },
    ]
    reply = await model.complete(ask, [])
    text = (reply["content"] or "").strip()
    answers = json.loads(text[text.find("{") : text.rfind("}") + 1])
    right = sum(
        1
        for n, q in enumerate(questions, 1)
        if FACTS[q].lower() in str(answers.get(str(n), "")).lower()
    )
    assert right / len(questions) >= RECALL, (right, answers, compaction["summary"])
