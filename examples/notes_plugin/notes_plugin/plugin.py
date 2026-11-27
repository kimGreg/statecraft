"""The plugin: `register(registrar)` is all statecraft calls (entry point `notes`)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from statecraft_agentic import ScriptedModel, add_model, add_skills, openrouter, say

from .agents import Digest
from .tools import normalize


def summarize(messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
    """A stand-in model for the skill: one sentence about the notes it was given."""
    notes = [n for n in messages[1]["content"].splitlines() if n]
    return say(
        json.dumps(
            {"summary": f"{len(notes)} notes, starting with: {notes[0]}", "notes": len(notes)}
        )
    )


def register(registrar: Any) -> None:
    registrar.tools.register(normalize)
    registrar.agents.register(
        "notes:digest",
        Digest,
        version="0.1.0",
        input={"type": "object", "required": ["notes"]},
        tools=["notes.normalize", "words.count"],
        spawns=["agentic:skill"],
        reads=["summary"],
        writes=["digest"],
    )
    add_skills(registrar, Path(__file__).parent / "skills")
    real = os.environ.get("STATECRAFT_REAL_MODEL")
    add_model(registrar, "default", openrouter(real) if real else ScriptedModel(step_fn=summarize))
