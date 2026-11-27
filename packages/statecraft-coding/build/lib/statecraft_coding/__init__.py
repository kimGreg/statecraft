"""statecraft-coding: a coding agent on the agentic harness (prompts, project discovery,
`AGENTS.md`, file tools that are safe to repeat, the shell through the granted shell effect,
output shaping) and a CLI session (`python -m statecraft_coding`)."""

from .agent import PROMPT, CodingAgent, project_path
from .discovery import discover
from .shaping import clip, numbered, shell_result

__all__ = ["PROMPT", "CodingAgent", "clip", "discover", "numbered", "project_path", "shell_result"]
