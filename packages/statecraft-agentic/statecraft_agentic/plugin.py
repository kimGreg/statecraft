"""The extension as a plugin (`plugins: [agentic]`). Configuration (`extensions.agentic`):

    models:
      default: {provider: openai-compatible, endpoint: ..., model: ..., api_key_env: ...}
      cheap:   {provider: openrouter, model: openai/gpt-oss-20b}
      test:    {provider: scripted, steps: [{say: "..."}, {call: tool, arguments: {...}}]}
    skill_paths: [./skills]
    concurrency: 4                     # model calls at once

Registers the `model.<name>` tools, the skill catalog (a service its agents use), the reusable
agents (`agentic:decider`, `agentic:skill`, `agentic:sequence`, `agentic:fanout`) and a validator
for agents' `extensions.agentic` settings.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .agents import Decider, FanOut, Sequence, SkillAgent
from .loop import SKILLS_SERVICE, AgenticAgent
from .models import PROVIDER, ModelProvider, client_from
from .skills import SkillCatalog


def register(registrar: Any) -> None:
    settings = registrar.settings("agentic")
    provider = model_provider(registrar, concurrency=settings.get("concurrency"))
    for name, model in (settings.get("models") or {}).items():
        provider.add(name, client_from(model))
    catalog = skill_catalog(registrar)
    for folder in settings.get("skill_paths") or []:
        catalog.add_folder(Path(folder))
    models = [f"model.{n}" for n in (settings.get("models") or {})] or ["model.*"]
    registrar.agents.register("agentic:decider", Decider, tools=models, writes=["decision"])
    registrar.agents.register("agentic:skill", SkillAgent, tools=["*"], spawns=[])
    registrar.agents.register("agentic:sequence", Sequence, spawns=["*"], writes=["sequence"])
    registrar.agents.register("agentic:fanout", FanOut, spawns=["*"], writes=["fanout"])
    registrar.validator(check)


def model_provider(registrar: Any, concurrency: int | None = None) -> ModelProvider:
    found = registrar._r.providers.get(PROVIDER)
    if found is None:
        found = registrar.providers.register(PROVIDER, ModelProvider(concurrency=concurrency))
    return found


def skill_catalog(registrar: Any) -> SkillCatalog:
    found = registrar.service(SKILLS_SERVICE)
    if found is None:
        found = registrar.provide(SKILLS_SERVICE, SkillCatalog())
    return found


def add_model(registrar: Any, name: str, client: Any) -> None:
    """Register a model client in code (tests, embedded use)."""
    model_provider(registrar).add(name, client)


def add_skills(registrar: Any, folder: str | Path) -> None:
    skill_catalog(registrar).add_folder(Path(folder))


def check(registry: Any) -> list[str]:
    """Agents' agentic settings: their model tool is granted, offered tools are granted, skills
    exist, and a skill asks only for tools the agent may call."""
    problems: list[str] = []
    catalog: SkillCatalog | None = registry.services.get(SKILLS_SERVICE)
    for name, definition in registry.agents.items():
        cls = definition.cls
        if cls is None or not (isinstance(cls, type) and issubclass(cls, AgenticAgent)):
            continue
        settings = definition.extension("agentic")
        model = f"model.{settings.get('model') or cls.model}"
        if not definition.permits_tool(model):
            problems.append(f"agent {name} uses {model} but may not call it (add it to tools)")
        for tool in settings.get("tools") or []:
            if not definition.permits_tool(tool):
                problems.append(f"agent {name} offers {tool} to its model but may not call it")
        for skill in settings.get("skills") or []:
            if catalog is None or skill not in catalog.skills:
                problems.append(f"agent {name} uses unknown skill {skill}")
                continue
            for tool in catalog.skills[skill].allowed_tools:
                if not definition.permits_tool(tool):
                    problems.append(
                        f"agent {name}: skill {skill} asks for {tool}, which the "
                        "agent may not call (a skill grants no permission)"
                    )
    return problems


__all__ = ["add_model", "add_skills", "check", "model_provider", "register", "skill_catalog"]
