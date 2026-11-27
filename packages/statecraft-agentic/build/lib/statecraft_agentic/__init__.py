"""statecraft-agentic: model-driven agents as ordinary statecraft state machines.

    plugins: [agentic]                     # enable it; configure `extensions.agentic`
    from statecraft_agentic import AgenticAgent

The kernel never sees a model: model calls are tools (`model.<name>`) this extension registers,
so they are checked, retried by policy and recorded like every other call. Skills are this
extension's catalog, pinned when an agent starts.
"""

from .agents import Decider, FanOut, Sequence, SkillAgent
from .loop import AgenticAgent, model_name_of, parse_json
from .models import ChatModel, ModelProvider, ScriptedModel, call, openrouter, say
from .plugin import add_model, add_skills, register
from .skills import Skill, SkillCatalog

__all__ = [
    "AgenticAgent",
    "ChatModel",
    "Decider",
    "FanOut",
    "ModelProvider",
    "ScriptedModel",
    "Sequence",
    "Skill",
    "SkillAgent",
    "SkillCatalog",
    "add_model",
    "add_skills",
    "call",
    "model_name_of",
    "openrouter",
    "parse_json",
    "register",
    "say",
]
