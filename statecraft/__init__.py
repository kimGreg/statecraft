"""statecraft: a durable execution runtime for composable agents.

    kernel      durable execution of state machines: records, transitions, the store contract,
                effect policies, the executor
    sdk         what authors write: Agent, @state, Spawn, Wait, Done, @tool, Context
    runtime     registries, the effect manager, workspaces, configuration, the Runtime
    providers   stores (memory, SQLite), workspaces (local, memory), Python tools
    server, client, cli   the same API over HTTP, remotely, and in a shell
    testing     an in-memory runtime and function providers

The kernel has no notion of a model, a prompt, a skill or reasoning; extensions add them
(statecraft-agentic, statecraft-mcp, statecraft-console). Design: docs/design.md (how a step
runs) and docs/architecture.md (what surrounds it).
"""

from .kernel.effects import EffectError, EffectPolicy
from .ports.effects import classify
from .runtime.registry import AgentDefinition, Registrar
from .runtime.runtime import Runtime
from .sdk.agent import Agent, ChildFailed, Done, ExecutableAgent, Spawn, Wait, state
from .sdk.contracts import Artifact
from .sdk.effect import EffectDefinition, effect, tool

__all__ = [
    "Agent",
    "AgentDefinition",
    "Artifact",
    "ChildFailed",
    "Done",
    "EffectDefinition",
    "EffectError",
    "EffectPolicy",
    "ExecutableAgent",
    "Registrar",
    "Runtime",
    "Spawn",
    "Wait",
    "classify",
    "effect",
    "state",
    "tool",
]
