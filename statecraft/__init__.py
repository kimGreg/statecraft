"""statecraft: the engine, a durable process runtime for semi-agentic systems.

    agent       Agent, @state, Spawn, Wait, Done: an agent is a state machine
    agentic     AgenticAgent (the model loop as states), the Model port, core:decider
    engine      Engine: spawn, signal, answer, events, run
    stores      the state store: memory (tests) and SQLite (local)
    views       files derived from the records, and the tree's cumulative state
    workspace   the files port: a local folder, or any store with get/put/create
    testing     tools answered by plain functions

It knows no application: agents, their tools and their data live above it. Design:
docs/design.md.
"""

from .agent import Agent, ChildFailed, Done, Spawn, Wait, state
from .artifacts import Artifact
from .catalog import Catalog
from .engine import Engine

__all__ = [
    "Agent",
    "Artifact",
    "Catalog",
    "ChildFailed",
    "Done",
    "Engine",
    "Spawn",
    "Wait",
    "state",
]
