"""The artifact envelope: one typed output of an agent, as it is stored.

An application may give the engine its own envelope: a subclass with more fields and their
defaults (`Engine(envelope=...)`). The engine writes and reads every artifact through it.
Envelopes hold no time stamps, so an unchanged output gives an unchanged object.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = "0.1.0"


class Artifact(BaseModel):
    """One typed output of an agent: its type, its payload and a short summary."""

    model_config = ConfigDict(extra="allow")

    artifact_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] | str = Field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION


__all__ = ["SCHEMA_VERSION", "Artifact"]
