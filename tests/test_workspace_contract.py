"""The memory and local workspaces pass the workspace files conformance suite."""

from __future__ import annotations

import pytest

from statecraft.providers.workspaces import LocalWorkspace, MemoryWorkspace
from statecraft.testing.workspace_contract import *  # noqa: F403


@pytest.fixture(params=["memory", "local"])
async def files(request, tmp_path):
    workspace = MemoryWorkspace() if request.param == "memory" else LocalWorkspace(tmp_path)
    return await workspace.open("w")
