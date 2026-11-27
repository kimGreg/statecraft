"""The memory and local workspaces pass the workspace files conformance suite."""

from __future__ import annotations

import pytest

from statecraft.providers.workspaces import LocalWorkspace, MemoryWorkspace
from statecraft.testing.workspace_contract import *  # noqa: F403


@pytest.fixture(params=["memory", "local"])
def workspace(request, tmp_path):
    return MemoryWorkspace() if request.param == "memory" else LocalWorkspace(tmp_path)


@pytest.fixture
async def files(workspace):
    return await workspace.open("w")
