"""The OpenSandbox provider passes the effect provider conformance suite on the fake backend
(deduplicated, reconcile, remote_work; tree_termination with destroy_on_cancel)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from statecraft_opensandbox import FakeBackend, OpenSandboxProvider

from statecraft.testing.effect_contract import *  # noqa: F403
from statecraft.testing.effect_contract import EffectHarness


class View:
    def __init__(self, root: Path) -> None:
        self.root = root

    def host_path(self, workspace: str, relative: str) -> str:
        path = self.root / workspace / relative
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    async def ended(self, agent_ids):
        return set()


@pytest.fixture(params=["keep", "destroy_on_cancel"])
def effect_harness(request, tmp_path: Path) -> EffectHarness:
    provider = OpenSandboxProvider(
        FakeBackend(),
        receipts=tmp_path / "receipts.db",
        poll=0.02,
        destroy_on_cancel=request.param == "destroy_on_cancel",
    )
    provider.bind(View(tmp_path / "ws"))
    shared = tmp_path / "ws" / "w" / "shared"
    log = shared / "applied.txt"

    def applied(tag: str) -> int:
        return log.read_text().split().count(tag) if log.exists() else 0

    def alive(tag: str) -> bool:
        pid_file = shared / f"{tag}.pid"
        if not pid_file.exists() or not pid_file.read_text().strip():
            return False
        pid = int(pid_file.read_text())
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        stat = Path(f"/proc/{pid}/stat")
        return not stat.exists() or stat.read_text().rsplit(")", 1)[1].split()[0] != "Z"

    scope = {"workspace": "w", "shared": "rw", "private": "home/a/private"}
    scope["mounts"] = [
        {"source": "shared", "target": "/workspace", "mode": "rw"},
        {"source": "home/a/private", "target": "/home/agent", "mode": "rw"},
    ]
    return EffectHarness(
        provider=provider,
        operation="exec",
        request=lambda tag: {"command": f"echo {tag} >> applied.txt"},
        applied=applied,
        slow_request=lambda tag, s: {
            "command": f"echo {tag} >> applied.txt; sleep {s} & echo $! > {tag}.pid; wait"
        },
        bad_request={"command": ""},
        alive=alive,
        scope=scope,
    )
