"""Against a real OpenSandbox server (skipped unless STATECRAFT_OPENSANDBOX is its URL, with
STATECRAFT_OPENSANDBOX_KEY and a `allowed_host_paths` entry covering the test's folder in
STATECRAFT_OPENSANDBOX_ROOT): the conformance suite and a command in the shared workspace."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from statecraft_opensandbox import OpenSandboxHTTP, OpenSandboxProvider

from statecraft.testing.effect_contract import *  # noqa: F403
from statecraft.testing.effect_contract import EffectHarness

ENDPOINT = os.environ.get("STATECRAFT_OPENSANDBOX")
ROOT = os.environ.get("STATECRAFT_OPENSANDBOX_ROOT")
pytestmark = pytest.mark.skipif(
    not (ENDPOINT and ROOT), reason="no OpenSandbox server (STATECRAFT_OPENSANDBOX)"
)


class View:
    def __init__(self, root: Path) -> None:
        self.root = root

    def host_path(self, workspace: str, relative: str) -> str:
        path = self.root / workspace / relative
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o777)
        return str(path)

    async def ended(self, agent_ids):
        return set()


@pytest.fixture
def effect_harness(tmp_path: Path) -> EffectHarness:
    run = Path(ROOT or "") / f"it-{uuid.uuid4().hex[:8]}"
    backend = OpenSandboxHTTP(
        ENDPOINT or "", api_key=os.environ.get("STATECRAFT_OPENSANDBOX_KEY", "")
    )
    provider = OpenSandboxProvider(
        backend, receipts=tmp_path / "receipts.db", poll=0.2, destroy_on_cancel=True
    )
    provider.bind(View(run))
    shared = run / "w" / "shared"

    def applied(tag: str) -> int:
        log = shared / "applied.txt"
        return log.read_text().split().count(tag) if log.exists() else 0

    def alive(tag: str) -> bool:  # inside the sandbox: a marker the command removes on exit
        return (shared / f"{tag}.running").exists()

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
            "command": f"echo {tag} >> applied.txt; touch {tag}.running; "
            f"trap 'rm -f {tag}.running' EXIT; sleep {s}"
        },
        bad_request={"command": ""},
        alive=alive,
        scope=scope,
    )
