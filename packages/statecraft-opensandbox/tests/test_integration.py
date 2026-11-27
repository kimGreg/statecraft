"""Against a real OpenSandbox server (skipped unless STATECRAFT_OPENSANDBOX is its URL, with
STATECRAFT_OPENSANDBOX_KEY and a `allowed_host_paths` entry covering the test's folder in
STATECRAFT_OPENSANDBOX_ROOT): the conformance suite and a command in the shared workspace."""

from __future__ import annotations

import os
import time
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


@pytest.fixture(params=["interrupt", "destroy_on_cancel"])
async def effect_harness(request, tmp_path: Path):
    run = Path(ROOT or "") / f"it-{uuid.uuid4().hex[:8]}"
    backend = OpenSandboxHTTP(
        ENDPOINT or "", api_key=os.environ.get("STATECRAFT_OPENSANDBOX_KEY", "")
    )
    provider = OpenSandboxProvider(
        backend,
        receipts=tmp_path / "receipts.db",
        poll=0.2,
        destroy_on_cancel=request.param == "destroy_on_cancel",
        scope="call" if request.param == "destroy_on_cancel" else "agent",
    )
    provider.bind(View(run))
    shared = run / "w" / "shared"

    def applied(tag: str) -> int:
        log = shared / "applied.txt"
        return log.read_text().split().count(tag) if log.exists() else 0

    def alive(tag: str) -> bool:  # the command beats every 0.2 s while it runs
        beat = shared / f"{tag}.beat"
        if not beat.exists():
            return False
        before = beat.stat().st_mtime_ns
        time.sleep(0.6)
        return beat.stat().st_mtime_ns != before

    scope = {"workspace": "w", "shared": "rw", "private": "home/a/private"}
    scope["mounts"] = [
        {"source": "shared", "target": "/workspace", "mode": "rw"},
        {"source": "home/a/private", "target": "/home/agent", "mode": "rw"},
    ]
    yield EffectHarness(
        provider=provider,
        operation="exec",
        request=lambda tag: {"command": f"echo {tag} >> applied.txt"},
        applied=applied,
        slow_request=lambda tag, s: {
            "command": f"echo {tag} >> applied.txt; "
            f"i=0; while [ $i -lt {max(1, round(s / 0.2))} ]; do "
            f"touch {tag}.beat; sleep 0.2; i=$((i + 1)); done"
        },
        bad_request={"command": ""},
        alive=alive,
        scope=scope,
    )
    for row in await provider.receipts.sandboxes():  # the test's sandboxes go with it
        if row["sandbox"]:
            await backend.delete(row["sandbox"])
