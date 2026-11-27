"""The local shell passes the effect provider conformance suite, every optional capability
included (deduplicated, reconcile, remote_work)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from statecraft.testing.effect_contract import *  # noqa: F403
from statecraft.testing.effect_contract import EffectHarness
from statecraft.testing.local_shell import LocalShell


@pytest.fixture
def effect_harness(tmp_path: Path) -> EffectHarness:
    shell = LocalShell(receipts=tmp_path / "receipts", workspaces=tmp_path / "ws")
    log = tmp_path / "ws" / "w" / "shared" / "applied.txt"

    def applied(tag: str) -> int:
        return log.read_text().split().count(tag) if log.exists() else 0

    shared = tmp_path / "ws" / "w" / "shared"

    def alive(tag: str) -> bool:  # the slow request's child, which outlives the wrapper's shell
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

    return EffectHarness(
        provider=shell,
        operation="exec",
        request=lambda tag: {"command": f"echo {tag} >> applied.txt"},
        applied=applied,
        slow_request=lambda tag, s: {
            "command": f"echo {tag} >> applied.txt; sleep {s} & echo $! > {tag}.pid; wait"
        },
        bad_request={"command": ""},
        alive=alive,
    )


def test_receipts_are_kept_outside_the_workspaces(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        LocalShell(receipts=tmp_path / "ws" / "receipts", workspaces=tmp_path / "ws")
