"""The local shell passes the effect provider conformance suite, every optional capability
included (deduplicated, reconcile, remote_work)."""

from __future__ import annotations

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

    return EffectHarness(
        provider=shell,
        operation="exec",
        request=lambda tag: {"command": f"echo {tag} >> applied.txt"},
        applied=applied,
        slow_request=lambda tag, s: {"command": f"echo {tag} >> applied.txt; sleep {s}"},
        bad_request={"command": ""},
    )


def test_receipts_are_kept_outside_the_workspaces(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        LocalShell(receipts=tmp_path / "ws" / "receipts", workspaces=tmp_path / "ws")
