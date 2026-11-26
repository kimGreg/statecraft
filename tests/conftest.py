from __future__ import annotations

import itertools
import sys
import textwrap

import pytest

_features = itertools.count()


@pytest.fixture
def make_feature(tmp_path, monkeypatch):
    """Builds a throwaway feature package: `agents/<id>/{agent.yaml, agent.py}`. Call it with a
    dict id -> (agent.yaml text, agent.py text); it returns the package name."""

    def make(agents: dict[str, tuple[str, str]]) -> str:
        name = f"test_feature_{next(_features)}"
        root = tmp_path / "pkg" / name
        root.mkdir(parents=True)
        (root / "__init__.py").write_text("")
        for agent_id, (spec, code) in agents.items():
            folder = root / "agents" / agent_id
            folder.mkdir(parents=True)
            (root / "agents" / "__init__.py").write_text("")
            (folder / "__init__.py").write_text("")
            (folder / "agent.yaml").write_text(textwrap.dedent(spec))
            (folder / "agent.py").write_text(textwrap.dedent(code))
        monkeypatch.syspath_prepend(str(tmp_path / "pkg"))
        for module in [m for m in sys.modules if m.startswith(name)]:
            del sys.modules[module]
        return name

    return make
