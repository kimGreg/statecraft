"""statecraft stands alone: every import is the standard library, statecraft itself or a
declared dependency, and the declared dependencies are the ones it imports.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = sorted((ROOT / "statecraft").rglob("*.py"))
#: Import names of the declared dependencies (a distribution's import name where it differs).
IMPORT_NAMES = {"pyyaml": "yaml"}


def declared(optional: bool = True) -> set[str]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    listed = list(project["dependencies"])
    if optional:
        extras = project.get("optional-dependencies", {})
        listed += [*extras.get("schema", []), *extras.get("console", [])]
    names = {re.split(r"[<>=\[ ;]", d)[0].lower() for d in listed}
    return {IMPORT_NAMES.get(n, n) for n in names}


def imported() -> set[str]:
    found = set()
    for path in SOURCE:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                found |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
                found.add(node.module.split(".")[0])
    return found - {"statecraft"} - set(sys.stdlib_module_names)


def test_imports_are_the_standard_library_itself_or_declared():
    assert sorted(imported() - declared()) == []


def test_every_declared_dependency_is_used():
    assert sorted(declared(optional=False) - imported()) == []
