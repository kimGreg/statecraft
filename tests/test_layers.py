"""Layers point one way, and statecraft knows no extension (architecture §3).

What must hold
  * kernel imports only kernel; sdk imports kernel and sdk; providers import kernel, sdk and
    providers; runtime adds itself; server, client, cli and testing may use all of them;
  * nothing in statecraft imports an extension (statecraft_agentic, statecraft_mcp,
    statecraft_console) or names a model;
  * the kernel and the sdk use only the standard library and pydantic;
  * every third-party import is a declared dependency (required or an extra).
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "statecraft"
ALLOWED = {
    "kernel": {"kernel"},
    "ports": {"kernel", "ports"},
    "sdk": {"kernel", "sdk"},
    "providers": {"kernel", "ports", "sdk", "providers"},
    "runtime": {"kernel", "ports", "sdk", "providers", "runtime"},
    "server": {"kernel", "ports", "sdk", "providers", "runtime", "server"},
    "client": {"client"},
    "cli": {"kernel", "ports", "sdk", "providers", "runtime", "server", "client", "cli"},
    "testing": {"kernel", "ports", "sdk", "providers", "runtime", "testing"},
}
LEAN: dict[str, set[str]] = {"kernel": set(), "ports": set(), "sdk": {"pydantic"}}
EXTENSIONS = {"statecraft_agentic", "statecraft_mcp", "statecraft_console"}
IMPORT_NAMES = {"pyyaml": "yaml"}


def modules() -> list[tuple[str, Path]]:
    found = []
    for path in sorted(PACKAGE.rglob("*.py")):
        relative = path.relative_to(PACKAGE).parts
        found.append((relative[0] if len(relative) > 1 else "", path))
    return found


def imports(path: Path) -> list[tuple[str, int, str]]:
    """(top package, level, absolute module) of every import, the lazy ones too."""
    package = ["statecraft", *path.relative_to(PACKAGE).parts[:-1]]
    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found += [(a.name.split(".")[0], 0, a.name) for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level == 0:
                found.append((module.split(".")[0], 0, module))
                continue
            base = package[: len(package) - (node.level - 1)]
            target = ".".join([*base, *([module] if module else [])])
            found.append(("statecraft", node.level, target))
    return found


def layer_of(module: str) -> str:
    parts = module.split(".")
    return parts[1] if len(parts) > 1 and parts[0] == "statecraft" else ""


def test_layers_point_one_way() -> None:
    wrong = []
    for layer, path in modules():
        if not layer:
            continue
        for top, _level, module in imports(path):
            if top == "statecraft":
                target = layer_of(module)
                if target and target not in ALLOWED[layer]:
                    wrong.append(f"{path.relative_to(ROOT)}: {layer} imports {module}")
    assert wrong == []


def test_no_extension_is_known() -> None:
    wrong = [
        f"{path.relative_to(ROOT)}: {module}"
        for _layer, path in modules()
        for top, _l, module in imports(path)
        if top in EXTENSIONS
    ]
    assert wrong == []
    named = [
        str(path.relative_to(ROOT))
        for layer, path in modules()
        if layer in ("kernel", "sdk", "runtime")
        and re.search(r"\b(ctx\.model|ModelProvider|ChatModel|openrouter)\b", path.read_text())
    ]
    assert named == []


def test_the_kernel_and_sdk_stay_lean() -> None:
    stdlib = set(sys.stdlib_module_names)
    wrong = []
    for layer, path in modules():
        if layer not in LEAN:
            continue
        for top, _l, module in imports(path):
            if top not in stdlib and top != "statecraft" and top not in LEAN[layer]:
                wrong.append(f"{path.relative_to(ROOT)}: {module}")
    assert wrong == []


def declared() -> set[str]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    listed = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        listed += extra
    names = {re.split(r"[<>=\[ ;]", d)[0].lower() for d in listed}
    return {IMPORT_NAMES.get(n, n) for n in names}


def test_third_party_imports_are_declared() -> None:
    stdlib = set(sys.stdlib_module_names)
    found = {
        top
        for _layer, path in modules()
        for top, _l, _m in imports(path)
        if top and top not in stdlib and top != "statecraft"
    }
    assert sorted(found - declared()) == []


#: The kernel knows execution only: none of these words appear in its code (comments included).
KERNEL_UNKNOWN = re.compile(
    r"\b(model|prompt|token|mcp|sandbox|volume|mount|docker|kubernetes|container|shell|"
    r"context window|http)\w*",
    re.IGNORECASE,
)


def test_the_kernel_knows_nothing_of_models_tools_or_infrastructure() -> None:
    found = []
    for path in sorted((PACKAGE / "kernel").rglob("*.py")):
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if KERNEL_UNKNOWN.search(line):
                found.append(f"{path.relative_to(ROOT)}:{n}: {line.strip()}")
    assert found == []


#: Where the runtime may construct a concrete provider: configuration (bootstrap), the
#: `Runtime.local` convenience, and the built-in Python provider behind `@tool` registration.
BUILDS_PROVIDERS = {
    ("config.py", "runtime_from_config"),
    ("config.py", "_python_module"),
    ("runtime.py", "local"),
    ("registry.py", "register"),
}


def test_only_bootstrap_code_builds_concrete_providers() -> None:
    found = set()
    for path in sorted((PACKAGE / "runtime").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for func in ast.walk(tree):
            if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(func):
                if isinstance(node, ast.ImportFrom) and "providers" in (node.module or ""):
                    found.add((path.name, func.name))
        for node in tree.body:  # module level
            if isinstance(node, ast.ImportFrom) and "providers" in (node.module or ""):
                found.add((path.name, "<module>"))
    assert found <= BUILDS_PROVIDERS, found - BUILDS_PROVIDERS
