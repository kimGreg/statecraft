"""The stable surface (roadmap R1): what 1.0 promises not to break without a deprecation.

What must hold
  * every route the HTTP server answers is in the OpenAPI document with the same methods, and
    the document has nothing else; admin-only routes say `admin`;
  * the public names below are exported; a change to any list is deliberate (it is a change of
    the stable surface: see docs/stability.md);
  * the store adapter contract (`Tables`) and the configuration sections are the listed ones.
"""

from __future__ import annotations

import re

import statecraft
import statecraft.ports.effects as effect_port
import statecraft.ports.workspace as workspace_port
import statecraft.testing as testing
from statecraft.kernel.store import Tables
from statecraft.runtime.config import RUNTIME_KEYS, SECTIONS
from statecraft.server import server_app
from statecraft.server.openapi import PATHS, document
from statecraft.testing import memory_runtime

CORE = [
    "Agent",
    "AgentDefinition",
    "Artifact",
    "ChildFailed",
    "Done",
    "EffectDefinition",
    "EffectError",
    "EffectPolicy",
    "ExecutableAgent",
    "Registrar",
    "Runtime",
    "Spawn",
    "Wait",
    "classify",
    "effect",
    "state",
    "tool",
]
EFFECT_PORT = [
    "CAPABILITIES",
    "CAPACITY_SCOPES",
    "EffectContext",
    "EffectProvider",
    "Reconciled",
    "RuntimeView",
    "classify",
    "classify_status",
]
WORKSPACE_PORT = [
    "HOME_FOLDERS",
    "MOUNT_TARGETS",
    "Files",
    "WorkspaceProvider",
    "clean",
    "home",
    "link_anchor",
    "mount_plan",
]
TESTING = ["FunctionProvider", "memory_runtime", "permissive_runtime"]
TABLES = [
    "call_get",
    "call_put",
    "calls_of",
    "event_append",
    "event_drop",
    "event_read",
    "forget",
    "get",
    "get_lease",
    "history_append",
    "history_read",
    "history_trim",
    "inbox_append",
    "inbox_compact",
    "inbox_read",
    "put",
    "put_lease",
    "root_ids",
    "setting_get",
    "setting_put",
    "tree_ids",
    "work_ids",
]
ADMIN = {
    ("/agents/{id}/budget", "post"),
    ("/calls/{key}/resolve", "post"),
    ("/maintenance", "post"),
}


def test_every_route_is_documented_and_nothing_else() -> None:
    app = server_app(memory_runtime())
    served = {}
    for route in app.routes:
        path = re.sub(r"\{(\w+):\w+\}", r"{\1}", route.path)
        methods = {m.lower() for m in route.methods or ()} - {"head"}
        served.setdefault(path, set()).update(methods)
    documented = {path: set(ops) for path, ops in PATHS.items()}
    assert served == documented
    found = document()
    assert found["openapi"].startswith("3.1")
    admin = {
        (path, method)
        for path, ops in PATHS.items()
        for method, op in ops.items()
        if op["x-statecraft-scope"] == "admin"
    }
    assert admin == ADMIN


def test_the_public_names_are_the_stable_ones() -> None:
    assert statecraft.__all__ == CORE
    assert effect_port.__all__ == EFFECT_PORT
    assert workspace_port.__all__ == WORKSPACE_PORT
    assert testing.__all__ == TESTING
    for module, names in (
        (statecraft, CORE),
        (effect_port, EFFECT_PORT),
        (workspace_port, WORKSPACE_PORT),
        (testing, TESTING),
    ):
        assert all(hasattr(module, n) for n in names)


def test_the_store_contract_and_configuration_are_the_listed_ones() -> None:
    methods = sorted(n for n in vars(Tables) if not n.startswith("_"))
    assert methods == TABLES
    assert set(SECTIONS) >= {"runtime", "store", "workspace", "plugins", "agents"}
    assert "limits" in RUNTIME_KEYS
