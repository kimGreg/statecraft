"""The plugin (`plugins: [opensandbox]`): configuration can name `provider: opensandbox`.

    effect_providers:
      sh: {provider: opensandbox, endpoint: http://127.0.0.1:8080, api_key: ${OSB_KEY},
           image: alpine:latest, scope: agent, receipts: ./state/sandbox-receipts.db,
           destroy_on_cancel: false, concurrency: 4,
           policies: {exec: {kind: deduplicated, timeout: 10m}}}

`backend: fake` runs commands on this machine with no isolation (tests and demos only).
"""

from __future__ import annotations

from typing import Any

from .backend import OpenSandboxHTTP
from .fake import FakeBackend
from .provider import OpenSandboxProvider

BACKEND_KEYS = ("endpoint", "api_key", "image", "entrypoint", "execd_port", "timeout", "resources")


def make(name: str, **cfg: Any) -> OpenSandboxProvider:
    backend_kind = cfg.pop("backend", "opensandbox")
    backend_cfg = {k: cfg.pop(k) for k in BACKEND_KEYS if k in cfg}
    if backend_kind == "fake":
        backend: Any = FakeBackend()
    elif backend_kind == "opensandbox":
        if "endpoint" not in backend_cfg:
            raise TypeError("opensandbox needs an endpoint")
        backend = OpenSandboxHTTP(**backend_cfg)
    else:
        raise TypeError(f"backend must be opensandbox or fake, not {backend_kind!r}")
    if "receipts" not in cfg:
        raise TypeError("opensandbox needs `receipts`: a file outside every workspace")
    return OpenSandboxProvider(backend, name=name, **cfg)


def register(registrar: Any) -> None:
    registrar.providers.factory("opensandbox", make)


__all__ = ["make", "register"]
