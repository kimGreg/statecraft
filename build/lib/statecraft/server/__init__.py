"""The runtime served over HTTP (`statecraft serve`): the same API and semantics as embedded.

Needs the `server` extra (starlette, uvicorn).
"""

from .app import server_app

__all__ = ["server_app"]
