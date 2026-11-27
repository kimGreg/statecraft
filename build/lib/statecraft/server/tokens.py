"""Named API tokens: who may do what over HTTP, and to which runs (roadmap D1).

    book = TokenBook("tokens.json")
    secret = book.add("ci", scopes=["read", "control"], roots=["build-"])   # shown once
    principal = book.verify(secret)                                       # -> Principal or None

What must hold
  * a token is stored only as a salted, slow hash (PBKDF2-SHA256): the file never holds it, and
    it is shown once, when it is made;
  * verification compares in constant time and names the principal it belongs to;
  * scopes: `read` (look), `control` (spawn, signal, answer, repin), `admin` (everything,
    including settling unknown outcomes); `admin` implies the others, `control` implies `read`;
  * `roots`: run id prefixes the token may touch (None: every run); a restricted token sees and
    acts on those runs only;
  * revoking a name removes it; the file is replaced atomically; a running server sees changes
    to the file (added and revoked tokens) at its next check;
  * a token names its entry (`sc_<id>.<secret>`): one slow hash per check, whatever the number
    of tokens, so unknown tokens cannot make the server hash for every entry.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCOPES = ("read", "control", "admin")
IMPLIES = {"admin": {"admin", "control", "read"}, "control": {"control", "read"}, "read": {"read"}}
ITERATIONS = 120_000


@dataclass(frozen=True)
class Principal:
    name: str
    scopes: frozenset[str]
    roots: tuple[str, ...] | None = None  # run id prefixes; None: every run

    def allows(self, scope: str) -> bool:
        return any(scope in IMPLIES[s] for s in self.scopes)

    def owns(self, root: str) -> bool:
        return self.roots is None or any(root.startswith(prefix) for prefix in self.roots)


#: the principal of the single shared token (`--token`): every scope, every run
OPERATOR = Principal("operator", frozenset({"admin"}))


def _hash(token: str, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac("sha256", token.encode(), salt, ITERATIONS).hex()


class TokenBook:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._seen: tuple[int, int] | None = None
        self._entries = self._load()
        #: sha256 of a token -> its principal (the slow hash runs once per token per process)
        self._verified: dict[str, Principal | None] = {}

    def _stamp(self) -> tuple[int, int] | None:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            return None
        return (st.st_mtime_ns, st.st_size)

    def _load(self) -> list[dict[str, Any]]:
        self._seen = self._stamp()
        if self._seen is None:
            return []
        return list(json.loads(self.path.read_text(encoding="utf-8")).get("tokens", []))

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=self.path.parent, prefix=".tokens-")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"tokens": self._entries}, f, indent=1)
        os.chmod(temp, 0o600)
        os.replace(temp, self.path)

    def add(self, name: str, *, scopes: list[str], roots: list[str] | None = None) -> str:
        """Make a token for `name` (replacing an earlier one of that name); returns it, once."""
        unknown = set(scopes) - set(SCOPES)
        if not scopes or unknown:
            raise ValueError(f"scopes must be some of {SCOPES}, not {sorted(unknown) or scopes}")
        ident = secrets.token_hex(6)
        token = f"sc_{ident}." + secrets.token_urlsafe(32)
        salt = secrets.token_bytes(16)
        self._entries = [e for e in self._load() if e["name"] != name]
        self._entries.append(
            {
                "id": ident,
                "name": name,
                "salt": salt.hex(),
                "hash": _hash(token, salt),
                "scopes": sorted(scopes),
                "roots": roots,
            }
        )
        self._save()
        self._verified.clear()
        return token

    def revoke(self, name: str) -> bool:
        before = self._load()
        self._entries = [e for e in before if e["name"] != name]
        self._save()
        self._verified.clear()
        return len(self._entries) != len(before)

    def names(self) -> list[dict[str, Any]]:
        return [
            {"name": e["name"], "scopes": e["scopes"], "roots": e["roots"]} for e in self._load()
        ]

    def verify(self, token: str) -> Principal | None:
        """The principal a token belongs to, or None. The entry is the one the token names
        (tokens made before ids existed: every entry without one is checked). Remembered for
        this process while the file is unchanged."""
        if self._stamp() != self._seen:
            self.reload()
        quick = hashlib.sha256(token.encode()).hexdigest()
        if quick in self._verified:
            return self._verified[quick]
        found = None
        ident = token[3:].split(".", 1)[0] if token.startswith("sc_") and "." in token else None
        candidates = [e for e in self._entries if e.get("id") == ident] if ident else []
        if not candidates:
            candidates = [e for e in self._entries if "id" not in e]
        for entry in candidates:
            candidate = _hash(token, bytes.fromhex(entry["salt"]))
            if hmac.compare_digest(candidate, entry["hash"]):
                found = Principal(
                    entry["name"],
                    frozenset(entry["scopes"]),
                    tuple(entry["roots"]) if entry.get("roots") is not None else None,
                )
        if len(self._verified) > 1024:
            self._verified.clear()
        self._verified[quick] = found
        return found

    def reload(self) -> None:
        self._entries = self._load()
        self._verified.clear()


__all__ = ["OPERATOR", "SCOPES", "Principal", "TokenBook"]
