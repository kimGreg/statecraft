# Stability, versions and compatibility

## The stable surface

From 1.0 these are stable: changed only with a deprecation (below). `tests/test_public_api.py`
lists them, so a change is always deliberate.

- `statecraft.__all__` (the SDK: `Agent`, `state`, `Spawn`, `Wait`, `Done`, `tool`, `effect`,
  `EffectDefinition`, `EffectPolicy`, `EffectError`, `Runtime`, ...);
- the ports: `statecraft.ports.effects` and `statecraft.ports.workspace`;
- the store adapter contract (`Tables`) and the conformance kits in `statecraft.testing`;
- the configuration sections and their keys (unknown keys are refused, so additions are safe);
- the HTTP API (`GET /openapi.json`; every served route is in it);
- the store data format (`meta.format`) and the meaning of history entries and events.

Everything else (module internals, the console's private `/api`) may change in any release.

## Deprecation policy

A stable name, key, route or field is deprecated for at least one minor release before it goes:
it keeps working, warns (`DeprecationWarning`, or a `Deprecation` header over HTTP), and the
release notes say what replaces it. Removing it is a major release.

## Store formats

A store records its data format. A newer statecraft reads an older format only after
`statecraft store migrate` (which backs the file up first); an older statecraft refuses a newer
format. Every format change comes with a migration and a test from every released format.

| statecraft | store format |
|---|---|
| 0.2 | 1 |
| 0.3 | 3 (2: the call journal; 3: history deltas, compacted inbox) |

## Versions and compatibility

Until 1.0 every package is released in lockstep and requires the same minor version of the core
(`statecraft>=0.3,<0.4`):

| Package | 0.3 requires |
|---|---|
| statecraft | Python 3.11+ |
| statecraft-agentic | statecraft 0.3 |
| statecraft-mcp | statecraft 0.3, mcp 1.20+ |
| statecraft-console | statecraft 0.3, starlette, uvicorn |
| statecraft-opensandbox | statecraft 0.3, an OpenSandbox server (tested with 1.2.1) |
| statecraft-postgres | statecraft 0.3, PostgreSQL (tested with 16), psycopg 3.1+ |
| statecraft-coding | statecraft 0.3, statecraft-agentic 0.3 |

`scripts/check_wheels.sh` builds every wheel and installs each extension alone with the core in
a fresh environment.
