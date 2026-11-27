# Guarantees

What statecraft promises, and what it does not. Every row is held by a test named beside it.

## Exactly once: logical commits

| What | Guarantee | Held by |
|---|---|---|
| a step | commits at most once; a state that did not commit runs again (at least once) | `tests/test_executor_ordering.py`, `tests/test_crash_recovery.py`, chaos |
| fencing | only the holder of the current lease epoch commits; a stale or paused executor's commit is refused | store contract kit, chaos (SIGSTOP past the lease) |
| intents (spawn, signal) | carried out exactly once, after the commit that made them | `tests/test_intents_exactly_once.py`, chaos |
| messages (signals, answers, USR) | each accepted message is delivered once or kept in `undelivered` with why | chaos `messages`, `tests/test_review_regressions.py` |
| publications | an artifact is a create-only, content-addressed object named by a commit | chaos `artifacts` |
| tree limits | `max_total_agents`, `max_live_agents` counted in the committing transaction | `tests/test_limits.py` |
| budgets | a metered call is reserved before it is sent; over budget it is not sent | `tests/test_budgets.py` |

## Not exactly once: external effects

A state can run again, so a call made from it can be sent again. What happens then is the
effect's policy, declared and enforced by the effect manager:

| Policy | A state that runs again | Unknown outcome |
|---|---|---|
| `read_only` | calls again | n/a |
| `idempotent` | calls again (the effect says repeating is harmless) | n/a |
| `deduplicated` | sends the same key; the provider applies a key once, or reconciles it | `indeterminate` until reconciled or resolved |
| `non_repeatable` | refused: such a tool cannot be called from a state at all (a state may run again); give it a key and a `deduplicated` provider, or call it outside | n/a |

`indeterminate` blocks the agent's further effects and its normal commit, survives restarts, and
is cleared only by the provider's reconciliation or by an operator (`statecraft resolve KEY
completed|failed|replay`; a replay allows exactly one new send). A new call identity is never a
way around it. Statecraft does **not** promise that an arbitrary external system sees a request
once: only that supported side-effecting providers either prevent an unsafe second send or report
`indeterminate`.

## What is not promised

- Isolation of agent code: `ctx` enforces an agent's contract; it is not a sandbox. Untrusted
  agent code needs isolated workers (roadmap D2, not in 1.0).
- Isolation of shell commands: `LocalShell` (testing) has none; `statecraft-opensandbox` runs them
  in sandboxes whose hardening is the deployment's (its README).
- Wall-clock deadlines beyond the lease: a lease bounds how long a step may run uncommitted, not
  how long an external call takes to have an effect.
- Multi-tenancy: tokens restrict runs by prefix and scope; they are not tenants.
- A `usd` budget without client prices is checked against what was spent, so one request can
  pass it.
