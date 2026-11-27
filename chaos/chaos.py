"""Chaos and stress test for the statecraft kernel: independent executor processes on one SQLite
store, with random SIGKILLs, restarts, pauses past the lease (stale executors), STOP/CONT, USR
messages, answers and TERM; then the faults stop and every invariant is checked.

    python chaos.py run  --seed N [--workers 4] [--roots 6] [--seconds 60] [--out DIR] [--pg DSN]
    python chaos.py worker DIR            (started by `run`)

Everything is reproducible from the seed: the workload (tree shapes, inputs) and the schedule of
chaos actions come from `random.Random(seed)`; process timing is not, so a failing run is
replayed by its trace (`DIR/trace.jsonl`: every action with its time) and its store and
workspace, which are kept.

Invariants (checked from the store, the workspace and the effect logs):
  lifecycle    every agent ended; no agent faulted; roots succeeded
  fencing      per agent: history revisions are 0..n without gaps or repeats; committed steps
               are 1..step without repeats; no lease is left
  spawns       every child named by a parent exists with that parent and name; every non-root
               agent is named by its parent; no duplicate children
  intents      every intent is done; each spawn intent made exactly one child
  artifacts    every published reference has its object; the object's sha256 matches; values are
               right (a leaf: 2 * v; a node: the sum of its succeeded children)
  messages     every USR message the runtime accepted is consumed exactly once (in the agent's
               committed `seen`) or kept in `undelivered`; none twice, none lost
  effects      logical (committed) calls are exactly once per committed step; external
               executions of an idempotent tool may be more (reruns: reported, not a failure);
               a deduplicated tool applies its effect exactly once per idempotency key
  shell        a shell command (local shell provider, receipts outside the store) runs at most
               once per call identity; a succeeded agent's commands each ran exactly once; an
               unknown outcome is settled the way an operator would (completed when its run is
               in the log, one replay when it is not)
  progress     after the faults stop, every root ends within the deadline
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from statecraft import (
    Agent,
    Done,
    EffectDefinition,
    EffectPolicy,
    Runtime,
    Spawn,
    Wait,
    state,
    tool,
)
from statecraft.kernel.store import Ended
from statecraft.kernel.transitions import InvalidAnswer
from statecraft.providers.stores import SqliteStore
from statecraft.providers.workspaces import LocalWorkspace
from statecraft.testing.local_shell import LocalShell

LEASE_TTL = 1.5
#: message-heavy runs: leaves live longer (more steps, each followed by a short sleep)
LONG = False
BEAT = 0.3
EFFECT_LOG = "CHAOS_EFFECT_LOG"
DEDUP_DIR = "CHAOS_DEDUP_DIR"
SHELL_LOG = "CHAOS_SHELL_LOG"


# ---- tools: an idempotent effect logged outside, a deduplicated one applied once per key -----


@tool(name="chaos.touch", policy="idempotent", timeout=5)
async def touch(agent: str, step: int, n: int) -> int:
    """Logs one execution (outside the store) and returns n."""
    with open(os.environ[EFFECT_LOG], "a", encoding="utf-8") as f:
        f.write(json.dumps({"agent": agent, "step": step, "n": n, "pid": os.getpid()}) + "\n")
    return n


@tool(name="chaos.charge", policy="deduplicated", timeout=5)
async def charge(amount: int, idempotency_key: str) -> str:
    """Applies a charge once per key: the key file is created exclusively (the effect), then the
    execution is logged."""
    folder = Path(os.environ[DEDUP_DIR])
    marker = folder / hashlib.sha256(idempotency_key.encode()).hexdigest()
    try:
        fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, json.dumps({"key": idempotency_key, "amount": amount}).encode())
        os.close(fd)
        applied = True
    except FileExistsError:
        applied = False
    with open(folder / "executions.log", "a", encoding="utf-8") as f:
        f.write(json.dumps({"key": idempotency_key, "applied": applied}) + "\n")
    return "charged" if applied else "already charged"


# ---- the workload -------------------------------------------------------------------------------


def note_messages(ctx: Any, data: dict[str, Any]) -> None:
    data.setdefault("seen", []).extend(
        m["id"] for m in ctx.signals if isinstance(m, dict) and "id" in m
    )


class Node(Agent):
    """Spawns its children, joins them, publishes the sum of the succeeded ones (a leaf when it
    has none)."""

    start = "spawn"

    @state
    async def spawn(self, ctx, data):
        note_messages(ctx, data)
        children = ctx.input.get("children") or []
        if not children:
            return "work"
        return Spawn({c["name"]: ("node", c) for c in children}, then="wait")

    @state
    async def wait(self, ctx, data):
        note_messages(ctx, data)
        return Wait(join=[c["name"] for c in ctx.input["children"]], then="sum")

    @state
    async def sum(self, ctx, data):
        note_messages(ctx, data)
        total = 0
        for c in ctx.input["children"]:
            if ctx.woke.get("children", {}).get(c["name"]) == "succeeded":
                total += (await ctx.read("value", by=c["name"])).payload["value"]
        data["outcome"] = dict(ctx.woke.get("children", {}))
        await ctx.write("value", {"value": total})
        return Done

    @state
    async def work(self, ctx, data):
        note_messages(ctx, data)
        i = data.get("i", 0)
        await ctx.call("chaos.touch", agent=ctx.id, step=ctx.step, n=i)
        if ctx.input.get("charge"):
            key = ctx.effect_key(f"charge-{i}")
            await ctx.call("chaos.charge", idempotency_key=key, amount=i + 1)
        if ctx.input.get("shell"):  # a command whose every run is logged outside the store
            token = f"{ctx.id}#{i}"
            await ctx.call(
                "sh.exec",
                call_name=f"sh-{i}",
                command=f"echo '{token}' >> {os.environ[SHELL_LOG]}; sleep 0.05",
            )
        data["i"] = i + 1
        if data["i"] == ctx.input.get("ask_at"):
            return Wait(answer={"name": f"q{i}", "question": "go on?"}, then="work")
        if data["i"] < ctx.input.get("steps", 3):
            if ctx.input.get("sleepy"):
                return Wait(sleep=0.25 if LONG else 0.05, then="work")
            return "work"
        await ctx.write("value", {"value": 2 * ctx.input["v"]})
        return Done

    @state
    async def cleanup(self, ctx, data):
        note_messages(ctx, data)
        return Done

    on_term = "cleanup"


def register(runtime: Runtime, folder: Path) -> Runtime:
    r = runtime.registrar
    r.tools.register(touch)
    r.tools.register(charge)
    shell = LocalShell(receipts=folder / "receipts", workspaces=folder / "ws")
    r.providers.register("sh", shell)
    r.tools.register(
        EffectDefinition(
            name="sh.exec",
            provider="sh",
            operation="exec",
            policy=EffectPolicy(kind="deduplicated", timeout=10, max_attempts=2),
        )
    )
    r.agents.register("node", Node, tools=["chaos.*", "sh.*"], spawns=["node"], writes=["value"])
    return runtime


def make_store(folder: Path) -> Any:
    """SQLite in the run's folder, or PostgreSQL (`--pg DSN`): a schema per run, shared by the
    driver and every worker process."""
    dsn = os.environ.get("STATECRAFT_CHAOS_PG")
    if not dsn:
        return SqliteStore(folder / "state.db")
    from statecraft_postgres import PostgresStore

    schema = "chaos_" + "".join(c if c.isalnum() else "_" for c in folder.resolve().name)
    return PostgresStore(dsn, schema=schema.lower())


def make_runtime(folder: Path, executors: int) -> Runtime:
    return register(
        Runtime(
            store=make_store(folder),
            workspace=LocalWorkspace(folder / "ws"),
            executors=executors,
            concurrency=8,
            lease_ttl=LEASE_TTL,
            beat=BEAT,
            scan_every=0.05,
            drain=0.5,
        ),
        folder,
    )


def tree(rng: random.Random, name: str, depth: int, counter: list[int]) -> dict[str, Any]:
    counter[0] += 1
    node: dict[str, Any] = {"name": name, "v": counter[0]}
    if depth > 0 and rng.random() < 0.8:
        node["children"] = [
            tree(rng, f"{name}{chr(97 + i)}", depth - 1, counter) for i in range(rng.randint(1, 3))
        ]
    else:
        node["steps"] = rng.randint(2, 6) * (8 if LONG else 1)
        node["charge"] = rng.random() < 0.5
        node["shell"] = rng.random() < 0.5
        node["sleepy"] = LONG or rng.random() < 0.3
        if rng.random() < 0.3:
            node["ask_at"] = rng.randint(1, node["steps"])
    return node


# ---- the worker process -------------------------------------------------------------------------


async def work(folder: Path) -> None:
    runtime = make_runtime(folder, executors=1)
    await runtime.start()
    stop = folder / "stop"
    while not stop.exists():
        await asyncio.sleep(0.1)
    await runtime.close()


# ---- the driver ---------------------------------------------------------------------------------


class Driver:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.rng = random.Random(args.seed)
        self.folder = Path(args.out)
        self.folder.mkdir(parents=True, exist_ok=True)
        (self.folder / "dedup").mkdir(exist_ok=True)
        self.env = {
            **os.environ,
            EFFECT_LOG: str(self.folder / "effects.log"),
            DEDUP_DIR: str(self.folder / "dedup"),
            SHELL_LOG: str(self.folder / "shell.log"),
        }
        os.environ.update({k: self.env[k] for k in (EFFECT_LOG, DEDUP_DIR, SHELL_LOG)})
        self.workers: dict[int, subprocess.Popen] = {}
        self.resolved = 0
        self.paused: dict[int, float] = {}
        self.trace = open(self.folder / "trace.jsonl", "a", encoding="utf-8")
        self.start = time.monotonic()
        self.sent: dict[str, str] = {}  # message id -> agent, for accepted USR messages
        self.refused: list[dict[str, Any]] = []
        self.stopped: set[str] = set()
        self.terminated: set[str] = set()
        self.n = 0

    def shell_tokens(self) -> list[str]:
        log = self.folder / "shell.log"
        return log.read_text().split() if log.exists() else []

    def log(self, action: str, **detail: Any) -> None:
        entry = {"t": round(time.monotonic() - self.start, 3), "action": action, **detail}
        self.trace.write(json.dumps(entry) + "\n")
        self.trace.flush()

    def launch(self, slot: int) -> None:
        self.workers[slot] = subprocess.Popen(
            [sys.executable, __file__, "worker", str(self.folder)],
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=open(self.folder / f"worker-{slot}.err", "a"),
        )
        self.log("launch", slot=slot, pid=self.workers[slot].pid)

    async def chaos(self, runtime: Runtime, roots: list[str]) -> None:
        deadline = time.monotonic() + self.args.seconds
        while time.monotonic() < deadline:
            await asyncio.sleep(self.rng.uniform(0.05, 0.4))
            for slot, until in list(self.paused.items()):
                if time.monotonic() >= until:
                    os.kill(self.workers[slot].pid, signal.SIGCONT)
                    del self.paused[slot]
                    self.log("resume", slot=slot)
            roll = self.rng.random()
            slot = self.rng.randrange(self.args.workers)
            if self.args.messages:  # message-heavy: few process faults, many USR/TERM/KILL
                roll = 0.12 + roll * 0.88 if roll >= 0.06 else roll * 2
            if roll < 0.12 and slot not in self.paused:
                self.workers[slot].send_signal(signal.SIGKILL)
                self.workers[slot].wait()
                self.log("sigkill", slot=slot)
                self.launch(slot)
            elif roll < 0.18 and slot not in self.paused:
                os.kill(self.workers[slot].pid, signal.SIGSTOP)
                self.paused[slot] = time.monotonic() + LEASE_TTL * self.rng.uniform(1.2, 2.5)
                self.log("sigstop", slot=slot)
            else:
                await self.signal_something(runtime, roll)

    async def agents(self, runtime: Runtime) -> list[str]:
        """Every agent of the roots not ended yet (running, waiting, stopped)."""
        found = []
        for root in self.roots:
            found += [r.id for r in await runtime.store.tree(root) if not r.ended]
        return sorted(found)

    async def signal_something(self, runtime: Runtime, roll: float) -> None:
        live = await self.agents(runtime)
        if not live:
            return
        target = self.rng.choice(live)
        try:
            usr_until = 0.55 if self.args.messages else 0.40
            if roll < usr_until:
                self.n += 1
                mid = f"m{self.n}"
                await runtime.signal(target, ("USR", {"id": mid}))
                self.sent[mid] = target
                self.log("usr", agent=target, id=mid)
            elif roll < 0.50:
                await runtime.signal(target, "STOP")
                self.stopped.add(target)
                self.log("stop", agent=target)
            elif roll < 0.62 and self.stopped:
                again = self.rng.choice(sorted(self.stopped))
                await runtime.signal(again, "CONT")
                self.stopped.discard(again)
                self.log("cont", agent=again)
            elif roll < (0.75 if self.args.messages else 0.64) and target.count("/") >= 2:
                how = "KILL" if self.rng.random() < 0.3 else "TERM"
                await runtime.signal(target, how)
                self.terminated.add(target)
                self.log(how.lower(), agent=target)
            else:
                questions = await runtime.questions()
                if questions:
                    q = self.rng.choice(questions)
                    await runtime.answer(q["question_id"], "yes")
                    self.log("answer", question=q["question_id"])
        except Ended as exc:
            self.refused.append({"agent": target, "error": str(exc)})
            self.log("refused", agent=target, roll=round(roll, 3))
        except (InvalidAnswer, KeyError) as exc:
            self.log("ignored", agent=target, error=str(exc)[:120])

    async def settle(self, runtime: Runtime, roots: list[str]) -> bool:
        """Faults stop: every worker runs, stopped agents continue, questions are answered."""
        for slot in list(self.paused):
            os.kill(self.workers[slot].pid, signal.SIGCONT)
            self.log("resume", slot=slot)
        self.paused.clear()
        for slot, proc in self.workers.items():
            if proc.poll() is not None:
                self.launch(slot)
        deadline = time.monotonic() + self.args.settle
        while time.monotonic() < deadline:
            for agent in await self.agents(runtime):
                record = await runtime.store.load(agent)
                if any(r["kind"] == "stopped" for r in record.reasons):
                    try:
                        await runtime.signal(agent, "CONT")
                        self.log("cont", agent=agent, settle=True)
                    except Ended:
                        pass
            for agent in await self.agents(runtime):  # an operator settles unknown outcomes
                for call in await runtime.calls(agent):
                    if call["status"] != "indeterminate":
                        continue
                    i = call["key"].rsplit("/sh-", 1)[-1]
                    token = f"{agent}#{i}"
                    ran = token in self.shell_tokens()
                    try:
                        if ran:
                            await runtime.resolve_call(
                                call["key"], "completed", result={"exit": 0, "output": ""}
                            )
                        else:
                            await runtime.resolve_call(call["key"], "replay")
                        self.log("resolve", call=call["key"], ran=ran)
                        self.resolved += 1
                    except ValueError:
                        continue
                    try:
                        await runtime.signal(agent, "CONT")
                    except Ended:
                        pass
            for q in await runtime.questions():
                try:
                    await runtime.answer(q["question_id"], "yes")
                    self.log("answer", question=q["question_id"], settle=True)
                except (InvalidAnswer, Ended):
                    pass
            records = [await runtime.store.load(r) for r in roots]
            if all(r.ended for r in records):
                return True
            await asyncio.sleep(0.25)
        return False

    async def run(self) -> dict[str, Any]:
        runtime = make_runtime(self.folder, executors=0)
        await runtime.start()
        roots = self.roots = []
        for i in range(self.args.roots):
            spec = tree(self.rng, f"r{i}", self.args.depth, [i * 1000])
            root = await runtime.spawn("node", spec, root_id=spec["name"])
            roots.append(root)
            self.log("spawn", root=root)
        for slot in range(self.args.workers):
            self.launch(slot)
        await self.chaos(runtime, roots)
        progressed = await self.settle(runtime, roots)
        (self.folder / "stop").write_text("stop")
        for proc in self.workers.values():
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        report = await check(runtime, roots, self, progressed)
        await runtime.close()
        self.trace.close()
        return report


# ---- the checks -----------------------------------------------------------------------------------


async def check(runtime: Runtime, roots: list[str], d: Driver, progressed: bool) -> dict[str, Any]:
    store = runtime.store
    problems: list[str] = []
    records = {}
    for root in roots:
        for record in await store.tree(root):
            records[record.id] = record
    if not progressed:
        problems.append("progress: not every root ended after the faults stopped")

    # lifecycle
    for r in records.values():
        if not r.ended:
            problems.append(f"lifecycle: {r.id} not ended ({r.state}, {r.reasons})")
        if r.error and r.reason("fault"):
            problems.append(f"lifecycle: {r.id} faulted: {r.error}")
    for root in roots:
        if records[root].status != "succeeded":
            problems.append(f"lifecycle: root {root} is {records[root].status}")

    # fencing: history
    committed_steps = 0
    for r in records.values():
        history = await store.history(r.id)
        revisions = [h["revision"] for h in history]
        if revisions != list(range(1, len(revisions) + 1)):  # created is revision 1
            problems.append(f"fencing: {r.id} revisions {revisions[:12]}...")
        steps = [h["record"]["step"] for h in history if h["kind"] in ("step", "fault")]
        if steps != list(range(1, len(steps) + 1)):
            problems.append(f"fencing: {r.id} committed steps {steps}")
        committed_steps += len(steps)
        if steps and steps[-1] != r.step:
            problems.append(f"fencing: {r.id} record step {r.step} != last committed {steps[-1]}")
        lease = await store.lease(r.id)
        if lease is not None:
            problems.append(f"fencing: {r.id} still holds a lease {lease}")

    # spawns and intents
    for r in records.values():
        for name, child_id in r.children.items():
            child = records.get(child_id)
            if child is None or child.parent != r.id or child.name != name:
                problems.append(f"spawns: {r.id} names {name}={child_id}, which does not match")
        if r.parent is not None:
            parent = records.get(r.parent)
            if parent is None or parent.children.get(r.name) != r.id:
                problems.append(f"spawns: {r.id} is not named by its parent")
        for intent in r.intents:
            if not intent.get("done"):
                problems.append(f"intents: {r.id} intent {intent['id']} not done")
        spawned = [i for i in r.intents if i["kind"] == "spawn"]
        if len({i["id"] for i in spawned}) != len(spawned):
            problems.append(f"intents: {r.id} has duplicate spawn intents")
    # create-only: ids unique by construction in the store; count children by parent
    by_parent: dict[str, list[str]] = {}
    for r in records.values():
        if r.parent:
            by_parent.setdefault(r.parent, []).append(r.name)
    for parent, names in by_parent.items():
        if len(names) != len(set(names)):
            problems.append(f"spawns: {parent} has duplicate child names {names}")

    # artifacts
    for r in records.values():
        for kind, ref in r.published.items():
            try:
                body = await runtime.workspaces.read_object(ref["path"])
            except FileNotFoundError:
                problems.append(f"artifacts: {r.id} {kind} object missing")
                continue
            if hashlib.sha256(body).hexdigest() != ref["sha256"]:
                problems.append(f"artifacts: {r.id} {kind} object does not match its sha256")
            value = json.loads(body)["payload"]["value"]
            spec = r.input
            if spec.get("children"):
                expected = 0
                for c in spec["children"]:
                    child = records[r.children[c["name"]]]
                    if child.status == "succeeded":
                        expected += json.loads(
                            await runtime.workspaces.read_object(child.published["value"]["path"])
                        )["payload"]["value"]
                if value != expected:
                    problems.append(f"artifacts: {r.id} sum {value} != {expected}")
            elif value != 2 * spec["v"]:
                problems.append(f"artifacts: {r.id} value {value} != {2 * spec['v']}")
        if r.status == "succeeded" and "value" not in r.published:
            problems.append(f"artifacts: {r.id} succeeded without publishing")

    # messages
    consumed: dict[str, int] = {}
    undelivered: dict[str, int] = {}
    for r in records.values():
        for mid in r.data.get("seen", []):
            consumed[mid] = consumed.get(mid, 0) + 1
        for item in r.undelivered:
            mid = (item.get("message") or {}).get("id")
            if mid:
                undelivered[mid] = undelivered.get(mid, 0) + 1
        pending = await store.read_inbox(r.id, r.inbox_applied)
        for item in pending:
            msg = item["message"]
            if msg.get("kind") == "signal" and isinstance(msg.get("signal"), list):
                mid = (msg["signal"][1] or {}).get("id")
                if mid:
                    problems.append(f"messages: {mid} accepted but never applied to {r.id}")
    for mid, agent in d.sent.items():
        n = consumed.get(mid, 0) + undelivered.get(mid, 0)
        if n != 1:
            problems.append(
                f"messages: {mid} to {agent} consumed {consumed.get(mid, 0)}, "
                f"undelivered {undelivered.get(mid, 0)}"
            )

    # effects: logical vs external
    logical = 0
    for r in records.values():
        for h in await store.history(r.id):
            logical += sum(1 for c in h.get("crossings", []) if c["effect"] == "chaos.touch")
    log = Path(d.env[EFFECT_LOG])
    external = sum(1 for _ in open(log)) if log.exists() else 0
    executions = (
        [json.loads(x) for x in open(Path(d.env[DEDUP_DIR]) / "executions.log")]
        if (Path(d.env[DEDUP_DIR]) / "executions.log").exists()
        else []
    )
    applied: dict[str, int] = {}
    for e in executions:
        applied[e["key"]] = applied.get(e["key"], 0) + int(e["applied"])
    for key, n in applied.items():
        if n != 1:
            problems.append(f"effects: charge {key} applied {n} times")
    committed_charges = 0
    for r in records.values():
        for h in await store.history(r.id):
            committed_charges += sum(
                1 for c in h.get("crossings", []) if c["effect"] == "chaos.charge" and c["ok"]
            )
    charged_keys = len(applied)
    expected_touch = 0
    for r in records.values():
        if not r.input.get("children") and r.status == "succeeded":
            expected_touch += r.input["steps"]
    if logical < expected_touch:
        problems.append(f"effects: {logical} committed touch calls < {expected_touch} expected")

    # shell: every command run is logged by the command itself; one call identity, one run
    tokens = d.shell_tokens()
    runs: dict[str, int] = {}
    for token in tokens:
        runs[token] = runs.get(token, 0) + 1
    for token, n in runs.items():
        if n > 1:
            problems.append(f"shell: {token} ran {n} times")
    for r in records.values():
        if r.input.get("shell") and not r.input.get("children") and r.status == "succeeded":
            for i in range(r.input["steps"]):
                if runs.get(f"{r.id}#{i}", 0) != 1:
                    problems.append(f"shell: {r.id}#{i} ran {runs.get(f'{r.id}#{i}', 0)} times")

    actions = [json.loads(x) for x in open(d.folder / "trace.jsonl")]
    counts: dict[str, int] = {}
    for a in actions:
        counts[a["action"]] = counts.get(a["action"], 0) + 1
    return {
        "seed": d.args.seed,
        "ok": not problems,
        "problems": problems,
        "agents": len(records),
        "committed_steps": committed_steps,
        "actions": counts,
        "messages": {"accepted": len(d.sent), "refused_ended": len(d.refused)},
        "effects": {
            "touch_committed": logical,
            "touch_executed": external,
            "touch_reexecuted": external - logical,
            "charge_keys": charged_keys,
            "charge_committed": committed_charges,
            "charge_executions": len(executions),
            "shell_runs": len(tokens),
            "shell_calls": len(runs),
            "shell_settled_by_operator": d.resolved,
        },
    }


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="mode", required=True)
    r = sub.add_parser("run")
    r.add_argument("--seed", type=int, required=True)
    r.add_argument("--workers", type=int, default=4)
    r.add_argument("--roots", type=int, default=6)
    r.add_argument("--depth", type=int, default=2)
    r.add_argument("--seconds", type=float, default=60)
    r.add_argument("--settle", type=float, default=90)
    r.add_argument("--out", required=True)
    r.add_argument("--messages", action="store_true", help="message-heavy chaos")
    r.add_argument("--pg", help="a PostgreSQL DSN: the store for the driver and every worker")
    w = sub.add_parser("worker")
    w.add_argument("folder")
    args = p.parse_args()
    if args.mode == "worker":
        asyncio.run(work(Path(args.folder)))
        return
    global LONG
    LONG = args.messages
    if args.pg:
        os.environ["STATECRAFT_CHAOS_PG"] = args.pg  # the workers inherit it
        Path(args.out).mkdir(parents=True, exist_ok=True)
        make_store(Path(args.out)).drop()  # a fresh schema for this run
    report = asyncio.run(Driver(args).run())
    (Path(args.out) / "report.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
