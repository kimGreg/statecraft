"""A small team coding in one shared workspace, each agent in its own sandbox (no model).

The lead writes a spec, spawns two workers and does its own part meanwhile (asynchronous spawn);
it then joins the workers (selective: it decides what to do when one fails) and verifies the
whole in its sandbox. Each worker owns one folder: it writes its module with a repeat-safe edit
(made against what it read in an earlier step) and tests it with a shell command.
"""

from __future__ import annotations

from statecraft import Agent, Done, Spawn, Wait, state

MODULES = {
    "mathx": "def double(n):\n    return 2 * n\n",
    "textx": "def shout(s):\n    return s.upper() + '!'\n",
}
CHECKS = {
    "mathx": "python3 -c 'from mathx.core import double; assert double(21) == 42'",
    "textx": 'python3 -c \'from textx.core import shout; assert shout("hi") == "HI!"\'',
}


class Lead(Agent):
    start = "plan"

    @state
    async def plan(self, ctx, data):
        await ctx.files.write("shared/SPEC.md", "mathx.double, textx.shout\n")
        return Spawn(
            {name: ("shared_coding:worker", {"module": name}) for name in MODULES},
            then="own_part",
        )

    @state
    async def own_part(self, ctx, data):
        # the workers run meanwhile; the lead's own command runs in its own sandbox
        data["readme"] = await ctx.call(
            "sh.exec", call_name="readme", command="echo '# shared coding' > README.md"
        )
        return Wait(join=list(MODULES), then="verify", on_fault="worker_failed")

    @state
    async def worker_failed(self, ctx, data):
        # a worker's outcome is unknown: that is a person's to settle (resolve_call, then CONT);
        # the lead goes on with the others and waits for it afterwards
        failed = [f["child"] for f in ctx.woke["faulted"]]
        data["failed"] = sorted(set(data.get("failed", [])) | set(failed))
        rest = [n for n in MODULES if n not in data["failed"]]
        if rest:
            return Wait(join=rest, then="await_failed", on_fault="worker_failed")
        return "await_failed"

    @state
    async def await_failed(self, ctx, data):
        return Wait(join=data["failed"], then="verify")

    @state
    async def verify(self, ctx, data):
        command = " && ".join(CHECKS[n] for n in MODULES)
        result = await ctx.call("sh.exec", call_name="verify", command=command)
        await ctx.write(
            "result",
            {
                "verified": list(MODULES),
                "recovered": data.get("failed", []),
                "exit": result["exit"],
            },
        )
        return Done


class Worker(Agent):
    start = "read"

    @state
    async def read(self, ctx, data):
        module = ctx.input["module"]
        path = f"shared/{module}/core.py"
        data["path"] = path
        data["before"] = await ctx.files.digest(path)
        return "write"

    @state
    async def write(self, ctx, data):
        module = ctx.input["module"]
        await ctx.files.edit(data["path"], MODULES[module], expected=data["before"])
        await ctx.files.write(f"shared/{module}/__init__.py", "")
        return "test"

    @state
    async def test(self, ctx, data):
        module = ctx.input["module"]
        result = await ctx.call("sh.exec", call_name="test", command=CHECKS[module])
        await ctx.write("report", {"module": module, "exit": result["exit"]})
        return Done
