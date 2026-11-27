"""The orders sample: native state machines.

order     check stock ▸ (ask a person when stock is short) ▸ charge (deduplicated, keyed)
          ▸ spawn `shipping` ▸ (a person decides on its fault) ▸ notify ▸ receipt
shipping  print a label (its tool raises once: a fault the parent decides on)
"""

from __future__ import annotations

from statecraft import Agent, ChildFailed, Done, Spawn, Wait, state


class Order(Agent):
    start = "check"

    @state
    async def check(self, ctx, data):
        stock = await ctx.call("inventory.check", item=ctx.input["item"], qty=ctx.input["qty"])
        data["stock"] = stock
        if stock["enough"]:
            return "charge"
        return Wait(
            answer={
                "name": "backorder",
                "question": f"Only {stock['in_stock']} {stock['item']} in stock. "
                "Accept a backorder?",
                "schema": {"enum": ["accept", "cancel"]},
                "default": "cancel",
            },
            then="after_backorder",
        )

    @state
    async def after_backorder(self, ctx, data):
        if ctx.woke["answer"] == "cancel":
            await ctx.write("receipt", {"order": ctx.input["order_id"], "status": "cancelled"})
            return Done
        data["backorder"] = True
        return "charge"

    @state
    async def charge(self, ctx, data):
        data["payment"] = await ctx.call(
            "payments.charge",
            order_id=ctx.input["order_id"],
            amount=ctx.input["amount"],
            idempotency_key=ctx.effect_key("charge"),
        )
        return Spawn(
            {
                "shipping": (
                    "shipping",
                    {"order_id": ctx.input["order_id"], "address": ctx.input["address"]},
                )
            },
            then="wait_shipping",
        )

    @state
    async def wait_shipping(self, ctx, data):
        return Wait(join=["shipping"], then="notify", on_fault="shipping_failed")

    @state
    async def shipping_failed(self, ctx, data):
        [fault] = ctx.woke["faulted"]
        return Wait(
            answer={
                "name": "shipping",
                "question": f"Shipping failed: {fault['error']}",
                "schema": {"enum": ["retry", "cancel"]},
            },
            then="after_shipping_failed",
        )

    @state
    async def after_shipping_failed(self, ctx, data):
        ctx.signal("shipping", "CONT" if ctx.woke["answer"] == "retry" else "TERM")
        return "wait_shipping"

    @state
    async def notify(self, ctx, data):
        if ctx.woke["children"].get("shipping") != "succeeded":
            raise ChildFailed("shipping did not succeed")
        label = (await ctx.read("label", by="shipping")).payload
        await ctx.call(
            "notify.send", order_id=ctx.input["order_id"], text=f"Shipped with {label['label']}"
        )
        await ctx.write(
            "receipt",
            {
                "order": ctx.input["order_id"],
                "status": "shipped",
                "charge": data["payment"]["charge_id"],
                "label": label["label"],
                "backorder": data.get("backorder", False),
            },
        )
        return Done


class Shipping(Agent):
    start = "label"

    @state
    async def label(self, ctx, data):
        printed = await ctx.call(
            "shipping.label", order_id=ctx.input["order_id"], address=ctx.input["address"]
        )
        await ctx.write("label", printed)
        return Done
