"""Tools of the orders sample: an inventory, a payment service that deduplicates by key, a label
printer that fails once, and a notifier. Module-level state stands for the outside world."""

from __future__ import annotations

from statecraft import EffectError, tool

STOCK = {"book": 5, "lamp": 1}
#: the payment service's ledger: idempotency key -> charge (what a real service would keep)
LEDGER: dict[str, dict] = {}
#: how many times the payment service actually received a request
RECEIVED = {"charges": 0}
LABELS = {"failures_left": 1}
SENT: list[str] = []


@tool(name="inventory.check", policy="read_only")
async def check(item: str, qty: int) -> dict:
    """How many of an item are in stock, and whether the order fits."""
    have = STOCK.get(item, 0)
    return {"item": item, "in_stock": have, "enough": have >= qty}


@tool(name="payments.charge", policy={"kind": "deduplicated", "max_attempts": 3})
async def charge(order_id: str, amount: float, idempotency_key: str | None = None) -> dict:
    """Charge an order. The service applies a key once; the first response is lost on the way
    back (the charge was made), so the runtime retries with the same key."""
    RECEIVED["charges"] += 1
    if idempotency_key in LEDGER:
        return LEDGER[idempotency_key]
    LEDGER[idempotency_key] = {
        "order": order_id,
        "amount": amount,
        "charge_id": f"ch-{len(LEDGER) + 1}",
    }
    if RECEIVED["charges"] == 1:
        raise EffectError("transient", "connection reset after the request was processed")
    return LEDGER[idempotency_key]


@tool(name="shipping.label", policy="idempotent")
async def label(order_id: str, address: str) -> dict:
    """Print a shipping label (the printer jams once)."""
    if LABELS["failures_left"] > 0:
        LABELS["failures_left"] -= 1
        raise RuntimeError("label printer jammed")
    return {"label": f"LBL-{order_id}", "to": address}


@tool(name="notify.send", policy="idempotent")
async def send(order_id: str, text: str) -> bool:
    """Tell the customer (idempotent: the notifier keys messages by order)."""
    if order_id not in SENT:
        SENT.append(order_id)
    return True


__all__ = ["charge", "check", "label", "send"]
