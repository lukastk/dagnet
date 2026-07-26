"""Fan-out and join, written as ordinary functions."""

from collections import Counter

SALES = [
    {"region": "north", "month": "2026-05", "product": "widget", "amount": 120.0},
    {"region": "north", "month": "2026-06", "product": "widget", "amount": 80.0},
    {"region": "south", "month": "2026-05", "product": "gadget", "amount": 200.0},
    {"region": "south", "month": "2026-06", "product": "widget", "amount": 45.5},
    {"region": "east", "month": "2026-06", "product": "gizmo", "amount": 310.25},
]


def load_sales(ctx) -> {"records": list[dict]}:
    print(f"loaded {len(SALES)} sales records")
    return {"records": list(SALES)}


def by_region(ctx, records: list[dict]) -> {"totals": dict}:
    totals = Counter()
    for record in records:
        totals[record["region"]] += record["amount"]
    return {"totals": dict(totals)}


def by_month(ctx, records: list[dict]) -> {"totals": dict}:
    totals = Counter()
    for record in records:
        totals[record["month"]] += record["amount"]
    return {"totals": dict(totals)}


def top_products(ctx, records: list[dict]) -> {"ranking": list[tuple[str, float]]}:
    totals = Counter()
    for record in records:
        totals[record["product"]] += record["amount"]
    return {"ranking": totals.most_common()}


def build_report(
    ctx, regions: dict, months: dict, products: list[tuple[str, float]]
) -> {"report": dict}:
    report = {"by_region": regions, "by_month": months, "top_products": products}
    print(f"report covers {len(regions)} regions and {len(months)} months")
    return {"report": report}
