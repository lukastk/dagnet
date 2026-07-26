"""Nodes that write durable things, and nodes that read them by declared location.

No node contains a hard-coded path, database or table name. Every location comes
from `ctx.artifact(key)`, which resolves it out of the manifest — a file artifact
becomes a `Path`, a DuckDB table artifact becomes a frozen handle carrying
`.table` and `.database`.
"""

import json

import duckdb

CUSTOMERS = [
    {"id": 1, "name": "Ada", "country": "GB"},
    {"id": 2, "name": "Grace", "country": "US"},
    {"id": 3, "name": "Kathleen", "country": "US"},
]

ORDERS = [
    {"id": 10, "customer_id": 1, "total": 42.50},
    {"id": 11, "customer_id": 1, "total": 17.00},
    {"id": 12, "customer_id": 3, "total": 99.99},
]


def extract_customers(ctx) -> None:
    """An artifact-only node: it writes, and returns nothing."""
    path = ctx.artifact("raw/customers")
    path.write_text(json.dumps(CUSTOMERS, indent=2))
    print(f"wrote {len(CUSTOMERS)} customers to {path}")


def extract_orders(ctx) -> None:
    path = ctx.artifact("raw/orders")
    path.write_text(json.dumps(ORDERS, indent=2))
    print(f"wrote {len(ORDERS)} orders to {path}")


def _load(ctx, source, artifact_key: str) -> None:
    # A table artifact resolves to a handle: which table, in which database.
    location = ctx.artifact(artifact_key)
    rows = json.loads(source.read_text())
    with duckdb.connect(str(location.database)) as connection:
        connection.execute(
            f"CREATE OR REPLACE TABLE {location.table} AS SELECT * FROM read_json_auto(?)",
            [str(source)],
        )
        count = connection.execute(f"SELECT count(*) FROM {location.table}").fetchone()[0]
    assert count == len(rows), f"{location.table}: wrote {count} rows, expected {len(rows)}"
    print(f"loaded {count} rows into table {location.table} of {location.database.name}")


def load_customers(ctx, source) -> None:
    _load(ctx, source, "db/customers")


def load_orders(ctx, source) -> None:
    _load(ctx, source, "db/orders")


def report(ctx, customers, orders) -> {"summary": dict}:
    """Both inputs are table handles, resolved from the manifest."""
    with duckdb.connect(str(customers.database)) as connection:
        rows = connection.execute(
            f"SELECT c.country, count(*) AS n, sum(o.total) AS total "  # noqa: S608
            f"FROM {orders.table} o JOIN {customers.table} c ON c.id = o.customer_id "
            f"GROUP BY c.country ORDER BY c.country"
        ).fetchall()
    summary = {country: {"orders": n, "total": round(total, 2)} for country, n, total in rows}
    print(f"summary: {summary}")
    return {"summary": summary}
