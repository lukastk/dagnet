# 03_artifacts — durable objects with declared locations

Two JSON files extracted from "source systems", loaded into two DuckDB tables,
joined into a report. Every location is declared once in `pipeline.toml`; no node
contains a path or a table name.

## What it demonstrates

- **`[artifacts]` replaces a hand-maintained `paths.py`.** Scuttlebug's real
  problem was that its data topology lived out-of-band in a path registry while
  its pipeline file carried only ordering. Here the locations *are* the map.
- **An output can BE an artifact.** `artifacts = { customers = "raw/customers" }`
  means "this output is that durable thing; the node writes it itself". Such a
  node returns nothing — `extract_customers` is annotated `-> None` — and dagnet
  records the materialization, with the declared location as asset metadata.
- **`ctx.artifact(key)` resolves by kind.** A `file` artifact resolves to a `Path`
  (relative to the manifest); a `duckdb_table` artifact resolves to the table's
  *name*. An undeclared key raises, with a did-you-mean hint.
- **An artifact input is a real dependency.** `inputs = { source = "raw/customers" }`
  gives `load_customers` the resolved `Path` *and* an edge to whichever node
  writes that artifact. No value crosses the IO manager; the ordering is real.
- **Exactly one producer.** An artifact consumed by an input with no producing
  node — or with two — is a `dagnet check` error.
- **Writing is verified.** A node that declares a file artifact as an output but
  doesn't write it fails loudly at the end of the step, rather than materializing
  an asset that isn't there.
- **The database file is an artifact too.** `db/warehouse` is declared as a `file`
  artifact that no node lists as an input; nodes resolve it directly with
  `ctx.artifact("db/warehouse")`. That keeps the database path in the map rather
  than in a variable or a constant.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run
uv run dagnet graph    # files render as tilted boxes, tables as cylinders
```

Afterwards, `build/` holds `raw/customers.json`, `raw/orders.json` and
`warehouse.duckdb`. Re-running is idempotent (`CREATE OR REPLACE TABLE`).

## Why `after = ["load_customers"]` on `load_orders`

Both loaders open the same DuckDB file for writing, and DuckDB takes an exclusive
lock. They have no data dependency, so `after` is exactly the right tool: order
without a value. A concurrency pool would work too — see sample 04.
