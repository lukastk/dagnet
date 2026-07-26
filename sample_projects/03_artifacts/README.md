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
  under `[pipeline].store_root`; a `duckdb_table` artifact resolves to a frozen
  handle carrying `.table` and `.database` (itself a resolved `Path`). An
  undeclared key raises, with a did-you-mean hint.
- **An artifact input is a real dependency.** `inputs = { source = "raw/customers" }`
  gives `load_customers` the resolved `Path` *and* an edge to whichever node
  writes that artifact. No value crosses the IO manager; the ordering is real.
- **Exactly one producer.** An artifact consumed by an input with no producing
  node — or with two — is a `dagnet check` error.
- **Writing is verified.** A node that declares a file artifact as an output but
  doesn't write it fails loudly at the end of the step, rather than materializing
  an asset that isn't there.
- **A table artifact says which database it is in.** `database = "db/warehouse"`
  is required and must name a declared `file` artifact — `dagnet check` verifies
  both. That keeps the database path in the map rather than in a variable or a
  constant, which is the whole reason `[artifacts]` exists.
- **`store_root` is declared.** `[pipeline] store_root = "build"` puts every file
  artifact under `build/`, resolved relative to this manifest. `--store-root`
  overrides it, which is how the test suite runs this sample into a temp dir.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run
uv run dagnet graph    # files render as tilted boxes, tables as cylinders
```

Afterwards, `build/` holds `raw/customers.json`, `raw/orders.json` and
`warehouse.duckdb`. Re-running is idempotent (`CREATE OR REPLACE TABLE`).

## Mutual exclusion is a pool, not an ordering

Both loaders open the same DuckDB file for writing and DuckDB takes an exclusive
lock, so they must not run *at the same time*. But there is no reason one has to
come before the other — they are unordered, merely exclusive.

`[pools] duckdb_writer = 1` says exactly that, and nothing more. Writing
`after = ["load_customers"]` on `load_orders` would also work, but it would assert
an ordering the pipeline does not actually have — the same slow drift that left
netrun graphs carrying edges whose only payload was the string `"done"`. Use
`after` when B genuinely must follow A; use a pool when they merely must not
overlap.
