# 06_transient_ops — `asset = false`, folded into the asset downstream

Not every step deserves a catalog entry. `drop_nulls` and `normalise` here are two
lines of cleanup each: nothing needs to resume from them, validate them, or ask
for them by name. Marking them `asset = false` keeps them in the map file — the
graph stays flat and readable — while removing them from the asset catalog.

## What it demonstrates

- **`asset = false` makes a node transient.** No asset key, no materialization
  history, no `checks`, not `--select`-able. It is plumbing.
- **The manifest stays flat.** `pipeline.toml` declares four nodes in a straight
  line. The nesting is compile-time packaging, invisible in the map.
- **Ops fold into the nearest downstream asset.** `drop_nulls` and `normalise`
  compile into the graph backing `report`, and appear in the run as
  `report_graph.drop_nulls` and `report_graph.normalise` — separate steps, one
  level down in the UI.
- **Node code doesn't change.** All four functions in `nodes.py` are written
  identically. Whether a node is durable is a decision in the map file, not in
  the code.
- **Variables still reach folded nodes.** `drop_nulls` reads `ctx.vars["threshold"]`
  even though it lives inside a graph one level deeper in the run config.
- **Resume granularity survives.** Re-execution from failure restarts at the
  individual op, not the whole graph — so folding a node in costs you its catalog
  entry, not your ability to resume past it.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run           # threshold 0.0 -> keeps 4 readings
uv run dagnet run strict    # threshold 2.0 -> keeps 3
uv run dagnet graph         # transient nodes are drawn with a dashed outline
```

Only two assets materialize — `load_readings/readings` and `report/summary` —
while three steps run inside `report_graph`.

## The trade-offs, stated plainly

- An `asset = false` node **must** reach a downstream asset node through `inputs`.
  Transient work nothing durable consumes is dead code, and `dagnet check` says so.
- An op-node feeding **two** asset nodes merges them into a single multi-asset that
  always materializes together, because the op can only live in one graph.
  `dagnet check` warns when this happens; the fix is `asset = true`.
- A graph-backed asset does not subset, so selecting one output of a node that
  folds ops in pulls all of that node's outputs.
