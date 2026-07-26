# 00_basic — a linear pipeline with values on the edges

The smallest complete dagnet project: `extract → transform → summarise`, with real
values passed between nodes in memory.

## What it demonstrates

- **The map file is the whole pipeline.** `pipeline.toml` declares three nodes,
  their `fn` import paths, and each node's full interface (`inputs` / `outputs`).
  There is no edges list — an edge *is* an input referencing an output.
- **Node code is plain Python.** `src/basic_pipeline/nodes.py` imports neither
  dagster nor dagnet. Each function takes `ctx` first, then one parameter per
  declared input, and returns a dict keyed by its declared outputs.
- **Inputs rename as they wire.** `summarise` takes a parameter called `values`
  fed by `transform`'s output `clean`: `inputs = { values = "transform.clean" }`.
- **Multiple outputs from one node.** `transform` declares `clean` and `rejected`;
  each becomes its own asset (`transform/clean`, `transform/rejected`), so either
  can be selected, inspected, or resumed from independently.
- **The optional return annotation is checked.** The `-> {'clean': ..., 'rejected': ...}`
  annotations are documentation that `dagnet check` holds against the manifest.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run
uv run dagnet graph
uv run dagnet dev      # the Dagster UI, with run history and re-execution
```

`dagnet run` materializes four assets — `extract/readings`, `transform/clean`,
`transform/rejected`, `summarise/summary` — each step in its own subprocess.

Try breaking it: rename a parameter in `nodes.py`, or point an input at an output
that doesn't exist, and `dagnet check` will name both sides of the disagreement.
