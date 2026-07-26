# dagnet sample projects

Self-contained sample pipelines, each demonstrating one feature area of dagnet.
Together they are the de-facto spec and test corpus (mirroring the old netrun
repo's `sample_projects/`). Each sample is its own uv project with a
`pipeline.toml`, a `nodes.py` of plain-Python node functions, a `runs.toml` or
`runs/` where relevant, and a README stating what it demonstrates and how to run
it.

`tests/test_samples.py` checks and runs every one of them, so a change that
breaks a sample breaks the build.

## The corpus

| sample | demonstrates |
|---|---|
| [`00_basic`](00_basic) | a linear pipeline, values on the edges, a renamed input, a multi-output node |
| [`01_fanout_join`](01_fanout_join) | one output feeding several nodes; a multi-input join; `group` |
| [`02_ordering_after`](02_ordering_after) | `after` — ordering with no data, replacing signals/controls/broadcast |
| [`03_artifacts`](03_artifacts) | declared durable artifacts (files + DuckDB tables), `ctx.artifact()`, artifact inputs |
| [`04_pools_retries`](04_pools_retries) | `[pools]` as concurrency limits (`heavy = 1`), per-node `retries` |
| [`05_run_presets`](05_run_presets) | `runs/` as a folder, named runs, per-node overrides, `ctx.vars` |
| [`06_transient_ops`](06_transient_ops) | `asset = false` nodes folded into graph-backed assets |
| [`07_checks`](07_checks) | asset checks guarding a schema contract, and what a failure looks like |
| [`08_pull_select`](08_pull_select) | `dagnet run --select "+key"` pull semantics |

## Running one

Every sample works the same way:

```bash
cd 03_artifacts
uv sync
uv run dagnet check
uv run dagnet run          # add a run-preset name where the sample has one
uv run dagnet graph
uv run dagnet dev
```

## What they have in common

- **No framework imports in node code.** Not one `nodes.py` in this directory
  imports dagster or dagnet. `ctx` is the entire surface a node sees.
- **The map file is the interface.** Every node's inputs and outputs are declared
  in `pipeline.toml`; the function signature is checked against it, never the
  source of it.
- **No `defs.py` checked in.** `dagnet dev` generates it into `.dagster/`.
