# 08_pull_select — asking for one asset and getting what it needs

A diamond: `raw → clean → {metrics, sessions} → bundle`. Because every output is
an asset with a name, you can ask for any of them and Dagster works out the rest.

## What it demonstrates

- **Pull semantics.** `--select "+metrics/daily"` means "materialize
  `metrics/daily` and everything upstream of it". That is netrun's
  `run_to_targets`, natively, with no extra machinery in the map file.
- **Branches stay independent.** Selecting the `metrics` branch never touches
  `sessions`, even though both read `clean/events`.
- **Selection is Dagster's syntax, passed through.** dagnet does not invent a
  selection language; `--select` is handed to Dagster unchanged.

## Run it

```bash
uv sync
uv run dagnet check

uv run dagnet run                              # all five nodes
uv run dagnet run --select "+metrics/daily"    # raw, clean, metrics — not sessions
uv run dagnet run --select "clean/events"      # just that one node
uv run dagnet run --select "raw/events+"       # raw and everything downstream
uv run dagnet run --select "group:analysis"    # both analysis nodes (and their inputs)
```

## What `+` means

| expression | meaning |
|---|---|
| `metrics/daily` | that asset alone |
| `+metrics/daily` | it and everything upstream |
| `metrics/daily+` | it and everything downstream |
| `+metrics/daily+` | both directions |
| `++metrics/daily` | two layers upstream, not the whole chain |

Selecting an asset whose upstream inputs are **not** included makes Dagster load
them from the last materialization, so `--select "clean/events"` after a full run
reuses `raw/events` rather than recomputing it.

## One caveat

A node is atomic: it always computes all of its outputs. Selecting one output of
a multi-output node runs the whole node and records only the output you asked for.
For a node that folds in `asset = false` op-nodes (sample 06), selection pulls all
of that node's outputs together.
