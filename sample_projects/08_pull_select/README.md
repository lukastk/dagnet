# 08_pull_select — asking for one asset and getting what it needs

A diamond: `raw → clean → {metrics, sessions} → bundle`. Because every output is
an asset with a name, you can ask for any of them and Dagster works out the rest.

## What it demonstrates

- **Pull semantics.** `--select "*metrics/daily"` means "materialize
  `metrics/daily` and its whole upstream chain". That is netrun's
  `run_to_targets`, natively, with no extra machinery in the map file. Note the
  `*`: `+` is one layer, not the whole chain — see the table below.
- **Branches stay independent.** Selecting the `metrics` branch never touches
  `sessions`, even though both read `clean/events`.
- **Selection is Dagster's syntax, passed through.** dagnet does not invent a
  selection language; `--select` is handed to Dagster unchanged.

## Run it

```bash
uv sync
uv run dagnet check

uv run dagnet run                              # all five nodes
uv run dagnet run --select "*metrics/daily"    # raw, clean, metrics — not sessions
uv run dagnet run --select "+metrics/daily"    # clean and metrics only: one layer up
uv run dagnet run --select "clean/events"      # just that one node
uv run dagnet run --select "raw/events*"       # raw and everything downstream
uv run dagnet run --select "group:analysis"    # both analysis nodes
```

## What `*` and `+` mean

`*` is the whole chain; each `+` is one layer. Getting these confused is easy and
the failure is quiet — you get a smaller run than you meant — so the table is
worth reading once. Verified against this very pipeline
(`raw → clean → {metrics, sessions} → bundle`):

| expression | resolves to |
|---|---|
| `metrics/daily` | `metrics/daily` |
| `+metrics/daily` | `clean/events`, `metrics/daily` — **one** layer up |
| `++metrics/daily` | `raw/events`, `clean/events`, `metrics/daily` — two layers |
| `*metrics/daily` | the whole upstream chain, however deep |
| `metrics/daily*` | it and everything downstream |
| `*metrics/daily*` | both directions |

So `*key` is the one that means "and everything it needs", which is what
`run_to_targets` did. Reach for `+` only when you genuinely want a fixed number of
layers.

Selecting an asset whose upstream inputs are **not** included makes Dagster load
them from the last materialization, so `--select "clean/events"` after a full run
reuses `raw/events` rather than recomputing it.

## One caveat

A node is atomic: it always computes all of its outputs. Selecting one output of
a multi-output node runs the whole node and records only the output you asked for.
For a node that folds in `asset = false` op-nodes (sample 06), selection pulls all
of that node's outputs together.
