# 05_run_presets — named run configurations

The system AISI's `run_defs.toml` used daily, promoted from a hand-written merge
script to a wrapper feature. Four named runs of the same pipeline, from a 2-row
smoke test to a million-row production run.

## What it demonstrates

- **Variables are declared, not discovered.** `[vars]` lists every global; a node
  can add its own in `[nodes.<n>.vars]`. A runs-file key that matches no
  declaration is a `dagnet check` error, not a silently ignored line.
- **A run preset is `[defaults]` merged with `[runs.<name>]`.** Scalar keys set
  globals; a subtable named after a node sets that node's variables.
- **`runs/` can be a folder.** Both `runs/base.toml` and `runs/scratch.toml` are
  loaded and merged into one registry. A duplicate run name — or a duplicate
  `[defaults]` key — across files is a loud error, because with several files
  there is no defensible winner.
- **Node-local declarations shadow globals.** `classify` declares its own
  `sample_n`, so it sees `50` (its declared default) where the rest of the
  pipeline sees `1000`, unless a run sets `sample_n` explicitly.
- **`ctx.vars` is already resolved.** A node never merges anything: it reads a
  plain mapping of exactly the variables it can see. `ctx.run_name` is the preset's
  name.
- **A required variable is required.** `llm_model` has no default, so a run that
  never sets it fails `dagnet check` — before anything launches.
- **Runs are visible in the UI.** Each preset compiles to a Dagster job with its
  config already filled in, so `dagnet dev` shows `test_api`, `production`, … in
  the launchpad, editable before launch.

## Precedence, highest first

DESIGN §6 fixes the defaults/run merge; the full order once node-local
declarations exist is:

1. the run's per-node override — `[runs.<run>.<node>]`
2. `[defaults]`' per-node override — `[defaults.<node>]`
3. the run's global value — `[runs.<run>]`
4. `[defaults]`' global value — `[defaults]`
5. the node-local declared default — `[nodes.<node>.vars]`
6. the global declared default — `[vars]`

That is: values set by a run always beat declared defaults; among values, more
specific wins and the run beats the defaults section; among declared defaults,
node-local wins.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run smoke           # 2 rows, chunk_size 1, dry_run true
uv run dagnet run test_api        # 10 rows, gpt-5.2
uv run dagnet run validation_5k   # 5000 rows, the default model
uv run dagnet dev                 # every preset shows up as a job
```

Watch `classify`'s log line across runs — it is the one node that sees a
different `sample_n` from everybody else.
