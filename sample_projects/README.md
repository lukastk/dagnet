# dagnet sample projects

Self-contained sample pipelines, each demonstrating one feature area of dagnet. Together they are the de-facto spec and test corpus (mirroring the old netrun repo's `sample_projects/`). Each sample is its own uv project with a `pipeline.toml`, a `nodes/` (or `nodes.py`) with plain-Python node functions, a `runs.toml` where relevant, and a README stating what it demonstrates and how to run it.

Planned corpus (adjust as features land — see `_dev/DESIGN.md`):

- `00_basic` — linear three-node pipeline, in-memory value passing, `dagnet check` / `dagnet run`
- `01_fanout_join` — one output consumed by several nodes; a multi-input join node
- `02_ordering_after` — ordering-only dependencies (`after`), no data passed
- `03_artifacts` — declared durable artifacts (files + a DuckDB table), `ctx.artifact()`, artifact-input wiring
- `04_pools_retries` — concurrency pools (`heavy = 1`), per-node retries
- `05_run_presets` — `runs.toml` + a `runs/` folder, named runs, per-node variable overrides, `ctx.vars`
- `06_transient_ops` — `asset = false` nodes folded into graph-backed assets
- `07_checks` — asset checks guarding a schema contract, loud failure
- `08_pull_select` — `dagnet run --select "+key"` pull semantics
