# dagnet

A thin declarative wrapper around [Dagster](https://dagster.io). The entire pipeline — nodes, data dependencies, artifacts, concurrency limits, run presets — is declared in a single map file (`pipeline.toml` or `pipeline.json`); the only code you write is each node's body (plain Python functions with zero framework imports).

dagnet is **a compiler and a validator, not a runtime**: it parses and loudly validates the manifest, then compiles it to Dagster `Definitions`. Dagster provides scheduling, parallelism, retries, value transport, run history, resume, and the web UI.

Successor to [netrun](https://github.com/lukastk/netrun). The full design rationale, file formats, function contract, and build plan live in [`_dev/DESIGN.md`](_dev/DESIGN.md).

## Status

Pre-v0, under construction. See `_dev/DESIGN.md` §11 for the build plan.

## Layout

- `src/dagnet/` — the package (schema, validation, compiler, CLI)
- `sample_projects/` — self-contained sample pipelines, each demonstrating one feature area; the de-facto spec and test corpus
- `tests/` — unit tests (`uv run pytest`)
- `_dev/` — design docs
