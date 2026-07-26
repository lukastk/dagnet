# dagnet — agent guide

dagnet is a thin declarative wrapper around Dagster: the whole pipeline is declared in one map file (`pipeline.toml`/`pipeline.json`), nodes are plain Python functions with zero framework imports, and dagnet compiles the map to Dagster `Definitions`. It is a **compiler + validator, not a runtime** — Dagster does the executing.

## Source of truth

**`_dev/DESIGN.md` is authoritative.** It defines the manifest schema (§5), the runs file (§6), the node function contract (§7), validation (§7b), the compilation mapping (§8), what is deliberately dropped from netrun and why (§9), and the build plan (§11). Do not improvise on design points it settles; if it is silent or ambiguous on something that matters, raise the question with the parent thread / Lukas instead of guessing — design decisions are recorded back into DESIGN.md.

## Principles

- **The manifest is the map.** Every node's full interface (inputs/outputs) is declared in the file; the function signature is validated *against* it, never the source of it. No factories, no interface derived from code.
- **Loud errors, no fallbacks.** `dagnet check` aggregates ALL diagnostics with locations (compiler-style), never fail-fast-on-first. Never add a silent default that masks a bug — prefer exceptions.
- **Thin wrapper.** If an implementation problem tempts you to reimplement or patch Dagster semantics, stop and raise it — the moment we fight the host framework we have recreated the complexity we left behind.
- **Node code stays framework-free.** Nodes never import dagster or dagnet; `ctx` is a small dagnet-owned object.

## Conventions

- Plain Python, `src/` layout, **uv** (`uv sync`, `uv run pytest`). **msgspec** for the schema (TOML + JSON from one schema). No notebooks/nblite in this repo.
- `ruff` for lint/format.
- Every feature lands with (a) a unit test and (b) coverage in a `sample_projects/` sample where it makes sense — the samples are the de-facto spec, mirroring the old netrun repo's `sample_projects/`.
- Sample projects are self-contained uv projects, numbered (`00_basic`, `01_...`), each with a README stating what it demonstrates and a runnable `dagnet run` invocation.
- Git commit messages should read as a prompt another agent could use to recreate the work; end with the `Co-Authored-By: Claude <noreply@anthropic.com>` trailer.

## Working relationship

This repo is typically built by a sesh child thread supervised by a parent thread. Report meaningful progress and design questions upward (the parent checks in via sesh); don't sit on blockers.
