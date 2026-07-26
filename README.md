# dagnet

A thin declarative wrapper around [Dagster](https://dagster.io). The entire pipeline — nodes, data dependencies, artifacts, concurrency limits, run presets — is declared in a single map file (`pipeline.toml` or `pipeline.json`); the only code you write is each node's body (plain Python functions with zero framework imports).

dagnet is **a compiler and a validator, not a runtime**: it parses and loudly validates the manifest, then compiles it to Dagster `Definitions`. Dagster provides scheduling, parallelism, retries, value transport, run history, resume, and the web UI.

Successor to [netrun](https://github.com/lukastk/netrun). The full design rationale, file formats, function contract, and build plan live in [`_dev/DESIGN.md`](_dev/DESIGN.md).

## What it looks like

```toml
# pipeline.toml — the whole pipeline
[pipeline]
name = "ai_index"

[pools]
heavy = 1                                  # at most one GPU node at a time

[vars]
sample_n = { type = "int", default = 1000 }

[nodes.llm_filter]
fn      = "ai_index.nodes.llm_filter.main"
inputs  = { candidates = "embed.matches" }
outputs = ["successful_ad_ids"]
pool    = "heavy"
retries = { max = 3, wait_s = 10 }

[nodes.rerank]
fn      = "ai_index.nodes.rerank.main"
inputs  = { ad_ids = "llm_filter.successful_ad_ids" }   # renames as it wires
outputs = ["ranked"]
```

```python
# ai_index/nodes/rerank.py — no dagster import, no dagnet import
async def main(ctx, ad_ids: list[int]) -> {'ranked': list[int]}:
    return {"ranked": rerank(ad_ids, model=ctx.vars["llm_model"])}
```

```bash
dagnet check                          # every problem at once, each with a location
dagnet run production --select "+db/drugs"
dagnet dev                            # the Dagster UI, run history, re-execution
dagnet graph                          # Mermaid, for a README
```

## Status

**v0 is built** — the manifest and runs schema, `dagnet check`, the compiler to Dagster
`Definitions` (both the all-assets path and the `asset = false` partitioner), and the
`check` / `run` / `dev` / `graph` CLI. Nine sample projects run end to end.

Next per `_dev/DESIGN.md` §11: port the AISI exposure index, then Scuttlebug.

Design questions raised while building v0 are collected in
[`_dev/OPEN_QUESTIONS.md`](_dev/OPEN_QUESTIONS.md); what the pre-build spikes found
about Dagster's behaviour is in [`_dev/experiments/FINDINGS.md`](_dev/experiments/FINDINGS.md).

## Commands

| command | what it does |
|---|---|
| `dagnet check` | validate the manifest and run presets; reports **all** problems at once, each pointing at its manifest location |
| `dagnet run [preset]` | materialize the pipeline under the multiprocess executor |
| `dagnet run --select "+key"` | pull semantics: that asset and everything upstream |
| `dagnet run --from-failure last` | resume a failed run, skipping the steps that succeeded |
| `dagnet run --ephemeral` | no instance state left behind (in-process; pool limits are inert) |
| `dagnet dev` | generate `defs.py` and serve the Dagster UI |
| `dagnet graph` | export a Mermaid diagram from the manifest alone |

## Layout

- `src/dagnet/` — the package
  - `schema.py` — the manifest and runs file as msgspec structs (TOML and JSON, one schema)
  - `loader.py` — parsing, item by item so one bad node can't hide the rest
  - `diagnostics.py` — the aggregated, located diagnostics everything reports through
  - `graph.py` — the derived, framework-free view: references, deps, cycles, asset keys
  - `nodefn.py` — importing node functions and reading their signatures
  - `check.py` — the validation passes
  - `partition.py` — grouping nodes into what Dagster executes (`asset = false` folding)
  - `compile.py` — the mapping to Dagster `Definitions`
  - `context.py` — `ctx`, the whole surface a node function sees
  - `runs.py`, `instance.py`, `mermaid.py`, `cli.py`, `_reconstruct.py`
- `sample_projects/` — nine self-contained sample pipelines; the de-facto spec and test corpus
- `tests/` — unit tests (`uv run pytest`)
- `_dev/` — design docs, spike findings, open questions

## Development

```bash
uv sync
uv run pytest
uv run ruff check src tests sample_projects
```
