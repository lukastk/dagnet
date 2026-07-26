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
dagnet run production --select "*db/drugs"   # that asset and its whole upstream chain
dagnet dev                            # the Dagster UI, run history, re-execution
dagnet graph                          # Mermaid, for a README
```

## `ctx` — the whole surface a node sees

Node code imports no framework. Everything a node gets from dagnet arrives on `ctx`,
and there is nothing else:

| | |
|---|---|
| `ctx.vars` | this node's resolved variables — globals, its own declarations, and whatever the run preset set, already merged. A read-only mapping. |
| `ctx.artifact(key)` | the resolved location of any declared artifact: a `Path` for a file, a `.table`/`.database` handle for a DuckDB table. An undeclared key raises. |
| `ctx.run_name` | the run preset's name, or `""` when launched without one. |
| `ctx.node_name` | this node's name in the manifest. Read-only. |
| `ctx.manifest_path` | absolute path of the manifest this pipeline was compiled from. Read-only. |

The last two exist mainly for library helpers a node opts into: they answer "which
node am I, and where is the map?" without resolving anything relative to the working
directory, which is not stable across executors — each step of a multiprocess run is
its own process.

## Pre-run validation hooks

A pipeline can refuse its own launches. `[pipeline] pre_run` names callables that
run **before anything materializes**:

```toml
[pipeline]
name = "warehouse"
pre_run = ["warehouse.guards:no_partial_rebuild"]
```

```python
# warehouse/guards.py — plain Python, like everything else
from dagnet.diagnostics import Diagnostics

def no_partial_rebuild(context):
    diagnostics = Diagnostics()
    if not context.is_everything and "db/facts" in context.asset_keys:
        diagnostics.error(
            "partial-rebuild", "rebuilding db/facts alone would orphan its dimensions",
            context.location("pipeline.pre_run"),
            hint="rerun without --select, or select the whole group",
        )
    return diagnostics
```

It returns a `Diagnostics`, or `None` if it has nothing to say, or raises. What it
is told:

| | |
|---|---|
| `manifest_path` | absolute path of the manifest this launch was compiled from |
| `store_root` | the **effective** store root: the `--store-root` override if given, else `[pipeline] store_root`, else the default. Absolute. |
| `run_name` | the run preset's name, or `None` |
| `selection` | the `--select` expression, or `None` meaning everything — `is_everything` reads better |
| `node_names`, `asset_keys` | that selection resolved against the real graph |
| `run_config` | the resolved Dagster run config |
| `is_resume`, `parent_run_id` | whether this is a `--from-failure` resume, and of what |

**Use `store_root`; don't re-derive it.** A hook that resolved durable locations
from the manifest would, under `--store-root`, act on a different place from the
one the run writes to — and nothing in the manifest would reveal the difference.
For a hook that deletes things, that is the difference between clearing the right
database and the wrong one.

**Any error aborts the launch** with the usual aggregated output and a nonzero
exit; warnings print and the run proceeds. Every hook runs even after one
objects, so you see all the objections at once.

**Check `is_resume`.** A `--from-failure` resume re-executes a *subset* of the
selection — the steps that failed and what depends on them — so it presents
exactly like the narrow selection a guard exists to refuse. A guard that ignores
the flag will refuse every legitimate resume; and anything that clears state
before writing must not clear it again for steps that already succeeded.

### `pre_execute` — the side-effecting slot

`pre_run` validates. Once it has fully passed, the launch is committed, and
`[pipeline] pre_execute` is where run-level *setup* goes — clearing tables,
staging a directory, taking a lock:

```toml
[pipeline]
pre_run     = ["warehouse.guards:no_partial_rebuild"]
pre_execute = ["warehouse.setup:clear_target_tables"]
```

Same callable shape, same context object. The difference is side effects, and
everything else follows from it:

| | `pre_run` | `pre_execute` |
|---|---|---|
| purpose | validate; refuse a bad launch | set up; the launch is committed |
| side effects | **must not** have any | this is the slot for them |
| when | first, before anything | only after every `pre_run` hook passed |
| on failure | all hooks still run, then abort | stops at the first failure |
| a refused launch | reaches them | **never** reaches them |

`pre_run` runs all its hooks even after one objects, so you see every objection
at once — which is only safe because they change nothing. `pre_execute` cannot
work that way: running the next side effect on top of a half-applied one is worse
than an incomplete report, so it stops at the first failure. Either way, a failure
aborts before a single step executes.

Both kinds run on `--from-failure` resumes, with `is_resume` set. Whether to act
on a resume is the hook's decision — a table-clearer generally should not, since
the rows it would clear include ones a completed step already wrote.

> **Scope: this covers `dagnet run` — not the Dagster UI.** A run launched from the
> launchpad never passes through dagnet, so neither `pre_run` nor `pre_execute`
> hooks run for it. `dagnet dev` warns about this when a manifest declares either.
> Treat both as a guarantee about the CLI, not about the pipeline.

## Status

**v0 is built** — the manifest and runs schema, `dagnet check`, the compiler to Dagster
`Definitions` (both the all-assets path and the `asset = false` partitioner), and the
`check` / `run` / `dev` / `graph` CLI. Ten sample projects run end to end, the last of
which ([`09_ai_index`](sample_projects/09_ai_index)) is a structurally faithful,
stub-bodied replica of a real 16-node production pipeline.

The design questions v0 raised have all been decided and folded into
[`_dev/DESIGN.md`](_dev/DESIGN.md); the record of what was asked and answered is
[`_dev/OPEN_QUESTIONS.md`](_dev/OPEN_QUESTIONS.md), and what the pre-build spikes found
about Dagster's behaviour is in [`_dev/experiments/FINDINGS.md`](_dev/experiments/FINDINGS.md).

## Commands

| command | what it does |
|---|---|
| `dagnet check` | validate the manifest and run presets; reports **all** problems at once, each pointing at its manifest location |
| `dagnet run [preset]` | materialize the pipeline under the multiprocess executor |
| `dagnet run --select "*key"` | pull semantics: that asset and its whole upstream chain (`+key` is one layer up, `++key` two) |
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
- `sample_projects/` — ten self-contained sample pipelines; the de-facto spec and test corpus
- `tests/` — unit tests (`uv run pytest`)
- `_dev/` — design docs, spike findings, open questions

## Node code and type-aware linters

The dict-shaped return annotation is netrun's, kept as optional, validated
documentation (DESIGN §7 rule 2):

```python
def extract(ctx) -> {'rows': list[int]}: ...
```

It puts string literals in annotation position, and linters read a string in an
annotation as a forward reference to a type — so each output name is reported as
an undefined name (ruff `F821`; mypy and pyright object more loudly). Two ways
out, both fine:

- **Keep the annotation** and ignore the rule for node modules:

  ```toml
  [tool.ruff.lint.per-file-ignores]
  "src/*/nodes.py" = ["F821"]
  ```

- **Omit it.** The annotation is optional; `dagnet check` validates it when
  present and says nothing when absent. The manifest is the authoritative
  interface either way.

## Development

```bash
uv sync
uv run pytest
uv run ruff check src tests sample_projects
```
