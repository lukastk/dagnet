# Open design questions from building v0

*Raised 2026-07-26 while implementing DESIGN §11 step 1. Each names what DESIGN
settles, what it doesn't, what I did in the meantime, and what changes if the
answer differs. Decisions should be recorded back into DESIGN.md.*

Ordered by how much the answer would change.

---

## 1. What is the "store root"? (§5.4)

§5.4's example comments that a file artifact's `path` is "relative to the store
root", but no section defines a store root.

**What I did:** file artifacts resolve relative to **the manifest's directory**,
consistent with how §5.1 resolves `dagster_home`. A `--store-root` flag overrides
it per invocation (and the sample tests use it to keep output out of the repo).

**If it should be something else** — a `[pipeline].store_root` field, or
`dagster_home`-relative — it is a one-line change in `compile_definitions`, but
it changes where every existing consumer's files land, so it wants deciding now.

## 2. A DuckDB table artifact doesn't say which database it is in (§5.4)

`kind = "duckdb_table"` carries only `table`. So the *database file's* location
lives outside the manifest — which is the `paths.py` problem §5.4 exists to fix,
in miniature.

**What I did:** sample 03 declares the database itself as a `file` artifact
(`"db/warehouse" = { kind = "file", path = "build/warehouse.duckdb" }`) that no
node lists as an input; nodes resolve it with `ctx.artifact("db/warehouse")`. It
works and keeps the path in the map, but it is a convention, not a schema.

**Options:** add `database = "<artifact key>"` (or a path) to
`DuckDBTableArtifact`; or bless the convention and document it; or add a
`[pipeline].duckdb` default. Anything that puts it in the schema means
`ctx.artifact("db/customers")` could return something richer than a bare table
name — which would change the node contract, so it is worth settling before
Scuttlebug is ported.

## 3. Should a failing asset check fail the run?

DESIGN §3 says check failures are "loud, recorded, and visible in the UI" — which
is what happens. It does not say whether they should be fatal.

**What I did:** Dagster's default. Sample 07's `bad` run logs
`Asset check 'units_are_canonical' ... did not pass`, records it against the
asset, and the run still **exits 0**.

**The tension:** for CI, an exit code of 0 on a violated schema contract is close
to the silent failure AGENTS.md tells us to avoid. Dagster supports
`blocking=True` per check (halt downstream assets, fail the run). A manifest
field — `checks = { rows = [...] }` gaining a blocking form, or a per-node
`blocking = true` — would be small. This is the question I'd most like answered.

## 4. The check-function contract is undefined (§5.5, §7)

§5.5 introduces `checks` as "output name → list of check-function import paths"
and §8 maps them to asset checks, but no section gives the function signature.

**What I did:**

```python
def units_are_canonical(ctx, measurements):        # ctx, then the subject
    return {"passed": bool, "metadata": {...}}     # or just a bool
```

`subject` is the asset's loaded value for a normal output, and the **resolved
artifact location** for an artifact-bound output (there is no value to load).
Raising counts as a failure. Anything else returned raises `CheckReturnError`.

## 5. Variable precedence beyond defaults-vs-run (§6)

§6 fixes "`[defaults]` merged with `[runs.<name>]`, run wins" but not what happens
once node-local declarations and per-node overrides are all in play.

**What I did** (highest first, documented in `runs.py` and sample 05's README):

1. the run's per-node override — `[runs.<run>.<node>]`
2. `[defaults]`' per-node override — `[defaults.<node>]`
3. the run's global value — `[runs.<run>]`
4. `[defaults]`' global value — `[defaults]`
5. the node-local declared default — `[nodes.<node>.vars]`
6. the global declared default — `[vars]`

Values set by a run always beat declared defaults; among values, more specific
wins and the run beats defaults; among declared defaults, node-local wins.

## 6. `--ephemeral` cannot be multiprocess at all

Spike (a) found this is stronger than §8's caveat: Dagster raises
`DagsterUnmetExecutorRequirementsError` for multiprocess on an ephemeral instance,
and reports `supports_global_concurrency_limits = False`.

**What I did:** `--ephemeral` implies the in-process executor, and prints a
warning when the manifest declares `[pools]` saying the limits are NOT enforced.

**Question:** should that be an **error** instead? A CI run that silently ignores
`heavy = 1` may thrash a machine. Erroring would make `--ephemeral` unusable for
any pipeline with pools, which may be worse.

## 7. Additions I made that DESIGN doesn't mention

Each is small and removable; flagging so none becomes an unnoticed default.

- **`dagnet run` with no run name.** §8 writes `dagnet run <run_name>`; I made it
  optional so a project without a runs file works (samples 00–04, 08 rely on it).
  `ctx.run_name` is `""` in that case.
- **`dagnet run --from-failure <run_id|last>`.** §8 puts re-execution only in
  `dagnet dev`, but spike (e) showed it works from library mode and reaches
  individual ops. This is what replaces Scuttlebug's `skip_if_done`, so having it
  on the CLI seemed worth more than the ~15 lines.
- **`unfilled-var` reported at check time.** §5.3 says an unfilled non-default
  variable is a *launch* error. Since `check` already reads the run presets, it
  reports per-run which required variables a preset leaves unset. Still enforced
  at launch too.
- **A duplicate `[defaults]` key across runs files is an error**, symmetric with
  the duplicate-run-name rule §6 does state. With several files in a folder there
  is no defensible winner. May be too strict for the "checked-in `runs/` plus a
  local scratch file" case §6 mentions.
- **A node must declare at least one output** (`no-outputs`). Every example in
  DESIGN does, and a node with none cannot compile to a multi-asset — but a
  terminal "publish" node that only has side effects would have to declare a
  token output.
- **Node, output, artifact-key-component and run names must be plain identifiers**
  (`[A-Za-z0-9_]+`), because they become Dagster op/output/job names. Better as
  our diagnostic than as a Dagster traceback, but it is a constraint DESIGN
  doesn't state.

## 8. Variable types are scalars only

`[vars]` accepts `str`, `int`, `float`, `bool`. §6's examples are all scalar and
§5.3 says nothing wider, so I kept it tight. Lists come up in practice (a list of
regions, a list of model names). Widening is easy; narrowing later is not.

## 9. Diagnostic locations are logical paths, not line numbers

A `Location` renders as `pipeline.toml:nodes.rerank_candidates.inputs.ad_ids`.
Neither `tomllib`/`msgspec` nor `json` reports source positions, so a real
`file:line:col` needs a position-aware parse alongside the value parse.
`Location` already carries an unpopulated `line` field, so adding it later
changes no call sites. Is the dotted path enough for v0?

## 10. Two consequences worth knowing about

Not questions, but things that will surprise someone eventually:

- **A graph-backed asset does not subset.** A node that folds in `asset = false`
  op-nodes loses per-output selectability: selecting one of its outputs pulls all
  of them. Documented in sample 06's README. Plain asset nodes are unaffected.
- **The dict-shaped return annotation trips type-aware tooling.** `-> {'rows': list[int]}`
  puts string literals in annotation position, and linters read a string
  annotation as a forward reference to a type — so every output name reads as an
  undefined name (33 × F821 across the samples; mypy/pyright would object harder).
  Ignored per-file in `pyproject.toml` with an explanation. Consumer repos hit
  this too. Worth considering an alternative documented form, since §7 already
  makes the annotation optional.

## 11. Size

DESIGN §1 estimated ~500–800 lines. v0 is **~1,635 lines of code** (3,360
including docstrings and comments) across 15 modules. The overshoot is
concentrated in two places: `check.py` (599 lines) — the aggregated diagnostics
with per-case messages, did-you-mean hints and locations — and `compile.py` (794)
— which carries both the multi-asset path and the graph-backed partitioner.
Nothing here is Dagster reimplementation; it is validation surface and the
op/asset packaging. Flagging in case the number matters more than the content.
