# Design: a thin Dagster wrapper for declarative pipelines (netrun successor)

*2026-07-26 · written by Claude. Follows on from [[Netrun as a thin wrapper around Dagster]] (the general netrun↔Dagster evaluation) and [[Scuttlebug orchestration - netrun vs Dagster evaluation]] (the Scuttlebug-specific verdict). This pad is the concrete design: what the wrapper is, its file formats, its function contract, how it compiles to Dagster, and the build plan. Working name used throughout: **dagnet** (leading candidate — see §10).*

---

## 1. Purpose and context

netrun is Lukas's flow-based pipeline runtime. Its rewrite (the `rewrite` branch of the netrun repo) is at Phase 1 of 14. The evaluation pads concluded that for the workloads netrun actually serves, most of the planned rewrite duplicates what Dagster (a mature, Apache-2.0, open-source data orchestrator) already provides — and that the part of netrun Lukas actually values can be preserved as a *thin wrapper* over Dagster instead of a from-scratch runtime.

The part to preserve, in one sentence:

> **A single declarative file is the complete map of the pipeline — its nodes, its data dependencies, its artifacts, and its run configurations — and the only code you write is the body of each node.**

Everything else that netrun was (a packet-flow runtime, an execution manager with thread/process/remote worker pools, a caching layer, an in-graph control system) is deliberately dropped. The evidence for why dropping it is safe comes from the two real consumers examined below.

This is Tier 1 (+ a thin slice of Tier 2) from the earlier "do we even need a wrapper?" discussion: Dagster's own YAML mechanism (`defs.yaml`, part of its "Components" system) is only an *envelope* — the YAML can only express what some Python component class defines a schema for. So a small wrapper is irreducible if we want our own vocabulary; the design below keeps it genuinely small (~500–800 lines).

---

## 2. Evidence: what the two real consumers actually use

Two production pipelines were examined in detail. They sit at opposite ends of netrun usage, which makes them a good joint requirements base.

### 2a. Scuttlebug healthcare (the Scuttlebug pipeline repo, `netrun-pipeline/`)

A 38-node batch ETL: extract from ChEMBL/CMS/OpenFDA → transform → load into DuckDB → emit a versioned bundle. Findings (detailed in its own pad):

- **Every one of its 43 graph edges carries the literal string `"done"`.** The netrun graph encodes *ordering only*. The real data dependencies (which node reads which file/table) live out-of-band in `paths.py` (a hand-maintained path registry) and in the DuckDB schema.
- netrun's caching is disabled; resume-after-failure is hand-rolled (`skip_if_done` variable + shell scripts). The only successful full build bypassed netrun entirely with a sequential driver script, to control disk/memory pressure.
- Its natural unit is *the artifact* (a parquet file, a DuckDB table). Its two production bugs were schema-contract violations at artifact boundaries — a bug class that wants declared dependencies and validation checks, which netrun has no primitives for.

**Requirement extracted:** the map file must be able to declare *durable artifacts* and who produces/consumes them — not just node ordering — so the data topology is in the file instead of in a shadow registry.

### 2b. AISI exposure index ([github.com/Autonomy-Data-Unit/aisi-exposure-index](https://github.com/Autonomy-Data-Unit/aisi-exposure-index))

An 18-node, 21-edge DAG matching ~30M job ads to O*NET occupations (embedding similarity → LLM filter → rerank → exposure scores → geographic aggregation). This is the *faithful* netrun consumer — it uses the machinery as intended:

- **Real data flows on edges, in memory.** `ad_ids` (numpy arrays / lists of ints) flows through the matching chain; a join node merges four score DataFrames. Output ports are named meaningfully and *renamed* across edges: node `llm_filter_candidates` has an output port `successful_ad_ids` which feeds the *input* port `ad_ids` of node `rerank_candidates`.
- **The netrun function contract**, which is the thing worth keeping verbatim: a node's code is a plain Python function like `async def main(ctx, print, ad_ids: list[int]) -> {'successful_ad_ids': list[int]}` — parameters (other than the injected `ctx`/`print`) are the input ports; the dict-shaped return annotation names the output ports. Node files import **zero** netrun.
- **Ordering-only links** are expressed with netrun's *signal/control* system: an edge from one node's auto-generated `__signal_epoch_finished__` output port to another's `__control_start_epoch__` input port means "start B after A finishes" with no data passed. A `broadcast` factory node fans one such signal out to 5 downstream nodes (netrun forbids one output port feeding multiple edges, so fanning out requires an explicit broadcast node).
- **Pools as concurrency groups:** netrun "pools" are worker pools that execute nodes. AISI defines `main` (4 threads) and `heavy` (1 thread) — assigning the GPU/LLM-heavy nodes to `heavy` serializes them. Global `retries: 3, retry_wait: 10` guards flaky API/HPC steps.
- **The composable run system** — the centerpiece of how the pipeline is actually operated: `config/run_defs.toml` has a `[defaults]` section plus eight named `[runs.<name>]` sections (`test_api`, `test_local`, `validation_5k`, … `production_5m`). Scalar values become global node variables; subtables named after a node become per-node overrides. Node code reads them as `ctx.vars["sample_n"]`. A 136-line runner script does the merge and drives the net.
- **And the decisive negative finding:** the config sets `max_epochs: 1` — every node fires exactly once per run. Even the most netrun-faithful pipeline is a batch DAG. No streaming, no salvo conditions (netrun's boolean fire-when-ports-are-ready predicates), no multiple epochs. Caching: disabled here too. Remote execution (Hetzner deployment, Slurm/sbatch on the Isambard HPC) is hand-rolled in scripts and inside node code — netrun's remote pools are unused.

**Requirements extracted:** named/renamable in-memory data passing between nodes; ordering-only dependencies; concurrency groups; retries; and the named-run parameterization system, kept essentially as-is.

### 2c. Joint conclusion

Between them, the two consumers use: declarative graph file, function binding by import path, named typed ports with real values, ordering-only links, concurrency limits, retries, per-node + global variables with named run presets, and durable artifacts. Neither uses: streaming, multiple epochs, salvo conditions, netrun caching, netrun remote execution, controls/signals beyond plain ordering, subgraphs. That is the feature line the wrapper draws.

---

## 3. Primer: the Dagster concepts the wrapper compiles onto

(Defining these explicitly since the whole design hangs on them.)

- **Asset** — Dagster's core unit: a named durable object (a table, a file, a model) plus the function that produces ("materializes") it and the list of assets it depends on. A **run** materializes a selected set of assets in dependency order.
- **Asset key** — the asset's name, possibly namespaced (e.g. `db/drugs`).
- **`@multi_asset`** — one function that produces several assets at once. (Our "node with several output ports" compiles to this.)
- **deps** — dependencies that impose ordering *without* passing a value into the function. (Our `after:` compiles to this.)
- **`AssetIn`** — declares that a function parameter receives the *value* of an upstream asset, and lets the parameter name differ from the asset key. (Our input wiring/renaming compiles to this.)
- **IO manager** — the pluggable component that stores a function's returned value and loads it back as a downstream function's argument. The default writes pickle files to a storage directory; in-memory when everything runs in one process. This is how "data on the edges" is transported without either node knowing about storage.
- **`RetryPolicy`** — per-asset retry configuration (max retries, delay, backoff).
- **Concurrency pools** — instance-level named limits: tag assets with a pool name, give the pool a max concurrency; Dagster's scheduler enforces it (e.g. `heavy = 1` serializes the GPU steps). *(Enforcement in local/library mode is a spike-verify item, §11.)*
- **Run config** — a validated per-run parameter structure passed at launch time. (Our variables and run presets compile to this.)
- **Asset check** — a validation function attached to an asset, executed after materialization; failures are loud, recorded, and visible in the UI. (The home for Scuttlebug's schema-contract class of bug.)
- **`Definitions`** — the bundle object a Dagster project exposes (all assets, checks, resources, jobs). Our compiler's output.
- **`materialize(...)` / `execute_in_process(...)`** — library-mode execution: run assets in the current process with no server, no daemon, no database. (What `dagnet run` uses.)
- **`dagster dev`** — one command that serves the local web UI (asset graph, run timeline, per-step logs, launchpad, re-execution) backed by SQLite in a `DAGSTER_HOME` directory.
- **Re-execution from failure** — relaunch a failed run such that already-successful steps are skipped. (This replaces Scuttlebug's hand-rolled `skip_if_done` resume.)
- **Executors** — how steps within a run are parallelized: `in_process` (serial, same process) or `multiprocess` (each step in its own subprocess — which also gives the per-node crash/memory isolation Scuttlebug hand-rolled with `run_node.py`).
- **Components / `defs.yaml`** — Dagster's YAML packaging system. Not needed for v1; later, a ~20-line Component class can point at our manifest so `dg`-based projects can load it natively.

---

## 4. The design at a glance

The wrapper is **a compiler and a validator, not a runtime**:

```
pipeline.toml  (the map: nodes, wiring, artifacts, pools, vars)
runs.toml      (named run presets: defaults + overrides)
        │
        │  dagnet check   — parse → validate (aggregated, located diagnostics)
        ▼
   Dagster Definitions  (assets, deps, retry policies, pools, checks, config schema)
        │
        │  dagnet run <run_name> [--select …]   — library-mode materialize, no server
        │  dagnet dev                           — dagster dev: web UI, run history, re-execution
        ▼
     node functions (plain Python, zero framework imports)
```

Explicit division of labor:

| Concern | Owner |
|---|---|
| The map file, its schema, its validation, signature↔file agreement | **wrapper** |
| Named run presets → run config | **wrapper** |
| Scheduling, parallelism, retries, value transport, run history, resume, UI, checks execution | **Dagster** |
| What each node actually does, incl. its own file/DB/HPC IO | **node code** |

The only Dagster-touching code in a consumer repo is one generated 3-line `defs.py` (`defs = dagnet.build("pipeline.toml")`) so that `dagster dev` has an entry point. Node code never imports Dagster *or* the wrapper.

---

## 5. The manifest (`pipeline.toml` / `pipeline.json`)

Format: **TOML or JSON** — one schema, loaded from either (msgspec parses both natively; run-def files likewise). Examples in this pad use TOML. One file, five sections. **The manifest is the authoritative declaration of every node's interface** — the code is checked against it, never the other way around. (This resolves old netrun's central flaw, where ports were *derived from* function signatures at runtime and therefore invisible in the file.)

### 5.1 `[pipeline]` — metadata

```toml
[pipeline]
name = "ai_index"
description = "Match job ads to O*NET occupations and compute AI exposure."
dagster_home = ".dagster"    # optional; where Dagster keeps instance state
store_root = "."             # optional; where file artifacts resolve
retries = { max = 3, wait_s = 10 }   # optional; the default retry policy
```

`dagster_home` (optional) sets where Dagster keeps its instance state (the SQLite run-history DB, event logs, concurrency-pool bookkeeping). The path is resolved **relative to the manifest file** (absolute paths allowed), defaulting to `.dagster/` next to the manifest. A `--dagster-home` CLI flag overrides it per invocation.

`retries` (optional) is the **pipeline-wide default retry policy**: every node inherits it. A node's own `retries` (§5.5) **replaces it entirely** — an override is the whole policy, not a field-wise merge, so reading a node's `retries` tells you what that node does without also having to read the pipeline header. Absent both here and on the node means no retry. `max` is a count of *extra* attempts, so `max = 0` and omitting `retries` are the same thing; a negative `max` or `wait_s` is a check error. *(Decided 2026-07-27: netrun set `retries: 3, retry_wait: 10` once at the top level for all 18 of AISI's nodes, and having no equivalent forced the same three lines to be repeated eight times — see `sample_projects/09_ai_index`.)*

`store_root` (optional) is **the store root** that §5.4's file-artifact paths are relative to. Same resolution rule — relative to the manifest file, absolute allowed — defaulting to `.`, i.e. the manifest's own directory. Precedence: `--store-root` CLI flag > this field > the default. *(Decided 2026-07-27: the map should carry its own store location rather than it existing only as a CLI flag, symmetric with `dagster_home`.)*

### 5.2 `[pools]` — named concurrency limits

```toml
[pools]
main = 4     # at most 4 nodes tagged 'main' run at once
heavy = 1    # serializes the GPU/LLM nodes
```

Compiles to Dagster concurrency pools. This is netrun's `pools` *reinterpreted*: netrun pools were worker pools that owned threads/processes (an execution mechanism); here a pool is purely a named limit (a scheduling constraint). Parallelism itself comes from the Dagster executor.

### 5.3 `[vars]` — declared variables

```toml
[vars]
run_name    = { type = "str" }
sample_n    = { type = "int", default = 1000 }
llm_model   = { type = "str" }
s3_prefix   = { type = "str", env = "ADZUNA_S3_PREFIX" }   # sourced from the environment
```

Every variable a node may read via `ctx.vars` must be declared here (global) or on a node (node-local, §5.5). Undeclared variable referenced in a runs file → check error. Unfilled non-default variable at launch → launch error. (netrun's `node_vars`, minus the `inherit` machinery: a node-level declaration with the same name simply overrides the value for that node.)

**`env`** (optional, on any declaration) names an environment variable to take the value from. The resolution order for one variable is:

> a run-supplied value  >  the named environment variable, if set  >  the declared `default`  >  **loud launch error naming both the variable and the environment variable that would satisfy it**

Three reasons for putting this on the *declaration* rather than in values: the manifest must **name** the environment variable so configuration stays discoverable from the map (no orphan knowledge in someone's shell profile); the environment is the right ambient source for secrets and per-machine paths, so run presets don't hard-code machine-specific values; and an explicit run value stays the most intentional signal, so it wins. Environment values are strings, so the declared type converts them, loudly on mismatch (`bool` accepts `true/false/1/0/yes/no/on/off`, case-insensitive).

A declaration with `env` and no `default` is *optional at check time* and *required at launch* — `dagnet check` must say the same thing on every machine, so it never consults the environment; the enforcement is the launch error above. Dropping the `default` is therefore how a variable becomes "must come from the environment or a run".

**Value-side interpolation is deliberately excluded.** There is no `${VAR}` inside runs files or manifest values: one mechanism, on the declaration side, where it is discoverable. *(Decided 2026-07-27. If a consumer genuinely needs value-side interpolation, that is a question to reopen, not a thing to add locally.)*

### 5.4 `[artifacts]` — declared durable objects (optional section)

```toml
[artifacts."openfda/drug_ndc"]
kind = "file"
path = "extracted/openfda_drug_ndc.json"     # relative to the store root

[artifacts."db/warehouse"]
kind = "file"
path = "warehouse.duckdb"

[artifacts."db/drugs"]
kind = "duckdb_table"
table = "drugs"
database = "db/warehouse"    # required; names a declared file artifact
```

An artifact is a durable thing with a *declared location*. This section replaces Scuttlebug's `paths.py`: locations are stated once, in the map, and both the producing node and any consuming node resolve them through the manifest. Artifact keys become Dagster asset keys, so the UI shows them with their materialization history. AISI-style pipelines can omit this section entirely.

`kind = "duckdb_table"` carries a **required** `database` field naming a declared `file`-kind artifact. `dagnet check` verifies the reference resolves and that its target really is a file. *(Decided 2026-07-27: without it, the database's location lives outside the manifest — which is the `paths.py` problem this section exists to fix, in miniature.)* Consequently `ctx.artifact(key)` returns:

- a `Path` for a `file` artifact — resolved under `store_root` (§5.1);
- a frozen handle with `.table: str` and `.database: Path` for a `duckdb_table` artifact.

The handle is frozen because a node resolving a location must not be able to move it.

### 5.5 `[nodes.*]` — the nodes

Full form, using real examples from both consumers:

```toml
# — AISI-style node: in-memory values in and out, renamed input, pool, node-local vars —
[nodes.rerank_candidates]
fn          = "ai_index.nodes.rerank_candidates.main"
description = "Cross-encoder reranking of filtered candidates."
inputs      = { ad_ids = "llm_filter_candidates.successful_ad_ids" }
outputs     = ["ad_ids"]
pool        = "heavy"
retries     = { max = 3, wait_s = 10 }

[nodes.rerank_candidates.vars]
chunk_size = { type = "int", default = 512 }

# — ordering-only dependency (replaces netrun's signal→control edges AND broadcast nodes) —
[nodes.score_presence]
fn      = "ai_index.nodes.score_presence.main"
after   = ["prepare_onet_targets"]
outputs = ["out"]

# — Scuttlebug-style node: consumes and produces declared artifacts —
[nodes.load_drugs]
fn        = "healthcare_pipeline.nodes.load_drugs.main"
inputs    = { drug_ndc = "openfda/drug_ndc" }   # receives the resolved Path
outputs   = ["drugs"]
artifacts = { drugs = "db/drugs" }              # output 'drugs' IS artifact db/drugs
pool      = "duckdb_writer"
checks    = { drugs = ["healthcare_pipeline.checks.drugs_vocab"] }
```

Field-by-field:

- **`fn`** — import path to the node function. (Same as netrun's `factory_args.func`; there are no factories anymore — `from_function` behavior *is* the only binding, and `broadcast`/`join` factory nodes are unnecessary: fan-out is native in Dagster, and a "join" is just a node with several inputs.)
- **`inputs`** — a table mapping *this function's parameter name* → a reference. A reference is either `<node>.<output>` (the value of another node's output, passed via IO manager) or an artifact key (the consuming function receives the resolved location — a `Path` for files, a table name for DuckDB tables). References must resolve to exactly one producer; ambiguity or no-producer is a check error. **There is no separate edges list** — an edge *is* an input referencing an output, so the redundancy that let Scuttlebug's 43 edges go contentless cannot exist.
- **`outputs`** — the node's named outputs. Declared here (in the file, authoritative), validated against the function's return annotation when one is present. Each output becomes a Dagster asset (key `node/output` by default, or the bound artifact key). The duplication between the manifest and the function signature is deliberate: the file must show every node's full interface without reading any Python, and `dagnet check` keeps the two in lockstep (§7b). Ports are never derived from code.
- **`artifacts`** — optional table mapping an output name → an artifact key from §5.4, meaning "this output is that durable artifact; the node writes it itself" (see contract, §7).
- **`after`** — list of node names: pure ordering, no data. Compiles to non-argument deps. Replaces the entire signal/control/broadcast apparatus (9 of AISI's 21 edges plus 2 factory nodes collapse into this one field). Use it when B genuinely must follow A. When two nodes are merely *mutually exclusive* — both write the same DuckDB file, say — with no reason for either to go first, the right tool is a `pool` of 1 (§5.2), not an invented ordering. Asserting an order the pipeline does not have is how netrun graphs drifted into carrying edges whose only payload was `"done"`.
- **`asset`** — boolean, default `true`. When `false`, the node's outputs are *transient*: they compile to Dagster ops with no asset identity — no catalog entry, no materialization history, no `checks`, not `--select`-able. Use it for very small nodes producing intermediates that nothing needs to resume from or validate. The compiler folds each op-node into the graph backing the nearest downstream asset node(s) (Dagster's "graph-backed assets") — that nesting is compile-time packaging, invisible here: the manifest stays a flat graph. Two consequences: an op-node must reach an asset node downstream via `inputs` (a transient chain nothing durable consumes is dead code → check error), and an op-node feeding *two* asset nodes merges them into one multi-asset that always materializes together (check-time warning).
- **`pool`**, **`description`**, **`group`** (optional UI grouping, e.g. `extract`/`transform`/`load`).
- **`retries`** — this node's retry policy, e.g. `{ max = 3, wait_s = 10 }`. Omitted, the node inherits `[pipeline].retries` (§5.1); given, it **replaces** that default entirely rather than merging field-by-field.
- **`checks`** — output name → list of check declarations, compiled to Dagster asset checks. Each entry is either a bare import path or the long form `{ fn = "...", blocking = false }`. **Checks block by default**: a failing blocking check stops the assets downstream of it and fails the run with a nonzero exit. `blocking = false` makes a check *advisory* — still executed, still recorded and visible in the UI (at WARN severity), but the run continues. *(Decided 2026-07-27: exiting 0 on a violated schema contract is precisely the silent failure this design's loud-errors principle forbids; advisory exists for things worth seeing that are not contract breaches.)*

  ```toml
  checks = { measurements = [
      "pkg.checks.units_are_canonical",                             # blocking
      { fn = "pkg.checks.row_count_is_usual", blocking = false },   # advisory
  ] }
  ```

### 5.6 What deliberately has no place in the manifest

Salvo conditions, port slot capacities, `max_epochs`, signals/controls, subgraphs, cache config, pool *specs* (thread/process/num_workers — pools are now just limits), dead-letter queues. See §8.

---

## 6. The runs file (`runs.toml`, or a `runs/` folder)

AISI's `run_defs.toml` semantics, kept as-is because they are exactly right, promoted from a hand-written merge script to a wrapper feature:

```toml
[defaults]
sample_n = 1000
llm_model = "qwen-0.5b"
[defaults.rerank_candidates]       # subtable named after a node = per-node override
chunk_size = 512

[runs.test_api]
sample_n = 10
llm_model = "gpt-5.2"

[runs.production_5m]
sample_n = 5000000
llm_model = "gemma-27b"
```

Rules, explicitly: `[defaults]` merged with `[runs.<name>]` (run wins); scalar keys set global variables; subtable keys must name a node and set that node's variables. Every key must match a declared variable (§5.3/§5.5) — unknown keys are check errors, not silently ignored. The merged result compiles to Dagster run config, so it is also visible/editable in the Dagster UI's launchpad when launching from there.

**Full precedence**, once node-local declarations and per-node overrides are all in play — highest first *(decided 2026-07-27)*:

1. the run's per-node override — `[runs.<run>.<node>]`
2. `[defaults]`' per-node override — `[defaults.<node>]`
3. the run's global value — `[runs.<run>]`
4. `[defaults]`' global value — `[defaults]`
5. the environment, via the governing declaration's `env` (§5.3), when that variable is set
6. the node-local declared default — `[nodes.<node>.vars]`
7. the global declared default — `[vars]`

That is: values set by a run always beat anything a declaration can supply; among values, more specific beats less specific and the run beats the defaults section; a declaration's environment source beats its own default, since the default is the fallback for when the environment is silent; among declarations, node-local beats global (§5.3). Levels 5 and 6/7 both come from the *governing* declaration for that node, so a node-local `env` shadows the global one.

`[defaults]` applies to a run launched with **no preset name** too — a run without a preset is not a run without config.

**Variable types are scalars only** for now — `str`, `int`, `float`, `bool`. Widen to typed lists (`list[str]`, `list[int]`) when a real consumer forces it, deliberately rather than preemptively *(decided 2026-07-27)*.

Run defs can also be a **folder**: point the wrapper at a `runs/` directory and every `*.toml`/`*.json` file in it is loaded and merged into one run registry (a duplicate run name across files is a loud check error). The underlying API is `load_runs(path)`, callable repeatedly and accumulating — the folder form is just a loop over it, and a consumer can load several sources (e.g. a checked-in `runs/` plus a local scratch file).

---

## 7. The node function contract

A node is a plain Python function, sync or async, with **zero framework imports**:

```python
# netrun (AISI today):
async def main(ctx, print, ad_ids: list[int]) -> {'successful_ad_ids': list[int]}: ...

# wrapper (one mechanical change: the injected print parameter is gone):
async def main(ctx, ad_ids: list[int]) -> {'successful_ad_ids': list[int]}: ...
```

Explicit rules:

1. **Parameters.** `ctx` first, then one parameter per declared input, names matching the manifest's `inputs` keys exactly. Mismatch (missing/extra/renamed parameter) → `dagnet check` error naming both sides. An input wired to a `node.output` reference receives that value; an input wired to an artifact receives its resolved location.
2. **Return.** A dict with one entry per *value* output. Outputs bound to artifacts are **not** returned — the node writes the artifact itself (to `ctx.artifact("db/drugs")`-resolved location), and the wrapper records the materialization. A node with only artifact outputs returns nothing. The netrun dict-shaped return annotation is kept as *optional, validated documentation*: if present it must match the declared outputs.
3. **`ctx`** is a small wrapper-owned object (not Dagster's context — nodes stay framework-agnostic): `ctx.vars` (resolved variables: globals + this node's overrides), `ctx.artifact(key)` (resolved location of any declared artifact), `ctx.run_name`, `ctx.node_name` (this node's manifest name), `ctx.manifest_path` (absolute path of the manifest this pipeline was compiled from). The last two are read-only properties and carry no semantics of their own; they exist so a library helper a node opts into can answer "which node am I, and where is the map?" without cwd-relative guessing — the working directory is not stable across executors, since each step of a multiprocess run is its own process. *(Added 2026-07-27 at the request of `dagnet-db`; see §12.)*
4. **Printing/logging.** netrun injected a special `print`; that's gone. Use normal `print` — Dagster captures stdout/stderr per step into the run's logs, viewable in the UI.
5. **nblite is untouched.** `fn` points at the exported function in `src/`; whether it was authored as a `.pct.py` notebook is invisible to the wrapper.
6. **Check functions** (§5.5) follow the same shape *(decided 2026-07-27)*:

   ```python
   def units_are_canonical(ctx, measurements):
       return {"passed": bool(...), "metadata": {...}}    # or just a bool
   ```

   `ctx` first, then the **subject**: the asset's loaded value for a normal output, or the resolved artifact location for an artifact-bound one (there is no value to load). Returning a bool or a `{"passed": bool, "metadata": {...}}` dict are the only accepted shapes — anything else raises `CheckReturnError`. Raising from the check counts as a failure. Check functions import no framework either.

## 7b. Validation (`dagnet check`) — the loud-errors inheritance

The one piece of netrun's rewrite-identity kept beyond the file itself: **aggregated, located diagnostics** — report *all* problems at once, each pointing at its manifest location, rather than Dagster's fail-on-first-exception style. The checklist:

- every `inputs` reference resolves to exactly one existing `node.output` or artifact key;
- the dependency graph (through `inputs` *and* `after`) is acyclic;
- every `fn` imports; parameters match declared inputs; return annotation (if present) matches declared outputs;
- `pool` names exist in `[pools]`; check/`fn` import paths exist; artifact keys are unique; artifact-bound outputs exist on their node;
- every `asset = false` node reaches a downstream asset node through `inputs` references (transient work that no durable output consumes is dead code); an op-node shared by two asset nodes triggers the merge warning (§5.5);
- runs files: every run key matches a declared variable / node; types match declarations; no duplicate run names across files;
- when both sides carry type annotations, wired output↔input annotations are compared (equal-or-warn — cheap static type compatibility Dagster itself doesn't do until runtime);
- a `duckdb_table` artifact's `database` resolves to a declared artifact, and that artifact is a file (§5.4);
- a variable's `env` is a usable environment-variable name, and a declared `default` matches its declared type (§5.3);
- retry counts and delays are non-negative, on `[pipeline]` and on every node (§5.1);
- node, output, artifact-key-component and run names are plain identifiers (`[A-Za-z0-9_]+`), since they become Dagster op/output/job names — better as our located diagnostic than as a Dagster traceback;
- every node declares at least one output. A node with none cannot compile to a multi-asset. *(If a real pipeline surfaces a genuine pure-side-effect terminal node, that is a design question to reopen — not a reason to invent a token output, which is the netrun `"done"` disease returning.)*
- a run preset that leaves a declared no-default variable unset is reported **at check time**, per run, as well as being a launch error (§5.3).

All decided 2026-07-27.

Dagster's own definition-time validation (which runs when we build `Definitions`) remains as a backstop, but `dagnet check` should catch everything first, in our vocabulary, with our locations.

---

## 8. Compilation to Dagster — the explicit mapping

| Manifest concept | Compiles to | Notes |
|---|---|---|
| node | one multi-asset definition (built programmatically, no decorators) | 1:1 naming so the Dagster UI graph reads like the manifest |
| output | asset (key `node/output` or bound artifact key) | value outputs stored/loaded by the IO manager; artifact outputs recorded as materializations of the declared location |
| `inputs` (value ref) | `AssetIn` with key mapping | handles the renaming case (`successful_ad_ids` → param `ad_ids`) |
| `inputs` (artifact ref) | dep + location injection by the wrapper | function receives `Path`/table name |
| `after` | non-argument deps | ordering only |
| `asset = false` node | op(s) inside the graph backing the downstream asset(s) ("graph-backed asset") | transient plumbing: no catalog entry, no history, no checks, not selectable; nested one level down in the UI |
| `pool` | concurrency pool tag; `[pools]` limits set on the instance | spike-verify enforcement in library mode |
| `retries` | `RetryPolicy(max_retries, delay)` | |
| `checks` | asset checks | the PND/umol schema-contract bug class gets a declared, UI-visible home |
| `[vars]` + runs file | run config schema + per-run config values | `ctx.vars` is backed by it |
| `group`, `description` | asset group + metadata | UI organization |

Execution surfaces, explicitly:

- **`dagnet run <run_name> [--select expr]`** — resolves the run preset, builds `Definitions`, calls `materialize(...)` in-process (or with the multiprocess executor via a job — default: multiprocess, since both consumers want parallel steps and process isolation). `--select` passes through Dagster's asset-selection syntax, which is the pull model: e.g. `--select "+db/drugs"` = "materialize `db/drugs` and everything upstream of it" (netrun's `run_to_targets`, natively). A `--ephemeral` flag runs with no instance state at all — Dagster's native default when no `DAGSTER_HOME` is set — leaving no history behind (for CI). **Spike (a) found this is stronger than a caveat:** an ephemeral instance cannot use the multiprocess executor at all (Dagster raises `DagsterUnmetExecutorRequirementsError`) and reports `supports_global_concurrency_limits = False`. So `--ephemeral` implies **in-process** execution, and `[pools]` limits are inert in that mode. dagnet prints a warning naming them; it is deliberately **not** an error, since the mode is an explicit opt-in and erroring would make it unusable for any pooled pipeline *(decided 2026-07-27)*.

Pool *limits* are instance state, not definition state: `pool = "heavy"` on a node is only a tag, so `dagnet run`/`dagnet dev` write `[pools]` onto the instance on every invocation (`set_concurrency_slots`), with `concurrency.pools.granularity: op` so limits apply per step rather than per run. Without that sync, editing a limit in the manifest would silently do nothing.
- **`dagnet dev`** — wraps `dagster dev` pointed at the generated `defs.py`, with `DAGSTER_HOME` set per the manifest's `dagster_home` field (default: `.dagster/` next to the manifest file — §5.1). This is where run history, log browsing, the launchpad, and **re-execution from failure** live — the latter replacing `skip_if_done` / `resume_on_quota.sh` / `from_raw.py` entirely.
- **`dagnet run <run_name> --from-failure <run_id|last>`** — resume a failed run, skipping the steps that already succeeded. Spike (e) confirmed this works from library mode and reaches individual ops inside graph-backed assets, so it is what replaces Scuttlebug's hand-rolled `skip_if_done` *(added 2026-07-27; §8 previously put re-execution only in `dagnet dev`)*.
- **`dagnet graph`** — Mermaid export from the manifest (no Dagster needed), for READMEs.

The run-preset name is **optional**: `dagnet run` with no preset uses the declared defaults, so a project with no runs file works. `ctx.run_name` is `""` in that case *(decided 2026-07-27)*.

Multiprocess execution needs one piece of plumbing DESIGN did not anticipate: `materialize(...)` and `execute_in_process(...)` always run in-process whatever executor a job names, so real multiprocess requires `execute_job` with a *reconstructable* job. Since dagnet's job is built at runtime from a manifest path, a module-level reconstructor entry point (`dagnet/_reconstruct.py`) rebuilds it in each step subprocess from JSON-serializable arguments (spike (b)).

---

## 9. What is deliberately dropped, with the evidence

| netrun feature | Fate | Evidence |
|---|---|---|
| Packets, salvo conditions, multiple epochs, streaming/open mode, backpressure | **dropped** | both consumers run `max_epochs: 1` — strict batch |
| Signals/controls, broadcast + join factory nodes | **absorbed** into `after` + multi-input nodes | AISI's only use of them was plain ordering + fan-out |
| Pools as worker pools (thread/process/remote, RPC, ExecutionManager) | **dropped**; pools survive as pure concurrency limits; parallelism = Dagster executors | AISI's `heavy: 1` is a limit, not an execution strategy; remote execution was hand-rolled in both repos anyway |
| Caching / file-storage plugins | **dropped** for v1; resume = Dagster re-execution from failure | cache disabled in both consumers |
| Subgraphs, recipes, actions, `node_vars` inherit flags, dead-letter queues | **dropped** | unused by both consumers |
| netrun-ui / netrun-sim / dashboard | **dropped**; monitoring = Dagster UI; the manifest is edited as text | Scuttlebug pad: declared but never imported |
| The declarative map file, loud aggregated validation, function-binding contract, named-run parameterization | **kept — this is the product** | the parts both consumers lean on daily |

## 10. Name and home

It is no longer a runtime, so a new name is honest. **Leading candidate: `dagnet`** (Lukas's suggestion) — free on PyPI as of 2026-07-26. Also checked: `plat` is **taken**; `pipemap` and `dagmap` are free. CLI reads well as `dagnet check` / `dagnet run production_5m` / `dagnet dev`.

Home: its own small repo/box from the start (it has two target consumers immediately), plain `src/` Python — no nblite. The netrun repo's `rewrite` branch is superseded if this lands; the netrun repo then either hosts this under the new name or is archived (decide at the end, not now).

## 11. Build plan

1. **v0 of the package** (~500–800 lines): manifest + runs schema (msgspec), `check` with aggregated diagnostics, the compiler to `Definitions` (staged internally: the all-assets path first, then the `asset = false` partitioner), `run`/`dev`/`graph` CLI incl. `--ephemeral`. Includes the **spike-verify items**: (a) concurrency-pool enforcement under `dagnet run` (library mode / local instance — may need `DAGSTER_HOME` even for CLI runs); (b) async node functions under the multiprocess executor; (c) value passing of numpy arrays / DataFrames via the default pickle IO manager at AISI scale; (d) `AssetIn`-style renaming ergonomics; (e) re-execution-from-failure granularity for ops inside graph-backed assets.
   **Done 2026-07-27.** All five spike items verified before anything was built on them (`_dev/experiments/FINDINGS.md`); nine sample projects run end to end.
2. ~~**Port AISI**~~ — **dropped 2026-07-27.** The real port would have cost model downloads, HPC access and API keys for validation that is fundamentally *structural*. Replaced by `sample_projects/09_ai_index`: a topologically faithful, stub-bodied replica derived from the public `config/netrun.json` and `config/run_defs.toml` — the same 18 nodes, the same port renames, the signal/control/broadcast edges collapsed into `after`, the join node as a plain multi-input node, `heavy = 1`, retries, and the real run-preset structure with dummy URLs and model names. It keeps the "every feature composed in one realistic pipeline" validation and none of the weight — and it earned its keep immediately, surfacing the two schema gaps that became decisions 12 and 13 (§12).
3. **Port Scuttlebug** per its own pad's migration plan, with the wrapper as the declarative layer: the coupling audit's producer→consumer map becomes `inputs`/`artifacts` (this exercises the artifact half of the schema and replaces `paths.py`); add checks for the PND/umol class; full from-raw build; `check_reconcile.py` D1–D10 as the oracle.
4. **Then**: extract a `Component` shim for `defs.yaml`/`dg` interop if wanted; write the agent-facing skill/doc (small — the manifest schema + this pad's contract section is most of it); decide the netrun repo's fate.

## 12. Open decisions

- [P] Manifest format: **TOML and JSON**, one schema — decided 2026-07-26.
- [P] Schema library: **msgspec** — decided 2026-07-26.
- [+] Name: leaning **dagnet** (free on PyPI as of 2026-07-26; `plat` is taken) — confirm and create the box/repo.
- [P] Compile target: **assets by default, plus per-node `asset = false` for transient intermediates, both from the beginning** — decided 2026-07-26. Assets give pull semantics (`--select "+key"` = `run_to_targets`), re-execution-from-failure + per-output materialization history, asset checks, and the lineage/catalog UI; `asset = false` nodes compile to ops folded into the graphs backing their downstream assets, while the manifest stays a flat graph (§5.5). Built staged inside build-plan step 1: all-assets path first, then the partitioner. (Full pros/cons and mechanics: the design discussion.)
- [P] Scuttlebug sequencing: **build the wrapper first, then port Scuttlebug onto it** — decided 2026-07-26 (supersedes the plain-Dagster-first option mentioned in the Scuttlebug pad).
- [P] Dagster state location: `dagster_home` in `[pipeline]`, resolved **relative to the manifest file**, default `.dagster/` next to it — decided 2026-07-26 (§5.1).
- [P] Ephemeral CI mode: **include `--ephemeral`** — decided 2026-07-26. Trivial: a stateless in-memory instance is Dagster's native default when no `DAGSTER_HOME` is set. Caveat: pool limits may not be enforced in that mode (spike item (a)).

### Decided 2026-07-27, after building v0

Raised in `_dev/OPEN_QUESTIONS.md` while implementing §11 step 1; each is now folded into the section above that owns it.

- [P] **Store root**: `[pipeline] store_root`, resolved relative to the manifest, default `"."`; `--store-root` overrides (§5.1).
- [P] **DuckDB database in the schema**: `duckdb_table` gains a required `database` naming a declared file artifact; `ctx.artifact()` returns a frozen `.table`/`.database` handle (§5.4).
- [P] **Checks block by default**: a failing check fails the run; `{ fn = "...", blocking = false }` opts out to advisory (§5.5).
- [P] **Check-function contract**: `(ctx, subject) -> bool | {"passed", "metadata"}` (§7 rule 6).
- [P] **Variable precedence**: the six-level order in §6.
- [P] **`--ephemeral` + pools**: warn, do not error (§8).
- [P] **Six v0 additions accepted**: optional run name, `--from-failure`, check-time `unfilled-var`, duplicate-`[defaults]`-key error, at-least-one-output, identifier-only names (§7b, §8).
- [P] **Variables stay scalar** until a real consumer forces typed lists (§6).
- [P] **Diagnostic locations** stay dotted logical paths (`pipeline.toml:nodes.rerank.inputs.ad_ids`); the `line` field stays dormant. Neither `tomllib`/`msgspec` nor `json` reports source positions, so real line numbers need a separate position-aware parse.
- [P] **Dict-shaped return annotation kept as-is**, and kept optional (§7 rule 2). It puts string literals in annotation position, which type-aware linters read as forward references to types (33 × ruff `F821` across the sample corpus); the answer is a documented per-file ignore, or omitting the annotation, not a new form.
- [P] **Size**: v0 is ~1,635 lines of code against the §1 estimate of 500–800, concentrated in `check.py` (the diagnostic quality *is* the product) and `compile.py`. Accepted; `compile.py` crossing ~1k lines is a checkpoint to raise.

### Decided 2026-07-27, from what sample 09 could not express

Building `sample_projects/09_ai_index` — a topologically faithful replica of the AISI pipeline — surfaced exactly two things the schema could not say. Both are now in it, completing v0's schema before any consumer port.

- [P] **Global retries default**: `[pipeline] retries` (§5.1). A node's own `retries` replaces it **entirely**, with no field-wise merging — an override is the whole policy. No retries anywhere means no retry. Evidence: netrun's one top-level `retries: 3, retry_wait: 10` covered all 18 AISI nodes, and without an equivalent the replica repeated the same three lines eight times.
- [P] **Environment-sourced variables**: an optional `env = "NAME"` on any variable declaration (§5.3), with the resolution order run value > environment > declared default > loud launch error naming both. Declaration-side only — **no** value-side `${VAR}` interpolation in runs files or manifest values. Evidence: netrun wrote `{ "$env": "ADZUNA_S3_PREFIX" }`, and dagnet's only alternative was for node code to read `os.environ` itself, which puts configuration back outside the map.

### Decided 2026-07-27, requested by `dagnet-db`

`dagnet-db` is a sibling package built against dagnet as a git dependency, providing helpers a node opts into (its `init`/`connect`).

- [P] **`ctx.node_name` and `ctx.manifest_path`** (§7 rule 3): two read-only properties on the node context. A helper called from inside a node body needs to know which node is calling it and where the map is; the manifest is a pipeline's discovery root, so exposing its path is aligned with the rest of the design, and resolving anything cwd-relative would be fragile — a multiprocess step runs in its own process. `manifest_path` is absolute, resolved once at compile time. Pure additions: no semantic change, nothing existing altered, no version-scheme change.

### Not being done

- [D] **Porting the AISI exposure index** (§11 step 2) — dropped 2026-07-27. Its structural validation is preserved instead by `sample_projects/09_ai_index`, a topologically faithful, stub-bodied replica derived from the public `config/netrun.json`, which exercises every dagnet feature at realistic scale without model downloads, HPC or API keys.
