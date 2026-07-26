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

### 2a. Scuttlebug healthcare (box `20260531_55mxq1`, `netrun-pipeline/`)

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
```

`dagster_home` (optional) sets where Dagster keeps its instance state (the SQLite run-history DB, event logs, concurrency-pool bookkeeping). The path is resolved **relative to the manifest file** (absolute paths allowed), defaulting to `.dagster/` next to the manifest. A `--dagster-home` CLI flag overrides it per invocation.

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
run_name  = { type = "str" }
sample_n  = { type = "int", default = 1000 }
llm_model = { type = "str" }
```

Every variable a node may read via `ctx.vars` must be declared here (global) or on a node (node-local, §5.5). Undeclared variable referenced in a runs file → check error. Unfilled non-default variable at launch → launch error. (netrun's `node_vars`, minus the `inherit` machinery: a node-level declaration with the same name simply overrides the value for that node.)

### 5.4 `[artifacts]` — declared durable objects (optional section)

```toml
[artifacts."openfda/drug_ndc"]
kind = "file"
path = "extracted/openfda_drug_ndc.json"     # relative to the store root

[artifacts."db/drugs"]
kind = "duckdb_table"
table = "drugs"
```

An artifact is a durable thing with a *declared location*. This section replaces Scuttlebug's `paths.py`: locations are stated once, in the map, and both the producing node and any consuming node resolve them through the manifest. Artifact keys become Dagster asset keys, so the UI shows them with their materialization history. AISI-style pipelines can omit this section entirely.

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
- **`after`** — list of node names: pure ordering, no data. Compiles to non-argument deps. Replaces the entire signal/control/broadcast apparatus (9 of AISI's 21 edges plus 2 factory nodes collapse into this one field).
- **`asset`** — boolean, default `true`. When `false`, the node's outputs are *transient*: they compile to Dagster ops with no asset identity — no catalog entry, no materialization history, no `checks`, not `--select`-able. Use it for very small nodes producing intermediates that nothing needs to resume from or validate. The compiler folds each op-node into the graph backing the nearest downstream asset node(s) (Dagster's "graph-backed assets") — that nesting is compile-time packaging, invisible here: the manifest stays a flat graph. Two consequences: an op-node must reach an asset node downstream via `inputs` (a transient chain nothing durable consumes is dead code → check error), and an op-node feeding *two* asset nodes merges them into one multi-asset that always materializes together (check-time warning).
- **`pool`**, **`retries`**, **`description`**, **`group`** (optional UI grouping, e.g. `extract`/`transform`/`load`), **`checks`** (output name → list of check-function import paths, compiled to Dagster asset checks).

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
3. **`ctx`** is a small wrapper-owned object (not Dagster's context — nodes stay framework-agnostic): `ctx.vars` (resolved variables: globals + this node's overrides), `ctx.artifact(key)` (resolved location of any declared artifact), `ctx.run_name`.
4. **Printing/logging.** netrun injected a special `print`; that's gone. Use normal `print` — Dagster captures stdout/stderr per step into the run's logs, viewable in the UI.
5. **nblite is untouched.** `fn` points at the exported function in `src/`; whether it was authored as a `.pct.py` notebook is invisible to the wrapper.

## 7b. Validation (`dagnet check`) — the loud-errors inheritance

The one piece of netrun's rewrite-identity kept beyond the file itself: **aggregated, located diagnostics** — report *all* problems at once, each pointing at its manifest location, rather than Dagster's fail-on-first-exception style. The checklist:

- every `inputs` reference resolves to exactly one existing `node.output` or artifact key;
- the dependency graph (through `inputs` *and* `after`) is acyclic;
- every `fn` imports; parameters match declared inputs; return annotation (if present) matches declared outputs;
- `pool` names exist in `[pools]`; check/`fn` import paths exist; artifact keys are unique; artifact-bound outputs exist on their node;
- every `asset = false` node reaches a downstream asset node through `inputs` references (transient work that no durable output consumes is dead code); an op-node shared by two asset nodes triggers the merge warning (§5.5);
- runs files: every run key matches a declared variable / node; types match declarations; no duplicate run names across files;
- when both sides carry type annotations, wired output↔input annotations are compared (equal-or-warn — cheap static type compatibility Dagster itself doesn't do until runtime).

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

- **`dagnet run <run_name> [--select expr]`** — resolves the run preset, builds `Definitions`, calls `materialize(...)` in-process (or with the multiprocess executor via a job — default: multiprocess, since both consumers want parallel steps and process isolation). `--select` passes through Dagster's asset-selection syntax, which is the pull model: e.g. `--select "+db/drugs"` = "materialize `db/drugs` and everything upstream of it" (netrun's `run_to_targets`, natively). A `--ephemeral` flag runs with no instance state at all — Dagster's native default when no `DAGSTER_HOME` is set — leaving no history behind (for CI); caveat: pool limits may not be enforced in that mode (spike item (a)).
- **`dagnet dev`** — wraps `dagster dev` pointed at the generated `defs.py`, with `DAGSTER_HOME` set per the manifest's `dagster_home` field (default: `.dagster/` next to the manifest file — §5.1). This is where run history, log browsing, the launchpad, and **re-execution from failure** live — the latter replacing `skip_if_done` / `resume_on_quota.sh` / `from_raw.py` entirely.
- **`dagnet graph`** — Mermaid export from the manifest (no Dagster needed), for READMEs.

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
2. **Port AISI** (the faithful consumer, no rearchitecting needed): generate `pipeline.toml` mechanically from its `netrun.json` (18 nodes; signal/control/broadcast edges → `after`), copy `run_defs.toml` nearly verbatim, drop the `print` parameter from node signatures, delete the 136-line runner. Validate with its `test_api` / `test_local` runs.
3. **Port Scuttlebug** per its own pad's migration plan, with the wrapper as the declarative layer: the coupling audit's producer→consumer map becomes `inputs`/`artifacts` (this exercises the artifact half of the schema and replaces `paths.py`); add checks for the PND/umol class; full from-raw build; `check_reconcile.py` D1–D10 as the oracle.
4. **Then**: extract a `Component` shim for `defs.yaml`/`dg` interop if wanted; write the agent-facing skill/doc (small — the manifest schema + this pad's contract section is most of it); decide the netrun repo's fate.

## 12. Open decisions

- [P] Manifest format: **TOML and JSON**, one schema — decided 2026-07-26.
- [P] Schema library: **msgspec** — decided 2026-07-26.
- [+] Name: leaning **dagnet** (free on PyPI as of 2026-07-26; `plat` is taken) — confirm and create the box/repo.
- [P] Compile target: **assets by default, plus per-node `asset = false` for transient intermediates, both from the beginning** — decided 2026-07-26. Assets give pull semantics (`--select "+key"` = `run_to_targets`), re-execution-from-failure + per-output materialization history, asset checks, and the lineage/catalog UI; `asset = false` nodes compile to ops folded into the graphs backing their downstream assets, while the manifest stays a flat graph (§5.5). Built staged inside build-plan step 1: all-assets path first, then the partitioner. (Full pros/cons and mechanics: thread `ti34`.)
- [P] Scuttlebug sequencing: **build the wrapper first, then port Scuttlebug onto it** — decided 2026-07-26 (supersedes the plain-Dagster-first option mentioned in the Scuttlebug pad).
- [P] Dagster state location: `dagster_home` in `[pipeline]`, resolved **relative to the manifest file**, default `.dagster/` next to it — decided 2026-07-26 (§5.1).
- [P] Ephemeral CI mode: **include `--ephemeral`** — decided 2026-07-26. Trivial: a stateless in-memory instance is Dagster's native default when no `DAGSTER_HOME` is set. Caveat: pool limits may not be enforced in that mode (spike item (a)).
