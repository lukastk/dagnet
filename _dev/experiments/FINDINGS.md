# Spike findings — DESIGN §11 items (a)–(e)

*Run 2026-07-26 against dagster 1.13.14 / Python 3.14.6. Scripts in this folder are
throwaway; this file is the durable output.*

Headline: **all five assumptions hold**, with two consequences that change the CLI
design (§(a)) and one that confirms an aggressive design choice was safe (§(e)).

---

## (a) Concurrency-pool enforcement under library-mode runs — `spike_a_pools.py`

Six nodes, three tagged pool `heavy` (limit 1) and three tagged `main` (limit 4),
multiprocess executor with `max_concurrent: 6` so only a pool limit can serialize
them. Each records its wall-clock span; max simultaneous spans is the measurement.

| instance mode | `supports_global_concurrency_limits` | heavy (limit 1) | main (limit 4) | run |
|---|---|---|---|---|
| persistent (`DAGSTER_HOME` dir with `dagster.yaml`) | `True` | max 1 simultaneous — **enforced** | 3 simultaneous | success |
| ephemeral (`DagsterInstance.ephemeral()`) | `False` | not enforced | — | **fails to start** |

**Findings:**

1. **Pools ARE enforced in library mode** — but only with a *real instance directory*.
   `DAGSTER_HOME` is therefore required for `dagnet run` whenever the manifest declares
   `[pools]`, exactly as §5.1 anticipated. Confirms the design.
2. **Pool limits are instance state, not definition state.** `pool="heavy"` on the
   asset is only a tag; the *limit* lives in the instance and must be written there by
   dagnet before executing: `instance.event_log_storage.set_concurrency_slots(name, limit)`.
   (`dagster.yaml`'s `concurrency.pools.default_limit` is only a global default for
   pools that have no explicit limit.) So `dagnet run`/`dagnet dev` must **sync `[pools]`
   into the instance** on every invocation — otherwise a limit change in the manifest
   silently does nothing. Granularity must be `op` (`concurrency: {pools: {granularity: op}}`)
   for per-step limits rather than per-run.
3. **`--ephemeral` cannot use the multiprocess executor at all.** Not just "pool limits
   may not be enforced" (the DESIGN §8 caveat) — Dagster hard-errors:
   `DagsterUnmetExecutorRequirementsError: You have attempted to use an executor that uses
   multiple processes with an ephemeral DagsterInstance`. Verified that ephemeral +
   `in_process_executor` works fine.
   → **`--ephemeral` must imply `in_process` execution**, and must warn (or error) if the
   manifest declares `[pools]`, since those limits become no-ops. *Open question for
   Lukas — see status.*

## (b) Async node functions under the multiprocess executor — `spike_b_multiprocess.py`

Works. `async def` node bodies wrapped with `asyncio.run(...)` inside the compiled op
execute correctly; `slow_a` and `slow_b` ran **concurrently in distinct subprocesses**
(PIDs 3240577 / 3240578, both `STEP_START` at the same second), so the multiprocess
executor genuinely parallelizes and the asyncio event loop per subprocess is fine.

**The important discovery is how you get multiprocess at all:** `materialize(...)` and
`job.execute_in_process(...)` *always* run in-process regardless of the job's
`executor_def`. Real multiprocess requires `dagster.execute_job(<ReconstructableJob>, ...)`,
because step subprocesses must rebuild the job definition from scratch.

Since dagnet's job is built at runtime from a manifest path, the reconstructor is:

```python
build_reconstructable_job("dagnet._reconstruct", "job_from_manifest",
                          reconstructable_kwargs={"manifest": ..., "run_name": ..., "select": ...})
```

Args must be JSON-serializable — manifest path + run name + selection string are, so this
is a clean fit. Verified end to end. **dagnet needs a module-level reconstructor entry point**
(a small `dagnet/_reconstruct.py`); this was not in DESIGN and is the one piece of
plumbing the multiprocess default requires.

## (c) numpy / DataFrame transport via the default pickle IO manager — `spike_cd_assetin_values.py`

Works, and is fast enough at AISI scale.

- 10k int64 array → DataFrame → scalar, across three multi-assets: fine.
- Scale test: **30M-element int64 array (240 MB) plus a 5M-row 2-column DataFrame,
  full round-trip through the default `fs_io_manager`: 1.5 s, 321 MB on disk.**

No custom IO manager needed for v0; the default pickle manager covers both consumers'
value-passing. (numpy/pandas are not dagnet dependencies — they were `uv run --with`'d
for the spike only.)

## (d) `AssetIn`-style input renaming — `spike_cd_assetin_values.py`

Works, and the whole compiler shape it implies is confirmed:

- `dg.multi_asset(name=..., outs=..., ins=..., can_subset=...)(body)` applied
  **as a plain function call, not decorator syntax** — so nodes can be built in a loop
  from the manifest (DESIGN §8's "built programmatically, no decorators").
- `outs={out_name: dg.AssetOut(key=[node, out])}` gives the `node/output` asset keys.
- `ins={param: dg.AssetIn(key=AssetKey([...]))}` does the netrun renaming case exactly:
  upstream asset `produce/successful_ad_ids` arrives as parameter `ad_ids`.
- Dagster introspects the compute function's **signature**, so the generated wrapper must
  carry a synthetic `__signature__` with one parameter per declared input. `**kwargs` alone
  is not enough. (Cheap, but a real requirement on the compiler.)

## (e) Re-execution-from-failure granularity inside graph-backed assets — `spike_e_reexec_graph_asset.py`

**Op-level. The best possible outcome.**

Shape: asset `head/out` → graph-backed asset `tail/out` whose graph is `op1 → op2 → op3`;
`op3` fails on the first attempt only.

```
run 1 (fails):  steps = ['head_op', 'tail_graph.op1', 'tail_graph.op2', 'tail_graph.op3']
run 2 (from failure): steps = ['tail_graph.op3']          <- only the failed op re-ran
```

So `asset = false` nodes (DESIGN §5.5) do **not** cost resume granularity: folding them
into a downstream graph-backed asset still lets re-execution-from-failure restart at the
individual op. The `--from-failure` story survives the op/asset partition intact.

Mechanism used: `dg.ReexecutionOptions.from_failure(run_id, instance)` passed to
`dg.execute_job` — available from library mode, no UI needed, so `dagnet run --from-failure`
is implementable (worth adding; DESIGN §8 only mentions re-execution via `dagnet dev`).

---

## Consequences for the build

1. `dagnet/_reconstruct.py` — module-level `job_from_manifest(**json_args)` entry point,
   required for the multiprocess default. (New; not in DESIGN.)
2. Pool limits must be **synced into the instance** at run/dev time, with `granularity: op`.
3. `--ephemeral` ⇒ `in_process` executor, and pools declared in the manifest are inert
   in that mode. Needs a decision on warn-vs-error.
4. Compiled node wrappers need a synthetic `__signature__`.
5. `asset = false` is safe to build as designed — no resume-granularity penalty.

---

## (f) Artifact outputs and variables-as-run-config — `spike_f_artifacts_and_config.py`

Two mechanics DESIGN §8 names but doesn't pin down, both confirmed:

1. **An artifact-bound output declares `AssetOut(dagster_type=Nothing)` and yields
   `Output(None, name)`.** The node wrote the artifact itself, so nothing crosses
   the IO manager — and the asset is *still* materialized (`openfda/drug_ndc`
   appeared in the materialization events), so it keeps its catalog entry and
   history. Its declared location rides along as asset metadata.
2. **A consumer takes `deps=[<artifact asset key>]`** for ordering and receives the
   resolved `Path` from dagnet, not from Dagster.
3. **Per-node `config_schema` plus a `ConfigurableResource` both reach the body**
   (`sample_n=42` from `ops.load.config`, `run_name='test_api'` from the resource),
   and a required config field left unset **fails loudly at launch** with a
   Dagster error naming the missing entry — no silent None. That is what makes
   `is_required=True` for a variable with no declared default the right choice.

## (g) Graph-backed assets, built programmatically — `spike_g_graph_backed.py`

For the `asset = false` partitioner. Three findings:

1. Ops built by calling `dg.op(...)` as a function, wired inside a `dg.graph(...)`
   body that is a closure over a topological order, work — same synthetic
   `__signature__` requirement as multi-assets.
2. **`AssetsDefinition.from_graph` has no `deps=` parameter.** Ordering-only
   dependencies must therefore arrive as `Nothing`-typed *graph inputs* mapped to
   the upstream asset key via `keys_by_input_name`. And a `Nothing` input must
   **not** appear as a parameter of the op function — Dagster rejects it
   explicitly ("no data will be passed for it") — it is declared in `ins` and
   supplied only when the op is invoked inside the graph.
3. A multi-output op invoked in a graph body unpacks as a tuple in declaration
   order.

Steps come out as `sink_graph.op_double`, `sink_graph.op_split`, `sink_graph.sink`
— separate steps nested one level, exactly as §5.5 describes, and spike (e)
already proved re-execution reaches them individually.
