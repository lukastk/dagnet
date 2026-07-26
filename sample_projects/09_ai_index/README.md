# 09_ai_index — a whole real pipeline, in one map file

A **structurally faithful, stub-bodied replica** of the [AISI exposure
index](https://github.com/Autonomy-Data-Unit/aisi-exposure-index) pipeline —
netrun's most faithful real consumer, matching ~30M job ads to O*NET occupations
and computing AI exposure scores by local authority.

The topology is derived from that repo's public `config/netrun.json` and
`config/run_defs.toml`: the same node names, the same edges, the same port
renames, the same pools, the same run-preset structure. The **bodies are
instant stubs** — plain lists and dicts, no numpy, no pandas, no network, no
models, no API keys. Model names and endpoints are dummies.

It exists so dagnet has one sample where every feature is composed at once, at
realistic scale, rather than each demonstrated in isolation.

`_netrun_reference.json` holds the slice of the real graph this was derived from,
and `tests/test_sample_09_parity.py` mechanically re-derives the expected dagnet
topology from it and compares — so a mistranscribed edge fails the build.

## What the translation actually did

| netrun | dagnet | count |
|---|---|---|
| 18 graph nodes | **16 nodes** — the two factory nodes cease to exist | −2 |
| 8 `__signal_epoch_finished__` / `__control_start_epoch__` edges | 7 `after` entries | −1 |
| `broadcast_onet_ready` (a `broadcast` factory with 5 outputs) | nothing — 5 nodes each say `after = ["prepare_onet_targets"]` | −1 node |
| `join_scores` (a `join` factory over 4 ports) | nothing — `combine_onet_exposure` takes 4 inputs | −1 node |
| 8 data edges | 8 `inputs` entries | — |
| 21 edges total | 8 ordering + 5 through the join + 8 data | |
| `config/run_defs.toml` + a 136-line runner script | `runs.toml` | −136 lines |
| `run_name` as a declared variable | `ctx.run_name`, built in | −1 var |
| `node_vars` with `inherit: true` | nothing — globals are visible to every node | −7 redeclarations |
| top-level `retries: 3, retry_wait: 10` | `[pipeline] retries` | 1 line, 16 nodes |
| `adzuna_s3_prefix = { value = { "$env" = ... } }` | `env = "ADZUNA_S3_PREFIX"` on the declaration | — |

## Features it exercises, all at once

- **A renamed input**, the case that motivated naming inputs at all:
  `llm_filter_candidates` outputs `successful_ad_ids`; `rerank_candidates` takes
  it as `ad_ids`.
- **Fan-out with no broadcast node** — five nodes gated on `prepare_onet_targets`.
- **A join with no join node** — `combine_onet_exposure` takes four score tables.
- **Ordering vs. data on the same node** — `sample_ads` waits for `fetch_adzuna`
  and takes nothing from it; `cosine_candidates` takes a real value from
  `embed_ads` and a completion handshake from `embed_onet`.
- **A concurrency pool** — `heavy = 1` serializes the three GPU/LLM-shaped nodes.
- **Global and node-local variables**, including a per-node override of a global:
  `score_task_exposure` runs a different `llm_model` from the rest of the pipeline.
- **A pipeline-wide retry default** — `[pipeline] retries = { max = 3, wait_s = 10 }`
  covers all sixteen nodes, exactly as netrun's one top-level setting did. A node
  that wanted different behaviour would write its own `retries`, which replaces the
  default entirely rather than merging into it.
- **An environment-sourced variable** — `adzuna_s3_prefix` declares
  `env = "ADZUNA_S3_PREFIX"`, so the manifest *names* where the value comes from
  instead of the knowledge living in someone's shell profile:

  ```bash
  uv run dagnet run test_local                                  # uses the default
  ADZUNA_S3_PREFIX=s3://real/prod uv run dagnet run test_local  # uses the environment
  ```

  It keeps a `default` so this sample runs anywhere. **Dropping the default is how
  a variable becomes required**: with only `env`, a run that neither sets it nor
  finds the environment variable fails to launch, naming both the variable and the
  environment variable that would satisfy it.
- **Seven named run presets** from a 10-ad smoke test to a 5M-ad production run,
  with per-node overrides.
- **Groups** — `ingest` / `onet` / `match` / `score` / `combine` in the UI and in
  `dagnet graph`.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run test_local        # 10 ads
uv run dagnet run validation_5k
uv run dagnet run production_5m     # the stub caps its own work, so this is instant
uv run dagnet graph
uv run dagnet dev                   # every preset appears as a job
```

## The two gaps this sample found — both now closed

Reproducing the pipeline surfaced exactly two things the manifest could not
express. Both were taken back to the design and answered, and this sample now
uses the results:

1. **No pipeline-wide `retries` default.** netrun set `retries: 3,
   retry_wait: 10` once at the top level for every node; dagnet had only per-node
   `retries`, so the first version of this manifest repeated the same three lines
   eight times. Resolved by DESIGN §5.1: `[pipeline] retries` is the default and a
   node's own `retries` replaces it entirely.
2. **No way to name an environment variable in `[vars]`.** netrun wrote
   `adzuna_s3_prefix = { value = { "$env" = "ADZUNA_S3_PREFIX" }, type = "str" }`;
   dagnet's only route was for node code to read `os.environ` itself, putting
   configuration back outside the map. Resolved by DESIGN §5.3: a declaration may
   name an environment variable, and the resolution order is run value >
   environment > declared default > loud launch error.

Value-side interpolation (`${VAR}` inside a runs file) was deliberately **not**
added — one mechanism, on the declaration side, where it stays discoverable.
