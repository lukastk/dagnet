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
- **Retries** on the flaky fetch/LLM steps.
- **Global and node-local variables**, including a per-node override of a global:
  `score_task_exposure` runs a different `llm_model` from the rest of the pipeline.
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

## Two things the manifest could not express

Reproducing this pipeline surfaced exactly two gaps. Both are recorded rather
than worked around:

1. **No global `retries` default.** netrun's `netrun.json` sets `retries: 3,
   retry_wait: 10` once, at the top level, covering every node. dagnet has only
   per-node `retries`, so this manifest repeats
   `retries = { max = 3, wait_s = 10 }` on the eight nodes that need it. A
   `[pipeline] retries` default that nodes inherit and can override would remove
   the repetition.
2. **No environment-variable interpolation in `[vars]`.** netrun writes
   `adzuna_s3_prefix = { value = { "$env" = "ADZUNA_S3_PREFIX" }, type = "str" }`.
   dagnet has no equivalent, so this sample declares a dummy literal default.
   Real pipelines need secrets and per-machine paths from the environment; today
   the only route is for node code to read `os.environ` itself, which puts
   configuration back outside the map.

Neither blocked the translation, and neither is invented scope — they are what a
faithful reproduction ran into.
