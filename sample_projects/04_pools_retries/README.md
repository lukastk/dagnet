# 04_pools_retries — concurrency limits and retries

Three independent `embed_*` nodes that the executor would gladly run at once, held
to one-at-a-time by `heavy = 1`; plus a flaky API call that succeeds on its third
attempt because the node declares `retries`.

## What it demonstrates

- **A pool is a limit, not a worker pool.** `[pools] heavy = 1` caps how many of
  its members run simultaneously. Parallelism itself comes from the Dagster
  executor. netrun's pools owned threads and processes and were an execution
  mechanism; this is purely a scheduling constraint.
- **The limit is observable.** Each `heavy` node returns the wall-clock window it
  occupied, and `collect` asserts the three windows do not overlap. If the limit
  were not enforced the run would fail — this sample cannot pass by accident.
- **Retries are declarative.** `retries = { max = 3, wait_s = 0.5 }` on `call_api`
  replaces a shell wrapper. The node fails twice and succeeds on the third
  attempt; the run succeeds.
- **`main = 4` bounds the rest.** The non-heavy nodes share a four-slot pool.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run
```

## Pools need a real instance

Pool *limits* live on the Dagster instance, not in the definitions. `dagnet run`
writes them there on every invocation (so editing a limit here takes effect
immediately) and sets `concurrency.pools.granularity: op` in `.dagster/dagster.yaml`
so limits apply per step.

`--ephemeral` therefore **cannot** enforce pools: an ephemeral instance has no
concurrency bookkeeping at all, and cannot even use the multiprocess executor.
`dagnet run --ephemeral` prints a warning saying so, and this sample's assertion
in `collect` will fail — which is the honest outcome, not a bug.
