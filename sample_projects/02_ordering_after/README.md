# 02_ordering_after — dependencies with no data on the edge

Three fetchers write into a scratch directory that `reset_workspace` empties
first. They depend on its *effect*, not on its value.

## What it demonstrates

- **`after` is ordering, nothing else.** `after = ["reset_workspace"]` makes a
  node wait, and gives its function no extra parameter. It compiles to Dagster
  non-argument deps.
- **It replaces the whole signal/control apparatus.** netrun expressed this by
  wiring an auto-generated `__signal_epoch_finished__` output port to a
  `__control_start_epoch__` input port — and because one output port could not
  feed three edges, it also needed a `broadcast` factory node in the middle. Both
  disappear: three nodes each write `after = ["reset_workspace"]`.
- **The two kinds of dependency mix freely.** `publish` takes one real value
  (`users`) and also declares `after = ["fetch_orders", "fetch_events"]`, so it
  waits for work whose results it doesn't want.
- **`dagnet graph` tells them apart.** Value edges are solid and labelled with the
  output name; `after` edges are dotted.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run
uv run dagnet graph
```

`publish` asserts that all three fetch files exist — if the ordering were not
enforced, the run would fail loudly rather than quietly producing a short report.
