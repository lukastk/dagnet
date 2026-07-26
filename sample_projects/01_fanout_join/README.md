# 01_fanout_join — one output to many nodes, many outputs to one node

`load_sales` feeds three aggregators in parallel; `build_report` joins all three.

## What it demonstrates

- **Fan-out is native.** Three nodes name `load_sales.records` as an input. netrun
  forbade one output port from feeding several edges and needed an explicit
  `broadcast` factory node to work around it; here there is nothing to work
  around, and the three aggregators run concurrently in separate processes.
- **A join is just a node with several inputs.** `build_report` takes three, so
  netrun's `join` factory node has nothing left to do either.
- **Renaming makes collisions a non-issue.** `by_region` and `by_month` both
  declare an output called `totals`. The consumer receives them as `regions` and
  `months`, because an input names its own parameter:
  `inputs = { regions = "by_region.totals", months = "by_month.totals" }`.
  The asset keys stay distinct too (`by_region/totals`, `by_month/totals`).
- **`group` organizes the UI.** The three aggregators are grouped as `aggregate`
  and the join as `report`; `dagnet graph` renders each group as a subgraph.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run
uv run dagnet graph
```

Watch the run log: the three `aggregate` steps start together, then
`build_report` waits for all of them.
