# 07_checks — validating an asset after it materializes

The bug class this exists for: an upstream source quietly switches units from
`umol/L` to `nmol/L`, every downstream number is wrong by 1000x, and nothing
notices for weeks. A declared check turns that into a run that **fails**.

## What it demonstrates

- **Checks are declared per output.** `checks = { measurements = [...] }` attaches
  functions to an output; they run after it materializes and their results are
  recorded against the asset.
- **A check blocks by default.** A bare import path is a blocking check: if it
  fails, assets downstream of that one do not run and `dagnet run` exits **1**.
  Exiting 0 on a violated schema contract would be exactly the silent failure
  this project exists to prevent.
- **Advisory checks opt out.** The long form
  `{ fn = "...", blocking = false }` still runs the check and still records and
  shows the result — at WARN severity — but the pipeline continues. Use it for
  things worth seeing that are not contract breaches, like an unusual row count.
- **A check is a plain function too.** `checks.py` imports nothing from dagster or
  dagnet. It takes `ctx` and the value being checked, and returns a bool or
  `{"passed": bool, "metadata": {...}}`. Raising also counts as a failure.
- **Metadata is worth returning.** `units_are_canonical` reports how many rows
  offended and which units it saw, so a failure tells you what to look at.
- **Check paths are validated at check time.** A typo'd import path is a
  `dagnet check` error, not a surprise mid-run.

## Run it

```bash
uv sync
uv run dagnet check

uv run dagnet run good     # all four checks pass; exit 0
uv run dagnet run bad      # units_are_canonical fails -> aggregate is skipped, exit 1
uv run dagnet run noisy    # only the advisory check fails; everything runs, exit 0
uv run dagnet dev          # check results in the asset catalog
```

Neither the `bad` nor the `noisy` run changes a line of code — each flips a
declared variable, and the pipeline starts producing data that trips a check.

| run | what trips | blocking? | downstream | exit |
|---|---|---|---|---|
| `good` | nothing | — | runs | 0 |
| `bad` | `units_are_canonical` | yes | **skipped** | **1** |
| `noisy` | `rows_within_expected_range` | no | runs | 0 |
