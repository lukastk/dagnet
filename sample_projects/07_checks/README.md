# 07_checks — validating an asset after it materializes

The bug class this exists for: an upstream source quietly switches units from
`umol/L` to `nmol/L`, every downstream number is wrong by 1000x, and nothing
notices for weeks. A declared check turns that into a visible, recorded failure.

## What it demonstrates

- **Checks are declared per output.** `checks = { measurements = ["...", "..."] }`
  attaches functions to an output; they run after it materializes and their
  results are recorded against the asset.
- **A check is a plain function too.** `checks.py` imports nothing from dagster or
  dagnet. It takes `ctx` and the value being checked, and returns a bool or
  `{"passed": bool, "metadata": {...}}`. Raising also counts as a failure.
- **Metadata is worth returning.** `units_are_canonical` reports how many rows
  offended and which units it saw, so a failure tells you what to look at.
- **A failing check is recorded, not fatal.** The `bad` run logs
  `Asset check 'units_are_canonical' ... did not pass`, records the failure
  against the asset and shows it in the UI — and still exits 0, because the
  materialization itself succeeded. That is Dagster's default. Whether dagnet
  should offer a `blocking` flag (halt downstream assets, fail the run) is an
  open design question, not a settled one.
- **Check paths are validated at check time.** A typo'd import path is a
  `dagnet check` error, not a surprise mid-run.

## Run it

```bash
uv sync
uv run dagnet check
uv run dagnet run good    # all four checks pass
uv run dagnet run bad     # units_are_canonical fails, and says why
uv run dagnet dev         # check results in the asset catalog
```

The `bad` run changes no code — it flips a declared variable, and the pipeline
starts producing data that violates its own contract.
