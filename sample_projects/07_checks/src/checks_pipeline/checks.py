"""Check functions: plain Python, same shape as a node.

A check takes `ctx` and the thing it is checking — the asset's value for a normal
output, or the resolved location for an artifact-bound one. It returns either a
bool, or a dict `{"passed": bool, "metadata": {...}}` when it has something worth
recording. Raising counts as a failure too.
"""

CANONICAL_UNITS = {"PND": "umol/L", "CRP": "mg/L"}


def units_are_canonical(ctx, measurements: list[dict]):
    offenders = [row for row in measurements if row["unit"] != CANONICAL_UNITS.get(row["analyte"])]
    return {
        "passed": not offenders,
        "metadata": {
            "offending_rows": len(offenders),
            "units_seen": sorted({row["unit"] for row in measurements}),
        },
    }


def no_missing_values(ctx, measurements: list[dict]) -> bool:
    return all(row.get("value") is not None for row in measurements)


def rows_within_expected_range(ctx, measurements: list[dict]):
    n = len(measurements)
    return {"passed": 1 <= n <= 10_000, "metadata": {"rows": n}}


def totals_are_positive(ctx, by_analyte: dict):
    negative = {k: v for k, v in by_analyte.items() if v <= 0}
    return {"passed": not negative, "metadata": {"analytes": len(by_analyte)}}
